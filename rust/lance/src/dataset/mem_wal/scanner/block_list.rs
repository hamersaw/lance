// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Per-source block-list construction for LSM vector search.
//!
//! A generation's membership is a [`GenMembership`]: in-memory generations
//! (active / frozen) are **probed by value** against their maintained
//! primary-key BTrees (no per-query set), while on-disk generations (flushed,
//! base) carry a cached `Arc<HashSet<u64>>` of PK hashes ([`compute_pk_hash`]).
//! Each source gets a `Vec<GenMembership>` of the newer generations
//! (`NEWER(G)`; base: all of them); the KNN drops a candidate whose PK is in any
//! (see [`super::exec::PkHashFilterExec`]).
//!
//! Cross-generation only: within-gen dups share a hash and fall to the global
//! dedup's `(generation, freshness)` tiebreaker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::common::ScalarValue;
use futures::TryStreamExt;
use lance_core::Result;

use uuid::Uuid;

use super::data_source::{LsmDataSource, LsmGeneration};
use super::exec::{compute_pk_hash, resolve_pk_indices};
use super::flushed_cache::{FlushedMemTableCache, open_flushed_dataset};
use crate::dataset::Dataset;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
use crate::session::Session;

/// One newer generation's PK membership, used to decide whether it shadows an
/// older source's row. In-memory generations probe their primary-key BTrees by
/// value (snapshot-bounded, so a not-yet-visible write can't shadow an older
/// visible copy); on-disk generations carry a cached PK-hash set.
#[derive(Debug, Clone)]
pub enum GenMembership {
    /// Probe the in-memory memtable's index, bounded to its visible prefix.
    Index {
        index_store: Arc<IndexStore>,
        /// Inclusive visible row watermark; `None` when no rows are visible.
        max_visible_row: Option<u64>,
    },
    /// A materialized PK-hash set (flushed / base, or an in-memory memtable that
    /// has no primary-key index).
    Set(Arc<HashSet<u64>>),
}

impl GenMembership {
    /// Whether this generation visibly contains the primary key. The on-disk
    /// `Set` case matches on `pk_hash`; the in-memory `Index` case probes by
    /// `pk_values` (collision-free). The caller supplies both for a candidate
    /// row — see [`Self::needs_values`].
    pub fn contains(&self, pk_hash: u64, pk_values: &[ScalarValue]) -> bool {
        match self {
            Self::Index {
                index_store,
                max_visible_row,
            } => {
                let Some(max) = max_visible_row else {
                    return false;
                };
                index_store.pk_contains_visible(pk_values, *max)
            }
            Self::Set(set) => set.contains(&pk_hash),
        }
    }

    /// Whether [`Self::contains`] needs the primary-key values (the `Index`
    /// case) rather than just the hash. Lets the filter skip per-row value
    /// extraction when every membership is an on-disk `Set`.
    pub fn needs_values(&self) -> bool {
        matches!(self, Self::Index { .. })
    }

    /// Whether this generation has no (visible) membership — used to skip adding
    /// an empty blocked set. Approximate for the index case (it ignores the
    /// watermark when counting), which only ever leaves a harmless no-op entry.
    fn is_empty(&self) -> bool {
        match self {
            Self::Index {
                index_store,
                max_visible_row,
            } => max_visible_row.is_none() || index_store.pk_is_empty(),
            Self::Set(set) => set.is_empty(),
        }
    }
}

/// Per-source blocked memberships, keyed by `(shard_id, generation)`. Each value
/// is the memberships of the generations newer than that source.
pub type SourceBlockLists = HashMap<(Option<Uuid>, LsmGeneration), Vec<GenMembership>>;

/// A shard's generations paired with their membership, before sorting.
type ShardGenSets = HashMap<Uuid, Vec<(LsmGeneration, GenMembership)>>;

/// Per-source `NEWER(G)`, keyed by `(shard_id, generation)`. Generations are
/// per-shard, so a source is superseded only by strictly-newer generations of
/// the **same** shard — it never appears in its own blocked list. The base table
/// is shardless (`None`, oldest) and superseded by every non-base generation.
/// Only superseded sources get an entry; the newest of each shard never does.
pub async fn compute_source_block_lists(
    sources: &[LsmDataSource],
    pk_columns: &[String],
    session: Option<&Arc<Session>>,
    flushed_cache: Option<&Arc<FlushedMemTableCache>>,
) -> Result<SourceBlockLists> {
    // Hash each non-base source's membership, grouped by shard (generations are
    // per-shard, so supersession is within-shard only).
    let mut by_shard: ShardGenSets = HashMap::new();
    let mut has_base = false;
    for source in sources {
        match source {
            LsmDataSource::BaseTable { .. } => has_base = true,
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                shard_id,
                generation,
                ..
            } => {
                let membership = in_memory_membership(batch_store, index_store, pk_columns)?;
                by_shard
                    .entry(*shard_id)
                    .or_default()
                    .push((*generation, membership));
            }
            LsmDataSource::FlushedMemTable {
                path,
                shard_id,
                generation,
                ..
            } => {
                // Cached by immutable path so repeated searches skip the PK scan.
                let hashes = flushed_pk_hashes(path, pk_columns, session, flushed_cache).await?;
                by_shard
                    .entry(*shard_id)
                    .or_default()
                    .push((*generation, GenMembership::Set(hashes)));
            }
        }
    }

    let mut blocked: SourceBlockLists = HashMap::new();
    // Base (shardless, oldest) is superseded by every non-base generation.
    let mut base_blocked: Vec<GenMembership> = Vec::new();
    for (shard, mut gens) in by_shard {
        // Newest-first: a gen's blocked list is its own shard's newer gens.
        gens.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
        let mut newer: Vec<GenMembership> = Vec::new();
        for (generation, membership) in gens {
            if !newer.is_empty() {
                blocked.insert((Some(shard), generation), newer.clone());
            }
            if !membership.is_empty() {
                base_blocked.push(membership.clone());
                newer.push(membership);
            }
        }
    }
    if has_base && !base_blocked.is_empty() {
        blocked.insert((None, LsmGeneration::BASE_TABLE), base_blocked);
    }
    Ok(blocked)
}

/// The fresh-tier block-list: one [`GenMembership`] per generation that shadows
/// the base table — active + frozen memtables (probed against their index) and
/// flushed generations (cached PK-hash sets). A base/external reader can test
/// any PK against these (e.g. via `contains`) to decide whether the fresh tier
/// shadows it. The base source, if present, is skipped (it is what gets
/// shadowed).
pub async fn fresh_tier_block_list(
    sources: &[LsmDataSource],
    pk_columns: &[String],
    session: Option<&Arc<Session>>,
    flushed_cache: Option<&Arc<FlushedMemTableCache>>,
) -> Result<Vec<GenMembership>> {
    let mut memberships = Vec::new();
    for source in sources {
        let membership = match source {
            LsmDataSource::BaseTable { .. } => continue,
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                ..
            } => in_memory_membership(batch_store, index_store, pk_columns)?,
            LsmDataSource::FlushedMemTable { path, .. } => GenMembership::Set(
                flushed_pk_hashes(path, pk_columns, session, flushed_cache).await?,
            ),
        };
        if !membership.is_empty() {
            memberships.push(membership);
        }
    }
    Ok(memberships)
}

/// Cross-source membership of an in-memory (active / frozen) memtable.
///
/// Prefers a snapshot-bounded **probe** of the maintained primary-key index (no
/// per-query set built), falling back to a one-time `BatchStore` scan only when
/// the memtable has no such index (e.g. a table without a primary key) — which
/// the production vector-search path never hits, since that index is always
/// enabled alongside the secondary indexes.
fn in_memory_membership(
    batch_store: &Arc<BatchStore>,
    index_store: &Arc<IndexStore>,
    pk_columns: &[String],
) -> Result<GenMembership> {
    if index_store.has_pk_index() {
        let max_visible_row = batch_store.max_visible_row(index_store.max_visible_batch_position());
        Ok(GenMembership::Index {
            index_store: index_store.clone(),
            max_visible_row,
        })
    } else {
        Ok(GenMembership::Set(Arc::new(pk_hashes_from_batch_store(
            batch_store,
            pk_columns,
        )?)))
    }
}

/// Hash the PK membership of an in-memory memtable (active or frozen) from its
/// committed `BatchStore` rows.
pub fn pk_hashes_from_batch_store(
    store: &BatchStore,
    pk_columns: &[String],
) -> Result<HashSet<u64>> {
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(store.len());
    for i in 0..store.len() {
        if let Some(stored) = store.get(i) {
            batches.push(stored.data.clone());
        }
    }
    pk_hashes_from_batches(&batches, pk_columns)
}

/// Hash every row's primary key across `batches` into a membership set.
fn pk_hashes_from_batches(batches: &[RecordBatch], pk_columns: &[String]) -> Result<HashSet<u64>> {
    let mut pk_hashes = HashSet::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let pk_indices = resolve_pk_indices(batch, pk_columns)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
        for row_idx in 0..batch.num_rows() {
            pk_hashes.insert(compute_pk_hash(batch, &pk_indices, row_idx));
        }
    }
    Ok(pk_hashes)
}

/// Build (or fetch the cached) PK-hash membership for one flushed generation.
/// Cached by immutable path (single-flight); the build scans the flushed
/// dataset's PK columns.
async fn flushed_pk_hashes(
    path: &str,
    pk_columns: &[String],
    session: Option<&Arc<Session>>,
    flushed_cache: Option<&Arc<FlushedMemTableCache>>,
) -> Result<Arc<HashSet<u64>>> {
    match flushed_cache {
        Some(cache) => {
            let build_cache = cache.clone();
            let build_path = path.to_string();
            let build_session = session.cloned();
            let build_pk = pk_columns.to_vec();
            cache
                .get_or_build_pk_hashes(
                    path,
                    // `Box::pin` keeps this build future off the caller's future
                    // (avoids `clippy::large_futures`).
                    Box::pin(async move {
                        let dataset = open_flushed_dataset(
                            &build_path,
                            build_session.as_ref(),
                            Some(&build_cache),
                        )
                        .await?;
                        scan_pk_hashes(&dataset, &build_pk).await
                    }),
                )
                .await
        }
        None => {
            let dataset = open_flushed_dataset(path, session, None).await?;
            Ok(Arc::new(scan_pk_hashes(&dataset, pk_columns).await?))
        }
    }
}

/// Scan a dataset's PK columns and fold them into a membership set, one batch
/// resident at a time (no full PK-column buffer).
async fn scan_pk_hashes(dataset: &Dataset, pk_columns: &[String]) -> Result<HashSet<u64>> {
    let pk_refs: Vec<&str> = pk_columns.iter().map(String::as_str).collect();
    let mut scanner = dataset.scan();
    scanner.project(&pk_refs)?;
    let mut stream = scanner.try_into_stream().await?;
    let mut hashes = HashSet::new();
    while let Some(batch) = stream.try_next().await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let pk_indices = resolve_pk_indices(&batch, pk_columns)
            .map_err(|e| lance_core::Error::invalid_input(e.to_string()))?;
        for row in 0..batch.num_rows() {
            hashes.insert(compute_pk_hash(&batch, &pk_indices, row));
        }
    }
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn id_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    /// Hash a single Int32 `id` PK the way the planner does, so a test can probe
    /// a returned blocked set by value.
    fn hash_id(id: i32) -> u64 {
        let batch = id_batch(&[id]);
        let pk_indices = resolve_pk_indices(&batch, &["id".to_string()]).unwrap();
        compute_pk_hash(&batch, &pk_indices, 0)
    }

    /// Whether `id`'s PK is blocked by any of a source's newer-gen memberships
    /// (supplying both the hash and the value, as the filter does).
    fn blocks(memberships: &[GenMembership], id: i32) -> bool {
        let values = [ScalarValue::Int32(Some(id))];
        memberships.iter().any(|m| m.contains(hash_id(id), &values))
    }

    #[test]
    fn pk_hashes_collapse_within_gen_duplicates() {
        // Two rows share pk=1 (a within-gen duplicate); pk=2 is unique.
        let hashes = pk_hashes_from_batches(&[id_batch(&[1, 2, 1])], &["id".to_string()]).unwrap();
        assert_eq!(hashes.len(), 2); // distinct pks: 1, 2
    }

    #[test]
    fn empty_batches_yield_empty_membership() {
        let hashes = pk_hashes_from_batches(&[id_batch(&[])], &["id".to_string()]).unwrap();
        assert!(hashes.is_empty());
    }

    #[test]
    fn batch_store_membership_collapses_within_gen_dups() {
        let store = BatchStore::with_capacity(8);
        // Two single-row batches, both pk=1 (a within-gen update).
        store.append(id_batch(&[1])).unwrap();
        store.append(id_batch(&[1])).unwrap();
        // A two-row batch: pk=2, pk=3.
        store.append(id_batch(&[2, 3])).unwrap();

        let hashes = pk_hashes_from_batch_store(&store, &["id".to_string()]).unwrap();
        assert_eq!(hashes.len(), 3); // distinct pks: 1, 2, 3
    }

    #[tokio::test]
    async fn fresh_tier_block_list_one_set_per_in_memory_gen() {
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use uuid::Uuid;

        let shard = Uuid::new_v4();
        let mk = |ids: &[i32], generation: u64| {
            let store = BatchStore::with_capacity(8);
            store.append(id_batch(ids)).unwrap();
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(store),
                index_store: Arc::new(IndexStore::new()),
                schema: id_batch(&[1]).schema(),
                shard_id: shard,
                generation: LsmGeneration::memtable(generation),
            }
        };
        // Active gen 2: pk=1,2. Frozen gen 1: pk=3.
        let sources = vec![mk(&[1, 2], 2), mk(&[3], 1)];

        let memberships = fresh_tier_block_list(&sources, &["id".to_string()], None, None)
            .await
            .unwrap();

        // One membership per generation; together they cover pk=1,2,3 (not 4).
        assert_eq!(memberships.len(), 2);
        let fresh_blocks = |id: i32| blocks(&memberships, id);
        for id in [1, 2, 3] {
            assert!(fresh_blocks(id));
        }
        assert!(!fresh_blocks(4));
    }

    #[tokio::test]
    async fn block_lists_suppress_stale_across_in_memory_gens() {
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use uuid::Uuid;

        let shard = Uuid::new_v4();
        let mk = |batches: &[&[i32]], generation: u64| {
            let store = BatchStore::with_capacity(8);
            for ids in batches {
                store.append(id_batch(ids)).unwrap();
            }
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(store),
                index_store: Arc::new(IndexStore::new()),
                schema: id_batch(&[1]).schema(),
                shard_id: shard,
                generation: LsmGeneration::memtable(generation),
            }
        };

        // Frozen gen 1: stale pk=1.
        // Active gen 2: pk=1 re-written, pk=2 new.
        let sources = vec![mk(&[&[1]], 1), mk(&[&[1], &[2]], 2)];

        let blocked = Box::pin(compute_source_block_lists(
            &sources,
            &["id".to_string()],
            None,
            None,
        ))
        .await
        .unwrap();

        let g1 = LsmGeneration::memtable(1);
        let g2 = LsmGeneration::memtable(2);
        // The newer active write supersedes the frozen copy: gen 1 is blocked on
        // pk=1, so its KNN drops pk=1.
        assert!(blocks(&blocked[&(Some(shard), g1)], 1));
        // The active (newest) generation is superseded by nothing — no entry.
        assert!(!blocked.contains_key(&(Some(shard), g2)));
    }

    #[tokio::test]
    async fn block_lists_suppress_stale_base_row() {
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::RecordBatchIterator;
        use uuid::Uuid;

        // Base (gen 0): pk=1 (stale), pk=3 (live).
        let base_batch = id_batch(&[1, 3]);
        let schema = base_batch.schema();
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(base_batch)], schema.clone());
        let base = Arc::new(
            Dataset::write(reader, &uri, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        // Active gen 1: pk=1 re-written, pk=2 new.
        let store = BatchStore::with_capacity(8);
        store.append(id_batch(&[1])).unwrap();
        store.append(id_batch(&[2])).unwrap();

        let sources = vec![
            LsmDataSource::BaseTable { dataset: base },
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(store),
                index_store: Arc::new(IndexStore::new()),
                schema,
                shard_id: Uuid::new_v4(),
                generation: LsmGeneration::memtable(1),
            },
        ];

        let blocked = Box::pin(compute_source_block_lists(
            &sources,
            &["id".to_string()],
            None,
            None,
        ))
        .await
        .unwrap();

        // Base is blocked by every newer gen: pk=1 (re-written in gen 1) is
        // blocked, pk=3 (base-only) is not. End-to-end drop: vector_search specs.
        let base_blocked = blocked
            .get(&(None, LsmGeneration::BASE_TABLE))
            .expect("base has a blocked set");
        assert!(blocks(base_blocked, 1));
        assert!(!blocks(base_blocked, 3));
    }

    #[tokio::test]
    async fn block_lists_are_keyed_per_shard() {
        // Regression: generations are per-shard, so a source must only be blocked
        // by newer generations of its OWN shard. A generation-only key would
        // cross-block same-generation sources from different shards.
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use uuid::Uuid;

        let mk = |shard: Uuid, ids: &[i32], generation: u64| {
            let store = BatchStore::with_capacity(8);
            store.append(id_batch(ids)).unwrap();
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(store),
                index_store: Arc::new(IndexStore::new()),
                schema: id_batch(&[1]).schema(),
                shard_id: shard,
                generation: LsmGeneration::memtable(generation),
            }
        };

        // Two shards, each: frozen gen 1 (stale) + active gen 2 (re-write).
        // Shard A keys pk=1; shard B keys pk=2 (disjoint partitions).
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let sources = vec![
            mk(a, &[1], 1),
            mk(a, &[1], 2),
            mk(b, &[2], 1),
            mk(b, &[2], 2),
        ];

        let blocked = Box::pin(compute_source_block_lists(
            &sources,
            &["id".to_string()],
            None,
            None,
        ))
        .await
        .unwrap();

        let g1 = LsmGeneration::memtable(1);
        let g2 = LsmGeneration::memtable(2);
        // Each shard's gen 1 is blocked by its OWN gen 2 only.
        assert!(blocks(&blocked[&(Some(a), g1)], 1));
        assert!(!blocks(&blocked[&(Some(a), g1)], 2));
        assert!(blocks(&blocked[&(Some(b), g1)], 2));
        assert!(!blocks(&blocked[&(Some(b), g1)], 1));
        // The newest generation of each shard is superseded by nothing.
        assert!(!blocked.contains_key(&(Some(a), g2)));
        assert!(!blocked.contains_key(&(Some(b), g2)));
    }

    #[tokio::test]
    async fn in_memory_membership_reads_from_pk_index() {
        // When the memtable has a maintained PK-position index, the block-list
        // sources its membership from that index (no BatchStore re-scan) and
        // still suppresses an older generation's stale copy.
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use uuid::Uuid;

        let shard = Uuid::new_v4();

        // Frozen gen 1: stale pk=1, no PK-position index (exercises the fallback).
        let stale_store = BatchStore::with_capacity(8);
        stale_store.append(id_batch(&[1])).unwrap();

        // Active gen 2: pk=1 re-written + pk=2, with the index enabled + populated.
        // `insert_with_batch_position(Some(bp))` advances the visibility watermark
        // so the snapshot-bounded probe sees both rows.
        let active_store = BatchStore::with_capacity(8);
        let mut active_index = IndexStore::new();
        active_index.enable_pk_index(&[("id".to_string(), 0)]);
        let b1 = id_batch(&[1]);
        let (bp1, off1, _) = active_store.append(b1.clone()).unwrap();
        active_index
            .insert_with_batch_position(&b1, off1, Some(bp1))
            .unwrap();
        let b2 = id_batch(&[2]);
        let (bp2, off2, _) = active_store.append(b2.clone()).unwrap();
        active_index
            .insert_with_batch_position(&b2, off2, Some(bp2))
            .unwrap();

        let schema = id_batch(&[1]).schema();
        let sources = vec![
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(stale_store),
                index_store: Arc::new(IndexStore::new()),
                schema: schema.clone(),
                shard_id: shard,
                generation: LsmGeneration::memtable(1),
            },
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(active_store),
                index_store: Arc::new(active_index),
                schema,
                shard_id: shard,
                generation: LsmGeneration::memtable(2),
            },
        ];

        let blocked = Box::pin(compute_source_block_lists(
            &sources,
            &["id".to_string()],
            None,
            None,
        ))
        .await
        .unwrap();

        // gen 2's index-sourced membership is {pk=1, pk=2}; gen 1 (stale pk=1)
        // is blocked on both, and the newest gen 2 has no blocked set.
        let g1 = LsmGeneration::memtable(1);
        assert!(blocks(&blocked[&(Some(shard), g1)], 1));
        assert!(blocks(&blocked[&(Some(shard), g1)], 2));
        assert!(!blocked.contains_key(&(Some(shard), LsmGeneration::memtable(2))));
    }

    #[tokio::test]
    async fn index_membership_is_snapshot_bounded() {
        // The index-sourced membership only counts a PK whose version is visible
        // at the source's watermark, so a newer generation's not-yet-visible
        // write can't shadow an older generation's visible copy.
        use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
        use crate::dataset::mem_wal::write::IndexStore;
        use uuid::Uuid;

        let shard = Uuid::new_v4();
        let schema = id_batch(&[1]).schema();

        // Older frozen gen 1: pk=1 (no PK-position index → Set fallback).
        let g1_store = BatchStore::with_capacity(8);
        g1_store.append(id_batch(&[1])).unwrap();

        // Newer active gen 2: pk=99 visible at position 0, then pk=1 written at
        // position 1 but with the watermark left at batch 0 (so pk=1 is in the
        // index yet not visible) — the concurrent-write race.
        let g2_store = BatchStore::with_capacity(8);
        let mut g2_index = IndexStore::new();
        g2_index.enable_pk_index(&[("id".to_string(), 0)]);
        let b0 = id_batch(&[99]);
        let (bp0, off0, _) = g2_store.append(b0.clone()).unwrap();
        g2_index
            .insert_with_batch_position(&b0, off0, Some(bp0)) // advances watermark to 0
            .unwrap();
        let b1 = id_batch(&[1]);
        let (_, off1, _) = g2_store.append(b1.clone()).unwrap();
        g2_index
            .insert_with_batch_position(&b1, off1, None) // index updated, watermark unchanged
            .unwrap();

        let sources = vec![
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(g1_store),
                index_store: Arc::new(IndexStore::new()),
                schema: schema.clone(),
                shard_id: shard,
                generation: LsmGeneration::memtable(1),
            },
            LsmDataSource::ActiveMemTable {
                batch_store: Arc::new(g2_store),
                index_store: Arc::new(g2_index),
                schema,
                shard_id: shard,
                generation: LsmGeneration::memtable(2),
            },
        ];

        let blocked = Box::pin(compute_source_block_lists(
            &sources,
            &["id".to_string()],
            None,
            None,
        ))
        .await
        .unwrap();

        let g1_block = &blocked[&(Some(shard), LsmGeneration::memtable(1))];
        // pk=99 is visible in gen 2 → it blocks gen 1's pk=99.
        assert!(blocks(g1_block, 99));
        // pk=1's only gen-2 copy is not yet visible → it must NOT shadow gen 1.
        assert!(
            !blocks(g1_block, 1),
            "a not-yet-visible newer write must not shadow an older visible copy"
        );
    }
}
