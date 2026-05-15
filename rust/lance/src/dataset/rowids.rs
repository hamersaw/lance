// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::Dataset;
use crate::session::caches::{Assembled, RowIdIndexKey, RowIdSequenceKey};
use crate::{Error, Result};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use lance_core::utils::deletion::DeletionVector;
use lance_table::{
    format::{Fragment, RowIdMeta},
    rowids::{
        FragmentRowIdIndex, RowIdIndex, RowIdSequence, SequenceBound, peek_row_id_bound,
        read_row_ids,
    },
};
use std::sync::{Arc, RwLock};

/// Load a row id sequence from the given dataset and fragment.
pub async fn load_row_id_sequence(
    dataset: &Dataset,
    fragment: &Fragment,
) -> Result<Arc<RowIdSequence>> {
    // Virtual path to prevent collisions in the cache.
    match &fragment.row_id_meta {
        None => Err(Error::internal("Missing row id meta")),
        Some(RowIdMeta::Inline(data)) => {
            let data = data.clone();
            let key = RowIdSequenceKey {
                fragment_id: fragment.id,
            };
            dataset
                .metadata_cache
                .get_or_insert_with_key(key, || async move { read_row_ids(&data) })
                .await
        }
        Some(RowIdMeta::External(file_slice)) => {
            let file_slice = file_slice.clone();
            let dataset_clone = dataset.clone();
            let key = RowIdSequenceKey {
                fragment_id: fragment.id,
            };
            dataset
                .metadata_cache
                .get_or_insert_with_key(key, || async move {
                    let path = dataset_clone.base.clone().join(file_slice.path.as_str());
                    let range = file_slice.offset as usize
                        ..(file_slice.offset as usize + file_slice.size as usize);
                    let data = dataset_clone
                        .object_store
                        .open(&path)
                        .await?
                        .get_range(range)
                        .await?;
                    read_row_ids(&data)
                })
                .await
        }
    }
}

/// Load row id sequences from the given dataset and fragments.
///
/// Returned as a vector of (fragment_id, sequence) pairs. These are not
/// guaranteed to be in the same order as the input fragments.
pub fn load_row_id_sequences<'a>(
    dataset: &'a Dataset,
    fragments: &'a [Fragment],
) -> impl Stream<Item = Result<(u32, Arc<RowIdSequence>)>> + 'a {
    futures::stream::iter(fragments)
        .map(|fragment| {
            load_row_id_sequence(dataset, fragment).map_ok(move |seq| (fragment.id as u32, seq))
        })
        .buffer_unordered(dataset.object_store.io_parallelism())
}

/// Build a full `RowIdIndex` covering every fragment in the dataset.
///
/// Used in tests that need to assert against the entire mapping; production
/// code should use [`build_row_id_index_for`] (when row ids are known) or
/// [`get_full_row_id_index`] (for streaming execs).
#[cfg(test)]
pub async fn get_row_id_index(
    dataset: &Dataset,
) -> Result<Option<Arc<lance_table::rowids::RowIdIndex>>> {
    get_full_row_id_index(dataset).await
}

/// Fetch (creating if absent) the per-manifest-version assembled-index slot.
/// Initialization is cheap and does no I/O — the slot starts empty and is
/// grown lazily by the take path and to full coverage by the scan path.
async fn get_row_id_index_slot(dataset: &Dataset) -> Result<Arc<RwLock<Assembled>>> {
    let key = RowIdIndexKey {
        version: dataset.manifest.version,
    };
    dataset
        .metadata_cache
        .get_or_insert_with_key(key, || async { Ok(RwLock::new(Assembled::empty())) })
        .await
}

/// Manifest fragments ordered newest-first (descending id). Updated stable
/// rows live in the newest owning fragment, so a take tends to resolve on
/// the first cold candidate(s), maximizing fast-stop.
fn fragments_newest_first(fragments: &[Fragment]) -> Vec<&Fragment> {
    let mut v: Vec<&Fragment> = fragments.iter().collect();
    v.sort_unstable_by(|a, b| b.id.cmp(&a.id));
    v
}

/// Load one fragment's `FragmentRowIdIndex` (row id sequence + deletion
/// vector). The sequence goes through the per-fragment cache; `parsed`
/// lets the caller hand back a sequence it already decoded (the `Opaque`
/// peek result) so the inline bytes are not decoded twice.
async fn load_fragment_index(
    dataset: &Dataset,
    fragment: &Fragment,
    parsed: Option<RowIdSequence>,
) -> Result<FragmentRowIdIndex> {
    let fragment_id = fragment.id as u32;
    let row_id_sequence = match parsed {
        Some(seq) => {
            // Seed (or reuse) the per-fragment cache without re-decoding.
            let key = RowIdSequenceKey {
                fragment_id: fragment.id,
            };
            dataset
                .metadata_cache
                .get_or_insert_with_key(key, || async move { Ok(seq) })
                .await?
        }
        None => load_row_id_sequence(dataset, fragment).await?,
    };
    let deletion_vector = match dataset.get_fragment(fragment.id as usize) {
        Some(ff) if ff.metadata().deletion_file.is_some() => ff
            .get_deletion_vector()
            .await
            .ok()
            .flatten()
            .unwrap_or_default(),
        _ => Arc::new(DeletionVector::default()),
    };
    Ok(FragmentRowIdIndex {
        fragment_id,
        row_id_sequence,
        deletion_vector,
    })
}

/// Merge freshly loaded fragments into the shared per-version slot and
/// return the resulting index. Re-checks `covered` under the write lock so
/// a fragment a concurrent take loaded in parallel is merged exactly once.
///
/// Fast path: splice the new (disjoint) fragments straight into the
/// existing map. Only when their row id ranges overlap an already-covered
/// fragment — the updated-and-moved-row case — do we pay a full rebuild.
fn merge_into_slot(
    slot: &RwLock<Assembled>,
    fresh: Vec<FragmentRowIdIndex>,
) -> Result<Arc<RowIdIndex>> {
    if fresh.is_empty() {
        return Ok(slot
            .read()
            .expect("row id index slot poisoned")
            .index
            .clone());
    }
    let mut g = slot.write().expect("row id index slot poisoned");
    let mut truly_new = Vec::with_capacity(fresh.len());
    for fi in fresh {
        if g.covered.insert(fi.fragment_id) {
            truly_new.push(fi);
        }
    }
    if truly_new.is_empty() {
        return Ok(g.index.clone());
    }
    if Arc::make_mut(&mut g.index).try_additive_extend(&truly_new) {
        g.fragment_indices.append(&mut truly_new);
    } else {
        g.fragment_indices.append(&mut truly_new);
        g.index = Arc::new(RowIdIndex::new(&g.fragment_indices)?);
    }
    Ok(g.index.clone())
}

/// Build (or extend) a `RowIdIndex` covering every fragment in the dataset.
/// Suitable for streaming execs like `AddRowAddrExec` that translate
/// arbitrary row ids. Shares the per-version slot with the lazy take path,
/// so a scan warms it to full coverage and subsequent takes hit phase 0.
pub async fn get_full_row_id_index(dataset: &Dataset) -> Result<Option<Arc<RowIdIndex>>> {
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(None);
    }
    let slot = get_row_id_index_slot(dataset).await?;
    let covered = slot
        .read()
        .expect("row id index slot poisoned")
        .covered
        .clone();
    let missing: Vec<&Fragment> = dataset
        .manifest
        .fragments
        .iter()
        .filter(|f| !covered.contains(f.id as u32))
        .collect();
    if missing.is_empty() {
        return Ok(Some(
            slot.read()
                .expect("row id index slot poisoned")
                .index
                .clone(),
        ));
    }
    let load_futs: Vec<_> = missing
        .iter()
        .map(|&f| load_fragment_index(dataset, f, None))
        .collect();
    let fresh: Vec<FragmentRowIdIndex> = futures::stream::iter(load_futs)
        .buffer_unordered(dataset.object_store.io_parallelism())
        .try_collect()
        .await?;
    Ok(Some(merge_into_slot(&slot, fresh)?))
}

/// Build a `RowIdIndex` that resolves the requested `row_ids`. Returns
/// `None` for non-stable-rowid datasets.
///
/// Phase 0 resolves against the cached per-version index (zero I/O). Phase
/// 1 loads only the uncovered fragments that could own a still-unresolved
/// id — gated by a cheap inline-metadata bound peek — and merges them into
/// the shared slot, stopping as soon as every requested id is resolved.
pub async fn build_row_id_index_for(
    dataset: &Dataset,
    row_ids: &[u64],
) -> Result<Option<Arc<RowIdIndex>>> {
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(None);
    }
    let slot = get_row_id_index_slot(dataset).await?;

    // Fast-stop working set: ids still needing a live resolution.
    let mut pending: std::collections::HashSet<u64> = row_ids.iter().copied().collect();

    // Phase 0 — warm: resolve against the cached index. `get() == Some` is a
    // definitive live resolution (the DV is baked into the index).
    let cached = {
        let g = slot.read().expect("row id index slot poisoned");
        g.index.clone()
    };
    pending.retain(|&id| cached.get(id).is_none());
    if pending.is_empty() {
        return Ok(Some(cached));
    }

    // Phase 1 — cold: load uncovered candidate fragments, newest first.
    let covered_snapshot = slot
        .read()
        .expect("row id index slot poisoned")
        .covered
        .clone();
    let mut fresh: Vec<FragmentRowIdIndex> = Vec::new();
    for fragment in fragments_newest_first(&dataset.manifest.fragments) {
        if pending.is_empty() {
            break;
        }
        if covered_snapshot.contains(fragment.id as u32) {
            continue;
        }
        let (is_candidate, parsed) = match &fragment.row_id_meta {
            Some(RowIdMeta::Inline(bytes)) => match peek_row_id_bound(bytes)? {
                SequenceBound::Empty => (false, None),
                SequenceBound::Bounded(range) => {
                    (pending.iter().any(|id| range.contains(id)), None)
                }
                SequenceBound::Opaque(seq) => (true, Some(seq)),
            },
            // External sequence: cannot peek without I/O — mandatory candidate.
            Some(RowIdMeta::External(_)) => (true, None),
            None => {
                return Err(Error::internal(format!(
                    "Fragment {} missing row id meta under stable row ids",
                    fragment.id
                )));
            }
        };
        if !is_candidate {
            continue;
        }
        let fi = load_fragment_index(dataset, fragment, parsed).await?;
        // Live-resolution probe (invariant 1): a single-fragment index bakes
        // in this fragment's DV, so a tombstoned (moved) id resolves to
        // `None` here and stays pending for its real owner.
        let probe = RowIdIndex::new(std::slice::from_ref(&fi))?;
        pending.retain(|&id| probe.get(id).is_none());
        fresh.push(fi);
    }

    Ok(Some(merge_into_slot(&slot, fresh)?))
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::dataset::{UpdateBuilder, WriteMode, WriteParams, builder::DatasetBuilder};

    use super::*;

    use crate::dataset::optimize::{CompactionOptions, compact_files};
    use crate::index::DatasetIndexExt;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Int32Type, UInt64Type};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, UInt64Array};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::Future;
    use lance_core::datatypes::Schema;
    use lance_core::{ROW_ADDR, ROW_ID, utils::address::RowAddress};
    use lance_datagen::Dimension;
    use lance_index::{IndexType, scalar::ScalarIndexParams};
    use std::collections::HashMap;
    use std::collections::HashSet;

    fn sequence_batch(values: Range<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(values))]).unwrap()
    }

    #[tokio::test]
    async fn test_empty_dataset_rowids() {
        let schema = sequence_batch(0..0).schema();
        let reader = RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        assert!(dataset.manifest.uses_stable_row_ids());

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_none());

        assert_eq!(dataset.manifest().next_row_id, 0);
    }

    #[tokio::test]
    async fn test_must_set_on_creation() {
        let tmp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &tmp_dir;

        let batch = sequence_batch(0..10);
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: false,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();
        assert!(!dataset.manifest().uses_stable_row_ids());

        // Trying to append without stable row ids should pass (a warning is emitted) but should not
        // affect the stable_row_ids setting.
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            mode: WriteMode::Append,
            ..Default::default()
        };
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), batch.schema());
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();
        assert!(!dataset.manifest().uses_stable_row_ids());
    }

    #[tokio::test]
    async fn test_new_row_ids() {
        let num_rows = 25u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 10,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();

        let found_addresses = (0..num_rows)
            .map(|i| index.get(i).unwrap())
            .collect::<Vec<_>>();
        let expected_addresses = (0..num_rows)
            .map(|i| {
                let fragment_id = i / 10;
                RowAddress::new_from_parts(fragment_id as u32, (i % 10) as u32)
            })
            .collect::<Vec<_>>();
        assert_eq!(found_addresses, expected_addresses);

        assert_eq!(dataset.manifest().next_row_id, num_rows);
    }

    #[tokio::test]
    async fn test_row_ids_overwrite() {
        // Validate we don't re-use after overwriting
        let num_rows = 10u64;
        let batch = sequence_batch(0..num_rows as i32);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &temp_dir;
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.manifest().next_row_id, num_rows);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();

        // Overwriting should NOT reset the row id counter.
        assert_eq!(dataset.manifest().next_row_id, 2 * num_rows);

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_none());
        assert!(index.get(num_rows).is_some());
    }

    #[tokio::test]
    async fn test_row_ids_append() {
        // Validate we handle row ids well when appending concurrently.
        fn write_batch(uri: &str, start: i32) -> impl Future<Output = Result<()>> + '_ {
            let batch = sequence_batch(start..(start + 10));
            let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
            let write_params = WriteParams {
                enable_stable_row_ids: true,
                mode: WriteMode::Append,
                ..Default::default()
            };
            async move {
                let _ = Dataset::write(reader, uri, Some(write_params)).await?;
                Ok(())
            }
        }

        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &temp_dir;
        let mut start = 0;
        // Just do one first to create the dataset.
        write_batch(tmp_path, start).await.unwrap();
        start += 10;
        // Now do the rest concurrently.
        let futures = (0..5)
            .map(|offset| write_batch(tmp_path, start + offset * 10))
            .collect::<Vec<_>>();
        futures::future::try_join_all(futures).await.unwrap();

        let dataset = DatasetBuilder::from_uri(tmp_path).load().await.unwrap();

        assert_eq!(dataset.manifest().next_row_id, 60);

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_some());
        assert!(index.get(60).is_none());
    }

    #[tokio::test]
    async fn test_scan_row_ids() {
        // Write dataset with multiple files -> _rowid != _rowaddr
        // Scan with and without each.;
        let batch = sequence_batch(0..6);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 2,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);

        for with_row_id in [true, false] {
            for with_row_address in &[true, false] {
                for projection in &[vec![], vec!["id"]] {
                    if !with_row_id && !with_row_address && projection.is_empty() {
                        continue;
                    }

                    let mut scan = dataset.scan();
                    if with_row_id {
                        scan.with_row_id();
                    }
                    if *with_row_address {
                        scan.with_row_address();
                    }
                    let scan = scan.project(projection).unwrap();
                    let result = scan.try_into_batch().await.unwrap();

                    if with_row_id {
                        let row_ids = result[ROW_ID]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let expected = vec![0, 1, 2, 3, 4, 5].into();
                        assert_eq!(row_ids, &expected);
                    }

                    if *with_row_address {
                        let row_addrs = result[ROW_ADDR]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let expected =
                            vec![0, 1, 1 << 32, (1 << 32) + 1, 2 << 32, (2 << 32) + 1].into();
                        assert_eq!(row_addrs, &expected);
                    }

                    if !projection.is_empty() {
                        let ids = result["id"].as_any().downcast_ref::<Int32Array>().unwrap();
                        let expected = vec![0, 1, 2, 3, 4, 5].into();
                        assert_eq!(ids, &expected);
                    }
                }
            }
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_delete_with_row_ids(#[values(true, false)] with_scalar_index: bool) {
        let batch = sequence_batch(0..6);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 2,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);

        if with_scalar_index {
            dataset
                .create_index(
                    &["id"],
                    IndexType::Scalar,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
        }

        dataset.delete("id = 3 or id = 4").await.unwrap();

        let mut scan = dataset.scan();
        scan.with_row_id().with_row_address();
        let result = scan.try_into_batch().await.unwrap();

        let row_ids = result[ROW_ID]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let expected = vec![0, 1, 2, 5].into();
        assert_eq!(row_ids, &expected);

        let row_addrs = result[ROW_ADDR]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let expected = vec![0, 1, 1 << 32, (2 << 32) + 1].into();
        assert_eq!(row_addrs, &expected);
    }

    #[tokio::test]
    async fn test_row_ids_update() {
        // Updated fragments get fresh row ids.
        let num_rows = 5u64;
        let batch = sequence_batch(0..num_rows as i32);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.manifest().next_row_id, num_rows);

        let update_result = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id = 3")
            .unwrap()
            .set("id", "100")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let dataset = update_result.new_dataset;
        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_some());
        // the updated row ids mapping to new address
        assert_eq!(index.get(3), Some(RowAddress::new_from_parts(1, 0)));
        // there is no new row id
        assert_eq!(index.get(5), None);
    }

    fn build_rowid_to_i_map(row_ids: &UInt64Array, i_array: &Int32Array) -> HashMap<u64, i32> {
        row_ids
            .values()
            .iter()
            .zip(i_array.values().iter())
            .map(|(&row_id, &i)| (row_id, i))
            .collect()
    }

    async fn scan_rowid_map(dataset: &Dataset) -> HashMap<u64, i32> {
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan.try_into_batch().await.unwrap();
        let i = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let row_ids = result[ROW_ID]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        build_rowid_to_i_map(row_ids, i)
    }

    async fn compact(dataset: &mut Dataset, target_rows: usize) {
        let options = CompactionOptions {
            target_rows_per_fragment: target_rows,
            ..Default::default()
        };
        let _ = compact_files(dataset, options, None).await.unwrap();
    }

    async fn delete(dataset: &mut Dataset, expr: &str) {
        dataset.delete(expr).await.unwrap();
    }

    #[tokio::test]
    async fn test_stable_row_id_after_multiple_deletion_and_compaction() {
        async fn delete(dataset: &mut Dataset, expr: &str) {
            dataset.delete(expr).await.unwrap();
        }

        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(6),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    enable_v2_manifest_paths: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // first delete and compact
        delete(&mut dataset, "i = 2 or i = 3 or i = 5").await;
        let map_before = scan_rowid_map(&dataset).await;
        compact(&mut dataset, 20).await;
        let map_after = scan_rowid_map(&dataset).await;

        // verify row id
        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // second delete
        delete(&mut dataset, "i = 9").await;
        let mut scan = dataset.scan();
        let result = scan
            .filter("i >= 0")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let ids = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let id_set = ids.values().iter().cloned().collect::<HashSet<_>>();
        let expected: Vec<i32> = (0..60)
            .filter(|&i| i != 2 && i != 3 && i != 5 && i != 9)
            .collect();
        assert_eq!(id_set, expected.iter().cloned().collect::<HashSet<_>>());

        // get the row_id where i == 15
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan
            .filter("i == 15")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let row_id_vec = result[ROW_ID]
            .as_primitive::<UInt64Type>()
            .values()
            .to_vec();

        // third delete and compact
        delete(&mut dataset, "i = 15 or i = 25").await;
        let map_before = scan_rowid_map(&dataset).await;
        compact(&mut dataset, 30).await;
        let map_after = scan_rowid_map(&dataset).await;

        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // verify the rowid represent i == 15 has been deleted
        let result = dataset
            .take_rows(&row_id_vec, Schema::try_from(dataset.schema()).unwrap())
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_stable_row_id_after_deletion_update_and_compaction() {
        // gen dataset
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "i",
                lance_datagen::array::step::<arrow_array::types::Int32Type>(),
            )
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(6),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    enable_v2_manifest_paths: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // delete some rows
        delete(&mut dataset, "i = 2 or i = 3 or i = 5").await;
        let map_before = scan_rowid_map(&dataset).await;

        // update some rows
        let updated_dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("i >= 15")
            .unwrap()
            .set("category", "999")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        // compact the dataset
        let mut dataset = Arc::try_unwrap(updated_dataset).expect("no other Arc references");
        compact(&mut dataset, 20).await;
        let map_after = scan_rowid_map(&dataset).await;

        // verify row id
        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // verify category filed
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan.try_into_batch().await.unwrap();
        let i = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let category = result["category"]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for idx in 0..i.len() {
            if i.value(idx) >= 15 {
                assert_eq!(category.value(idx), 999);
            }
        }
    }

    /// Equivalence test: the lazy `build_row_id_index_for` and the eager
    /// `get_row_id_index` must resolve every existing row id to the same
    /// address. The lazy variant is just allowed to skip fragments — never
    /// to misroute or drop ids that the full index would have resolved.
    #[tokio::test]
    async fn test_lazy_index_matches_full_index() {
        let num_rows = 60u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let full = get_row_id_index(&dataset).await.unwrap().unwrap();
        let row_ids: Vec<u64> = (0..num_rows).collect();
        let lazy = build_row_id_index_for(&dataset, &row_ids)
            .await
            .unwrap()
            .unwrap();

        for id in 0..num_rows {
            assert_eq!(
                full.get(id),
                lazy.get(id),
                "Address mismatch at row id {id}",
            );
        }
        // Out-of-range ids resolve to None on both.
        assert_eq!(full.get(num_rows), lazy.get(num_rows));
    }

    /// Invariant 1: an updated row keeps its stable id but physically moves
    /// to a new fragment; the old fragment carries a tombstone whose range
    /// still covers the id. The lazy translator must resolve to the live
    /// (new) owner, never the tombstoned slot — even though the old fragment
    /// is also a range candidate.
    #[tokio::test]
    async fn test_lazy_index_handles_update_overlap() {
        let num_rows = 30u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let updated = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id = 2")
            .unwrap()
            .set("id", "999")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        // The update must have produced an extra fragment for the moved row.
        assert!(
            updated.manifest.fragments.len() >= 2,
            "expected the update to add a fragment, got {}",
            updated.manifest.fragments.len(),
        );

        // The lazy index must resolve rowid 2 to the new fragment, not the
        // tombstoned original, and agree with the full index.
        let lazy = build_row_id_index_for(&updated, &[2])
            .await
            .unwrap()
            .unwrap();
        let resolved = lazy.get(2).expect("rowid 2 must resolve after update");
        let full = get_row_id_index(&updated).await.unwrap().unwrap();
        assert_eq!(full.get(2), Some(resolved));
    }

    /// The per-version slot grows monotonically: a take only ingests the
    /// fragments that could own a requested id, and a later take reuses what
    /// is already covered while extending with what is not.
    #[tokio::test]
    async fn test_lazy_index_incremental_growth() {
        let num_rows = 60u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        // Contiguous write: rowid i lives in fragment i / 10.
        assert_eq!(dataset.manifest.fragments.len(), 6);

        // First take only needs fragment 0.
        let idx = build_row_id_index_for(&dataset, &[5])
            .await
            .unwrap()
            .unwrap();
        assert!(idx.get(5).is_some());
        let slot = get_row_id_index_slot(&dataset).await.unwrap();
        {
            let g = slot.read().unwrap();
            assert!(g.covered.contains(0), "fragment 0 must be covered");
            assert!(
                !g.covered.contains(5),
                "fragment 5 must NOT be loaded by a take for rowid 5",
            );
        }

        // Second take extends the slot with fragment 5 and keeps fragment 0.
        let idx = build_row_id_index_for(&dataset, &[55])
            .await
            .unwrap()
            .unwrap();
        assert!(idx.get(55).is_some());
        assert!(idx.get(5).is_some(), "previously covered id still resolves");
        {
            let g = slot.read().unwrap();
            assert!(g.covered.contains(0) && g.covered.contains(5));
        }
    }

    /// The slot is keyed by manifest version. A delete creates a new version
    /// with a tombstone; the new version's slot must resolve the deleted id
    /// to `None` while the pre-delete version still resolves it.
    #[tokio::test]
    async fn test_lazy_index_version_isolation() {
        let num_rows = 20u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let before = dataset.clone();
        dataset.delete("id = 3").await.unwrap();

        let after = build_row_id_index_for(&dataset, &[3, 4])
            .await
            .unwrap()
            .unwrap();
        assert!(after.get(3).is_none(), "deleted rowid must not resolve");
        assert!(after.get(4).is_some(), "surviving rowid must resolve");

        let prev = build_row_id_index_for(&before, &[3])
            .await
            .unwrap()
            .unwrap();
        assert!(
            prev.get(3).is_some(),
            "pre-delete version must still resolve rowid 3",
        );
    }

    /// Concurrent takes for disjoint ids must each resolve correctly and the
    /// shared slot must end up covering every needed fragment exactly once.
    #[tokio::test]
    async fn test_lazy_index_concurrent_takes() {
        let num_rows = 60u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let (a, b, c) = tokio::join!(
            build_row_id_index_for(&dataset, &[5]),
            build_row_id_index_for(&dataset, &[25]),
            build_row_id_index_for(&dataset, &[55]),
        );
        assert!(a.unwrap().unwrap().get(5).is_some());
        assert!(b.unwrap().unwrap().get(25).is_some());
        assert!(c.unwrap().unwrap().get(55).is_some());

        let slot = get_row_id_index_slot(&dataset).await.unwrap();
        let g = slot.read().unwrap();
        for fid in [0u32, 2, 5] {
            assert!(g.covered.contains(fid), "fragment {fid} must be covered");
        }
        // Each covered fragment appears once (no double-merge under races).
        assert_eq!(g.fragment_indices.len() as u64, g.covered.len());
    }
}
