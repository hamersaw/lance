// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Maintained MVCC primary-key → newest-position index for in-memory memtables.
//!
//! Keyed on `(pk_hash, row_position)` in the same lock-free arena skiplist the
//! [`super::BTreeMemIndex`] uses, so every version of a primary key is a
//! distinct, position-ordered entry — an MVCC version chain
//! `(k,p0),(k,p1),(k,p2)…`. [`Self::get_newest_visible`] answers "the newest
//! row position of this PK that is visible at the watermark" with a single
//! seek-and-stop, the same primitive `BTreeMemIndex::get_newest_visible`
//! exposes and that point-lookup already trusts — but keyed on
//! [`compute_pk_hash`] rather than a single column, so it covers composite and
//! otherwise-unindexed primary keys uniformly.
//!
//! The active vector / FTS search arms use this to drop a stale hit whose PK
//! has a newer version that the (append-only) secondary index didn't return —
//! the predicate-crossing stale read those arms otherwise leak. Because the row
//! position is itself the MVCC version stamp, a reader filtering on its latched
//! `max_visible` watermark is unaffected by concurrent appends (which only add
//! larger positions), so no snapshot needs to be co-published with the query.

use std::sync::Mutex;

use arrow_array::RecordBatch;
use lance_core::{Error, Result};

use super::RowPosition;
use super::arena_skiplist::{SkipListReader, SkipListWriter, new_skiplist};
use crate::dataset::mem_wal::scanner::exec::{compute_pk_hash, resolve_pk_indices};

/// Skiplist key: `(pk_hash, row_position)`. Sorting by hash then position means
/// a seek to `(hash, watermark)` lands on the newest version of that hash at or
/// below the watermark. The row position keeps every entry unique.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PkPosKey {
    hash: u64,
    position: RowPosition,
}

/// Append-only, lock-free-read index from `compute_pk_hash(pk_columns)` to the
/// row positions that key was written at. Single-writer (the MemTable serializes
/// inserts behind an uncontended `Mutex`); reads take no lock.
pub struct PkPositionIndex {
    reader: SkipListReader<PkPosKey>,
    writer: Mutex<SkipListWriter<PkPosKey>>,
    /// Primary-key column names, resolved to indices against each batch's schema.
    pk_columns: Vec<String>,
}

impl std::fmt::Debug for PkPositionIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkPositionIndex")
            .field("pk_columns", &self.pk_columns)
            .field("len", &self.len())
            .finish()
    }
}

impl PkPositionIndex {
    /// Create an index over `pk_columns` (the unenforced primary key).
    pub fn new(pk_columns: Vec<String>) -> Self {
        let (writer, reader) = new_skiplist::<PkPosKey>();
        Self {
            reader,
            writer: Mutex::new(writer),
            pk_columns,
        }
    }

    /// Insert every row's `(pk_hash, row_offset + row_idx)`.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let pk_indices = resolve_pk_indices(batch, &self.pk_columns)
            .map_err(|e| Error::invalid_input(e.to_string()))?;
        let mut writer = self.writer.lock().unwrap();
        for row in 0..batch.num_rows() {
            let hash = compute_pk_hash(batch, &pk_indices, row);
            writer.insert(PkPosKey {
                hash,
                position: row_offset + row as u64,
            });
        }
        Ok(())
    }

    /// The newest row position written for `pk_hash` that is `<= max_visible_row`
    /// (inclusive), or `None` if the key has no visible version. A single
    /// seek-and-stop on the skiplist (largest key `<= (pk_hash, max_visible_row)`)
    /// — no range collect, no allocation.
    pub fn get_newest_visible(
        &self,
        pk_hash: u64,
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        let target = PkPosKey {
            hash: pk_hash,
            position: max_visible_row,
        };
        self.reader
            .upper_bound_with(&target, |key| (key.hash == pk_hash).then_some(key.position))
            .flatten()
    }

    /// Whether `pk_hash` has any version visible at `max_visible_row`. The
    /// cross-source block-list's existence query — "does a newer generation
    /// contain this PK?" — reduces to this, position-bounded so a not-yet-visible
    /// write can't shadow an older visible copy.
    pub fn contains_visible(&self, pk_hash: u64, max_visible_row: RowPosition) -> bool {
        self.get_newest_visible(pk_hash, max_visible_row).is_some()
    }

    /// All row positions written for `pk_hash` that are `<= max_visible_row`, in
    /// ascending (oldest-first) order. Used by point-lookup to resolve a hash
    /// collision: walk the matches newest-first and keep the first whose actual
    /// primary-key value equals the query, so a colliding key never returns the
    /// wrong row. Empty (and allocation-free past the seek) when the hash is
    /// absent; the common no-collision lookup uses [`Self::get_newest_visible`].
    pub fn visible_positions(
        &self,
        pk_hash: u64,
        max_visible_row: RowPosition,
    ) -> Vec<RowPosition> {
        let start = PkPosKey {
            hash: pk_hash,
            position: 0,
        };
        let mut positions = Vec::new();
        for key in self.reader.range_from(&start) {
            if key.hash != pk_hash {
                break;
            }
            if key.position <= max_visible_row {
                positions.push(key.position);
            }
        }
        positions
    }

    /// Number of entries (one per inserted row, not per distinct key).
    pub fn len(&self) -> usize {
        self.reader.len()
    }

    /// Whether the index has no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    /// Hash a single-column `id` PK the way the index does, so a test can probe
    /// `get_newest_visible` by value.
    fn hash_id(id: i32) -> u64 {
        let b = batch(&[id], &["x"]);
        let pk_indices = resolve_pk_indices(&b, &["id".to_string()]).unwrap();
        compute_pk_hash(&b, &pk_indices, 0)
    }

    #[test]
    fn newest_visible_tracks_updates_under_watermark() {
        let index = PkPositionIndex::new(vec!["id".to_string()]);
        // id=1 at positions 0 and 3 (an update); id=2 at position 1.
        index.insert(&batch(&[1, 2], &["a", "b"]), 0).unwrap();
        index.insert(&batch(&[1], &["a2"]), 3).unwrap();

        // Watermark above the update sees the newest position.
        assert_eq!(index.get_newest_visible(hash_id(1), 5), Some(3));
        assert_eq!(index.get_newest_visible(hash_id(2), 5), Some(1));
        // Watermark below the update hides it — the older position wins.
        assert_eq!(index.get_newest_visible(hash_id(1), 2), Some(0));
        // Watermark below every version of a key.
        assert_eq!(index.get_newest_visible(hash_id(1), 0), Some(0));
        assert_eq!(index.get_newest_visible(hash_id(2), 0), None);
        // Absent key.
        assert_eq!(index.get_newest_visible(hash_id(999), 5), None);
    }

    #[test]
    fn composite_pk_is_hashed_as_a_tuple() {
        // Two-column PK (id, name): (1,"a") and (1,"b") are distinct keys.
        let index = PkPositionIndex::new(vec!["id".to_string(), "name".to_string()]);
        index.insert(&batch(&[1, 1], &["a", "b"]), 0).unwrap();

        let b = batch(&[1], &["a"]);
        let pk_indices = resolve_pk_indices(&b, &["id".to_string(), "name".to_string()]).unwrap();
        let hash_1a = compute_pk_hash(&b, &pk_indices, 0);
        let b2 = batch(&[1], &["b"]);
        let hash_1b = compute_pk_hash(&b2, &pk_indices, 0);

        assert_eq!(index.get_newest_visible(hash_1a, 10), Some(0));
        assert_eq!(index.get_newest_visible(hash_1b, 10), Some(1));
        assert_ne!(hash_1a, hash_1b);
    }

    #[test]
    fn empty_batch_is_a_noop() {
        let index = PkPositionIndex::new(vec!["id".to_string()]);
        index.insert(&batch(&[], &[]), 0).unwrap();
        assert!(index.is_empty());
    }
}
