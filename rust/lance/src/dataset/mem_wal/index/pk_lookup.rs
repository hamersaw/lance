// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tuple-level MVCC primary-key lookups over the per-column BTree indexes.
//!
//! The memtable maintains a [`super::BTreeMemIndex`] on every primary-key
//! column (reusing a user's scalar index when one exists, otherwise an
//! auto-created one — see [`super::IndexStore::enable_pk_index`]). Each BTree is
//! value-keyed as `(value, row_position)`, so for one column
//! [`super::BTreeMemIndex::get_newest_visible`] answers "the newest row position
//! for this value visible at the watermark" with a single seek.
//!
//! [`PkLookup`] composes those per-column answers into a single primary-key
//! question. Because `row_position` is the physical row identity, the rows
//! matching a tuple `(v0 … vn)` are exactly the intersection of each column's
//! position set, so the newest visible row of the tuple is the largest position
//! present in *every* column at or below the watermark. This is collision-free
//! (no hashing) and covers single and composite primary keys uniformly.
//!
//! Cost: a single-column primary key is one seek. A composite key walks the
//! leading column's positions for `v0` and point-probes the rest, so its cost
//! tracks the leading column's cardinality for that value — put the most
//! selective column first in a composite primary key.

use std::sync::Arc;

use datafusion::common::ScalarValue;

use super::RowPosition;
use super::btree::BTreeMemIndex;

/// A read-only view over the primary-key BTree indexes (one per PK column, in
/// primary-key order), composing them into tuple-level MVCC lookups. Borrowed
/// from the [`super::IndexStore`]; cheap to construct (it holds a slice).
pub struct PkLookup<'a> {
    columns: &'a [Arc<BTreeMemIndex>],
}

impl<'a> PkLookup<'a> {
    pub(super) fn new(columns: &'a [Arc<BTreeMemIndex>]) -> Self {
        Self { columns }
    }

    /// The newest row position of the tuple `values` (in primary-key order)
    /// visible at `max_visible_row`, or `None` if no version is visible.
    ///
    /// `values` must have one entry per primary-key column.
    pub fn get_newest_visible(
        &self,
        values: &[ScalarValue],
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        match self.columns {
            [] => None,
            // Single-column primary key: one seek-and-stop, no intersection.
            [only] => only.get_newest_visible(&values[0], max_visible_row),
            // Composite: walk the leading column's visible positions newest-first
            // and keep the first that every other column also holds at the same
            // physical position (⇒ the whole tuple matches that row).
            [leading, rest @ ..] => {
                let mut positions = leading.visible_positions(&values[0], max_visible_row);
                positions.reverse();
                positions.into_iter().find(|&position| {
                    rest.iter()
                        .zip(&values[1..])
                        .all(|(column, value)| column.contains_position(value, position))
                })
            }
        }
    }

    /// Whether `position` is the newest visible row of `values` — the recency
    /// check the active index-search arms apply to drop predicate-crossing
    /// stale hits.
    pub fn is_newest(
        &self,
        values: &[ScalarValue],
        position: RowPosition,
        max_visible_row: RowPosition,
    ) -> bool {
        self.get_newest_visible(values, max_visible_row) == Some(position)
    }

    /// Whether `values` has any version visible at `max_visible_row` — the
    /// cross-source block-list's existence query, snapshot-bounded so a
    /// not-yet-visible write can't shadow an older visible copy.
    pub fn contains_visible(&self, values: &[ScalarValue], max_visible_row: RowPosition) -> bool {
        self.get_newest_visible(values, max_visible_row).is_some()
    }

    /// Whether the primary-key index holds no rows. All columns are inserted
    /// together, so the leading column's emptiness answers for the tuple.
    pub fn is_empty(&self) -> bool {
        self.columns.first().is_none_or(|c| c.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn btree(field_id: i32, column: &str) -> Arc<BTreeMemIndex> {
        Arc::new(BTreeMemIndex::new(field_id, column.to_string()))
    }

    fn composite_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn single_column_matches_the_btree_directly() {
        let id = btree(0, "id");
        // id=1 at positions 0 and 2 (an update), id=2 at position 1.
        let b = Int32Array::from(vec![1, 2]);
        id.insert(
            &RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![Arc::new(b)],
            )
            .unwrap(),
            0,
        )
        .unwrap();
        id.insert(
            &RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap(),
            2,
        )
        .unwrap();

        let columns = [id];
        let pk = PkLookup::new(&columns);
        let one = [ScalarValue::Int32(Some(1))];
        // Watermark above the update sees the newest position; below it, the older.
        assert_eq!(pk.get_newest_visible(&one, 5), Some(2));
        assert_eq!(pk.get_newest_visible(&one, 1), Some(0));
        assert!(pk.is_newest(&one, 2, 5));
        assert!(!pk.is_newest(&one, 0, 5));
    }

    #[test]
    fn composite_intersects_by_position() {
        let id = btree(0, "id");
        let name = btree(1, "name");
        // Rows: (1,"a")@0, (1,"b")@1, (1,"a")@2 — an update of (1,"a").
        let b = composite_batch(&[1, 1, 1], &["a", "b", "a"]);
        id.insert(&b, 0).unwrap();
        name.insert(&b, 0).unwrap();

        let columns = [id, name];
        let pk = PkLookup::new(&columns);
        let tuple_1a = [ScalarValue::Int32(Some(1)), ScalarValue::from("a")];
        let tuple_1b = [ScalarValue::Int32(Some(1)), ScalarValue::from("b")];

        // (1,"a") newest visible row is its re-write at position 2.
        assert_eq!(pk.get_newest_visible(&tuple_1a, 5), Some(2));
        // (1,"b") only exists at position 1.
        assert_eq!(pk.get_newest_visible(&tuple_1b, 5), Some(1));
        // The stale (1,"a")@0 is not the newest; the re-write @2 is.
        assert!(!pk.is_newest(&tuple_1a, 0, 5));
        assert!(pk.is_newest(&tuple_1a, 2, 5));
        // Watermark below the re-write: the older (1,"a")@0 is the newest visible.
        assert_eq!(pk.get_newest_visible(&tuple_1a, 1), Some(0));
        assert!(pk.is_newest(&tuple_1a, 0, 1));
        // An absent tuple.
        let tuple_2a = [ScalarValue::Int32(Some(2)), ScalarValue::from("a")];
        assert_eq!(pk.get_newest_visible(&tuple_2a, 5), None);
        assert!(!pk.contains_visible(&tuple_2a, 5));
    }
}
