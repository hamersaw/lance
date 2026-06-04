// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Composite primary-key encoding + index for MemWAL dedup.
//!
//! A multi-column primary key is reduced to a single order-preserving byte
//! string ([`encode_pk_tuple`]) so the whole tuple is one comparable key:
//! lexicographic byte order equals tuple order, and distinct tuples never
//! collide. The same key drives both the in-memory composite index
//! ([`PkKeyIndex`], a skiplist) and the flushed on-disk BTree (the key is the
//! index's `Binary` value column), so a probe builds `ScalarValue::Binary(key)`
//! and both ends agree.
//!
//! Single-column primary keys do **not** use this — they keep the compact typed
//! BTree directly (see [`super::BTreeMemIndex`]).

use std::sync::Mutex;

use arrow_array::{BinaryArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use lance_core::{Error, ROW_ID, Result};
use lance_index::scalar::registry::VALUE_COLUMN_NAME;
use std::sync::Arc;

use super::RowPosition;
use super::arena_skiplist::{SkipListReader, SkipListWriter, new_skiplist};

/// Sign-flip a signed integer to an order-preserving unsigned key (matches the
/// fixed-int BTree backend). Big-endian bytes of the result sort like the value.
#[inline]
fn encode_signed(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// Append an order-preserving encoding of one non-null byte string: each `0x00`
/// is escaped to `0x00 0xFF`, then a `0x00 0x00` terminator is appended. The
/// terminator sorts before any escaped content, so a prefix orders before its
/// extensions and no value can forge a column boundary.
fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// Append the order-preserving encoding of a single PK column value. A leading
/// tag (`0x00` null / `0x01` non-null) makes nulls sort first and keeps the
/// per-column encoding self-delimiting (fixed-width for ints, terminated for
/// bytes), so concatenating columns stays injective and order-preserving.
fn encode_value(out: &mut Vec<u8>, value: &ScalarValue) -> Result<()> {
    if value.is_null() {
        out.push(0x00);
        return Ok(());
    }
    out.push(0x01);
    macro_rules! be_signed {
        ($v:expr) => {
            out.extend_from_slice(&encode_signed($v as i64).to_be_bytes())
        };
    }
    match value {
        ScalarValue::Int8(Some(v)) => be_signed!(*v),
        ScalarValue::Int16(Some(v)) => be_signed!(*v),
        ScalarValue::Int32(Some(v)) => be_signed!(*v),
        ScalarValue::Int64(Some(v)) => be_signed!(*v),
        ScalarValue::Date32(Some(v)) => be_signed!(*v),
        ScalarValue::Date64(Some(v)) => be_signed!(*v),
        ScalarValue::UInt8(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt16(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt32(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt64(Some(v)) => out.extend_from_slice(&v.to_be_bytes()),
        ScalarValue::Boolean(Some(b)) => out.push(*b as u8),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            encode_bytes(out, s.as_bytes())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => encode_bytes(out, b),
        other => {
            return Err(Error::invalid_input(format!(
                "Unsupported primary-key column type for composite key: {other:?}"
            )));
        }
    }
    Ok(())
}

/// Encode a PK tuple (values in PK column order) to one order-preserving key.
pub fn encode_pk_tuple(values: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * 9);
    for value in values {
        encode_value(&mut out, value)?;
    }
    Ok(out)
}

/// Encode row `row` of `batch`'s PK columns (at `pk_indices`) to one key.
fn encode_pk_row(batch: &RecordBatch, pk_indices: &[usize], row: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(pk_indices.len() * 9);
    for &col in pk_indices {
        let value = ScalarValue::try_from_array(batch.column(col), row)?;
        encode_value(&mut out, &value)?;
    }
    Ok(out)
}

/// Skiplist key: the encoded tuple plus the row position (makes every entry
/// unique, so a non-unique key keeps every version). Sorts by `(bytes, pos)`.
#[derive(PartialEq, Eq)]
struct EncodedKey {
    bytes: Box<[u8]>,
    position: RowPosition,
}

impl PartialOrd for EncodedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncodedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes
            .cmp(&other.bytes)
            .then(self.position.cmp(&other.position))
    }
}

/// In-memory composite primary-key index: a skiplist over `(encoded_tuple,
/// position)`. Answers "newest visible version of this tuple" in one seek, the
/// composite analogue of [`super::BTreeMemIndex::get_newest_visible`].
pub struct PkKeyIndex {
    reader: SkipListReader<EncodedKey>,
    writer: Mutex<SkipListWriter<EncodedKey>>,
}

impl std::fmt::Debug for PkKeyIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkKeyIndex")
            .field("len", &self.len())
            .finish()
    }
}

impl PkKeyIndex {
    pub fn new() -> Self {
        let (writer, reader) = new_skiplist::<EncodedKey>();
        Self {
            reader,
            writer: Mutex::new(writer),
        }
    }

    /// Insert every row of `batch`, encoding the PK columns at `pk_indices`.
    /// `row_offset` is the absolute position of the first row.
    pub fn insert(&self, batch: &RecordBatch, pk_indices: &[usize], row_offset: u64) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        for row in 0..batch.num_rows() {
            let bytes = encode_pk_row(batch, pk_indices, row)?;
            writer.insert(EncodedKey {
                bytes: bytes.into_boxed_slice(),
                position: row_offset + row as u64,
            });
        }
        Ok(())
    }

    /// Newest position of the pre-encoded tuple `key` visible at
    /// `max_visible_row`, or `None`. A single seek-and-stop (no allocation).
    pub fn get_newest_visible(
        &self,
        key: &[u8],
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        let target = EncodedKey {
            bytes: key.into(),
            position: max_visible_row,
        };
        self.reader
            .upper_bound_with(&target, |found| {
                (found.bytes.as_ref() == key).then_some(found.position)
            })
            .flatten()
    }

    pub fn len(&self) -> usize {
        self.reader.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reader.len() == 0
    }

    /// Export as sorted `(Binary value, row_id)` batches to train the flushed
    /// on-disk BTree. Entries are already in `(bytes, position)` order, so the
    /// stream is sorted by value as `train_btree_index` requires.
    pub fn to_training_batches(&self, batch_size: usize) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Binary, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let mut batches = Vec::new();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
        let mut row_ids: Vec<u64> = Vec::with_capacity(batch_size);
        for entry in self.reader.iter() {
            keys.push(entry.bytes.to_vec());
            row_ids.push(entry.position);
            if keys.len() >= batch_size {
                batches.push(build_batch(&schema, &keys, &row_ids)?);
                keys.clear();
                row_ids.clear();
            }
        }
        if !keys.is_empty() {
            batches.push(build_batch(&schema, &keys, &row_ids)?);
        }
        Ok(batches)
    }
}

fn build_batch(schema: &Arc<Schema>, keys: &[Vec<u8>], row_ids: &[u64]) -> Result<RecordBatch> {
    let values = BinaryArray::from_iter_values(keys.iter());
    let ids = UInt64Array::from(row_ids.to_vec());
    RecordBatch::try_new(schema.clone(), vec![Arc::new(values), Arc::new(ids)])
        .map_err(|e| Error::io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};

    fn tuple(a: i32, b: &str) -> Vec<ScalarValue> {
        vec![ScalarValue::Int32(Some(a)), ScalarValue::from(b)]
    }

    #[test]
    fn encoding_is_order_preserving_and_injective() {
        // Sorting tuples by their encoding must match tuple order, and distinct
        // tuples must produce distinct bytes.
        let tuples = [
            tuple(1, "a"),
            tuple(1, "ab"),
            tuple(1, "b"),
            tuple(2, "a"),
            tuple(-1, "z"),
        ];
        let mut encoded: Vec<(Vec<u8>, &Vec<ScalarValue>)> = tuples
            .iter()
            .map(|t| (encode_pk_tuple(t).unwrap(), t))
            .collect();
        encoded.sort_by(|x, y| x.0.cmp(&y.0));
        let order: Vec<_> = encoded.iter().map(|(_, t)| (*t).clone()).collect();
        // -1 < 1 < 2; within id=1, "a" < "ab" < "b".
        assert_eq!(
            order,
            vec![
                tuple(-1, "z"),
                tuple(1, "a"),
                tuple(1, "ab"),
                tuple(1, "b"),
                tuple(2, "a"),
            ]
        );
        // Injective: 5 distinct tuples → 5 distinct keys.
        let mut keys: Vec<Vec<u8>> = tuples.iter().map(|t| encode_pk_tuple(t).unwrap()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 5);
    }

    #[test]
    fn null_sorts_first_and_is_distinct() {
        let null_a = vec![ScalarValue::Int32(None), ScalarValue::from("a")];
        let one_a = tuple(1, "a");
        assert!(encode_pk_tuple(&null_a).unwrap() < encode_pk_tuple(&one_a).unwrap());
        assert_ne!(
            encode_pk_tuple(&null_a).unwrap(),
            encode_pk_tuple(&one_a).unwrap()
        );
    }

    #[test]
    fn prefix_safety_with_embedded_zero() {
        // A string containing 0x00 must not collide with or sort incorrectly
        // against a shorter one (escaping + terminator).
        let with_zero = vec![ScalarValue::Binary(Some(vec![0x00]))];
        let empty = vec![ScalarValue::Binary(Some(vec![]))];
        assert!(encode_pk_tuple(&empty).unwrap() < encode_pk_tuple(&with_zero).unwrap());
    }

    fn id_name_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
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
    fn pk_key_index_newest_visible_is_snapshot_bounded() {
        let index = PkKeyIndex::new();
        // (1,"a")@0, (1,"b")@1, (1,"a")@2 — an update of (1,"a").
        index
            .insert(&id_name_batch(&[1, 1, 1], &["a", "b", "a"]), &[0, 1], 0)
            .unwrap();

        let key_1a = encode_pk_tuple(&tuple(1, "a")).unwrap();
        let key_1b = encode_pk_tuple(&tuple(1, "b")).unwrap();
        // Newest visible (1,"a") is its re-write at position 2...
        assert_eq!(index.get_newest_visible(&key_1a, 5), Some(2));
        // ...but bounded below the re-write, the older copy at 0.
        assert_eq!(index.get_newest_visible(&key_1a, 1), Some(0));
        assert_eq!(index.get_newest_visible(&key_1b, 5), Some(1));
        // Absent tuple.
        let key_2a = encode_pk_tuple(&tuple(2, "a")).unwrap();
        assert_eq!(index.get_newest_visible(&key_2a, 5), None);
    }

    #[test]
    fn training_batches_are_value_sorted() {
        let index = PkKeyIndex::new();
        index
            .insert(&id_name_batch(&[2, 1], &["a", "b"]), &[0, 1], 0)
            .unwrap();
        let batches = index.to_training_batches(8192).unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        // (1,"b") encodes below (2,"a"), so it comes first.
        assert!(values.value(0) < values.value(1));
    }
}
