// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use lance_arrow::DataTypeExt;
use roaring::RoaringBitmap;

use crate::io::exec::filtered_read::FilteredReadExec;

const DEFAULT_SPLIT_SIZE: usize = 128 * 1024 * 1024;
const DEFAULT_VARIABLE_FIELD_SIZE: usize = 64;

/// Options for configuring split generation.
///
/// This struct allows specifying constraints on the maximum size and row count
/// for splits. Both fields are optional; if neither is set, default behavior
/// will be used.
#[derive(Debug, Clone, Default)]
pub struct SplittingOptions {
    /// Maximum size in bytes per split.
    ///
    /// The scanner estimates the row size from the output schema and calculates
    /// how many rows fit within this budget.
    pub max_size_bytes: Option<usize>,
    /// Maximum number of rows per split.
    pub max_row_count: Option<usize>,
}

/// Result of [`super::scanner::Scanner::plan_splits`], representing a single unit of work
/// for distributed execution.
///
/// Contains a fully-configured execution node with row selections, filters, and read options.
/// `output_columns` captures the user's originally requested columns in order so
/// that a consuming scanner can restrict its output accordingly. The exec's own
/// projection may include additional filter-only columns. Metadata columns like
/// `_rowid` and `_rowaddr` are included when the original scanner requested them.
#[derive(Debug, Clone)]
pub struct Split {
    pub exec: Arc<FilteredReadExec>,
    pub output_columns: Vec<String>,
}

#[cfg(feature = "substrait")]
impl Split {
    /// Serialize this split to a protobuf message.
    ///
    /// The `state` is needed to Substrait-encode any filter expressions inside a
    /// [`FilteredReadExec`].
    pub fn to_proto(
        &self,
        state: &datafusion::execution::SessionState,
    ) -> lance_core::Result<lance_datafusion::pb::SplitProto> {
        use crate::io::exec::filtered_read_proto::filtered_read_exec_to_proto;

        let exec_proto = filtered_read_exec_to_proto(&self.exec, state)?;

        Ok(lance_datafusion::pb::SplitProto {
            filtered_read_exec: Some(exec_proto),
            output_columns: self.output_columns.clone(),
        })
    }

    /// Serialize this split to bytes using a default session state.
    ///
    /// This is a convenience wrapper around [`Self::to_proto`] that creates a
    /// default [`datafusion::prelude::SessionContext`] internally, then encodes
    /// the resulting protobuf to bytes via [`prost::Message`].
    pub fn to_bytes(&self) -> lance_core::Result<Vec<u8>> {
        use datafusion::prelude::SessionContext;
        use prost::Message;

        let state = SessionContext::new().state();
        let proto = self.to_proto(&state)?;
        Ok(proto.encode_to_vec())
    }

    /// Deserialize a split from bytes using a default session state.
    ///
    /// This is a convenience wrapper around [`Self::from_proto`] that creates a
    /// default [`datafusion::prelude::SessionContext`] internally, then decodes
    /// the bytes into a [`lance_datafusion::pb::SplitProto`] via [`prost::Message`].
    pub async fn from_bytes(
        bytes: &[u8],
        dataset: &Arc<crate::Dataset>,
    ) -> lance_core::Result<Self> {
        use datafusion::prelude::SessionContext;
        use prost::Message;

        let state = SessionContext::new().state();
        let proto = lance_datafusion::pb::SplitProto::decode(bytes).map_err(|e| {
            lance_core::Error::InvalidInput {
                source: format!("Failed to decode SplitProto: {e}").into(),
                location: snafu::location!(),
            }
        })?;
        Self::from_proto(proto, dataset, &state).await
    }

    /// Deserialize a split from a protobuf message.
    ///
    /// The `dataset` and `state` are needed to reconstruct a [`FilteredReadExec`]
    /// from the proto.
    pub async fn from_proto(
        proto: lance_datafusion::pb::SplitProto,
        dataset: &Arc<crate::Dataset>,
        state: &datafusion::execution::SessionState,
    ) -> lance_core::Result<Self> {
        use crate::io::exec::filtered_read_proto::filtered_read_exec_from_proto;

        let exec_proto =
            proto
                .filtered_read_exec
                .ok_or_else(|| lance_core::Error::InvalidInput {
                    source: "SplitProto has no filtered_read_exec set".into(),
                    location: snafu::location!(),
                })?;

        let exec = filtered_read_exec_from_proto(exec_proto, dataset.clone(), None, state).await?;
        Ok(Self {
            exec: Arc::new(exec),
            output_columns: proto.output_columns,
        })
    }
}

/// Computes the maximum number of rows per split from optional row-count and byte-size
/// constraints.
///
/// When `max_bytes` is provided the row count is estimated from the schema's field sizes.
/// When neither constraint is set, a default split size of 128 MiB is used.
pub(crate) fn max_rows_per_split(
    max_row_count: Option<usize>,
    max_bytes: Option<usize>,
    schema: &ArrowSchema,
) -> usize {
    match (max_row_count, max_bytes) {
        (Some(max_rows), Some(max_bytes)) => {
            max_rows.min(estimate_rows_from_bytes(schema, max_bytes))
        }
        (Some(max_rows), None) => max_rows,
        (None, Some(max_bytes)) => estimate_rows_from_bytes(schema, max_bytes),
        (None, None) => estimate_rows_from_bytes(schema, DEFAULT_SPLIT_SIZE),
    }
}

/// Estimates the number of rows that fit in `max_bytes` based on the schema's field sizes.
///
/// Fixed-size fields use their exact byte width; variable-size fields are estimated at
/// [`DEFAULT_VARIABLE_FIELD_SIZE`] bytes each.
fn estimate_rows_from_bytes(schema: &ArrowSchema, max_bytes: usize) -> usize {
    let estimated_row_size: usize = schema
        .fields()
        .iter()
        .map(|f| {
            f.data_type()
                .byte_width_opt()
                .unwrap_or(DEFAULT_VARIABLE_FIELD_SIZE)
        })
        .sum();
    max_bytes / estimated_row_size.max(1)
}

/// A lightweight item used during bin-packing in [`super::scanner::Scanner::plan_splits`].
pub(crate) struct BinItem {
    pub fragment_id: u32,
    pub bitmap: RoaringBitmap,
    pub row_count: u64,
}

/// Packs bin items into bins where each bin's total row count is at most `maximum_count`.
///
/// Uses a head-tail algorithm: items must be sorted by row count in descending order. The
/// largest remaining item (head) starts a new bin, then the smallest remaining items (tail)
/// are added while they fit. This pairs large and small items together for better utilization.
///
/// Items that exceed `maximum_count` on their own are placed in their own bin.
pub(crate) fn bin_pack(items: Vec<BinItem>, maximum_count: u64) -> Vec<Vec<BinItem>> {
    let mut deque: VecDeque<BinItem> = VecDeque::from(items);
    let mut bins: Vec<Vec<BinItem>> = Vec::new();

    while let Some(head) = deque.pop_front() {
        // Items that exceed the maximum get their own bin
        if head.row_count > maximum_count {
            bins.push(vec![head]);
            continue;
        }

        let mut bin_count = head.row_count;
        let mut bin = vec![head];

        // Fill from the tail with the smallest items that fit
        while let Some(tail) = deque.back() {
            if bin_count + tail.row_count > maximum_count {
                break;
            }
            let tail = deque.pop_back().unwrap();
            bin_count += tail.row_count;
            bin.push(tail);
        }

        bins.push(bin);
    }

    bins
}

#[cfg(all(test, feature = "substrait"))]
mod test_serde {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::dataset::split::SplittingOptions;

    #[tokio::test]
    async fn test_split_roundtrip_bytes() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(Int32Array::from_iter_values(100..200)),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let dataset = Arc::new(
            crate::Dataset::write(reader, "memory://split_roundtrip", None)
                .await
                .unwrap(),
        );

        let splits = dataset
            .scan()
            .plan_splits(Some(SplittingOptions {
                max_row_count: Some(30),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(!splits.is_empty());

        for original in &splits {
            let bytes = original.to_bytes().unwrap();
            let restored = super::Split::from_bytes(&bytes, &dataset).await.unwrap();
            assert_eq!(original.output_columns, restored.output_columns);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn bitmap_from_range(range: std::ops::Range<u32>) -> RoaringBitmap {
        RoaringBitmap::from_sorted_iter(range).unwrap()
    }

    #[test]
    fn test_bin_pack_empty() {
        let bins = bin_pack(vec![], 100);
        assert!(bins.is_empty());
    }

    #[test]
    fn test_bin_pack_single_item_fits() {
        let items = vec![BinItem {
            fragment_id: 0,
            bitmap: bitmap_from_range(0..50),
            row_count: 50,
        }];
        let bins = bin_pack(items, 100);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 1);
        assert_eq!(bins[0][0].fragment_id, 0);
    }

    #[test]
    fn test_bin_pack_single_item_exceeds_maximum() {
        let items = vec![BinItem {
            fragment_id: 0,
            bitmap: bitmap_from_range(0..200),
            row_count: 200,
        }];
        let bins = bin_pack(items, 100);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 1);
        assert_eq!(bins[0][0].row_count, 200);
    }

    #[test]
    fn test_bin_pack_multiple_items_fit_single_bin() {
        let items = vec![
            BinItem {
                fragment_id: 0,
                bitmap: bitmap_from_range(0..30),
                row_count: 30,
            },
            BinItem {
                fragment_id: 1,
                bitmap: bitmap_from_range(0..30),
                row_count: 30,
            },
            BinItem {
                fragment_id: 2,
                bitmap: bitmap_from_range(0..30),
                row_count: 30,
            },
        ];
        let bins = bin_pack(items, 100);
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 3);
    }

    #[test]
    fn test_bin_pack_items_split_across_bins() {
        let items = vec![
            BinItem {
                fragment_id: 0,
                bitmap: bitmap_from_range(0..60),
                row_count: 60,
            },
            BinItem {
                fragment_id: 1,
                bitmap: bitmap_from_range(0..60),
                row_count: 60,
            },
            BinItem {
                fragment_id: 2,
                bitmap: bitmap_from_range(0..60),
                row_count: 60,
            },
        ];
        let bins = bin_pack(items, 100);
        // 60+60 > 100, so each item needs its own bin
        assert_eq!(bins.len(), 3);
        for bin in &bins {
            assert_eq!(bin.len(), 1);
        }
    }

    fn fixed_schema(field_sizes: &[i32]) -> ArrowSchema {
        use arrow_schema::{DataType, Field};
        ArrowSchema::new(
            field_sizes
                .iter()
                .enumerate()
                .map(|(i, &size)| {
                    let dt = match size {
                        4 => DataType::Int32,
                        8 => DataType::Int64,
                        _ => DataType::FixedSizeBinary(size),
                    };
                    Field::new(format!("f{i}"), dt, false)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn variable_schema(n: usize) -> ArrowSchema {
        use arrow_schema::{DataType, Field};
        ArrowSchema::new(
            (0..n)
                .map(|i| Field::new(format!("s{i}"), DataType::Utf8, false))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_max_rows_only_row_count() {
        let schema = fixed_schema(&[4, 4]);
        assert_eq!(max_rows_per_split(Some(500), None, &schema), 500);
    }

    #[test]
    fn test_max_rows_only_bytes() {
        // Two Int32 fields → 8 bytes/row, 800 bytes budget → 100 rows
        let schema = fixed_schema(&[4, 4]);
        assert_eq!(max_rows_per_split(None, Some(800), &schema), 100);
    }

    #[test]
    fn test_max_rows_both_row_count_wins() {
        // 8 bytes/row, 8000 bytes → 1000 rows from bytes; row_count = 50 wins
        let schema = fixed_schema(&[4, 4]);
        assert_eq!(max_rows_per_split(Some(50), Some(8000), &schema), 50);
    }

    #[test]
    fn test_max_rows_both_bytes_wins() {
        // 8 bytes/row, 80 bytes → 10 rows from bytes; row_count = 500 → bytes wins
        let schema = fixed_schema(&[4, 4]);
        assert_eq!(max_rows_per_split(Some(500), Some(80), &schema), 10);
    }

    #[test]
    fn test_max_rows_neither_uses_default_split_size() {
        // Two Int32 fields → 8 bytes/row, default 128 MiB → 128*1024*1024/8
        let schema = fixed_schema(&[4, 4]);
        let expected = DEFAULT_SPLIT_SIZE / 8;
        assert_eq!(max_rows_per_split(None, None, &schema), expected);
    }

    #[test]
    fn test_max_rows_variable_fields() {
        // Variable-size fields estimated at DEFAULT_VARIABLE_FIELD_SIZE each
        let schema = variable_schema(2);
        // 2 * 64 = 128 bytes/row, 1280 bytes → 10 rows
        assert_eq!(
            max_rows_per_split(None, Some(1280), &schema),
            1280 / (2 * DEFAULT_VARIABLE_FIELD_SIZE)
        );
    }

    #[test]
    fn test_max_rows_mixed_fields() {
        use arrow_schema::{DataType, Field};
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),  // 8 bytes
            Field::new("name", DataType::Utf8, false), // 64 bytes (estimated)
        ]);
        let row_size = 8 + DEFAULT_VARIABLE_FIELD_SIZE; // 72
        assert_eq!(max_rows_per_split(None, Some(720), &schema), 720 / row_size);
    }

    #[test]
    fn test_max_rows_empty_schema() {
        // Empty schema → 0 byte row size, clamped to 1 to avoid division by zero
        let schema = ArrowSchema::empty();
        assert_eq!(max_rows_per_split(None, Some(100), &schema), 100);
    }

    #[test]
    fn test_bin_pack_all_rows_preserved() {
        let items: Vec<BinItem> = (0..10)
            .map(|i| {
                let count = i * 10 + 10;
                BinItem {
                    fragment_id: i,
                    bitmap: bitmap_from_range(0..count),
                    row_count: count as u64,
                }
            })
            .collect();
        let total_input: u64 = items.iter().map(|i| i.row_count).sum();

        let bins = bin_pack(items, 75);
        let total_output: u64 = bins
            .iter()
            .flat_map(|bin| bin.iter())
            .map(|item| item.row_count)
            .sum();
        assert_eq!(total_input, total_output);
    }
}
