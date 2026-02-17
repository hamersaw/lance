// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::VecDeque;

use arrow_schema::Schema as ArrowSchema;
use lance_arrow::DataTypeExt;
use roaring::RoaringBitmap;

use crate::io::exec::filtered_read::FilteredReadPlan;

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
#[derive(Debug, Clone)]
pub enum Split {
    /// A detailed per-fragment read plan with row ranges and residual filters.
    FilteredReadPlan(FilteredReadPlan),
    /// Fragment IDs only — a collection of fragment IDs to scan.
    Fragments(Vec<u32>),
}

#[cfg(feature = "substrait")]
impl Split {
    /// Serialize this split to a protobuf message.
    ///
    /// The `filter_schema` and `state` are needed to Substrait-encode any filter
    /// expressions inside a [`FilteredReadPlan`]. For the `Fragments` variant they
    /// are unused.
    pub fn to_proto(
        &self,
        filter_schema: &std::sync::Arc<ArrowSchema>,
        state: &datafusion::execution::SessionState,
    ) -> lance_core::Result<lance_datafusion::pb::SplitProto> {
        use crate::io::exec::filtered_read_proto::plan_to_proto;
        use lance_datafusion::pb::split_proto;

        let split = match self {
            Self::FilteredReadPlan(plan) => {
                split_proto::Split::FilteredReadPlan(plan_to_proto(plan, filter_schema, state)?)
            }
            Self::Fragments(ids) => {
                split_proto::Split::Fragments(lance_datafusion::pb::FragmentIdList {
                    fragment_ids: ids.clone(),
                })
            }
        };

        Ok(lance_datafusion::pb::SplitProto { split: Some(split) })
    }

    /// Deserialize a split from a protobuf message.
    ///
    /// The `dataset` and `state` are needed to reconstruct filter expressions
    /// inside a [`FilteredReadPlan`]. For the `Fragments` variant they are unused.
    pub async fn from_proto(
        proto: lance_datafusion::pb::SplitProto,
        dataset: &std::sync::Arc<crate::Dataset>,
        state: &datafusion::execution::SessionState,
    ) -> lance_core::Result<Self> {
        use crate::io::exec::filtered_read_proto::plan_from_proto;
        use lance_datafusion::pb::split_proto;

        match proto.split.ok_or_else(|| lance_core::Error::InvalidInput {
            source: "SplitProto has no split variant set".into(),
            location: snafu::location!(),
        })? {
            split_proto::Split::FilteredReadPlan(plan_proto) => {
                let plan = plan_from_proto(plan_proto, dataset, state).await?;
                Ok(Self::FilteredReadPlan(plan))
            }
            split_proto::Split::Fragments(frag_list) => Ok(Self::Fragments(frag_list.fragment_ids)),
        }
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
