// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;

use arrow_schema::Schema as ArrowSchema;
use lance_arrow::DataTypeExt;

const DEFAULT_SPLIT_SIZE: usize = 128 * 1024 * 1024;
const DEFAULT_VARIABLE_FIELD_SIZE: usize = 64;

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
    pub ranges: Vec<Range<u64>>,
    pub row_count: u64,
}

/// Packs bin items into bins where each bin's total row count is at most `maximum_count`.
///
/// Uses a first-fit algorithm: each item is placed in the first bin that has room for it.
/// For best results, the input should be sorted by row count in descending order.
///
/// Items that exceed `maximum_count` on their own are placed in their own bin.
pub(crate) fn bin_pack(items: Vec<BinItem>, maximum_count: u64) -> Vec<Vec<BinItem>> {
    let mut bins: Vec<(Vec<BinItem>, u64)> = Vec::new(); // (items, current_count)

    for item in items {
        let item_count = item.row_count;

        // Items that exceed the maximum get their own bin
        if item_count > maximum_count {
            bins.push((vec![item], item_count));
            continue;
        }

        // Find first bin with enough remaining capacity
        let target_bin = bins
            .iter()
            .position(|(_, bin_count)| *bin_count + item_count <= maximum_count);

        match target_bin {
            Some(idx) => {
                bins[idx].0.push(item);
                bins[idx].1 += item_count;
            }
            None => {
                // Create new bin if no existing bin has room
                bins.push((vec![item], item_count));
            }
        }
    }

    bins.into_iter().map(|(items, _)| items).collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bin_pack_empty() {
        let bins = bin_pack(vec![], 100);
        assert!(bins.is_empty());
    }

    #[test]
    fn test_bin_pack_single_item_fits() {
        let items = vec![BinItem {
            fragment_id: 0,
            ranges: vec![0..50],
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
            ranges: vec![0..200],
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
                ranges: vec![0..30],
                row_count: 30,
            },
            BinItem {
                fragment_id: 1,
                ranges: vec![0..30],
                row_count: 30,
            },
            BinItem {
                fragment_id: 2,
                ranges: vec![0..30],
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
                ranges: vec![0..60],
                row_count: 60,
            },
            BinItem {
                fragment_id: 1,
                ranges: vec![0..60],
                row_count: 60,
            },
            BinItem {
                fragment_id: 2,
                ranges: vec![0..60],
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

    #[test]
    fn test_bin_pack_all_rows_preserved() {
        let items: Vec<BinItem> = (0..10)
            .map(|i| BinItem {
                fragment_id: i,
                ranges: vec![0..(i as u64 * 10 + 10)],
                row_count: i as u64 * 10 + 10,
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
