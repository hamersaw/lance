// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;

use lance_table::format::{Fragment, IndexMetadata};

/// A split representing a set of fragments that share common characteristics.
#[derive(Debug, Clone)]
pub struct Split {
    fragments: Vec<Fragment>,
}

impl Split {
    /// Creates a new Split from the given fragments.
    pub fn new(fragments: Vec<Fragment>) -> Self {
        Self { fragments }
    }

    /// Returns the fragments in this split.
    pub fn fragments(&self) -> &[Fragment] {
        &self.fragments
    }

    /// Consumes the split and returns the fragments.
    pub fn into_fragments(self) -> Vec<Fragment> {
        self.fragments
    }
}

/// Options for configuring split computation.
#[derive(Debug, Clone, Default)]
pub struct SplitOptions {
    // Reserved for future configuration options (ex. max_splits, max_fragments_per_split, etc)
}

impl SplitOptions {
    /// Creates a new SplitOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Computes splits from the given fragments based on index coverage.
///
/// For each `IndexMetadata` that contains all the specified field IDs,
/// creates a `Split` containing the fragments covered by that index.
/// Each fragment is assigned to exactly one split - either to an index-based split
/// (the first matching index wins) or to its own individual split if not covered
/// by any matching index.
///
/// # Arguments
///
/// * `filtered_field_ids` - The field IDs to match against index fields
/// * `indices` - The index metadata to check for field coverage
/// * `fragments` - The fragments to partition into splits
/// * `_options` - Configuration options for split computation
///
/// # Returns
///
/// A vector of `Split` instances. Index-based splits come first (one for each index
/// that contains all the specified field IDs), followed by individual splits for
/// any fragments not covered by any matching index. Each fragment appears in exactly
/// one split.
pub fn compute_splits(
    filtered_field_ids: &[i32],
    indices: &[IndexMetadata],
    fragments: &[Fragment],
    _options: Option<&SplitOptions>,
) -> Vec<Split> {
    let mut splits = Vec::new();
    // Track which fragments have already been assigned to a split
    let mut assigned_fragments: HashSet<u64> = HashSet::new();

    // For each index, check if it contains all the requested fields
    for index in indices {
        let index_field_ids: HashSet<i32> = index.fields.iter().copied().collect();

        // Check if this index covers all requested fields
        if filtered_field_ids
            .iter()
            .all(|id| index_field_ids.contains(id))
        {
            // Get fragments covered by this index that haven't been assigned yet
            if let Some(fragment_bitmap) = &index.fragment_bitmap {
                let covered_fragments: Vec<Fragment> = fragments
                    .iter()
                    .filter(|f| {
                        fragment_bitmap.contains(f.id as u32) && !assigned_fragments.contains(&f.id)
                    })
                    .cloned()
                    .collect();

                if !covered_fragments.is_empty() {
                    // Mark these fragments as assigned
                    for frag in &covered_fragments {
                        assigned_fragments.insert(frag.id);
                    }
                    splits.push(Split::new(covered_fragments));
                }
            }
        }
    }

    // Assign any remaining unassigned fragments into their own split(s)
    for fragment in fragments {
        if !assigned_fragments.contains(&fragment.id) {
            splits.push(Split::new(vec![fragment.clone()]));
        }
    }

    splits
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use uuid::Uuid;

    fn create_test_fragment(id: u64) -> Fragment {
        Fragment::new(id)
    }

    fn create_test_index(fields: Vec<i32>, fragment_ids: Vec<u32>) -> IndexMetadata {
        let mut bitmap = RoaringBitmap::new();
        for id in fragment_ids {
            bitmap.insert(id);
        }
        IndexMetadata {
            uuid: Uuid::new_v4(),
            fields,
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap: Some(bitmap),
            index_details: None,
            index_version: 1,
            created_at: None,
            base_id: None,
        }
    }

    #[test]
    fn test_compute_splits_all_fragments_indexed() {
        let fragments: Vec<Fragment> = (0..3).map(create_test_fragment).collect();

        // Index covers all fragments - no unassigned fragments
        let indices = vec![create_test_index(vec![0], vec![0, 1, 2])];

        let splits = compute_splits(&[0], &indices, &fragments, None);

        // Only 1 index-based split, no individual splits
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].fragments().len(), 3);
    }

    #[test]
    fn test_compute_splits_multiple_field_ids() {
        let fragments: Vec<Fragment> = (0..5).map(create_test_fragment).collect();

        // Create an index covering multiple fields (0 and 1)
        let indices = vec![create_test_index(vec![0, 1], vec![0, 1, 2])];

        // Request both fields - should match the index
        let splits = compute_splits(&[0, 1], &indices, &fragments, None);
        assert_eq!(splits.len(), 3); // 1 index split + 2 individual

        // Request only one field - should still match (index contains the field)
        let splits = compute_splits(&[0], &indices, &fragments, None);
        assert_eq!(splits.len(), 3); // 1 index split + 2 individual

        // Request a field not in the index
        let splits = compute_splits(&[2], &indices, &fragments, None);
        assert_eq!(splits.len(), 5); // all individual splits
    }

    #[test]
    fn test_compute_splits_overlapping_indices() {
        let fragments: Vec<Fragment> = (0..10).map(create_test_fragment).collect();

        // Create two indices with overlapping fragment coverage
        // First index covers fragments 0, 1, 2, 3
        // Second index covers fragments 2, 3, 4, 5
        // Unassigned: fragments 6, 7, 8, 9
        let indices = vec![
            create_test_index(vec![0], vec![0, 1, 2, 3]),
            create_test_index(vec![0], vec![2, 3, 4, 5]),
        ];

        let splits = compute_splits(&[0], &indices, &fragments, None);

        // 2 index-based splits + 4 individual splits for unassigned fragments
        assert_eq!(splits.len(), 6);

        // First split gets fragments 0, 1, 2, 3
        assert_eq!(splits[0].fragments().len(), 4);
        let first_ids: Vec<u64> = splits[0].fragments().iter().map(|f| f.id).collect();
        assert!(first_ids.contains(&0));
        assert!(first_ids.contains(&1));
        assert!(first_ids.contains(&2));
        assert!(first_ids.contains(&3));

        // Second split only gets fragments 4, 5 (2, 3 already assigned)
        assert_eq!(splits[1].fragments().len(), 2);
        let second_ids: Vec<u64> = splits[1].fragments().iter().map(|f| f.id).collect();
        assert!(second_ids.contains(&4));
        assert!(second_ids.contains(&5));
        assert!(!second_ids.contains(&2));
        assert!(!second_ids.contains(&3));

        // Remaining are individual unassigned fragments (6, 7, 8, 9)
        let unassigned_ids: Vec<u64> = splits[2..]
            .iter()
            .map(|s| {
                assert_eq!(s.fragments().len(), 1);
                s.fragments()[0].id
            })
            .collect();
        assert!(unassigned_ids.contains(&6));
        assert!(unassigned_ids.contains(&7));
        assert!(unassigned_ids.contains(&8));
        assert!(unassigned_ids.contains(&9));
    }

    #[test]
    fn test_compute_splits_empty_inputs() {
        // Empty fragments
        let splits = compute_splits(&[0], &[create_test_index(vec![0], vec![0, 1])], &[], None);
        assert_eq!(splits.len(), 0);

        // Empty indices - all fragments become individual splits
        let fragments: Vec<Fragment> = (0..5).map(create_test_fragment).collect();
        let splits = compute_splits(&[0], &[], &fragments, None);
        assert_eq!(splits.len(), 5);
        for (i, split) in splits.iter().enumerate() {
            assert_eq!(split.fragments().len(), 1);
            assert_eq!(split.fragments()[0].id, i as u64);
        }

        // Empty field IDs - matches any index, remaining fragments become individual splits
        let splits = compute_splits(
            &[] as &[i32],
            &[create_test_index(vec![0], vec![0, 1])],
            &fragments,
            None,
        );
        // Index covers 0, 1; fragments 2, 3, 4 become individual splits
        assert_eq!(splits.len(), 4);
        assert_eq!(splits[0].fragments().len(), 2); // index-based split
        for split in splits.iter().skip(1) {
            assert_eq!(split.fragments().len(), 1); // individual splits
        }
    }

    #[test]
    fn test_compute_splits_index_without_bitmap() {
        let fragments: Vec<Fragment> = (0..5).map(create_test_fragment).collect();

        // Create an index without a fragment bitmap
        let index = IndexMetadata {
            uuid: Uuid::new_v4(),
            fields: vec![0],
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 1,
            created_at: None,
            base_id: None,
        };

        let splits = compute_splits(&[0], &[index], &fragments, None);

        // Index without bitmap doesn't cover any fragments, all become individual splits
        assert_eq!(splits.len(), 5);
        for (i, split) in splits.iter().enumerate() {
            assert_eq!(split.fragments().len(), 1);
            assert_eq!(split.fragments()[0].id, i as u64);
        }
    }
}
