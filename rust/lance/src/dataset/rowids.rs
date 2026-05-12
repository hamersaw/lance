// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::Dataset;
use crate::session::caches::{RowIdIndexKey, RowIdSequenceKey};
use crate::{Error, Result};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use lance_core::utils::deletion::DeletionVector;
use lance_table::{
    format::{Fragment, RowIdHint, RowIdMeta},
    rowids::{FragmentRowIdIndex, RowIdIndex, RowIdSequence, read_row_ids},
};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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

/// Build (or fetch from cache) a `RowIdIndex` covering every fragment in
/// the dataset. Suitable for streaming execs like `AddRowAddrExec` that
/// need to translate arbitrary row ids and would otherwise rebuild the
/// index per batch.
pub async fn get_full_row_id_index(dataset: &Dataset) -> Result<Option<Arc<RowIdIndex>>> {
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(None);
    }
    let fragments = &dataset.manifest.fragments;
    let fragment_ids: Vec<u32> = fragments.iter().map(|f| f.id as u32).collect();
    let index = get_or_build_index_for_ids(dataset, fragments, fragment_ids).await?;
    Ok(Some(index))
}

/// Hash of a sorted-unique set of fragment ids. Used as part of the cache
/// key so two callers requesting the same fragment set share one entry.
fn hash_fragment_ids(sorted_ids: &[u32]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sorted_ids.hash(&mut hasher);
    hasher.finish()
}

/// Build a `RowIdIndex` over the supplied fragments, going through the
/// session cache. The caller is responsible for passing the exact fragment
/// set to include — the cache key is derived from it.
async fn get_or_build_index_for_ids(
    dataset: &Dataset,
    all_fragments: &[Fragment],
    mut fragment_ids: Vec<u32>,
) -> Result<Arc<RowIdIndex>> {
    fragment_ids.sort_unstable();
    fragment_ids.dedup();
    let key = RowIdIndexKey {
        version: dataset.manifest.version,
        fragment_set_hash: hash_fragment_ids(&fragment_ids),
    };

    // Resolve fragment refs once so we can hand them to the build closure
    // without re-scanning the manifest on cache hit.
    let candidate_refs: Vec<&Fragment> = {
        let id_set: std::collections::HashSet<u32> = fragment_ids.iter().copied().collect();
        all_fragments
            .iter()
            .filter(|f| id_set.contains(&(f.id as u32)))
            .collect()
    };

    dataset
        .metadata_cache
        .get_or_insert_with_key(key, || load_row_id_index_for_refs(dataset, candidate_refs))
        .await
}

/// Assemble a `RowIdIndex` from the given fragment references. Loads each
/// fragment's row id sequence (cached per-fragment) and deletion vector.
async fn load_row_id_index_for_refs<'a>(
    dataset: &'a Dataset,
    fragments: Vec<&'a Fragment>,
) -> Result<RowIdIndex> {
    // Materialize per-fragment futures eagerly so the stream's closure
    // doesn't need to be HRTB-general — each future captures concrete
    // references tied to 'a.
    let sequence_futs: Vec<_> = fragments
        .iter()
        .map(|&fragment| {
            let fragment_id = fragment.id as u32;
            let fut = load_row_id_sequence(dataset, fragment);
            async move { fut.await.map(|seq| (fragment_id, seq)) }
        })
        .collect();
    let sequences: Vec<(u32, Arc<RowIdSequence>)> = futures::stream::iter(sequence_futs)
        .buffer_unordered(dataset.object_store.io_parallelism())
        .try_collect()
        .await?;

    let all_fragments = dataset.get_fragments();
    let fragment_map: std::collections::HashMap<u32, &crate::dataset::fragment::FileFragment> =
        all_fragments.iter().map(|f| (f.id() as u32, f)).collect();

    let fragment_indices: Vec<_> =
        futures::stream::iter(sequences.into_iter().map(|(fragment_id, sequence)| {
            let fragment = fragment_map
                .get(&fragment_id)
                .expect("Fragment should exist");
            let has_deletion_file = fragment.metadata().deletion_file.is_some();
            let fragment_clone = (*fragment).clone();
            async move {
                let deletion_vector = if has_deletion_file {
                    match fragment_clone.get_deletion_vector().await {
                        Ok(Some(dv)) => dv,
                        Ok(None) | Err(_) => Arc::new(DeletionVector::default()),
                    }
                } else {
                    Arc::new(DeletionVector::default())
                };

                Ok::<FragmentRowIdIndex, Error>(FragmentRowIdIndex {
                    fragment_id,
                    row_id_sequence: sequence,
                    deletion_vector,
                })
            }
        }))
        .buffer_unordered(dataset.object_store.io_parallelism())
        .try_collect()
        .await?;

    RowIdIndex::new(&fragment_indices)
}

/// Threshold above which we sort the row id slice and binary-search per
/// fragment hint, instead of doing a linear scan per fragment. Small lookup
/// batches (the common take-by-rowid case) skip the up-front sort cost.
const HINT_SORT_THRESHOLD: usize = 32;

/// Return the ids of fragments that could own at least one of the requested
/// row ids. Fragments without a hint are always included (legacy data).
fn select_candidate_fragment_ids(fragments: &[Fragment], row_ids: &[u64]) -> Vec<u32> {
    let sorted_ids: Option<Vec<u64>> = if row_ids.len() > HINT_SORT_THRESHOLD {
        let mut v = row_ids.to_vec();
        v.sort_unstable();
        Some(v)
    } else {
        None
    };

    fragments
        .iter()
        .filter_map(|frag| {
            let include = match &frag.row_id_hint {
                None => true,
                Some(RowIdHint::MinMax { min, max }) => match &sorted_ids {
                    Some(sorted) => {
                        let pos = sorted.partition_point(|id| id < min);
                        sorted.get(pos).is_some_and(|id| id <= max)
                    }
                    None => row_ids.iter().any(|id| min <= id && id <= max),
                },
            };
            include.then_some(frag.id as u32)
        })
        .collect()
}

/// Build a `RowIdIndex` covering only the fragments that could own the
/// requested row ids. Falls back to `None` for non-stable-rowid datasets.
///
/// Lazy-loading entry point: a take that touches one fragment only pays
/// I/O for that fragment's row id sequence and deletion vector. The result
/// is memoized via the session cache, keyed by (manifest version, sorted
/// candidate fragment id set), so repeated queries against the same set
/// reuse the assembled index.
pub async fn build_row_id_index_for(
    dataset: &Dataset,
    row_ids: &[u64],
) -> Result<Option<Arc<RowIdIndex>>> {
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(None);
    }
    let candidate_ids = select_candidate_fragment_ids(&dataset.manifest.fragments, row_ids);
    let index =
        get_or_build_index_for_ids(dataset, &dataset.manifest.fragments, candidate_ids).await?;
    Ok(Some(index))
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
        assert_eq!(id_set, expected.iter().cloned().collect());

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

    /// After updating a row, the absorbing fragment's hint widens to include
    /// the moved row id. Translation must still resolve correctly even when
    /// multiple fragments are candidates for the same id.
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

        // Update an "early" row (id=2). With stable row ids the rowid stays
        // the same, but the row physically moves to a new fragment, so that
        // new fragment's hint will straddle a wide range that overlaps with
        // the original fragment's hint.
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

        // Confirm we have at least two fragments whose hints both cover
        // rowid 2 — that's the overlap case the lazy translator must handle.
        let candidates: Vec<_> = updated
            .manifest
            .fragments
            .iter()
            .filter(|f| match &f.row_id_hint {
                Some(h) => h.could_contain(2),
                None => true,
            })
            .collect();
        assert!(
            candidates.len() >= 2,
            "Expected at least two candidate fragments for the moved row id, got {}",
            candidates.len(),
        );

        // The lazy index must resolve rowid 2 to the new fragment, not the
        // original (which has a tombstone).
        let lazy = build_row_id_index_for(&updated, &[2])
            .await
            .unwrap()
            .unwrap();
        let resolved = lazy.get(2).expect("rowid 2 must resolve after update");
        // Resolved address must match the eager index's view of the world.
        let full = get_row_id_index(&updated).await.unwrap().unwrap();
        assert_eq!(full.get(2), Some(resolved));
    }

    /// Backward-compat: a fragment without a row_id_hint (legacy data) must
    /// still resolve correctly through the lazy translator. The translator
    /// treats it as a candidate for every row id, so it loads the sequence
    /// and routes accordingly — same correctness, no IOP improvement.
    #[tokio::test]
    async fn test_lazy_index_handles_missing_hint() {
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

        // Strip the hints from every fragment to simulate legacy data.
        let mut new_manifest = (*dataset.manifest).clone();
        let mut new_fragments = (*new_manifest.fragments).clone();
        for f in &mut new_fragments {
            f.row_id_hint = None;
        }
        new_manifest.fragments = Arc::new(new_fragments);
        dataset.manifest = Arc::new(new_manifest);

        // Lookups must still resolve — falling back to load all fragments.
        for id in 0..num_rows {
            let lazy = build_row_id_index_for(&dataset, &[id])
                .await
                .unwrap()
                .unwrap();
            assert!(
                lazy.get(id).is_some(),
                "rowid {id} must resolve when hints are absent",
            );
        }
    }
}
