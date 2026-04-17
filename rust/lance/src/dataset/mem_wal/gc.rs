// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Garbage collection for WAL entries.
//!
//! Periodically deletes WAL entries that have been flushed to storage,
//! using `replay_after_wal_entry_position` from the shard manifest to
//! determine which entries are safe to remove.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use lance_core::Result;
use lance_io::object_store::ObjectStore;
use log::{debug, info, warn};
use object_store::path::Path;
use tracing::instrument;

use super::manifest::ShardManifestStore;
use super::util::parse_bit_reversed_filename;
use super::write::MessageHandler;

type MessageFactory<T> = Box<dyn Fn() -> T + Send + Sync>;

/// Trigger message for WAL garbage collection.
#[derive(Debug)]
pub struct TriggerWalGc;

/// Background handler that periodically deletes flushed WAL entries.
pub struct WalGcHandler {
    object_store: Arc<ObjectStore>,
    manifest_store: Arc<ShardManifestStore>,
    wal_dir: Path,
    interval: Duration,
}

impl WalGcHandler {
    pub fn new(
        object_store: Arc<ObjectStore>,
        manifest_store: Arc<ShardManifestStore>,
        wal_dir: Path,
        interval: Duration,
    ) -> Self {
        Self {
            object_store,
            manifest_store,
            wal_dir,
            interval,
        }
    }

    #[instrument(name = "wal_gc", level = "debug", skip_all)]
    async fn collect(&self) -> Result<usize> {
        let Some(manifest) = self.manifest_store.read_latest().await? else {
            return Ok(0);
        };

        let gc_before = manifest.replay_after_wal_entry_position;
        if gc_before == 0 {
            return Ok(0);
        }

        // List all WAL entry files
        let entries: Vec<_> = self
            .object_store
            .inner
            .list(Some(&self.wal_dir))
            .collect::<Vec<_>>()
            .await;

        let mut deleted = 0usize;
        for item in entries {
            match item {
                Ok(meta) => {
                    if let Some(filename) = meta.location.filename()
                        && filename.ends_with(".arrow")
                        && let Some(position) = parse_bit_reversed_filename(filename)
                        && position <= gc_before
                    {
                        if let Err(e) = self.object_store.delete(&meta.location).await {
                            warn!("Failed to delete WAL entry {}: {}", position, e);
                        } else {
                            deleted += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("Error listing WAL directory: {}", e);
                }
            }
        }

        if deleted > 0 {
            info!(
                "WAL GC: deleted {} entries (positions <= {})",
                deleted, gc_before
            );
        } else {
            debug!("WAL GC: no entries to collect");
        }

        Ok(deleted)
    }
}

#[async_trait]
impl MessageHandler<TriggerWalGc> for WalGcHandler {
    fn tickers(&mut self) -> Vec<(Duration, MessageFactory<TriggerWalGc>)> {
        vec![(self.interval, Box::new(|| TriggerWalGc))]
    }

    async fn handle(&mut self, _message: TriggerWalGc) -> Result<()> {
        if let Err(e) = self.collect().await {
            warn!("WAL GC failed: {}", e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::manifest::ShardManifestStore;
    use crate::dataset::mem_wal::util::shard_wal_path;
    use crate::dataset::mem_wal::wal::WalFlusher;
    use crate::dataset::mem_wal::write::BatchStore;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use uuid::Uuid;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, num_rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                Arc::new(StringArray::from_iter_values(
                    (0..num_rows).map(|i| format!("name_{}", i)),
                )),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_wal_gc_deletes_flushed_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, base_path) = ObjectStore::from_uri(&uri).await.unwrap();
        // store is already Arc<ObjectStore> from from_uri
        let shard_id = Uuid::new_v4();

        // Write 3 WAL entries
        let schema = create_test_schema();
        let mut flusher = WalFlusher::new(&base_path, shard_id, 1, 1);
        flusher.set_object_store(store.clone());

        let batch_store = BatchStore::with_capacity(10);
        for _ in 0..3 {
            batch_store.append(create_test_batch(&schema, 5)).unwrap();
        }

        // Flush batches one at a time to create 3 separate WAL entries
        let _ = flusher.track_batch(0);
        flusher
            .flush_to_with_index_update(&batch_store, 1, None)
            .await
            .unwrap(); // position 1

        let _ = flusher.track_batch(1);
        flusher
            .flush_to_with_index_update(&batch_store, 2, None)
            .await
            .unwrap(); // position 2

        let _ = flusher.track_batch(2);
        flusher
            .flush_to_with_index_update(&batch_store, 3, None)
            .await
            .unwrap(); // position 3

        // Create manifest store and write a manifest with replay_after = 2
        // (positions 1 and 2 are safe to delete)
        let manifest_store = Arc::new(ShardManifestStore::new(
            store.clone(),
            &base_path,
            shard_id,
            2,
        ));
        let (_, _manifest) = manifest_store.claim_epoch(0).await.unwrap();

        // Update manifest to set replay_after_wal_entry_position = 2
        manifest_store
            .commit_update(1, |current| {
                let mut updated = current.clone();
                updated.version = current.version + 1;
                updated.replay_after_wal_entry_position = 2;
                updated.wal_entry_position_last_seen = 3;
                updated
            })
            .await
            .unwrap();

        // Verify all 3 WAL files exist
        let wal_dir = shard_wal_path(&base_path, &shard_id);
        let entries: Vec<_> = store
            .inner
            .list(Some(&wal_dir))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(entries.len(), 3);

        // Run GC
        let handler = WalGcHandler::new(
            store.clone(),
            manifest_store,
            wal_dir.clone(),
            Duration::from_secs(60),
        );
        let deleted = handler.collect().await.unwrap();
        assert_eq!(deleted, 2);

        // Verify only position 3 remains
        let remaining: Vec<_> = store
            .inner
            .list(Some(&wal_dir))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(remaining.len(), 1);

        let filename = remaining[0].location.filename().unwrap();
        let position = parse_bit_reversed_filename(filename).unwrap();
        assert_eq!(position, 3);
    }

    #[tokio::test]
    async fn test_wal_gc_no_manifest() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, base_path) = ObjectStore::from_uri(&uri).await.unwrap();
        // store is already Arc<ObjectStore> from from_uri
        let shard_id = Uuid::new_v4();

        let manifest_store = Arc::new(ShardManifestStore::new(
            store.clone(),
            &base_path,
            shard_id,
            2,
        ));

        let wal_dir = shard_wal_path(&base_path, &shard_id);
        let handler = WalGcHandler::new(store, manifest_store, wal_dir, Duration::from_secs(60));

        // Should return 0 when no manifest exists
        let deleted = handler.collect().await.unwrap();
        assert_eq!(deleted, 0);
    }
}
