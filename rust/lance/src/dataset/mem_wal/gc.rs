// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Garbage collection for WAL entries.
//!
//! Periodically deletes WAL entries that have been flushed to storage.
//! The current `replay_after_wal_entry_position` is delivered via a watch
//! channel fed by `MemTableFlusher`, so the handler neither re-reads the
//! manifest nor re-lists the WAL directory on idle ticks — it only does work
//! when the flusher has advanced the watermark since the last successful sweep.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use lance_core::Result;
use lance_io::object_store::ObjectStore;
use log::{debug, info, warn};
use object_store::path::Path;
use tokio::sync::watch;
use tracing::instrument;

use super::util::parse_bit_reversed_filename;
use super::write::MessageHandler;

type MessageFactory<T> = Box<dyn Fn() -> T + Send + Sync>;

/// Trigger message for WAL garbage collection.
#[derive(Debug)]
pub struct TriggerWalGc;

/// Background handler that periodically deletes flushed WAL entries.
pub struct WalGcHandler {
    object_store: Arc<ObjectStore>,
    wal_dir: Path,
    interval: Duration,
    replay_after_rx: watch::Receiver<u64>,
    // Highest `replay_after_wal_entry_position` for which a sweep has completed
    // without any list/delete errors. Ticks short-circuit when the watch value
    // hasn't advanced past this; a failed sweep leaves it unchanged so retries
    // happen naturally on the next tick.
    last_gc_position: u64,
}

impl WalGcHandler {
    pub fn new(
        object_store: Arc<ObjectStore>,
        wal_dir: Path,
        interval: Duration,
        replay_after_rx: watch::Receiver<u64>,
    ) -> Self {
        Self {
            object_store,
            wal_dir,
            interval,
            replay_after_rx,
            last_gc_position: 0,
        }
    }

    #[instrument(name = "wal_gc", level = "debug", skip_all)]
    async fn collect(&mut self) -> Result<usize> {
        let gc_before = *self.replay_after_rx.borrow();
        if gc_before == 0 || gc_before <= self.last_gc_position {
            return Ok(0);
        }

        let mut saw_error = false;

        // Stream eligible WAL entry paths into the batch-delete API so object
        // stores that support multi-key deletes (e.g. S3's up-to-1000-per-request
        // bulk delete) aren't reduced to one round-trip per file.
        //
        // Bit-reversed filenames mean the list order is not monotonic in
        // position, so we can't terminate the scan early — but with the
        // watermark cache above we only list when new entries have become
        // eligible since the last successful sweep.
        let paths = self
            .object_store
            .inner
            .list(Some(&self.wal_dir))
            .filter_map(move |item| async move {
                match item {
                    Ok(meta) => {
                        let filename = meta.location.filename()?;
                        if !filename.ends_with(".arrow") {
                            return None;
                        }
                        let position = parse_bit_reversed_filename(filename)?;
                        (position <= gc_before).then_some(Ok(meta.location))
                    }
                    Err(e) => {
                        warn!("Error listing WAL directory: {}", e);
                        None
                    }
                }
            })
            .boxed();

        let mut delete_stream = self.object_store.remove_stream(paths);
        let mut deleted = 0usize;
        while let Some(result) = delete_stream.next().await {
            match result {
                Ok(_) => deleted += 1,
                Err(e) => {
                    saw_error = true;
                    warn!("Failed to delete WAL entry: {}", e);
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

        // Only advance the watermark when the sweep completed cleanly, so any
        // entry we missed gets retried on the next tick.
        if !saw_error {
            self.last_gc_position = gc_before;
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

    async fn write_wal_entries(
        store: &Arc<ObjectStore>,
        base_path: &Path,
        shard_id: Uuid,
        count: usize,
    ) {
        let schema = create_test_schema();
        let mut flusher = WalFlusher::new(base_path, shard_id, 1, 1);
        flusher.set_object_store(store.clone());

        let batch_store = BatchStore::with_capacity(10);
        for _ in 0..count {
            batch_store.append(create_test_batch(&schema, 5)).unwrap();
        }

        for i in 0..count {
            let _ = flusher.track_batch(i);
            flusher
                .flush_to_with_index_update(&batch_store, i + 1, None)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_wal_gc_deletes_flushed_entries() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, base_path) = ObjectStore::from_uri(&uri).await.unwrap();
        let shard_id = Uuid::new_v4();

        write_wal_entries(&store, &base_path, shard_id, 3).await;

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

        // Positions 1 and 2 are safe to delete; position 3 must remain.
        let (_tx, rx) = watch::channel(2u64);

        let mut handler =
            WalGcHandler::new(store.clone(), wal_dir.clone(), Duration::from_secs(60), rx);
        let deleted = handler.collect().await.unwrap();
        assert_eq!(deleted, 2);

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
    async fn test_wal_gc_initial_watermark_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, base_path) = ObjectStore::from_uri(&uri).await.unwrap();
        let shard_id = Uuid::new_v4();

        let (_tx, rx) = watch::channel(0u64);
        let wal_dir = shard_wal_path(&base_path, &shard_id);
        let mut handler = WalGcHandler::new(store, wal_dir, Duration::from_secs(60), rx);

        // With replay_after == 0 nothing has been flushed; GC must no-op.
        let deleted = handler.collect().await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_wal_gc_skips_unchanged_watermark() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, base_path) = ObjectStore::from_uri(&uri).await.unwrap();
        let shard_id = Uuid::new_v4();

        write_wal_entries(&store, &base_path, shard_id, 2).await;

        let wal_dir = shard_wal_path(&base_path, &shard_id);
        let (tx, rx) = watch::channel(2u64);
        let mut handler =
            WalGcHandler::new(store.clone(), wal_dir.clone(), Duration::from_secs(60), rx);

        // First sweep deletes both entries.
        assert_eq!(handler.collect().await.unwrap(), 2);
        assert_eq!(handler.last_gc_position, 2);

        // Re-listing would surface no candidates, but we shouldn't even bother:
        // write another entry and confirm it survives a tick that hasn't seen
        // the watermark advance.
        write_wal_entries(&store, &base_path, shard_id, 1).await;
        assert_eq!(handler.collect().await.unwrap(), 0);
        let remaining: Vec<_> = store
            .inner
            .list(Some(&wal_dir))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(remaining.len(), 1);

        // Once the watermark advances, the new entry is swept.
        tx.send_replace(3);
        assert_eq!(handler.collect().await.unwrap(), 1);
        assert_eq!(handler.last_gc_position, 3);
    }
}
