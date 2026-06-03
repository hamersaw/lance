// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Drop predicate-crossing stale rows from an active-memtable index search.
//!
//! The active memtable's HNSW / inverted index are append-only, so an updated
//! row's old entries stay live. When an update moves a row out of the query's
//! match set, the fresh version isn't in the index result, so a result-set
//! dedup (keep-newest among the returned rows) has nothing to suppress the
//! stale version against — and it leaks.
//!
//! This node closes that hole with a predicate-independent recency check: for
//! each hit it asks the memtable's maintained MVCC PK-position index
//! ([`crate::dataset::mem_wal::index::PkPositionIndex`]) for the newest position
//! of that hit's primary key visible at the query's `max_visible` watermark, and
//! keeps the hit **iff that equals the hit's own row position**. A stale hit
//! (some newer version exists) is dropped even when that newer version never
//! appears in the result. This is exactly the seek point-lookup already does;
//! the index search arms simply didn't do it.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::filter_record_batch;
use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use super::pk::{compute_pk_hash, resolve_pk_indices};
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// Keeps only the index hits that are the newest visible version of their PK.
///
/// The input must expose all `pk_columns` and the `row_id_column` (`UInt64`,
/// the BatchStore row position). The output schema is unchanged.
pub struct NewestPkFilterExec {
    input: Arc<dyn ExecutionPlan>,
    pk_columns: Vec<String>,
    row_id_column: String,
    /// Holds the maintained `PkPositionIndex` queried per hit.
    index_store: Arc<IndexStore>,
    /// Resolves the `max_visible` row watermark from the visible batch prefix.
    batch_store: Arc<BatchStore>,
    /// The MVCC batch-position snapshot the index search latched. Captured once
    /// at plan time and shared with the search so the recency check keys on the
    /// same snapshot the hits came from.
    max_visible_batch_position: usize,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for NewestPkFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `BatchStore` / `IndexStore` aren't `Debug`; show only the knobs.
        f.debug_struct("NewestPkFilterExec")
            .field("pk_columns", &self.pk_columns)
            .field("row_id_column", &self.row_id_column)
            .field(
                "max_visible_batch_position",
                &self.max_visible_batch_position,
            )
            .finish()
    }
}

impl NewestPkFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        pk_columns: Vec<String>,
        row_id_column: impl Into<String>,
        index_store: Arc<IndexStore>,
        batch_store: Arc<BatchStore>,
        max_visible_batch_position: usize,
    ) -> Self {
        // A filter preserves the input schema and partitioning.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            pk_columns,
            row_id_column: row_id_column.into(),
            index_store,
            batch_store,
            max_visible_batch_position,
            properties,
        }
    }

    /// The inclusive max visible row position for this snapshot, or `None` when
    /// no rows are visible.
    fn max_visible_row(&self) -> Option<u64> {
        self.batch_store
            .max_visible_row(self.max_visible_batch_position)
    }
}

impl DisplayAs for NewestPkFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "NewestPkFilterExec: pk=[{}], row_id={}, max_visible_batch={}",
                    self.pk_columns.join(", "),
                    self.row_id_column,
                    self.max_visible_batch_position,
                )
            }
        }
    }
}

impl ExecutionPlan for NewestPkFilterExec {
    fn name(&self) -> &str {
        "NewestPkFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "NewestPkFilterExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.pk_columns.clone(),
            self.row_id_column.clone(),
            self.index_store.clone(),
            self.batch_store.clone(),
            self.max_visible_batch_position,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(NewestPkFilterStream {
            input: input_stream,
            pk_columns: self.pk_columns.clone(),
            row_id_column: self.row_id_column.clone(),
            index_store: self.index_store.clone(),
            max_visible_row: self.max_visible_row(),
            schema: self.schema(),
        }))
    }
}

struct NewestPkFilterStream {
    input: SendableRecordBatchStream,
    pk_columns: Vec<String>,
    row_id_column: String,
    index_store: Arc<IndexStore>,
    /// Inclusive watermark snapshot; `None` when no rows are visible.
    max_visible_row: Option<u64>,
    schema: SchemaRef,
}

impl NewestPkFilterStream {
    fn filter_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        // No PK-position index (memtable without a primary key), no visible
        // rows, or an empty batch: nothing to dedup against, so pass it through.
        let Some(pk_index) = self.index_store.pk_position_index() else {
            return Ok(batch);
        };
        let Some(max_visible_row) = self.max_visible_row else {
            return Ok(batch);
        };
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        let pk_indices = resolve_pk_indices(&batch, &self.pk_columns)?;
        let row_ids = batch
            .column_by_name(&self.row_id_column)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Row-id column '{}' not found in NewestPkFilterExec input",
                    self.row_id_column
                ))
            })?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Row-id column '{}' is not UInt64",
                    self.row_id_column
                ))
            })?;

        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row| {
                // A null row position can't be ordered; keep it rather than
                // guess (callers always project a real position here).
                if row_ids.is_null(row) {
                    return true;
                }
                let position = row_ids.value(row);
                let hash = compute_pk_hash(&batch, &pk_indices, row);
                // Keep iff this hit is the newest visible version of its PK.
                pk_index.get_newest_visible(hash, max_visible_row) == Some(position)
            })
            .collect();
        filter_record_batch(&batch, &keep)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for NewestPkFilterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.filter_batch(batch))),
            other => other,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for NewestPkFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
