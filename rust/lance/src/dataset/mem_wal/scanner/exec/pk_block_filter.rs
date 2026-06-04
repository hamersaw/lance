// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Drop superseded rows from a per-source result by primary-key membership.
//!
//! Drops a row when any newer generation's membership ([`GenMembership`])
//! contains its primary key — in-memory generations probe their PK index by
//! value, flushed generations probe their on-disk PK BTree with `Equals`. Used
//! both as the KNN post-filter (vector search, with over-fetch) and the
//! cross-generation scan filter (`k = 0`).
//!
//! Cross-generation only: within-gen duplicates collapse via the global dedup's
//! `(generation, freshness)` tiebreaker.
//!
//! Post-filters an over-fetched KNN (the planner's `overfetch_factor`); warns
//! when a source had >= k candidates but < k survived (over-fetch too small).
//!
//! Perf note: the on-disk probe is one `Equals` per row per flushed generation.
//! It is not disk-bound in steady state — the opened index and its (small,
//! memtable-sized) pages are held by the injected `FlushedMemTableCache` /
//! `LanceCache`, so after the first touch every probe is memory-resident. The
//! residual per-row cost is the in-memory BTree search plus a `SearchResult`
//! allocation per lookup; a batched/existence-only membership probe (avoiding
//! the per-row allocation) is the future optimization.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use tracing::warn;

use super::super::block_list::{GenMembership, on_disk_pk_key};
use super::pk::resolve_pk_indices;

/// Filters out rows whose PK is contained in any newer generation's membership.
#[derive(Debug)]
pub struct PkBlockFilterExec {
    input: Arc<dyn ExecutionPlan>,
    pk_columns: Vec<String>,
    /// Newer generations' membership; a row is blocked if any contains its PK.
    blocked: Vec<GenMembership>,
    /// Target neighbor count, used only to warn on a per-source under-fetch.
    k: usize,
    properties: Arc<PlanProperties>,
}

impl PkBlockFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        pk_columns: Vec<String>,
        blocked: Vec<GenMembership>,
        k: usize,
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
            blocked,
            k,
            properties,
        }
    }
}

impl DisplayAs for PkBlockFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PkBlockFilterExec: pk_cols=[{}], gens={}",
                    self.pk_columns.join(", "),
                    self.blocked.len(),
                )
            }
        }
    }
}

impl ExecutionPlan for PkBlockFilterExec {
    fn name(&self) -> &str {
        "PkBlockFilterExec"
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
                "PkBlockFilterExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.pk_columns.clone(),
            self.blocked.clone(),
            self.k,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(PkBlockFilterStream {
            input: input_stream,
            pk_columns: self.pk_columns.clone(),
            blocked: self.blocked.clone(),
            k: self.k,
            schema: self.schema(),
            pending: None,
            input_seen: 0,
            kept: 0,
            warned: false,
        }))
    }
}

struct PkBlockFilterStream {
    input: SendableRecordBatchStream,
    pk_columns: Vec<String>,
    blocked: Vec<GenMembership>,
    k: usize,
    schema: SchemaRef,
    /// The in-flight filter for the batch currently being processed (the probe
    /// is async, so a batch is filtered off-poll and resumed here).
    pending: Option<BoxFuture<'static, DFResult<RecordBatch>>>,
    input_seen: usize,
    kept: usize,
    warned: bool,
}

/// Keep only the rows no newer-gen membership contains. Async because flushed
/// generations are probed against their on-disk PK BTree.
async fn filter_batch(
    batch: RecordBatch,
    pk_columns: Vec<String>,
    blocked: Vec<GenMembership>,
) -> DFResult<RecordBatch> {
    if blocked.is_empty() || batch.num_rows() == 0 {
        return Ok(batch);
    }
    let pk_indices = resolve_pk_indices(&batch, &pk_columns)?;
    let to_df = |e: lance_core::Error| DataFusionError::Execution(e.to_string());

    let mut keep = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values: Vec<ScalarValue> = pk_indices
            .iter()
            .map(|&col| ScalarValue::try_from_array(batch.column(col), row))
            .collect::<DFResult<_>>()?;
        let key = on_disk_pk_key(&values).map_err(to_df)?;
        let mut blocked_row = false;
        for membership in &blocked {
            if membership.contains(&key).await.map_err(to_df)? {
                blocked_row = true;
                break;
            }
        }
        keep.push(!blocked_row);
    }
    filter_record_batch(&batch, &BooleanArray::from(keep))
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

impl Stream for PkBlockFilterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // Drive an in-flight filter to completion before pulling more input.
            if let Some(fut) = this.pending.as_mut() {
                return match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(out)) => {
                        this.pending = None;
                        this.kept += out.num_rows();
                        Poll::Ready(Some(Ok(out)))
                    }
                    Poll::Ready(Err(e)) => {
                        this.pending = None;
                        Poll::Ready(Some(Err(e)))
                    }
                    Poll::Pending => Poll::Pending,
                };
            }

            match this.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    this.input_seen += batch.num_rows();
                    this.pending = Some(
                        filter_batch(batch, this.pk_columns.clone(), this.blocked.clone()).boxed(),
                    );
                    // Loop to poll the just-created future.
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // >= k candidates in, < k out: over-fetch missed superseded rows.
                    if !this.warned && this.input_seen >= this.k && this.kept < this.k {
                        warn!(
                            k = this.k,
                            fetched = this.input_seen,
                            kept = this.kept,
                            "LSM vector search: < k live rows survived the PK post-filter; \
                             raise the over-fetch factor or use a true KNN prefilter."
                        );
                        this.warned = true;
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for PkBlockFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;

    fn int_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    /// An in-memory membership whose PK index holds `ids` (positions 0..n).
    fn membership(ids: &[i32]) -> GenMembership {
        let store = BatchStore::with_capacity(16);
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        for &id in ids {
            let b = int_batch(&[id]);
            let (bp, off, _) = store.append(b.clone()).unwrap();
            index.insert_with_batch_position(&b, off, Some(bp)).unwrap();
        }
        let max_visible_row = store.max_visible_row(index.max_visible_batch_position());
        GenMembership::InMemory {
            index_store: Arc::new(index),
            max_visible_row,
        }
    }

    async fn run(exec: PkBlockFilterExec) -> Vec<i32> {
        let ctx = SessionContext::new();
        let out: Vec<RecordBatch> = exec
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        out.iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn drops_rows_blocked_by_a_newer_generation() {
        let b = int_batch(&[10, 20, 30]);
        let input = TestMemoryExec::try_new_exec(&[vec![b.clone()]], b.schema(), None).unwrap();
        let exec =
            PkBlockFilterExec::new(input, vec!["id".to_string()], vec![membership(&[20])], 1);
        assert_eq!(run(exec).await, vec![10, 30]);
    }

    #[tokio::test]
    async fn blocks_a_pk_present_in_any_generation() {
        // Two newer-gen memberships: a row is dropped if either contains its PK.
        let b = int_batch(&[10, 20, 30]);
        let blocked = vec![membership(&[10]), membership(&[30])];
        let input = TestMemoryExec::try_new_exec(&[vec![b.clone()]], b.schema(), None).unwrap();
        let exec = PkBlockFilterExec::new(input, vec!["id".to_string()], blocked, 1);
        assert_eq!(run(exec).await, vec![20]);
    }

    #[tokio::test]
    async fn empty_blocked_keeps_all_rows() {
        let b = int_batch(&[1, 2, 3]);
        let input = TestMemoryExec::try_new_exec(&[vec![b.clone()]], b.schema(), None).unwrap();
        let exec = PkBlockFilterExec::new(input, vec!["id".to_string()], Vec::new(), 1);
        assert_eq!(run(exec).await, vec![1, 2, 3]);
    }
}
