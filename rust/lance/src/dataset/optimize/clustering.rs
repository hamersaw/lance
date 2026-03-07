// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Clustering rewrite strategy for compaction.
//!
//! Sorts data by one or more columns during compaction to improve query
//! locality. Three ordering modes are supported:
//!
//! * **Sort** – plain multi-column sort (ORDER BY col1, col2, …).
//! * **ZOrder** – bit-interleaved z-order curve (TODO: Phase 2).
//! * **Hilbert** – Hilbert curve ordering (TODO: Phase 3).
//!
//! The implementation follows the same DataFusion-based pattern used by
//! `lance-index`'s Hilbert sorter: wrap the input stream in [`OneShotExec`],
//! optionally add a computed column, sort via [`SortExec`], and execute with
//! [`execute_plan`].

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_physical_expr::expressions::Column as DFColumn;
use lance_core::{Error, Result};
use lance_datafusion::exec::{LanceExecutionOptions, OneShotExec, execute_plan};

use super::rewrite_strategy::{ClusteringMode, RewriteStrategy};

/// Rewrite strategy that clusters data by the given columns.
pub struct ClusteringRewriteStrategy {
    columns: Vec<String>,
    mode: ClusteringMode,
}

impl ClusteringRewriteStrategy {
    pub fn new(columns: Vec<String>, mode: ClusteringMode) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::invalid_input(
                "clustering requires at least one column",
            ));
        }
        Ok(Self { columns, mode })
    }
}

#[async_trait]
impl RewriteStrategy for ClusteringRewriteStrategy {
    async fn transform(
        &self,
        data: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        match self.mode {
            ClusteringMode::Sort => self.sort_transform(data),
            ClusteringMode::ZOrder => Err(Error::not_supported(
                "z-order clustering is not yet implemented",
            )),
            ClusteringMode::Hilbert => Err(Error::not_supported(
                "hilbert clustering is not yet implemented",
            )),
        }
    }
}

impl ClusteringRewriteStrategy {
    /// Plain multi-column sort using DataFusion's SortExec.
    fn sort_transform(&self, data: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let schema = data.schema();
        let source = Arc::new(OneShotExec::new(data));

        let sort_exprs: Vec<PhysicalSortExpr> = self
            .columns
            .iter()
            .map(|col_name| {
                let idx = schema.index_of(col_name).map_err(|_| {
                    Error::invalid_input(format!(
                        "clustering column \"{}\" not found in schema: {:?}",
                        col_name,
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                    ))
                })?;
                Ok(PhysicalSortExpr {
                    expr: Arc::new(DFColumn::new(col_name, idx)),
                    options: arrow_schema::SortOptions::default(),
                })
            })
            .collect::<Result<_>>()?;

        let lex_ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
            Error::invalid_input("clustering columns produced an empty sort ordering")
        })?;
        let sort_exec = Arc::new(SortExec::new(
            lex_ordering,
            source as Arc<dyn ExecutionPlan>,
        ));

        let sorted_stream = execute_plan(
            sort_exec,
            LanceExecutionOptions {
                use_spilling: true,
                ..Default::default()
            },
        )?;

        Ok(sorted_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::common::collect;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sort_single_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 4, 1, 5])),
                Arc::new(StringArray::from(vec!["c", "a", "d", "b", "e"])),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));

        let strategy =
            ClusteringRewriteStrategy::new(vec!["id".to_string()], ClusteringMode::Sort).unwrap();
        let sorted = strategy.transform(stream).await.unwrap();
        let batches = collect(sorted).await.unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![1, 1, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_sort_multi_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 2, 1])),
                Arc::new(Int32Array::from(vec![2, 2, 1, 1])),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));

        let strategy = ClusteringRewriteStrategy::new(
            vec!["a".to_string(), "b".to_string()],
            ClusteringMode::Sort,
        )
        .unwrap();
        let sorted = strategy.transform(stream).await.unwrap();
        let batches = collect(sorted).await.unwrap();

        let a_vals: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        let b_vals: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(a_vals, vec![1, 1, 2, 2]);
        assert_eq!(b_vals, vec![1, 2, 1, 2]);
    }

    #[tokio::test]
    async fn test_empty_columns_rejected() {
        let result = ClusteringRewriteStrategy::new(vec![], ClusteringMode::Sort);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_column_name() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));

        let strategy =
            ClusteringRewriteStrategy::new(vec!["nonexistent".to_string()], ClusteringMode::Sort)
                .unwrap();
        let result = strategy.transform(stream).await;
        assert!(result.is_err());
    }
}
