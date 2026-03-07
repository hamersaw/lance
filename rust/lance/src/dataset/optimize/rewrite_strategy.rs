// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use lance_core::Result;
use serde::{Deserialize, Serialize};

/// Controls how data is reorganized during compaction rewrites.
///
/// A rewrite strategy transforms the data stream between reading fragments
/// and writing new fragments. The default strategy passes data through
/// unchanged (preserving insertion order). A clustering strategy sorts or
/// reorders data for improved query locality.
#[async_trait]
pub trait RewriteStrategy: Send + Sync {
    /// Transform the data stream before writing.
    ///
    /// The input stream contains all rows from the fragments being rewritten
    /// (after deletion filtering). The output stream will be written to new
    /// fragments.
    async fn transform(&self, data: SendableRecordBatchStream)
    -> Result<SendableRecordBatchStream>;
}

/// Serializable specification of a rewrite strategy.
///
/// This enum is stored in [`super::CompactionOptions`] so that it can be
/// serialized alongside [`super::CompactionTask`] for distributed execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum RewriteStrategyConfig {
    /// Pass data through unchanged (default / current behavior).
    #[default]
    Default,
    /// Cluster data by the specified columns using the given ordering mode.
    Clustering {
        columns: Vec<String>,
        mode: ClusteringMode,
    },
}

/// Algorithm used to order data during clustering.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ClusteringMode {
    /// Sort rows by the given columns in order (like ORDER BY col1, col2, …).
    Sort,
    /// Z-order (bit-interleave) across columns for multi-dimensional locality.
    ZOrder,
    /// Hilbert curve ordering across columns for multi-dimensional locality.
    Hilbert,
}

impl RewriteStrategyConfig {
    /// Instantiate the concrete [`RewriteStrategy`] described by this config.
    pub fn to_strategy(&self) -> Result<Box<dyn RewriteStrategy>> {
        match self {
            Self::Default => Ok(Box::new(DefaultRewriteStrategy)),
            Self::Clustering { columns, mode } => {
                let strategy =
                    super::clustering::ClusteringRewriteStrategy::new(columns.clone(), *mode)?;
                Ok(Box::new(strategy))
            }
        }
    }

    /// Returns true if this is the default (passthrough) strategy.
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }
}

/// The default rewrite strategy: passes data through unchanged.
pub struct DefaultRewriteStrategy;

#[async_trait]
impl RewriteStrategy for DefaultRewriteStrategy {
    async fn transform(
        &self,
        data: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        Ok(data)
    }
}
