// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::pyarrow::*;
use arrow_array::RecordBatchReader;
use lance::dataset::scanner::{ExecutionSummaryCounts, SplitOptions as LanceSplitOptions, Splits};
use lance::io::exec::filtered_read::FilteredReadPlan;
use pyo3::prelude::*;
use pyo3::pyclass;

use ::lance::dataset::scanner::Scanner as LanceScanner;
use pyo3::exceptions::PyValueError;

use crate::reader::LanceReader;
use crate::rt;
use crate::schema::logical_arrow_schema;

/// This will be wrapped by a python class to provide
/// additional functionality
#[pyclass(name = "_Scanner", module = "_lib")]
#[derive(Clone)]
pub struct Scanner {
    scanner: Arc<LanceScanner>,
}

impl Scanner {
    pub fn new(scanner: Arc<LanceScanner>) -> Self {
        Self { scanner }
    }

    pub(crate) async fn to_reader(&self) -> ::lance::Result<LanceReader> {
        LanceReader::try_new(self.scanner.clone()).await
    }
}

#[pyclass(name = "ScanStatistics", module = "_lib", get_all)]
#[derive(Clone)]
/// Statistics about the scan.
pub struct ScanStatistics {
    /// Number of IO operations performed.  This may be slightly higher than
    /// the actual number due to coalesced I/O
    pub iops: usize,
    /// Number of requests made to the storage layer
    pub requests: usize,
    /// Number of bytes read from disk
    pub bytes_read: usize,
    /// Number of indices loaded
    pub indices_loaded: usize,
    /// Number of index partitions loaded
    pub parts_loaded: usize,
    /// Number of index comparisons performed
    pub index_comparisons: usize,
    /// Additional metrics for more detailed statistics. These are subject to change in the future
    /// and should only be used for debugging purposes.
    pub all_counts: HashMap<String, usize>,
}

impl ScanStatistics {
    pub fn from_lance(stats: &ExecutionSummaryCounts) -> Self {
        Self {
            iops: stats.iops,
            requests: stats.requests,
            bytes_read: stats.bytes_read,
            indices_loaded: stats.indices_loaded,
            parts_loaded: stats.parts_loaded,
            index_comparisons: stats.index_comparisons,
            all_counts: stats.all_counts.clone(),
        }
    }
}

#[pymethods]
impl ScanStatistics {
    fn __repr__(&self) -> String {
        format!(
            "ScanStatistics(iops={}, requests={}, bytes_read={}, indices_loaded={}, parts_loaded={}, index_comparisons={}, all_counts={:?})",
            self.iops, self.requests, self.bytes_read, self.indices_loaded, self.parts_loaded, self.index_comparisons, self.all_counts
        )
    }
}

#[pymethods]
impl Scanner {
    #[getter(schema)]
    fn schema<'py>(self_: PyRef<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self_.scanner.clone();
        let schema = rt()
            .spawn(Some(self_.py()), async move { scanner.schema().await })?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let logical_schema = logical_arrow_schema(schema.as_ref());
        logical_schema.to_pyarrow(self_.py())
    }

    #[pyo3(signature = (*, verbose = false))]
    fn explain_plan(self_: PyRef<'_, Self>, verbose: bool) -> PyResult<String> {
        let scanner = self_.scanner.clone();
        let res = rt()
            .spawn(Some(self_.py()), async move {
                scanner.explain_plan(verbose).await
            })?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(res)
    }

    #[pyo3(signature = (*))]
    fn analyze_plan(self_: PyRef<'_, Self>) -> PyResult<String> {
        let scanner = self_.scanner.clone();
        let res = rt()
            .spawn(
                Some(self_.py()),
                async move { scanner.analyze_plan().await },
            )?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(res)
    }

    fn count_rows(self_: PyRef<'_, Self>) -> PyResult<u64> {
        let scanner = self_.scanner.clone();
        rt().spawn(Some(self_.py()), async move { scanner.count_rows().await })?
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    fn to_pyarrow(
        self_: PyRef<'_, Self>,
    ) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let scanner = self_.scanner.clone();
        let reader = rt()
            .spawn(Some(self_.py()), async move {
                LanceReader::try_new(scanner).await
            })?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(PyArrowType(Box::new(reader)))
    }

    #[pyo3(signature = (*, max_size_bytes = None, max_row_count = None))]
    fn plan_splits(
        self_: PyRef<'_, Self>,
        max_size_bytes: Option<usize>,
        max_row_count: Option<usize>,
    ) -> PyResult<PySplits> {
        let scanner = self_.scanner.clone();
        let options = if max_size_bytes.is_some() || max_row_count.is_some() {
            Some(LanceSplitOptions {
                max_size_bytes,
                max_row_count,
            })
        } else {
            None
        };

        let splits = rt()
            .spawn(Some(self_.py()), async move {
                scanner.plan_splits(options).await
            })?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(PySplits::from(splits))
    }

    fn execute_filtered_read_plan(
        self_: PyRef<'_, Self>,
        split: PyFilteredReadPlan,
    ) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let scanner = self_.scanner.clone();
        let inner = split.inner;
        let stream = rt()
            .spawn(Some(self_.py()), async move {
                scanner.execute_filtered_read_plan(inner).await
            })?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(PyArrowType(Box::new(LanceReader::from_stream(stream))))
    }
}

/// Result of [`Scanner::plan_splits`], representing how to divide a scan
/// for distributed execution.
#[pyclass(name = "Splits", module = "_lib")]
#[derive(Clone)]
pub struct PySplits {
    /// The filtered read plans, if this is a `FilteredReadPlans` variant.
    #[pyo3(get)]
    pub filtered_read_plans: Option<Vec<PyFilteredReadPlan>>,
    /// The fragment IDs, if this is a `Fragments` variant.
    #[pyo3(get)]
    pub fragments: Option<Vec<u32>>,
}

impl From<Splits> for PySplits {
    fn from(splits: Splits) -> Self {
        match splits {
            Splits::FilteredReadPlans(plans) => Self {
                filtered_read_plans: Some(
                    plans.into_iter().map(PyFilteredReadPlan::from).collect(),
                ),
                fragments: None,
            },
            Splits::Fragments(fragment_ids) => Self {
                filtered_read_plans: None,
                fragments: Some(fragment_ids),
            },
        }
    }
}

#[pymethods]
impl PySplits {
    fn __repr__(&self) -> String {
        if let Some(plans) = &self.filtered_read_plans {
            format!("Splits(filtered_read_plans=[{} plans])", plans.len())
        } else if let Some(fragments) = &self.fragments {
            format!("Splits(fragments={:?})", fragments)
        } else {
            "Splits()".to_string()
        }
    }
}

/// A filtered read plan represents a unit of work for scanning a dataset.
/// It specifies which fragment row ranges to read and any residual filters to apply.
#[pyclass(name = "FilteredReadPlan", module = "_lib")]
#[derive(Clone)]
pub struct PyFilteredReadPlan {
    /// Per-fragment row ranges as `(fragment_id, [(start, end), ...])` pairs.
    #[pyo3(get)]
    pub fragment_ranges: Vec<(u32, Vec<(u64, u64)>)>,
    /// Row offset range to apply after filtering (skip N rows, take M rows).
    #[pyo3(get)]
    pub scan_range_after_filter: Option<(u64, u64)>,
    /// The inner Rust FilteredReadPlan, retained for execute_filtered_read_plan.
    pub(crate) inner: FilteredReadPlan,
}

impl From<FilteredReadPlan> for PyFilteredReadPlan {
    fn from(plan: FilteredReadPlan) -> Self {
        let fragment_ranges = plan
            .rows
            .iter()
            .map(|(frag_id, ranges)| {
                let py_ranges: Vec<(u64, u64)> = ranges.iter().map(|r| (r.start, r.end)).collect();
                (*frag_id, py_ranges)
            })
            .collect();
        let scan_range_after_filter = plan
            .scan_range_after_filter
            .as_ref()
            .map(|r| (r.start, r.end));
        Self {
            fragment_ranges,
            scan_range_after_filter,
            inner: plan,
        }
    }
}

#[pymethods]
impl PyFilteredReadPlan {
    fn __repr__(&self) -> String {
        let fragments_str = self
            .fragment_ranges
            .iter()
            .map(|(frag_id, ranges)| {
                let row_count: u64 = ranges.iter().map(|(s, e)| e - s).sum();
                format!(
                    "(fragment_id={}, ranges={}, rows={})",
                    frag_id,
                    ranges.len(),
                    row_count
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "FilteredReadPlan(fragments=[{}], scan_range_after_filter={:?})",
            fragments_str, self.scan_range_after_filter,
        )
    }
}
