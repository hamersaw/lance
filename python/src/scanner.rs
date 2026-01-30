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
use std::ops::Range;
use std::sync::Arc;

use arrow::pyarrow::*;
use arrow_array::RecordBatchReader;
use lance::dataset::scanner::{
    ExecutionSummaryCounts, FragmentSplit as LanceFragmentSplit, SplitOptions as LanceSplitOptions,
    Splits as LanceSplits,
};
use lance_core::utils::mask::RowAddrSelection;
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
    ) -> PyResult<Vec<PySplits>> {
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

        Ok(splits.into_iter().map(PySplits::from).collect())
    }
}

/// A split represents a unit of work for scanning a dataset.
/// It contains fragment splits and residual filters that need to be applied.
#[pyclass(name = "Splits", module = "_lib", get_all)]
#[derive(Clone)]
pub struct PySplits {
    /// The fragment splits in this split.
    pub fragment_splits: Vec<PyFragmentSplit>,
    /// Row offset range to apply after filtering (skip N rows, take M rows).
    pub scan_range_after_filter: Option<(u64, u64)>,
}

impl From<LanceSplits> for PySplits {
    fn from(splits: LanceSplits) -> Self {
        Self {
            fragment_splits: splits
                .fragment_splits
                .into_iter()
                .map(PyFragmentSplit::from)
                .collect(),
            scan_range_after_filter: splits
                .scan_range_after_filter
                .map(|r: Range<u64>| (r.start, r.end)),
        }
    }
}

#[pymethods]
impl PySplits {
    fn __repr__(&self) -> String {
        format!(
            "Splits(fragment_splits=[{}], scan_range_after_filter={:?})",
            self.fragment_splits
                .iter()
                .map(|fs| fs.__repr__())
                .collect::<Vec<_>>()
                .join(", "),
            self.scan_range_after_filter,
        )
    }
}

/// A fragment split represents a portion of a fragment to scan.
#[pyclass(name = "FragmentSplit", module = "_lib", get_all)]
#[derive(Clone)]
pub struct PyFragmentSplit {
    /// The fragment ID.
    pub fragment_id: u32,
    /// Row offsets to read. None means read all rows (Full selection).
    pub row_offsets: Option<Vec<u32>>,
    /// The number of rows in this split.
    pub row_count: u32,
}

impl From<LanceFragmentSplit> for PyFragmentSplit {
    fn from(split: LanceFragmentSplit) -> Self {
        let row_offsets = match split.selection {
            RowAddrSelection::Full => None,
            RowAddrSelection::Partial(bitmap) => Some(bitmap.iter().collect()),
        };
        Self {
            fragment_id: split.fragment_id,
            row_offsets,
            row_count: split.row_count,
        }
    }
}

#[pymethods]
impl PyFragmentSplit {
    fn __repr__(&self) -> String {
        let selection_str = match &self.row_offsets {
            None => "Full".to_string(),
            Some(offsets) => format!("Partial({} rows)", offsets.len()),
        };
        format!(
            "FragmentSplit(fragment_id={}, selection={}, row_count={})",
            self.fragment_id, selection_str, self.row_count
        )
    }
}
