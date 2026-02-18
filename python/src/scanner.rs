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
use lance::dataset::scanner::{
    ExecutionSummaryCounts, Split, SplittingOptions as LanceSplittingOptions,
};
use lance::io::exec::filtered_read::FilteredReadExec;

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
    ) -> PyResult<Vec<PySplit>> {
        let scanner = self_.scanner.clone();
        let options = if max_size_bytes.is_some() || max_row_count.is_some() {
            Some(LanceSplittingOptions {
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

        Ok(splits.into_iter().map(PySplit::from).collect())
    }

    fn with_filtered_read_exec(&self, exec: PyFilteredReadExec) -> Scanner {
        let mut scanner = (*self.scanner).clone();
        scanner.with_filtered_read_exec(exec.inner);
        Scanner::new(Arc::new(scanner))
    }
}

/// A single unit of work from [`Scanner::plan_splits`] for distributed execution.
#[pyclass(name = "Split", module = "_lib")]
#[derive(Clone)]
pub struct PySplit {
    /// The filtered read exec for this split.
    #[pyo3(get)]
    pub filtered_read_exec: PyFilteredReadExec,
    /// Ordered output column names from the original scan, including metadata
    /// columns like `_rowid`.
    #[pyo3(get)]
    pub output_columns: Vec<String>,
}

impl From<Split> for PySplit {
    fn from(split: Split) -> Self {
        Self {
            filtered_read_exec: PyFilteredReadExec { inner: split.exec },
            output_columns: split.output_columns,
        }
    }
}

#[pymethods]
impl PySplit {
    fn __repr__(&self) -> String {
        "Split(filtered_read_exec=<FilteredReadExec>)".to_string()
    }

    /// Serialize this split to bytes.
    ///
    /// Returns a bytes object that can be sent over the wire and later
    /// deserialized with :meth:`Split.from_bytes`.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let split = lance::dataset::split::Split {
            exec: self.filtered_read_exec.inner.clone(),
            output_columns: self.output_columns.clone(),
        };
        let bytes = split
            .to_bytes()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(pyo3::types::PyBytes::new(py, &bytes))
    }

    /// Deserialize a split from bytes.
    ///
    /// Parameters
    /// ----------
    /// data : bytes
    ///     The serialized split bytes produced by :meth:`to_bytes`.
    /// dataset : Dataset
    ///     The dataset that the split belongs to (the native ``_Dataset``).
    #[staticmethod]
    fn from_bytes(
        py: Python<'_>,
        data: &[u8],
        dataset: &crate::dataset::Dataset,
    ) -> PyResult<Self> {
        let ds = dataset.ds.clone();
        let bytes = data.to_vec();
        let split = rt()
            .spawn(Some(py), async move {
                lance::dataset::split::Split::from_bytes(&bytes, &ds).await
            })?
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PySplit::from(split))
    }
}

/// An opaque wrapper around a Rust [`FilteredReadExec`].
///
/// Created by :meth:`Scanner.plan_splits` and consumed by
/// :meth:`Scanner.with_filtered_read_exec`.
#[pyclass(name = "FilteredReadExec", module = "_lib")]
#[derive(Clone)]
pub struct PyFilteredReadExec {
    pub(crate) inner: Arc<FilteredReadExec>,
}

#[pymethods]
impl PyFilteredReadExec {
    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}
