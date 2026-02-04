/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.ipc;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A filtered read plan represents a unit of work for scanning a dataset.
 *
 * <p>It specifies which fragment row ranges to read and any residual filters to apply. Plans are
 * created by {@link LanceScanner#planSplits} and executed by {@link
 * LanceScanner#executeFilteredReadPlan}.
 *
 * <p>This class holds a native handle to a Rust {@code FilteredReadPlan} object and must be closed
 * when no longer needed.
 */
public class FilteredReadPlan implements AutoCloseable {
  private long nativeHandle;

  /**
   * Per-fragment row ranges: maps fragment ID to a list of [start, end) ranges.
   *
   * <p>Each inner long array has exactly two elements: {@code [start, end)}.
   */
  private Map<Integer, List<long[]>> fragmentRanges;

  /**
   * Row offset range to apply after filtering, or null if not needed.
   *
   * <p>The array has two elements: {@code [start, end)}.
   */
  private long[] scanRangeAfterFilter;

  private FilteredReadPlan() {}

  /**
   * Get the per-fragment row ranges.
   *
   * @return a map from fragment ID to a list of [start, end) ranges.
   */
  public Map<Integer, List<long[]>> getFragmentRanges() {
    return fragmentRanges;
  }

  /**
   * Get the row offset range applied after filtering.
   *
   * @return Optional containing a [start, end) range, or empty if not needed.
   */
  public Optional<long[]> getScanRangeAfterFilter() {
    return Optional.ofNullable(scanRangeAfterFilter);
  }

  /**
   * Get the native handle for this plan.
   *
   * @return the native handle value.
   */
  long getNativeHandle() {
    return nativeHandle;
  }

  @Override
  public void close() {
    if (nativeHandle != 0) {
      releaseNativePlan(nativeHandle);
      nativeHandle = 0;
    }
  }

  private static native void releaseNativePlan(long handle);
}
