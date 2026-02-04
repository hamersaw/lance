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
 * <p>It specifies which fragment row ranges to read, any residual filters to apply (encoded as
 * Substrait), and an optional post-filter scan range. Plans are created by {@link
 * LanceScanner#planSplits} and executed by {@link LanceScanner#executeFilteredReadPlan}.
 *
 * <p>This class is a pure data object that is fully serializable for distributed execution.
 */
public class FilteredReadPlan {
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

  /**
   * Deduplicated list of Substrait-encoded filter expressions.
   *
   * <p>Each element is a serialized Substrait ExtendedExpression message. Multiple fragments may
   * share the same filter; this list stores each unique filter exactly once.
   */
  private List<byte[]> filterExpressions;

  /**
   * Maps fragment ID to an index in {@link #filterExpressions}.
   *
   * <p>Fragments that do not require filtering are absent from this map.
   */
  private Map<Integer, Integer> fragmentFilterIndex;

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
   * Get the deduplicated Substrait-encoded filter expressions.
   *
   * @return list of serialized Substrait ExtendedExpression messages.
   */
  public List<byte[]> getFilterExpressions() {
    return filterExpressions;
  }

  /**
   * Get the mapping from fragment ID to filter expression index.
   *
   * @return map from fragment ID to index in {@link #getFilterExpressions()}.
   */
  public Map<Integer, Integer> getFragmentFilterIndex() {
    return fragmentFilterIndex;
  }
}
