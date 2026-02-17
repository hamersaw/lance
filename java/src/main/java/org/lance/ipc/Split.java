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
import java.util.Optional;

/**
 * A single unit of work from {@link LanceScanner#planSplits}, representing one split for
 * distributed execution.
 *
 * <p>A {@code Split} instance is one of two variants:
 *
 * <ul>
 *   <li>{@link #getFilteredReadPlan()} - a detailed per-fragment read plan with row ranges and
 *       residual filters.
 *   <li>{@link #getFragments()} - fragment IDs only, a collection of fragment IDs to scan.
 * </ul>
 */
public class Split {
  private final FilteredReadPlan filteredReadPlan;
  private final List<Integer> fragments;

  Split(FilteredReadPlan filteredReadPlan, List<Integer> fragments) {
    this.filteredReadPlan = filteredReadPlan;
    this.fragments = fragments;
  }

  /**
   * Get the filtered read plan, if this is a {@code FilteredReadPlan} variant.
   *
   * @return Optional containing the plan, or empty if this is a {@code Fragments} variant.
   */
  public Optional<FilteredReadPlan> getFilteredReadPlan() {
    return Optional.ofNullable(filteredReadPlan);
  }

  /**
   * Get the fragment IDs, if this is a {@code Fragments} variant.
   *
   * @return Optional containing the list of fragment IDs, or empty if this is a {@code
   *     FilteredReadPlan} variant.
   */
  public Optional<List<Integer>> getFragments() {
    return Optional.ofNullable(fragments);
  }
}
