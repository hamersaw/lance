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
 * Result of {@link LanceScanner#planSplits}, representing how to divide a scan for distributed
 * execution.
 *
 * <p>A {@code Splits} instance is one of two variants:
 *
 * <ul>
 *   <li>{@link #getFilteredReadPlans()} - detailed per-fragment read plans with row ranges and
 *       residual filters.
 *   <li>{@link #getFragments()} - fragment IDs only, where each fragment is a split.
 * </ul>
 *
 * <p>When using the {@code FilteredReadPlans} variant, the plans must be closed when no longer
 * needed.
 */
public class Splits implements AutoCloseable {
  private final List<FilteredReadPlan> filteredReadPlans;
  private final List<Integer> fragments;

  Splits(List<FilteredReadPlan> filteredReadPlans, List<Integer> fragments) {
    this.filteredReadPlans = filteredReadPlans;
    this.fragments = fragments;
  }

  /**
   * Get the filtered read plans, if this is a {@code FilteredReadPlans} variant.
   *
   * @return Optional containing the list of plans, or empty if this is a {@code Fragments} variant.
   */
  public Optional<List<FilteredReadPlan>> getFilteredReadPlans() {
    return Optional.ofNullable(filteredReadPlans);
  }

  /**
   * Get the fragment IDs, if this is a {@code Fragments} variant.
   *
   * @return Optional containing the list of fragment IDs, or empty if this is a {@code
   *     FilteredReadPlans} variant.
   */
  public Optional<List<Integer>> getFragments() {
    return Optional.ofNullable(fragments);
  }

  @Override
  public void close() {
    if (filteredReadPlans != null) {
      for (FilteredReadPlan plan : filteredReadPlans) {
        plan.close();
      }
    }
  }
}
