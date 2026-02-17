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
 *   <li>{@link #getFilteredReadExec()} - a self-contained execution node with row selections,
 *       filters, and read options.
 *   <li>{@link #getFragments()} - fragment IDs only, a collection of fragment IDs to scan.
 * </ul>
 */
public class Split {
  private final FilteredReadExec filteredReadExec;
  private final List<Integer> fragments;
  private final List<String> outputColumns;

  Split(FilteredReadExec filteredReadExec, List<Integer> fragments, List<String> outputColumns) {
    this.filteredReadExec = filteredReadExec;
    this.fragments = fragments;
    this.outputColumns = outputColumns;
  }

  /**
   * Get the filtered read exec, if this is a {@code FilteredReadExec} variant.
   *
   * @return Optional containing the exec, or empty if this is a {@code Fragments} variant.
   */
  public Optional<FilteredReadExec> getFilteredReadExec() {
    return Optional.ofNullable(filteredReadExec);
  }

  /**
   * Get the fragment IDs, if this is a {@code Fragments} variant.
   *
   * @return Optional containing the list of fragment IDs, or empty if this is a {@code
   *     FilteredReadExec} variant.
   */
  public Optional<List<Integer>> getFragments() {
    return Optional.ofNullable(fragments);
  }

  /**
   * Get the ordered output column names from the original scan.
   *
   * <p>Includes metadata columns like {@code _rowid} when the original scanner requested them.
   * Only set for the {@code FilteredReadExec} variant. Use these column names when configuring the
   * scanner's projection before calling {@link LanceScanner#withFilteredReadExec}.
   *
   * @return Optional containing the ordered list of column names, or empty if this is a {@code
   *     Fragments} variant.
   */
  public Optional<List<String>> getOutputColumns() {
    return Optional.ofNullable(outputColumns);
  }
}
