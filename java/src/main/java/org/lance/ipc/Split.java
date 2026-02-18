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

/**
 * A single unit of work from {@link LanceScanner#planSplits}, representing one split for
 * distributed execution.
 *
 * <p>Contains a self-contained execution node with row selections, filters, and read options.
 */
public class Split {
  private final FilteredReadExec filteredReadExec;
  private final List<String> outputColumns;

  Split(FilteredReadExec filteredReadExec, List<String> outputColumns) {
    this.filteredReadExec = filteredReadExec;
    this.outputColumns = outputColumns;
  }

  /**
   * Get the filtered read exec for this split.
   *
   * @return The exec for this split.
   */
  public FilteredReadExec getFilteredReadExec() {
    return filteredReadExec;
  }

  /**
   * Get the ordered output column names from the original scan.
   *
   * <p>Includes metadata columns like {@code _rowid} when the original scanner requested them.
   * Use these column names when configuring the scanner's projection before calling {@link
   * LanceScanner#withFilteredReadExec}.
   *
   * @return The ordered list of column names.
   */
  public List<String> getOutputColumns() {
    return outputColumns;
  }
}
