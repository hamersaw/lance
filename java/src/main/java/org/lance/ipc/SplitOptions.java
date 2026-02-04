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

import java.util.Optional;

/**
 * Options for configuring split generation via {@link LanceScanner#planSplits}.
 *
 * <p>Both fields are optional; if neither is set, a default split size of 128 MiB is used.
 */
public class SplitOptions {
  private final Optional<Long> maxSizeBytes;
  private final Optional<Long> maxRowCount;

  private SplitOptions(Optional<Long> maxSizeBytes, Optional<Long> maxRowCount) {
    this.maxSizeBytes = maxSizeBytes;
    this.maxRowCount = maxRowCount;
  }

  /**
   * Get the maximum size in bytes per split.
   *
   * @return Optional containing the max size if specified, otherwise empty.
   */
  public Optional<Long> getMaxSizeBytes() {
    return maxSizeBytes;
  }

  /**
   * Get the maximum number of rows per split.
   *
   * @return Optional containing the max row count if specified, otherwise empty.
   */
  public Optional<Long> getMaxRowCount() {
    return maxRowCount;
  }

  /** Builder for constructing SplitOptions. */
  public static class Builder {
    private Optional<Long> maxSizeBytes = Optional.empty();
    private Optional<Long> maxRowCount = Optional.empty();

    /**
     * Set the maximum size in bytes per split.
     *
     * <p>The scanner estimates the row size from the output schema and calculates how many rows fit
     * within this budget.
     *
     * @param maxSizeBytes maximum size in bytes per split
     * @return Builder instance for method chaining.
     */
    public Builder maxSizeBytes(long maxSizeBytes) {
      this.maxSizeBytes = Optional.of(maxSizeBytes);
      return this;
    }

    /**
     * Set the maximum number of rows per split.
     *
     * @param maxRowCount maximum number of rows per split
     * @return Builder instance for method chaining.
     */
    public Builder maxRowCount(long maxRowCount) {
      this.maxRowCount = Optional.of(maxRowCount);
      return this;
    }

    /**
     * Build the SplitOptions instance.
     *
     * @return SplitOptions instance with the specified parameters.
     */
    public SplitOptions build() {
      return new SplitOptions(maxSizeBytes, maxRowCount);
    }
  }
}
