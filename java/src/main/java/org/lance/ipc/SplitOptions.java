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
 * Options for configuring split generation in a scanner.
 *
 * <p>This class allows specifying constraints on the maximum size and row count for splits. Both
 * fields are optional; if neither is set, default behavior will be used.
 */
public class SplitOptions {
  private final Optional<Long> maxSizeBytes;
  private final Optional<Long> maxRowCount;

  private SplitOptions(Builder builder) {
    this.maxSizeBytes = builder.maxSizeBytes;
    this.maxRowCount = builder.maxRowCount;
  }

  /**
   * Returns the maximum size in bytes per split.
   *
   * @return the maximum size in bytes, or empty if not set
   */
  public Optional<Long> getMaxSizeBytes() {
    return maxSizeBytes;
  }

  /**
   * Returns the maximum number of rows per split.
   *
   * @return the maximum row count, or empty if not set
   */
  public Optional<Long> getMaxRowCount() {
    return maxRowCount;
  }

  /** Builder for {@link SplitOptions}. */
  public static class Builder {
    private Optional<Long> maxSizeBytes = Optional.empty();
    private Optional<Long> maxRowCount = Optional.empty();

    /**
     * Sets the maximum size in bytes per split.
     *
     * <p>The scanner estimates the row size from the output schema and calculates how many rows fit
     * within this budget.
     *
     * @param maxSizeBytes the maximum size in bytes
     * @return this builder
     */
    public Builder maxSizeBytes(long maxSizeBytes) {
      this.maxSizeBytes = Optional.of(maxSizeBytes);
      return this;
    }

    /**
     * Sets the maximum number of rows per split.
     *
     * @param maxRowCount the maximum number of rows
     * @return this builder
     */
    public Builder maxRowCount(long maxRowCount) {
      this.maxRowCount = Optional.of(maxRowCount);
      return this;
    }

    /**
     * Builds a new {@link SplitOptions} instance.
     *
     * @return a new SplitOptions instance
     */
    public SplitOptions build() {
      return new SplitOptions(this);
    }
  }
}
