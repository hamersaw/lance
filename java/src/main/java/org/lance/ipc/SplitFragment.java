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

import org.lance.FragmentMetadata;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fragment within a {@link Split}, along with metadata about the expected number of rows that
 * will be scanned from it.
 */
public class SplitFragment implements Serializable {
  private static final long serialVersionUID = 1L;
  private final FragmentMetadata fragment;
  private final long maxRowCount;

  /**
   * Creates a new SplitFragment.
   *
   * @param fragment the fragment metadata
   * @param maxRowCount an upper bound on the number of rows that will be read from this fragment
   *     after applying any filters or index pruning
   */
  public SplitFragment(FragmentMetadata fragment, long maxRowCount) {
    this.fragment = fragment;
    this.maxRowCount = maxRowCount;
  }

  /**
   * Returns the fragment metadata.
   *
   * @return the fragment metadata
   */
  public FragmentMetadata getFragment() {
    return fragment;
  }

  /**
   * Returns an upper bound on the number of rows that will be read from this fragment after
   * applying any filters or index pruning.
   *
   * @return the maximum row count
   */
  public long getMaxRowCount() {
    return maxRowCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SplitFragment that = (SplitFragment) o;
    return maxRowCount == that.maxRowCount && Objects.equals(fragment, that.fragment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fragment, maxRowCount);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("fragment", fragment)
        .add("maxRowCount", maxRowCount)
        .toString();
  }
}
