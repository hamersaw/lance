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

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Represents a split for parallel scanning of fragments.
 *
 * <p>A split contains one or more fragments that can be scanned together. Splits can be used to
 * distribute scanning work across multiple workers or threads.
 */
public class Split implements Serializable {
  private static final long serialVersionUID = 1L;
  private final List<SplitFragment> fragments;

  /**
   * Creates a new Split.
   *
   * @param fragments the list of fragments in this split
   */
  public Split(List<SplitFragment> fragments) {
    this.fragments = fragments;
  }

  /**
   * Returns the list of fragments in this split.
   *
   * @return the list of split fragments
   */
  public List<SplitFragment> getFragments() {
    return fragments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Split split = (Split) o;
    return Objects.equals(fragments, split.fragments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fragments);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fragments", fragments).toString();
  }
}
