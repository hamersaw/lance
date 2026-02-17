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

/**
 * An opaque handle to a native Rust {@code FilteredReadExec}.
 *
 * <p>Instances are created by {@link LanceScanner#planSplits} and consumed by {@link
 * LanceScanner#withFilteredReadExec}. The exec is stored in native memory and must be freed by
 * calling {@link #close()}.
 */
public class FilteredReadExec implements AutoCloseable {
  private long nativeHandle;

  private FilteredReadExec() {}

  /**
   * Release the native memory associated with this exec.
   *
   * <p>After calling this method, the exec can no longer be executed.
   */
  @Override
  public void close() {
    if (nativeHandle != 0) {
      releaseNativeExec(nativeHandle);
      nativeHandle = 0;
    }
  }

  long getNativeHandle() {
    if (nativeHandle == 0) {
      throw new IllegalStateException("FilteredReadExec has been closed");
    }
    return nativeHandle;
  }

  private static native void releaseNativeExec(long handle);
}
