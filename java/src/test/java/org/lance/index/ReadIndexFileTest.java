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
package org.lance.index;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;
import org.lance.index.scalar.ScalarIndexParams;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for {@link Dataset#readIndexFile(String, String)}. */
public class ReadIndexFileTest {

  private static Schema intSchema() {
    return new Schema(
        Arrays.asList(
            Field.nullable("id", new ArrowType.Int(32, true)),
            Field.nullable("value", new ArrowType.Int(32, true))),
        null);
  }

  private Dataset writeIntFragment(
      BufferAllocator allocator, String path, long version, int startValue, int rowCount) {
    Schema schema = intSchema();
    List<FragmentMetadata> metas;
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      IntVector idVec = (IntVector) root.getVector("id");
      IntVector valVec = (IntVector) root.getVector("value");
      for (int i = 0; i < rowCount; i++) {
        idVec.setSafe(i, startValue + i);
        valVec.setSafe(i, (startValue + i) * 10);
      }
      root.setRowCount(rowCount);
      metas = Fragment.create(path, allocator, root, new WriteParams.Builder().build());
    }
    FragmentOperation.Append appendOp = new FragmentOperation.Append(metas);
    return Dataset.commit(allocator, path, appendOp, Optional.of(version));
  }

  @Test
  public void testReadZonemapIndexFile(@TempDir Path tempDir) throws Exception {
    String path = tempDir.resolve("zonemap_read").toString();
    try (BufferAllocator allocator = new RootAllocator()) {
      // Create empty dataset then append a fragment
      try (Dataset ds =
          Dataset.create(allocator, path, intSchema(), new WriteParams.Builder().build())) {
        // empty
      }
      Dataset ds2 = writeIntFragment(allocator, path, 1, 0, 100);
      ds2.close();

      try (Dataset dataset = Dataset.open(path, allocator)) {
        // Create zonemap index on "value" column
        ScalarIndexParams params = ScalarIndexParams.create("zonemap", "{}");
        IndexParams indexParams = IndexParams.builder().setScalarIndexParams(params).build();
        dataset.createIndex(
            Collections.singletonList("value"),
            IndexType.ZONEMAP,
            Optional.of("value_zm"),
            indexParams,
            true);

        // Read the zonemap file via the new API
        try (ArrowReader reader = dataset.readIndexFile("value_zm", "zonemap.lance")) {
          assertTrue(reader.loadNextBatch());
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();

          // Verify expected columns exist
          assertNotNull(batch.getVector("fragment_id"));
          assertNotNull(batch.getVector("zone_start"));
          assertNotNull(batch.getVector("zone_length"));
          assertNotNull(batch.getVector("min"));
          assertNotNull(batch.getVector("max"));
          assertNotNull(batch.getVector("null_count"));

          assertTrue(batch.getRowCount() > 0, "Expected at least one zone row");
        }
      }
    }
  }

  @Test
  public void testReadZonemapMultiFragment(@TempDir Path tempDir) throws Exception {
    String path = tempDir.resolve("multi_frag").toString();
    try (BufferAllocator allocator = new RootAllocator()) {
      try (Dataset ds =
          Dataset.create(allocator, path, intSchema(), new WriteParams.Builder().build())) {
        // empty
      }
      Dataset ds2 = writeIntFragment(allocator, path, 1, 0, 50);
      ds2.close();
      Dataset ds3 = writeIntFragment(allocator, path, 2, 50, 50);
      ds3.close();

      try (Dataset dataset = Dataset.open(path, allocator)) {
        assertEquals(2, dataset.getFragments().size());

        ScalarIndexParams params = ScalarIndexParams.create("zonemap", "{}");
        IndexParams indexParams = IndexParams.builder().setScalarIndexParams(params).build();
        dataset.createIndex(
            Collections.singletonList("value"),
            IndexType.ZONEMAP,
            Optional.of("value_zm"),
            indexParams,
            true);

        try (ArrowReader reader = dataset.readIndexFile("value_zm", "zonemap.lance")) {
          assertTrue(reader.loadNextBatch());
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();
          assertTrue(batch.getRowCount() > 0);
        }
      }
    }
  }

  @Test
  public void testReadIndexFileNotFound(@TempDir Path tempDir) {
    String path = tempDir.resolve("not_found").toString();
    try (BufferAllocator allocator = new RootAllocator()) {
      try (Dataset dataset =
          Dataset.create(allocator, path, intSchema(), new WriteParams.Builder().build())) {
        // No index exists — should throw
        assertThrows(RuntimeException.class, () -> dataset.readIndexFile("no_such_index", "foo.lance"));
      }
    }
  }

  @Test
  public void testReadIndexFileNullArgs(@TempDir Path tempDir) {
    String path = tempDir.resolve("null_args").toString();
    try (BufferAllocator allocator = new RootAllocator()) {
      try (Dataset dataset =
          Dataset.create(allocator, path, intSchema(), new WriteParams.Builder().build())) {
        assertThrows(IllegalArgumentException.class, () -> dataset.readIndexFile(null, "foo.lance"));
        assertThrows(IllegalArgumentException.class, () -> dataset.readIndexFile("idx", null));
        assertThrows(IllegalArgumentException.class, () -> dataset.readIndexFile("", "foo.lance"));
        assertThrows(IllegalArgumentException.class, () -> dataset.readIndexFile("idx", ""));
      }
    }
  }
}
