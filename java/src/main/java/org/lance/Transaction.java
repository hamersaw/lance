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
package org.lance;

import org.lance.io.StorageOptionsProvider;
import org.lance.operation.Operation;

import com.google.common.base.MoreObjects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Align with the Transaction struct in rust. The transaction won't commit the status to original
 * dataset. It will return a new dataset after committed.
 */
public class Transaction {

  private final long readVersion;
  private final String uuid;
  private final Map<String, String> writeParams;
  private final Optional<Map<String, String>> transactionProperties;
  // Mainly for JNI usage
  private final Dataset dataset;
  private final Operation operation;

  // URI-based commit fields
  private final String uri;
  private final BufferAllocator allocator;
  private final StorageOptionsProvider storageOptionsProvider;
  private final Object namespace;
  private final List<String> tableId;
  private final boolean enableV2ManifestPaths;

  private Transaction(
      Dataset dataset,
      long readVersion,
      String uuid,
      Operation operation,
      Map<String, String> writeParams,
      Map<String, String> transactionProperties) {
    this(
        dataset,
        readVersion,
        uuid,
        operation,
        writeParams,
        transactionProperties,
        null,
        null,
        null,
        null,
        null,
        true);
  }

  private Transaction(
      Dataset dataset,
      long readVersion,
      String uuid,
      Operation operation,
      Map<String, String> writeParams,
      Map<String, String> transactionProperties,
      String uri,
      BufferAllocator allocator,
      StorageOptionsProvider storageOptionsProvider,
      Object namespace,
      List<String> tableId,
      boolean enableV2ManifestPaths) {
    this.dataset = dataset;
    this.readVersion = readVersion;
    this.uuid = uuid;
    this.operation = operation;
    this.writeParams = writeParams != null ? writeParams : new HashMap<>();
    this.transactionProperties = Optional.ofNullable(transactionProperties);
    this.uri = uri;
    this.allocator = allocator;
    this.storageOptionsProvider = storageOptionsProvider;
    this.namespace = namespace;
    this.tableId = tableId;
    this.enableV2ManifestPaths = enableV2ManifestPaths;
  }

  public long readVersion() {
    return readVersion;
  }

  public String uuid() {
    return uuid;
  }

  public Operation operation() {
    return operation;
  }

  public Map<String, String> writeParams() {
    return writeParams;
  }

  public Optional<Map<String, String>> transactionProperties() {
    return transactionProperties;
  }

  public String uri() {
    return uri;
  }

  public BufferAllocator allocator() {
    return allocator;
  }

  public StorageOptionsProvider storageOptionsProvider() {
    return storageOptionsProvider;
  }

  public Object namespace() {
    return namespace;
  }

  public List<String> tableId() {
    return tableId;
  }

  public boolean enableV2ManifestPaths() {
    return enableV2ManifestPaths;
  }

  /**
   * Commit the transaction and return a new Dataset.
   *
   * <p>When this transaction was built from an existing dataset, commits against that dataset. When
   * built from a URI, creates a new dataset at the specified location.
   *
   * @return a new Dataset at the committed version
   */
  public Dataset commit() {
    if (dataset != null) {
      return dataset.commitTransaction(this);
    }
    if (uri != null) {
      return commitToUri();
    }
    throw new IllegalStateException("Transaction requires either a dataset or a URI");
  }

  private Dataset commitToUri() {
    Dataset result =
        nativeCommitTransactionToUri(
            uri,
            this,
            enableV2ManifestPaths,
            storageOptionsProvider,
            namespace,
            tableId,
            allocator);
    return result;
  }

  public void release() {
    operation.release();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("readVersion", readVersion)
        .add("uuid", uuid)
        .add("operation", operation)
        .add("writeParams", writeParams)
        .add("transactionProperties", transactionProperties)
        .add("uri", uri)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Transaction that = (Transaction) o;
    return readVersion == that.readVersion
        && uuid.equals(that.uuid)
        && Objects.equals(operation, that.operation)
        && Objects.equals(writeParams, that.writeParams)
        && Objects.equals(transactionProperties, that.transactionProperties);
  }

  private static native Dataset nativeCommitTransactionToUri(
      String uri,
      Transaction transaction,
      boolean enableV2ManifestPaths,
      Object storageOptionsProvider,
      Object namespace,
      Object tableId,
      Object allocator);

  public static class Builder {
    private final String uuid;
    private Dataset dataset;
    private String uri;
    private BufferAllocator allocator;
    private long readVersion;
    private Operation operation;
    private Map<String, String> writeParams;
    private Map<String, String> transactionProperties;
    private StorageOptionsProvider storageOptionsProvider;
    private Object namespace;
    private List<String> tableId;
    private boolean enableV2ManifestPaths = true;

    /**
     * Create a builder for committing against an existing dataset.
     *
     * @param dataset the existing dataset to commit against
     */
    public Builder(Dataset dataset) {
      this.dataset = dataset;
      this.uuid = UUID.randomUUID().toString();
    }

    /**
     * Create a builder for creating a new dataset at the given URI.
     *
     * @param uri the target URI for the new dataset
     * @param allocator the Arrow buffer allocator for schema export
     */
    public Builder(String uri, BufferAllocator allocator) {
      Preconditions.checkNotNull(uri, "URI must not be null");
      Preconditions.checkNotNull(allocator, "Allocator must not be null");
      this.uri = uri;
      this.allocator = allocator;
      this.uuid = UUID.randomUUID().toString();
    }

    public Builder readVersion(long readVersion) {
      this.readVersion = readVersion;
      return this;
    }

    public Builder transactionProperties(Map<String, String> properties) {
      this.transactionProperties = properties;
      return this;
    }

    public Builder writeParams(Map<String, String> writeParams) {
      this.writeParams = writeParams;
      return this;
    }

    public Builder operation(Operation operation) {
      validateState();
      this.operation = operation;
      return this;
    }

    /**
     * Set the storage options provider for credential refresh during URI-based commits.
     *
     * @param provider the storage options provider
     * @return this builder instance
     */
    public Builder storageOptionsProvider(StorageOptionsProvider provider) {
      this.storageOptionsProvider = provider;
      return this;
    }

    /**
     * Set the namespace for managed versioning during URI-based commits.
     *
     * @param namespace the LanceNamespace instance
     * @return this builder instance
     */
    public Builder namespace(Object namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Set the table ID for namespace-based commit handling.
     *
     * @param tableId the table identifier (e.g., ["workspace", "table_name"])
     * @return this builder instance
     */
    public Builder tableId(List<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    /**
     * Enable or disable v2 manifest paths for new datasets.
     *
     * <p>Defaults to true. V2 manifest paths allow constant-time lookups for the latest manifest on
     * object storage. Warning: enabling this makes the dataset unreadable for Lance versions prior
     * to 0.17.0.
     *
     * @param enable whether to enable v2 manifest paths
     * @return this builder instance
     */
    public Builder enableV2ManifestPaths(boolean enable) {
      this.enableV2ManifestPaths = enable;
      return this;
    }

    private void validateState() {
      if (operation != null) {
        throw new IllegalStateException(
            String.format("Operation %s has been set", operation.name()));
      }
    }

    public Transaction build() {
      Preconditions.checkState(operation != null, "TransactionBuilder has no operations");

      return new Transaction(
          dataset,
          readVersion,
          uuid,
          operation,
          writeParams,
          transactionProperties,
          uri,
          allocator,
          storageOptionsProvider,
          namespace,
          tableId,
          enableV2ManifestPaths);
    }
  }
}
