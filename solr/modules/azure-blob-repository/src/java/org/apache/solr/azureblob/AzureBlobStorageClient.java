/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.azureblob;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.util.ResumableInputStream;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a {@link BlobServiceClient} for communicating with Azure Blob Storage. Utilizes the
 * default Azure credential provider chain.
 */
public class AzureBlobStorageClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String BLOB_FILE_PATH_DELIMITER = "/";

  private final BlobContainerClient containerClient;

  AzureBlobStorageClient(
      String containerName,
      String connectionString,
      String endpoint,
      String accountName,
      String accountKey,
      String sasToken,
      String tenantId,
      String clientId,
      String clientSecret) {
    this(
        createInternalClient(
            connectionString,
            endpoint,
            accountName,
            accountKey,
            sasToken,
            tenantId,
            clientId,
            clientSecret),
        containerName);
  }

  @VisibleForTesting
  AzureBlobStorageClient(BlobServiceClient blobServiceClient, String containerName) {
    this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
    try {
      containerClient.create();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() != 409) {
        throw e;
      }
    }
  }

  private static BlobServiceClient createInternalClient(
      String connectionString,
      String endpoint,
      String accountName,
      String accountKey,
      String sasToken,
      String tenantId,
      String clientId,
      String clientSecret) {

    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
    // Use default HTTP client (Netty) as provided by azure-core-http-netty

    if (StrUtils.isNotNullOrEmpty(connectionString)) {
      builder.connectionString(connectionString);
    } else if (StrUtils.isNotNullOrEmpty(endpoint)) {
      builder.endpoint(endpoint);
      if (StrUtils.isNotNullOrEmpty(accountName) && StrUtils.isNotNullOrEmpty(accountKey)) {
        builder.credential(
            new com.azure.storage.common.StorageSharedKeyCredential(accountName, accountKey));
      } else if (StrUtils.isNotNullOrEmpty(sasToken)) {
        builder.sasToken(sasToken);
      } else {
        // Use default Azure credential provider chain
        TokenCredential credential = new DefaultAzureCredentialBuilder().tenantId(tenantId).build();
        builder.credential(credential);
      }
    } else {
      throw new IllegalArgumentException("Either connectionString or endpoint must be provided");
    }

    return builder.buildClient();
  }

  /** Create a directory in Blob Storage, if it does not already exist. */
  void createDirectory(String path) throws AzureBlobException {
    String sanitizedDirPath = sanitizedDirPath(path);

    // Only create the directory if it does not already exist
    if (!pathExists(sanitizedDirPath)) {
      String parent = getParentDirectory(sanitizedDirPath);
      // Stop at root
      if (!parent.isEmpty() && !parent.equals(BLOB_FILE_PATH_DELIMITER)) {
        createDirectory(parent);
      }

      try {
        // Create empty blob and mark it as a directory via metadata
        BlobClient blobClient = containerClient.getBlobClient(sanitizedDirPath);
        blobClient.upload(new ByteArrayInputStream(new byte[0]), 0, true);
        java.util.Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("hdi_isfolder", "true");
        blobClient.setMetadata(metadata);
      } catch (BlobStorageException e) {
        throw handleBlobException(e);
      }
    }
  }

  /** Delete files from Blob Storage. Missing files are ignored (idempotent delete). */
  void delete(Collection<String> paths) throws AzureBlobException {
    Set<String> entries = new HashSet<>();
    for (String path : paths) {
      entries.add(sanitizedFilePath(path));
    }
    deleteBlobs(entries);
  }

  /** Delete directory, all the files and subdirectories from Blob Storage. */
  void deleteDirectory(String path) throws AzureBlobException {
    path = sanitizedDirPath(path);

    // Get all the files and subdirectories
    Set<String> entries = listAll(path);
    if (pathExists(path)) {
      entries.add(path);
    }

    deleteBlobs(entries);
  }

  /** List all the files and subdirectories directly under given path. */
  String[] listDir(String path) throws AzureBlobException {
    path = sanitizedDirPath(path);

    try {
      ListBlobsOptions options = new ListBlobsOptions().setPrefix(path).setMaxResultsPerPage(1000);

      final String finalPath = path; // Make path effectively final for lambda
      return containerClient.listBlobs(options, null).stream()
          .map(BlobItem::getName)
          .filter(s -> s.startsWith(finalPath))
          .map(s -> s.substring(finalPath.length()))
          .filter(s -> !s.isEmpty())
          .filter(
              s -> {
                int slashIndex = s.indexOf(BLOB_FILE_PATH_DELIMITER);
                return slashIndex == -1 || slashIndex == s.length() - 1;
              })
          .toArray(String[]::new);
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Check if path exists. */
  boolean pathExists(String path) throws AzureBlobException {
    final String blobPath = sanitizedPath(path);

    // for root return true
    if (blobPath.isEmpty() || BLOB_FILE_PATH_DELIMITER.equals(blobPath)) {
      return true;
    }

    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      return blobClient.exists();
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Check if path is directory. */
  boolean isDirectory(String path) throws AzureBlobException {
    final String dirPrefix = sanitizedDirPath(path);

    try {
      // First, if there are any child blobs under this prefix, it's a directory
      ListBlobsOptions options =
          new ListBlobsOptions().setPrefix(dirPrefix).setMaxResultsPerPage(1);
      if (containerClient.listBlobs(options, null).iterator().hasNext()) {
        return true;
      }

      // Otherwise, check if an empty blob exactly named with the trailing slash exists
      BlobClient markerClient = containerClient.getBlobClient(dirPrefix);
      if (markerClient.exists()) {
        long size = markerClient.getProperties().getBlobSize();
        if (size == 0) {
          // zero-byte marker with name ending in '/' is a directory
          return true;
        }
        // If it's a non-zero blob at a name with '/', treat conservatively as file
        java.util.Map<String, String> md = markerClient.getProperties().getMetadata();
        return md != null && md.containsKey("hdi_isfolder");
      }

      return false;
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Get length of file in bytes. */
  long length(String path) throws AzureBlobException {
    String blobPath = sanitizedFilePath(path);
    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      return blobClient.getProperties().getBlobSize();
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Open a new {@link InputStream} to file for read. */
  InputStream pullStream(String path) throws AzureBlobException {
    final String blobPath = sanitizedFilePath(path);

    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      final long contentLength = blobClient.getProperties().getBlobSize();

      InputStream initial = new IdempotentCloseInputStream(blobClient.openInputStream());

      return new ResumableInputStream(
          initial,
          bytesRead -> {
            if (contentLength > 0 && bytesRead >= contentLength) {
              return null;
            }
            try {
              long remaining =
                  contentLength > 0 ? Math.max(0, contentLength - bytesRead) : Long.MAX_VALUE;
              return pullRangeStream(path, bytesRead, remaining);
            } catch (AzureBlobException e) {
              // ResumableInputStream supplier cannot throw checked exceptions
              throw new RuntimeException(e);
            }
          });
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Open a ranged {@link InputStream} to file for read from offset for length bytes. */
  InputStream pullRangeStream(String path, long offset, long length) throws AzureBlobException {
    final String blobPath = sanitizedFilePath(path);
    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      com.azure.storage.blob.models.BlobRange range =
          new com.azure.storage.blob.models.BlobRange(offset, length);
      return new IdempotentCloseInputStream(blobClient.openInputStream(range, null));
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Wrapper that makes close() idempotent (second close is a no-op). */
  private static final class IdempotentCloseInputStream extends FilterInputStream {
    private boolean closed;

    IdempotentCloseInputStream(InputStream in) {
      super(in);
      this.closed = false;
    }

    @Override
    public int read() throws java.io.IOException {
      if (closed) {
        throw new java.io.IOException("Stream is already closed");
      }
      try {
        return super.read();
      } catch (RuntimeException re) {
        if (isAlreadyClosed(re)) {
          throw new java.io.IOException("Stream is already closed", re);
        }
        throw re;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws java.io.IOException {
      if (closed) {
        throw new java.io.IOException("Stream is already closed");
      }
      try {
        return super.read(b, off, len);
      } catch (RuntimeException re) {
        if (isAlreadyClosed(re)) {
          throw new java.io.IOException("Stream is already closed", re);
        }
        throw re;
      }
    }

    @Override
    public void close() throws java.io.IOException {
      if (closed) {
        return;
      }
      try {
        super.close();
      } catch (java.io.IOException e) {
        String msg = e.getMessage();
        if (msg == null || !msg.toLowerCase(java.util.Locale.ROOT).contains("already closed")) {
          throw e;
        }
        // swallow "already closed" to make close idempotent
      } finally {
        closed = true;
      }
    }

    @Override
    public long skip(long n) throws java.io.IOException {
      if (closed) {
        throw new java.io.IOException("Stream is already closed");
      }
      if (n <= 0) {
        return 0L;
      }
      long remaining = n;
      byte[] discard = new byte[8192];
      try {
        while (remaining > 0) {
          int toRead = (int) Math.min(discard.length, remaining);
          int read = super.read(discard, 0, toRead);
          if (read < 0) {
            break;
          }
          remaining -= read;
        }
        return n - remaining;
      } catch (RuntimeException re) {
        // Normalize runtime issues from Azure's stream into IOExceptions so upper layers can resume
        throw new java.io.IOException(re);
      }
    }

    private static boolean isAlreadyClosed(Throwable t) {
      String msg = t.getMessage();
      return msg != null && msg.toLowerCase(java.util.Locale.ROOT).contains("already closed");
    }
  }

  /** Open a new {@link OutputStream} to file for write. */
  OutputStream pushStream(String path) throws AzureBlobException {
    path = sanitizedFilePath(path);

    if (!parentDirectoryExist(path)) {
      // Auto-create missing parent directory to mirror Azure's virtual directory semantics
      String parentDirectory = getParentDirectory(path);
      if (!parentDirectory.isEmpty() && !parentDirectory.equals(BLOB_FILE_PATH_DELIMITER)) {
        createDirectory(parentDirectory);
      }
    }

    try {
      BlobClient blobClient = containerClient.getBlobClient(path);
      return new AzureBlobOutputStream(blobClient, path);
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  /** Close the client. */
  void close() {
    // Azure SDK clients don't need explicit closing
  }

  @VisibleForTesting
  void deleteContainerForTests() {
    try {
      containerClient.delete();
    } catch (BlobStorageException e) {
      // Ignore not found
      if (e.getStatusCode() != 404) {
        throw e;
      }
    }
  }

  private Collection<String> deleteBlobs(Collection<String> paths) throws AzureBlobException {
    try {
      return deleteBlobs(paths, 1000); // Azure supports batch delete
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  @VisibleForTesting
  Collection<String> deleteBlobs(Collection<String> entries, int batchSize)
      throws AzureBlobException {
    Set<String> deletedPaths = new HashSet<>();

    for (String path : entries) {
      try {
        BlobClient blobClient = containerClient.getBlobClient(path);
        boolean existed = blobClient.deleteIfExists();
        if (existed) {
          deletedPaths.add(path);
        }
      } catch (BlobStorageException e) {
        if (e.getStatusCode() == 404) {
          // ignore missing
          continue;
        }
        throw new AzureBlobException("Could not delete blob with path: " + path, e);
      }
    }

    return deletedPaths;
  }

  private Set<String> listAll(String path) throws AzureBlobException {
    String prefix = sanitizedDirPath(path);

    try {
      ListBlobsOptions options =
          new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(1000);

      return containerClient.listBlobs(options, null).stream()
          .map(BlobItem::getName)
          .filter(s -> s.startsWith(prefix))
          .collect(Collectors.toSet());
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  private boolean parentDirectoryExist(String path) throws AzureBlobException {
    String parentDirectory = getParentDirectory(path);

    if (parentDirectory.isEmpty() || parentDirectory.equals(BLOB_FILE_PATH_DELIMITER)) {
      return true;
    }

    return pathExists(parentDirectory);
  }

  private String getParentDirectory(String path) {
    if (!path.contains(BLOB_FILE_PATH_DELIMITER)) {
      return "";
    }

    int fromEnd = path.length() - 1;
    if (path.endsWith(BLOB_FILE_PATH_DELIMITER)) {
      fromEnd -= 1;
    }
    return fromEnd > 0
        ? path.substring(0, path.lastIndexOf(BLOB_FILE_PATH_DELIMITER, fromEnd) + 1)
        : "";
  }

  /** Ensures path adheres to some rules: -Doesn't start with a leading slash */
  String sanitizedPath(String path) throws AzureBlobException {
    String sanitizedPath = path.trim();
    // Remove all leading slashes so that blob names never start with '/'
    while (sanitizedPath.startsWith(BLOB_FILE_PATH_DELIMITER)) {
      sanitizedPath = sanitizedPath.substring(1).trim();
    }
    return sanitizedPath;
  }

  /** Ensures file path adheres to some rules */
  String sanitizedFilePath(String path) throws AzureBlobException {
    String sanitizedPath = sanitizedPath(path);

    if (sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
      throw new AzureBlobException("Invalid Path. Path for file can't end with '/'");
    }

    if (sanitizedPath.isEmpty()) {
      throw new AzureBlobException("Invalid Path. Path cannot be empty");
    }

    return sanitizedPath;
  }

  /** Ensures directory path adheres to some rules */
  String sanitizedDirPath(String path) throws AzureBlobException {
    String sanitizedPath = sanitizedPath(path);

    if (!sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
      sanitizedPath += BLOB_FILE_PATH_DELIMITER;
    }

    return sanitizedPath;
  }

  /** Handle Azure Blob Storage exceptions */
  static AzureBlobException handleBlobException(BlobStorageException e) {
    String errMessage =
        String.format(
            Locale.ROOT,
            "Azure Blob Storage error: [statusCode=%s] [errorCode=%s] [message=%s]",
            e.getStatusCode(),
            e.getErrorCode(),
            e.getMessage());

    log.error(errMessage);

    if (e.getStatusCode() == 404) {
      return new AzureBlobNotFoundException(errMessage, e);
    } else {
      return new AzureBlobException(errMessage, e);
    }
  }
}
