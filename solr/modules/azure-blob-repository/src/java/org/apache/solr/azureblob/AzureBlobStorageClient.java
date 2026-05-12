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

import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.util.CollectionUtil;
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
  private static final int HTTP_NOT_FOUND = 404;
  private static final int HTTP_CONFLICT = 409;
  private static final int SKIP_BUFFER_SIZE = 8192;
  // Azure Blob Storage caps batch operations at 256 sub-requests per HTTP request:
  // https://learn.microsoft.com/rest/api/storageservices/blob-batch
  // Package-private so tests can reference the boundary directly.
  static final int DELETE_BATCH_SIZE = 256;

  private final BlobContainerClient containerClient;
  private final BlobBatchClient batchClient;

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

  AzureBlobStorageClient(BlobServiceClient blobServiceClient, String containerName) {
    this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
    this.batchClient = new BlobBatchClientBuilder(blobServiceClient).buildClient();
    try {
      containerClient.create();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() != HTTP_CONFLICT) {
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

    if (StrUtils.isNotNullOrEmpty(connectionString)) {
      builder.connectionString(connectionString);
    } else if (StrUtils.isNotNullOrEmpty(endpoint)) {
      builder.endpoint(endpoint);
      if (StrUtils.isNotNullOrEmpty(accountName) && StrUtils.isNotNullOrEmpty(accountKey)) {
        builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
      } else if (StrUtils.isNotNullOrEmpty(sasToken)) {
        builder.sasToken(sasToken);
      } else if (StrUtils.isNotNullOrEmpty(tenantId)
          && StrUtils.isNotNullOrEmpty(clientId)
          && StrUtils.isNotNullOrEmpty(clientSecret)) {
        builder.credential(
            new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build());
      } else {
        DefaultAzureCredentialBuilder dac = new DefaultAzureCredentialBuilder();
        if (StrUtils.isNotNullOrEmpty(tenantId)) {
          dac.tenantId(tenantId);
        }

        builder.credential(dac.build());
      }
    } else {
      throw new IllegalArgumentException("Either connectionString or endpoint must be provided");
    }

    return builder.buildClient();
  }

  void createDirectory(String path) throws AzureBlobException {
    String sanitizedDirPath = sanitizedDirPath(path);

    if (!pathExists(sanitizedDirPath)) {
      String parent = getParentDirectory(sanitizedDirPath);
      if (!parent.isEmpty() && !parent.equals(BLOB_FILE_PATH_DELIMITER)) {
        createDirectory(parent);
      }

      try {
        BlobClient blobClient = containerClient.getBlobClient(sanitizedDirPath);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("hdi_isfolder", "true");
        BlobParallelUploadOptions options =
            new BlobParallelUploadOptions(new ByteArrayInputStream(new byte[0]))
                .setMetadata(metadata);
        blobClient.uploadWithResponse(options, null, Context.NONE);
      } catch (BlobStorageException e) {
        throw handleBlobException(e);
      }
    }
  }

  /**
   * Strict delete: throws {@link AzureBlobNotFoundException} if any path was missing. Use {@link
   * #deleteDirectory(String)} for lenient semantics. Not atomic — present paths may still be
   * deleted server-side when this throws.
   */
  void delete(Collection<String> paths) throws AzureBlobException {
    Set<String> entries = new HashSet<>();
    for (String path : paths) {
      entries.add(sanitizedFilePath(path));
    }

    Collection<String> deletedPaths = deleteBlobs(entries);

    if (entries.size() != deletedPaths.size()) {
      Set<String> missing = new HashSet<>(entries);
      missing.removeAll(deletedPaths);
      throw new AzureBlobNotFoundException("Blobs not found: " + missing);
    }
  }

  void deleteDirectory(String path) throws AzureBlobException {
    path = sanitizedDirPath(path);

    Set<String> entries = listAll(path);
    if (pathExists(path)) {
      entries.add(path);
    }

    deleteBlobs(entries);
  }

  String[] listDir(String path) throws AzureBlobException {
    path = sanitizedDirPath(path);

    try {
      ListBlobsOptions options = new ListBlobsOptions().setPrefix(path).setMaxResultsPerPage(1000);

      final String finalPath = path;
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

  boolean pathExists(String path) throws AzureBlobException {
    final String blobPath = sanitizedPath(path);

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

  boolean isDirectory(String path) throws AzureBlobException {
    final String dirPrefix = sanitizedDirPath(path);

    try {
      ListBlobsOptions options =
          new ListBlobsOptions().setPrefix(dirPrefix).setMaxResultsPerPage(1);
      if (containerClient.listBlobs(options, null).iterator().hasNext()) {
        return true;
      }

      BlobClient markerClient = containerClient.getBlobClient(dirPrefix);
      if (markerClient.exists()) {
        BlobProperties props = markerClient.getProperties();
        if (props.getBlobSize() == 0) {
          return true;
        }

        Map<String, String> md = props.getMetadata();
        return md != null && md.containsKey("hdi_isfolder");
      }

      return false;
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  long length(String path) throws AzureBlobException {
    String blobPath = sanitizedFilePath(path);
    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      return blobClient.getProperties().getBlobSize();
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  InputStream pullStream(String path) throws AzureBlobException {
    final String blobPath = sanitizedFilePath(path);

    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      BlobInputStream blobInputStream = blobClient.openInputStream();

      try {
        final long contentLength = blobInputStream.getProperties().getBlobSize();
        InputStream initial = new IdempotentCloseInputStream(blobInputStream);
        return new ResumableInputStream(
            initial,
            bytesRead -> {
              if (bytesRead >= contentLength) {
                return null;
              }
              try {
                return pullRangeStream(path, bytesRead, contentLength - bytesRead);
              } catch (AzureBlobException e) {
                throw new RuntimeException(e);
              }
            });
      } catch (RuntimeException | Error t) {
        blobInputStream.close();
        throw t;
      }
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  InputStream pullRangeStream(String path, long offset, long length) throws AzureBlobException {
    final String blobPath = sanitizedFilePath(path);
    try {
      BlobClient blobClient = containerClient.getBlobClient(blobPath);
      BlobRange range = new BlobRange(offset, length);
      return new IdempotentCloseInputStream(blobClient.openInputStream(range, null));
    } catch (BlobStorageException e) {
      throw handleBlobException(e);
    }
  }

  private static final class IdempotentCloseInputStream extends FilterInputStream {
    private boolean closed;

    IdempotentCloseInputStream(InputStream in) {
      super(in);
      this.closed = false;
    }

    @Override
    public int read() throws IOException {
      if (closed) {
        throw new IOException("Stream is already closed");
      }
      try {
        return super.read();
      } catch (RuntimeException re) {
        if (isAlreadyClosed(re)) {
          throw new IOException("Stream is already closed", re);
        }
        throw re;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("Stream is already closed");
      }
      try {
        return super.read(b, off, len);
      } catch (RuntimeException re) {
        if (isAlreadyClosed(re)) {
          throw new IOException("Stream is already closed", re);
        }
        throw re;
      }
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      try {
        super.close();
      } catch (IOException e) {
        String msg = e.getMessage();
        if (msg == null || !msg.toLowerCase(Locale.ROOT).contains("already closed")) {
          throw e;
        }
        // swallow "already closed" to make close idempotent
      } finally {
        closed = true;
      }
    }

    @Override
    public long skip(long n) throws IOException {
      if (closed) {
        throw new IOException("Stream is already closed");
      }
      if (n <= 0) {
        return 0L;
      }
      long remaining = n;
      byte[] discard = new byte[SKIP_BUFFER_SIZE];
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
        throw new IOException(re);
      }
    }

    private static boolean isAlreadyClosed(Throwable t) {
      String msg = t.getMessage();
      return msg != null && msg.toLowerCase(Locale.ROOT).contains("already closed");
    }
  }

  OutputStream pushStream(String path) throws AzureBlobException {
    path = sanitizedFilePath(path);

    if (!parentDirectoryExist(path)) {
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

  void close() {}

  @VisibleForTesting
  void deleteContainerForTests() {
    try {
      containerClient.delete();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() != HTTP_NOT_FOUND) {
        throw e;
      }
    }
  }

  private Collection<String> deleteBlobs(Collection<String> entries) throws AzureBlobException {
    if (entries.isEmpty()) {
      return Set.of();
    }

    Set<String> deletedPaths = new HashSet<>();
    List<String> all = new ArrayList<>(entries);

    for (int start = 0; start < all.size(); start += DELETE_BATCH_SIZE) {
      List<String> chunk = all.subList(start, Math.min(start + DELETE_BATCH_SIZE, all.size()));

      // The batch API addresses sub-requests by full blob URL, not container-relative path; keep
      // an inverse map so we can identify which chunk entries 404'd from sub-exception URLs.
      List<String> blobUrls = new ArrayList<>(chunk.size());
      Map<String, String> urlToPath = CollectionUtil.newHashMap(chunk.size());
      for (String path : chunk) {
        String url = containerClient.getBlobClient(path).getBlobUrl();
        blobUrls.add(url);
        urlToPath.put(url, path);
      }

      try {
        batchClient.deleteBlobs(blobUrls, null).forEach(r -> {});
        deletedPaths.addAll(chunk);
      } catch (BlobBatchStorageException e) {
        Set<String> notFound = new HashSet<>();
        int subExceptionCount = 0;
        for (BlobStorageException sub : e.getBatchExceptions()) {
          subExceptionCount++;
          if (sub.getStatusCode() != HTTP_NOT_FOUND) {
            throw new AzureBlobException(
                String.format(
                    Locale.ROOT,
                    "Batch delete failed (HTTP %d on %s)",
                    sub.getStatusCode(),
                    subRequestUrl(sub)),
                e);
          }
          String path = urlToPath.get(subRequestUrl(sub));
          if (path != null) {
            notFound.add(path);
          } else if (log.isWarnEnabled()) {
            log.warn(
                "Could not map batch sub-response URL {} back to a chunk path", subRequestUrl(sub));
          }
        }

        // URL attribution missed a sub-exception (canonical-form drift): fall back to
        // "whole chunk not deleted" so the strict check in delete() still fires.
        if (notFound.size() != subExceptionCount) {
          notFound.addAll(chunk);
        }

        if (log.isDebugEnabled()) {
          log.debug("Batch delete tolerated {} not-found sub-responses", notFound.size());
        }

        for (String path : chunk) {
          if (!notFound.contains(path)) {
            deletedPaths.add(path);
          }
        }
      } catch (BlobStorageException e) {
        throw handleBlobException(e);
      }
    }

    return deletedPaths;
  }

  /** Extracts the request URL from a batch sub-exception; returns {@code "<unknown>"} on null. */
  private static String subRequestUrl(BlobStorageException sub) {
    HttpResponse response = sub.getResponse();
    HttpRequest request = response == null ? null : response.getRequest();
    URL url = request == null ? null : request.getUrl();
    return url == null ? "<unknown>" : url.toString();
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

  String sanitizedPath(String path) throws AzureBlobException {
    String sanitizedPath = path.trim();
    while (sanitizedPath.startsWith(BLOB_FILE_PATH_DELIMITER)) {
      sanitizedPath = sanitizedPath.substring(1).trim();
    }

    return sanitizedPath;
  }

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

  String sanitizedDirPath(String path) throws AzureBlobException {
    String sanitizedPath = sanitizedPath(path);

    if (!sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
      sanitizedPath += BLOB_FILE_PATH_DELIMITER;
    }

    return sanitizedPath;
  }

  static AzureBlobException handleBlobException(BlobStorageException e) {
    String errMessage =
        String.format(
            Locale.ROOT,
            "Azure Blob Storage error: [statusCode=%s] [errorCode=%s] [message=%s]",
            e.getStatusCode(),
            e.getErrorCode(),
            e.getMessage());

    log.error(errMessage);

    if (e.getStatusCode() == HTTP_NOT_FOUND) {
      return new AzureBlobNotFoundException(errMessage, e);
    } else {
      return new AzureBlobException(errMessage, e);
    }
  }
}
