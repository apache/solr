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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.backup.repository.AbstractBackupRepository;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A concrete implementation of {@link BackupRepository} interface supporting backup/restore of Solr
 * indexes to Azure Blob Storage.
 */
public class AzureBlobBackupRepository extends AbstractBackupRepository {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String BLOB_SCHEME = "blob";
  private static final int CHUNK_SIZE = 16 * 1024 * 1024;
  private static final int COPY_BUFFER_SIZE = 8192;

  private AzureBlobStorageClient client;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    AzureBlobBackupRepositoryConfig backupConfig = new AzureBlobBackupRepositoryConfig(this.config);

    if (client != null) {
      client.close();
    }

    this.client = backupConfig.buildClient();
  }

  @VisibleForTesting
  public void setClient(AzureBlobStorageClient client) {
    this.client = client;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getConfigProperty(String name) {
    return (T) this.config.get(name);
  }

  @Override
  public URI createURI(String location) {
    if (StrUtils.isNullOrEmpty(location)) {
      throw new IllegalArgumentException("cannot create URI with an empty location");
    }

    URI result;
    try {
      if (location.startsWith(BLOB_SCHEME + ":")) {
        result = new URI(location);
      } else if (location.startsWith("/")) {
        result = new URI(BLOB_SCHEME, "", location, null);
      } else {
        result = new URI(BLOB_SCHEME, "", "/" + location, null);
      }
      return result;
    } catch (URISyntaxException ex) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ex);
    }
  }

  @Override
  public URI createDirectoryURI(String location) {
    if (StrUtils.isNullOrEmpty(location)) {
      throw new IllegalArgumentException("cannot create URI with an empty location");
    }

    if (!location.endsWith("/")) {
      location += "/";
    }

    return createURI(location);
  }

  @Override
  public URI resolve(URI baseUri, String... pathComponents) {
    if (!BLOB_SCHEME.equalsIgnoreCase(baseUri.getScheme())) {
      throw new IllegalArgumentException("URI must begin with 'blob:' scheme");
    }

    String path = baseUri + "/" + String.join("/", pathComponents);
    return URI.create(path).normalize();
  }

  @Override
  public URI resolveDirectory(URI baseUri, String... pathComponents) {
    if (pathComponents.length > 0) {
      if (!pathComponents[pathComponents.length - 1].endsWith("/")) {
        pathComponents[pathComponents.length - 1] = pathComponents[pathComponents.length - 1] + "/";
      }
    } else {
      if (!baseUri.toString().endsWith("/")) {
        baseUri = URI.create(baseUri + "/");
      }
    }
    return resolve(baseUri, pathComponents);
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot create directory to a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("Create directory '{}'", blobPath);
    }

    try {
      client.createDirectory(blobPath);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to create directory " + blobPath, e);
    }
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot delete directory with a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("Delete directory '{}'", blobPath);
    }

    try {
      client.deleteDirectory(blobPath);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to delete directory " + blobPath, e);
    }
  }

  @Override
  public void delete(URI path, Collection<String> files) throws IOException {
    Objects.requireNonNull(path, "cannot delete with a null URI");
    Objects.requireNonNull(files, "cannot delete with a null files collection");

    String basePath = getBlobPath(path);

    try {
      if (!client.isDirectory(basePath)) {
        int lastSlash = basePath.lastIndexOf('/');
        basePath = lastSlash >= 0 ? basePath.substring(0, lastSlash) : "";
      }
    } catch (AzureBlobException e) {
      throw new IOException("Failed to check path type for " + basePath, e);
    }

    final String baseForPaths = basePath;
    Set<String> fullPaths =
        files.stream()
            .map(file -> (baseForPaths.isEmpty() ? file : baseForPaths + "/" + file))
            .collect(Collectors.toSet());

    if (log.isDebugEnabled()) {
      log.debug("Delete files '{}'", fullPaths);
    }

    try {
      client.delete(fullPaths);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to delete files " + fullPaths, e);
    }
  }

  @Override
  public boolean exists(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot check existence with a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("Check existence '{}'", blobPath);
    }

    try {
      return client.pathExists(blobPath);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to check existence of " + blobPath, e);
    }
  }

  @Override
  public PathType getPathType(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot get path type with a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("Get path type '{}'", blobPath);
    }

    try {
      if (client.isDirectory(blobPath)) {
        return BackupRepository.PathType.DIRECTORY;
      } else {
        return BackupRepository.PathType.FILE;
      }
    } catch (AzureBlobException e) {
      throw new IOException("Failed to get path type for " + blobPath, e);
    }
  }

  @Override
  public String[] listAll(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot list with a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("List all '{}'", blobPath);
    }

    try {
      return client.listDir(blobPath);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to list directory " + blobPath, e);
    }
  }

  @Override
  public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
    Objects.requireNonNull(dirPath, "cannot open input with a null URI");
    Objects.requireNonNull(fileName, "cannot open input with a null fileName");

    String base = getBlobPath(dirPath);
    String blobPath = base.endsWith("/") ? base + fileName : base + "/" + fileName;

    if (log.isDebugEnabled()) {
      log.debug("Open input '{}'", blobPath);
    }

    try {
      return new AzureBlobIndexInput(blobPath, client, client.length(blobPath));
    } catch (AzureBlobException e) {
      throw new IOException("Failed to open input stream for " + blobPath, e);
    }
  }

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    Objects.requireNonNull(path, "cannot create output with a null URI");

    String blobPath = getBlobPath(path);

    if (log.isDebugEnabled()) {
      log.debug("Create output '{}'", blobPath);
    }

    try {
      return client.pushStream(blobPath);
    } catch (AzureBlobException e) {
      throw new IOException("Failed to create output stream for " + blobPath, e);
    }
  }

  @Override
  public void copyIndexFileFrom(
      Directory sourceDir, String sourceFileName, URI dest, String destFileName)
      throws IOException {
    Objects.requireNonNull(sourceDir, "cannot copy with a null sourceDir");
    Objects.requireNonNull(sourceFileName, "cannot copy with a null sourceFileName");
    Objects.requireNonNull(dest, "cannot copy with a null dest");

    String destPath = getBlobPath(dest);

    String blobPath = destPath.endsWith("/") ? destPath + destFileName : destPath;

    if (log.isDebugEnabled()) {
      log.debug("Copy index file from '{}' to '{}'", sourceFileName, blobPath);
    }

    String parentDir =
        blobPath.contains("/") ? blobPath.substring(0, blobPath.lastIndexOf('/') + 1) : "";
    try {
      if (!parentDir.isEmpty()) {
        client.createDirectory(parentDir);
      }
    } catch (AzureBlobException e) {
      // ignore; write will surface real issues
    }

    try (IndexInput input = sourceDir.openInput(sourceFileName, IOContext.DEFAULT);
        OutputStream output = client.pushStream(blobPath)) {
      byte[] buffer = new byte[COPY_BUFFER_SIZE];
      long remaining = input.length();
      while (remaining > 0) {
        int toRead = (int) Math.min(buffer.length, remaining);
        input.readBytes(buffer, 0, toRead);
        output.write(buffer, 0, toRead);
        remaining -= toRead;
      }
    } catch (AzureBlobException e) {
      throw new IOException("Failed to copy file from " + sourceFileName + " to " + blobPath, e);
    }
  }

  @Override
  public void copyIndexFileTo(
      URI sourceDir, String sourceFileName, Directory dest, String destFileName)
      throws IOException {
    if (StrUtils.isNullOrEmpty(sourceFileName)) {
      throw new IllegalArgumentException("must have a valid source file name to copy");
    }
    if (StrUtils.isNullOrEmpty(destFileName)) {
      throw new IllegalArgumentException("must have a valid destination file name to copy");
    }

    String basePath = getBlobPath(sourceDir);
    String blobPath;
    if (basePath.endsWith("/" + sourceFileName)
        || basePath.equals(sourceFileName)
        || basePath.equals("/" + sourceFileName)) {
      blobPath = basePath;
    } else {
      URI filePath = resolve(sourceDir, sourceFileName);
      blobPath = getBlobPath(filePath);
    }

    Instant start = Instant.now();
    if (log.isDebugEnabled()) {
      log.debug("Download started from blob '{}'", blobPath);
    }

    try (InputStream inputStream = client.pullStream(blobPath);
        IndexOutput indexOutput = dest.createOutput(destFileName, IOContext.DEFAULT)) {
      byte[] buffer = new byte[CHUNK_SIZE];
      int len;
      while ((len = inputStream.read(buffer)) != -1) {
        indexOutput.writeBytes(buffer, 0, len);
      }
    } catch (AzureBlobException e) {
      throw new IOException("Failed to copy file from " + blobPath + " to " + destFileName, e);
    }

    long timeElapsed = Duration.between(start, Instant.now()).toMillis();

    if (log.isInfoEnabled()) {
      log.info("Download from Azure Blob Storage '{}' finished in {}ms", blobPath, timeElapsed);
    }
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  private String getBlobPath(URI uri) {
    if (!BLOB_SCHEME.equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException("URI must begin with 'blob:' scheme");
    }
    return uri.getPath();
  }
}
