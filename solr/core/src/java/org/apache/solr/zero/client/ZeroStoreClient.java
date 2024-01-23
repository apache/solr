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

package org.apache.solr.zero.client;

import com.codahale.metrics.Counter;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.apache.solr.zero.util.IndexInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a wrapper around BackupRepository for easy reading and writing to an arbitrary
 * underlying Zero store.
 */
public class ZeroStoreClient {

  static final int CHUNK_SIZE = 16 * 1024 * 1024; // 16 MBs

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Using {@link BackupRepository} to instantiate the Zero store. That class will have to be
   * eventually renamed as with this change its role extends beyond backups (keeping it as is to
   * simplify looking at the Zero branch).
   */
  protected final BackupRepository zeroRepository;

  private static final String METRIC_BASE_NAME = "ZeroStore";

  /**
   * Identifiers for file pusher/puller thread pools that run on all Solr nodes containing Zero
   * collections
   */
  private static final String ZERO_STORE_FILE_PUSHER_THREAD_POOL = "ZeroStoreFilePusherThreadPool";

  private static final String ZERO_STORE_FILE_PULLER_THREAD_POOL = "ZeroStoreFilePullerThreadPool";
  private final ExecutorService filePushExecutor;
  private final ExecutorService filePullExecutor;

  private final Counter bytesPushed;
  private final Counter numFilesSuccessfullyPushed;
  private final Counter bytesPulled;
  private final Counter numFilesSuccessfullyPulled;

  public ZeroStoreClient(
      BackupRepository zeroRepository,
      SolrMetricManager metricManager,
      final int numFilePusherThreads,
      final int numFilePullerThreads) {
    this.zeroRepository = zeroRepository;

    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    MetricRegistry registry = metricManager.registry(registryName);

    // Executor for pushing individual files to Zero store
    ExecutorService unwrappedFilePushExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            numFilePusherThreads, new NamedThreadFactory(ZERO_STORE_FILE_PUSHER_THREAD_POOL));
    this.filePushExecutor =
        new InstrumentedExecutorService(
            unwrappedFilePushExecutor, registry, METRIC_BASE_NAME + ".push.execution");

    // Executor for pulling individual files from Zero store
    ExecutorService unwrappedFilePullExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            numFilePullerThreads, new NamedThreadFactory(ZERO_STORE_FILE_PULLER_THREAD_POOL));
    this.filePullExecutor =
        new InstrumentedExecutorService(
            unwrappedFilePullExecutor, registry, METRIC_BASE_NAME + ".pull.execution");

    SolrMetricsContext solrMetricsContext =
        new SolrMetricsContext(
            metricManager, registryName, SolrMetricProducer.getUniqueMetricTag(this, null));

    bytesPushed = solrMetricsContext.counter("push.numBytes", METRIC_BASE_NAME);
    numFilesSuccessfullyPushed =
        solrMetricsContext.counter("push.numFilesSuccessful", METRIC_BASE_NAME);
    solrMetricsContext.gauge(() -> numFilePusherThreads, true, "push.numThreads", METRIC_BASE_NAME);

    bytesPulled = solrMetricsContext.counter("pull.numBytes", METRIC_BASE_NAME);
    numFilesSuccessfullyPulled =
        solrMetricsContext.counter("pull.numFilesSuccessful", METRIC_BASE_NAME);
    solrMetricsContext.gauge(() -> numFilePullerThreads, true, "pull.numThreads", METRIC_BASE_NAME);
  }

  /**
   * Replaces the special SHARD_METADATA_ZERO_FILENAME file on the Zero store for the core by a new
   * version passed as a {@link ZeroStoreShardMetadata} instance.
   *
   * @param zeroShardMetadataFile description of the Zero shard index data for which to write the
   *     metadata file
   * @param shardMetadata Zero shard metadata content to be serialized and written to the Zero store
   */
  public void pushShardMetadata(
      ZeroFile.WithLocal zeroShardMetadataFile, ZeroStoreShardMetadata shardMetadata)
      throws ZeroException {
    if (zeroShardMetadataFile.pathEmpty())
      throw new ZeroException("Can't write shard metadata file to empty path");
    try {
      createCoreStorage(zeroShardMetadataFile);
      String zcmJson = shardMetadata.toJson();
      OutputStream os =
          zeroRepository.createOutput(zeroShardMetadataFile.getFileURI(zeroRepository));
      os.write(zcmJson.getBytes(StandardCharsets.UTF_8));
      os.close();
    } catch (IOException e) {
      throw handleIOException(e);
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }

  /**
   * Reads the special CORE_METADATA_ZERO_FILENAME file from the Zero store for the core and returns
   * the corresponding {@link ZeroStoreShardMetadata} object.
   *
   * @param zeroShardMetadataFile description of the Zero shard index data to get metadata for
   * @return <code>null</code> if the core does not exist on the Zero store or method {@link
   *     #pushShardMetadata} was never called for it. Otherwise returns the latest value written
   *     using {@link #pushShardMetadata} ("latest" here based on the consistency model of the
   *     underlying store, in practice the last value written by any server given the strong
   *     consistency of the Salesforce S3 implementation).
   */
  public ZeroStoreShardMetadata pullShardMetadata(ZeroFile zeroShardMetadataFile)
      throws ZeroException {
    try {

      if (!shardMetadataExists(zeroShardMetadataFile)) {
        return null;
      }
      try (IndexInput ii = pullStream(zeroShardMetadataFile)) {
        OutputStream os = new ByteArrayOutputStream();
        readIndexInput(ii, os);
        String decodedJson = os.toString();
        os.close();
        return ZeroStoreShardMetadata.fromJson(decodedJson);
      }
    } catch (IOException e) {
      throw handleIOException(e);
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }

  /**
   * Returns an input stream for the given Zero store file. The caller must close the stream when
   * done.
   *
   * @param zeroFile description of the Zero shard index data to get a stream from
   * @return the file's input stream
   */
  public IndexInput pullStream(ZeroFile zeroFile) throws ZeroException {
    if (zeroFile.pathEmpty()) throw new ZeroException("Can't read from empty Zero store path");
    try {
      return zeroRepository.openInput(
          zeroFile.getShardDirectoryURI(zeroRepository),
          zeroFile.getZeroFileName(),
          IOContext.DEFAULT);
    } catch (IOException e) {
      throw handleIOException(e);
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }

  /**
   * There are two interfaces to push a Zero core (shard) to the Zero store. In any case writes are
   * done file by file. This interface writes to the external Zero store using an input stream for
   * the given Zero store file.
   *
   * @param zeroFile description of the Zero store file to push data to
   * @param is input stream of the file to push, which will be closed by this method after use
   */
  public void pushStream(ZeroFile zeroFile, InputStream is) throws ZeroException {
    if (zeroFile.pathEmpty()) throw new ZeroException("Can't write to empty Zero store path");
    try {
      createCoreStorage(zeroFile);
      OutputStream os = zeroRepository.createOutput(zeroFile.getFileURI(zeroRepository));
      is.transferTo(os);
      is.close();
      os.close();
    } catch (IOException e) {
      throw handleIOException(e);
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }

  /**
   * Checks if the shard index data with the given Zero store shard metadata file exists. This
   * method could actually be used to check the existence of any Zero store file, but it happens to
   * only be used for shard metadata files.
   *
   * @param zeroShardMetadataFile description of the Zero store core (shard) metadata file to check
   * @return true if the metadata file exists on the Zero store
   */
  public boolean shardMetadataExists(ZeroFile zeroShardMetadataFile) throws ZeroException {
    try {
      return zeroRepository.exists(zeroShardMetadataFile.getFileURI(zeroRepository));
    } catch (IOException e) {
      throw handleIOException(e);
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }

  /**
   * Batch delete files from the Zero store. Any file path that specifies a non-existent file will
   * not be treated as an error and should return success.
   *
   * @param zeroFiles list of Zero file to be deleted
   */
  public void deleteZeroFiles(Collection<ZeroFile> zeroFiles) {
    // TODO do we need to do something specific around deleting directories?
    var files =
        zeroFiles.stream()
            .collect(Collectors.groupingBy(f -> f.getShardDirectoryURI(zeroRepository)));

    for (Map.Entry<URI, List<ZeroFile>> entry : files.entrySet()) {
      URI baseURI = entry.getKey();
      List<String> filesName =
          entry.getValue().stream().map(ZeroFile::getZeroFileName).collect(Collectors.toList());
      try {
        zeroRepository.delete(baseURI, filesName);
      } catch (IOException e) {
        if (log.isErrorEnabled()) {
          log.error(
              "Could not delete one of the following files in directory {} : {}",
              baseURI.toString(),
              StrUtils.join(filesName, ','),
              e);
        }
      }
    }
  }

  /** Lists all file names within the given path */
  public Set<ZeroFile> listShardZeroFiles(String collectionName, String shardName) {
    try {
      URI shardURI = getShardURI(collectionName, shardName);
      return Arrays.stream(zeroRepository.listAll(shardURI))
          .map(zeroFileName -> new ZeroFile(collectionName, shardName, zeroFileName))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      log.error(
          "Error while listing file for collection={} shard={}", collectionName, shardName, e);
      return new LinkedHashSet<>();
    }
  }

  public Set<ZeroFile> listCollectionZeroFiles(String collectionName) {
    try {
      URI collectionURI = getCollectionURI(collectionName);
      return Arrays.stream(zeroRepository.listAll(collectionURI))
          .flatMap(shardName -> listShardZeroFiles(collectionName, shardName).stream())
          .collect(Collectors.toSet());
    } catch (Exception e) {
      log.error("Error while listing file for collection={}", collectionName, e);
      return new LinkedHashSet<>();
    }
  }

  /** Closes any resources used by the client */
  public void shutdown() {
    if (log.isInfoEnabled()) {
      log.info("ZeroStoreClient is shutting down");
    }

    ExecutorUtil.shutdownAndAwaitTermination(filePushExecutor);
    if (log.isInfoEnabled()) {
      log.info("{} has shutdown", ZERO_STORE_FILE_PUSHER_THREAD_POOL);
    }
    ExecutorUtil.shutdownAndAwaitTermination(filePullExecutor);
    if (log.isInfoEnabled()) {
      log.info("{} has shutdown", ZERO_STORE_FILE_PULLER_THREAD_POOL);
    }

    try {
      zeroRepository.close();
      log.info("Zero store repository has shutdown");
    } catch (IOException e) {
      log.error("Zero store repository couldn't close properly");
    }
  }

  /**
   * Helper function to wrap a generic IOException into a ZeroException
   *
   * @param ioe exception to wrap
   * @return wrapping exception
   */
  private ZeroException handleIOException(IOException ioe) {
    String errMessage =
        String.format(Locale.ROOT, "A Zero store IOException was thrown! %s", ioe.toString());
    return new ZeroException(errMessage, ioe);
  }

  /**
   * Required for some {@link BackupRepository} like {@link LocalFileSystemRepository}. Others do
   * not need that like S3 since there are no directories, just keys with a prefix meaning no need
   * to create the empty prefix before creating the file method is synchronized to avoid multi
   * threading issues
   *
   * @param zeroFile file for which we want to be sure the directory hierarchy exists
   */
  private synchronized void createCoreStorage(ZeroFile zeroFile) throws IOException {
    try {
      URI collectionDirURI = zeroFile.getCollectionDirectoryURI(zeroRepository);
      try {
        // TODO the collection directory should be created once when collection created
        if (!zeroRepository.exists(collectionDirURI)) {
          zeroRepository.createDirectory(collectionDirURI);
        }
      } catch (FileAlreadyExistsException e) {
        log.warn(
            "Could not create collection directory as it already exists, collection={}",
            zeroFile.getCollectionName(),
            e);
      }

      // Maybe the collection directory was already created by another Solr node, but we anyway
      // need to try creating the shard directory.
      URI shardDirURI = zeroFile.getShardDirectoryURI(zeroRepository);
      try {
        if (!zeroRepository.exists(shardDirURI)) {
          zeroRepository.createDirectory(shardDirURI);
        }
      } catch (FileAlreadyExistsException e) {
        log.warn(
            "Could not create shard directory as it already exists, collection={} shard={}",
            zeroFile.getCollectionName(),
            zeroFile.getShardName(),
            e);
      }
    } catch (IOException e) {
      log.warn(
          "Error while creating directory for collection={} shard={}",
          zeroFile.getCollectionName(),
          zeroFile.getShardName(),
          e);
      throw e;
    }
  }

  public URI getCollectionURI(String collectionName) {
    URI baseURI = zeroRepository.createURI(zeroRepository.getBackupLocation(null));
    return zeroRepository.resolveDirectory(baseURI, collectionName);
  }

  public URI getShardURI(String collectionName, String shardName) {
    URI baseURI = zeroRepository.createURI(zeroRepository.getBackupLocation(null));
    return zeroRepository.resolveDirectory(baseURI, collectionName, shardName);
  }

  public static void readIndexInput(IndexInput ii, OutputStream os) throws IOException {
    byte[] buffer = new byte[CHUNK_SIZE];
    int bufferLen;
    long remaining = ii.length();
    while (remaining > 0) {
      bufferLen = remaining >= CHUNK_SIZE ? CHUNK_SIZE : (int) remaining;
      ii.readBytes(buffer, 0, bufferLen);
      os.write(buffer, 0, bufferLen);
      remaining -= bufferLen;
    }
  }

  public void deleteShardDirectory(String collectionName, String shardName) throws IOException {
    URI shardURI = getShardURI(collectionName, shardName);
    zeroRepository.deleteDirectory(shardURI);
  }

  public void deleteCollectionDirectory(String collectionName) throws IOException {
    URI collectionURI = getCollectionURI(collectionName);
    List<String> shardDirectoryName =
        new ArrayList<>(Arrays.asList(zeroRepository.listAll(collectionURI)));
    for (String d : shardDirectoryName) {
      deleteShardDirectory(collectionName, d);
    }
    zeroRepository.deleteDirectory(collectionURI);
  }

  /** Asynchronously push shard.metadata to Zero store, using the push thread pool. */
  public CompletableFuture<Void> pushShardMetadataAsync(
      ZeroFile.WithLocal zeroShardMetadataFile, ZeroStoreShardMetadata shardMetadata) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            pushShardMetadata(zeroShardMetadataFile, shardMetadata);
          } catch (ZeroException e) {
            throw new RuntimeException(
                "Exception while pushing " + zeroShardMetadataFile.getZeroFileName(), e);
          }
        },
        filePushExecutor);
  }

  /** Asynchronously push a local file to Zero store, using the push thread pool. */
  public CompletableFuture<Void> pushFileAsync(Directory dir, ZeroFile.WithLocal localZeroFile) {
    return CompletableFuture.runAsync(
        () -> pushFileToZeroStore(dir, localZeroFile), filePushExecutor);
  }

  private void pushFileToZeroStore(Directory dir, ZeroFile.WithLocal localZeroFile) {
    // This method pushes the normal segment files (as opposed to shard.metadata) and they should
    // have file size and checksum set
    assert localZeroFile.isChecksumPresent();
    // Use Lucene-level IndexInput to lock on file lifecycle; don't want files deleted from
    // underneath us
    try (IndexInput ii = dir.openInput(localZeroFile.getSolrFileName(), IOContext.READONCE);
        InputStream is = new IndexInputStream(ii)) {

      pushStream(localZeroFile, is);

      numFilesSuccessfullyPushed.inc();
      long pushedFileLength = dir.fileLength(localZeroFile.getSolrFileName());
      bytesPushed.inc(pushedFileLength);
      assert pushedFileLength == localZeroFile.getFileSize();
    } catch (Exception e) {
      throw new RuntimeException(
          "Exception while pushing file: " + localZeroFile.getSolrFileName(), e);
    }
  }

  /** Asynchronously pull a local file from Zero store, using the pull thread pool, */
  public CompletableFuture<Void> pullFileAsync(Directory dir, ZeroFile.WithLocal fileToDownload) {
    return CompletableFuture.runAsync(
        () -> pullFileFromZeroStore(dir, fileToDownload), filePullExecutor);
  }

  private void pullFileFromZeroStore(Directory destDir, ZeroFile.WithLocal fileToDownload) {
    try (IndexOutput io =
            destDir.createOutput(
                fileToDownload.getSolrFileName(), DirectoryFactory.IOCONTEXT_NO_CACHE);
        IndexInput ii = pullStream(fileToDownload)) {

      if (log.isDebugEnabled()) {
        log.debug(
            "Copying {} from Zero store",
            fileToDownload
                .getSolrFileName()); // Logline has MDC, no need to add core/shard/replica info
      }
      io.copyBytes(ii, ii.length());
      numFilesSuccessfullyPulled.inc();
      bytesPulled.inc(fileToDownload.getFileSize());
    } catch (ZeroException | IOException ex) {
      throw new RuntimeException(
          "Exception while pulling file: " + fileToDownload.getSolrFileName(), ex);
    }
  }
}
