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

package org.apache.solr.blob;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Accesses a {@link BackupRepository} to store persistently directories files.
 * Provides support to:
 * <ul>
 *   <li>List and pull repository files.</li>
 *   <li>Push local files to the repository.</li>
 * </ul>
 */
public class BlobRepository implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PATH_SEPARATOR = "/";
  private static final Pattern PATH_SPLITTER = Pattern.compile(PATH_SEPARATOR);

  // WORK IN PROGRESS!!

  private final BackupRepository repository;
  private final URI repositoryLocation;
  private final ExecutorService executor;
  // Pool of stream buffers, one per executor thread, to reuse them as the buffer size may be large
  // to have efficient stream transfer to the remote repository.
  private final ThreadLocal<byte[]> streamBuffers;

  /**
   * @param repository The {@link BackupRepository} to push files to.
   * @param repositoryLocation Base location in the repository. This is used to build the URIs.
   * @param maxThreads       Maximum number of threads to push files concurrently.
   * @param streamBufferSize Size of the stream copy buffer, in bytes. This determines the size of the chunk
   *                         of data sent to the repository during files transfer. There is one buffer per thread.
   */
  public BlobRepository(BackupRepository repository, URI repositoryLocation, int maxThreads, int streamBufferSize) {
    this.repository = Objects.requireNonNull(repository);
    this.repositoryLocation = Objects.requireNonNull(repositoryLocation);
    executor = ExecutorUtil.newMDCAwareCachedThreadPool(
        maxThreads,
        new SolrNamedThreadFactory(BlobRepository.class.getSimpleName()));
    streamBuffers = ThreadLocal.withInitial(() -> new byte[streamBufferSize]);
  }

  /**
   * Pushes a set of files to the repository.
   * @param blobDirPath The path to the directory where to put the files.
   * @param writes The local files to push and write.
   * @param inputSupplier Supplies the {@link IndexInput} to read each file.
   * @param deletes The names of the files to delete in the directory.
   */
  public void push(
      String blobDirPath,
      Collection<BlobFile> writes,
      IOUtils.IOFunction<BlobFile, IndexInput> inputSupplier,
      Collection<String> deletes)
      throws IOException {

    // update "foreign" listings
    //      TODO David

    // Send files to repository and delete our files too.
    log.debug("Pushing {} to {}", writes, blobDirPath);
    executeAll(pushFiles(blobDirPath, writes, inputSupplier));
    log.debug("Deleting {}", deletes);
    deleteFiles(blobDirPath, deletes);

    // update "our" listing
    //      TODO David
  }

  /**
   * Pulls selected files from a directory in the repository.
   * @param blobDirPath The path to the directory where to list the files.
   * @param outputSupplier Supplies the {@link IndexOutput} to write each file.
   * @param fileFilter Selects the files to pull.
   */
  public void pull(
      String blobDirPath,
      IOUtils.IOFunction<BlobFile, IndexOutput> outputSupplier,
      Predicate<String> fileFilter)
      throws IOException {
    URI blobDirUri = repository.resolve(repositoryLocation, blobDirPath);
    List<BlobFile> blobFiles = list(blobDirUri, fileFilter);
    log.debug("Pulling {} from {}", blobFiles, blobDirPath);
    executeAll(pullFiles(blobDirUri, blobFiles, outputSupplier));
  }

  private void executeAll(List<Callable<Void>> actions) throws IOException {
    try {
      for (Future<Void> future : executor.invokeAll(actions)) {
        future.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  private List<Callable<Void>> pushFiles(
      String blobDirPath,
      Collection<BlobFile> blobFiles,
      IOUtils.IOFunction<BlobFile, IndexInput> inputSupplier)
      throws IOException {
    URI blobDirUri = repository.resolve(repositoryLocation, blobDirPath);
    createDirectory(blobDirUri, blobDirPath);
    return blobFiles.stream()
        .map(
            (blobFile) ->
                (Callable<Void>)
                    () -> {
                      try (IndexInput in = inputSupplier.apply(blobFile);
                           OutputStream out = repository.createOutput(repository.resolve(blobDirUri, blobFile.fileName()))) {
                        copyStream(in, out::write);
                      }
                      return null;
                    })
        .collect(Collectors.toList());
  }

  private void createDirectory(URI blobDirUri, String blobDirPath) throws IOException {
    // Create the parent directories if needed.
    // The goal is to have a minimal number of calls to the repository in most cases.
    // Common case: the directory exists or the parent directory exists.
    // Try a direct call to 'createDirectory', avoiding a call to 'exists' in many cases.
    // The API says if the directory already exists, it is a no-op.
    try {
      repository.createDirectory(blobDirUri);
    } catch (IOException e) {
      // Create the parent directories.
      URI pathUri = repositoryLocation;
      for (String pathElement : PATH_SPLITTER.split(blobDirPath)) {
        pathUri = repository.resolve(pathUri, pathElement);
        repository.createDirectory(pathUri);
      }
    }
  }

  private void copyStream(IndexInput input, BytesWriter writer) throws IOException {
    byte[] buffer = streamBuffers.get();
    long remaining = input.length();
    while (remaining > 0) {
      int length = (int) Math.min(buffer.length, remaining);
      input.readBytes(buffer, 0, length, false);
      writer.write(buffer, 0, length);
      remaining -= length;
    }
  }

  private void deleteFiles(String blobDirPath, Collection<String> fileNames) throws IOException {
    URI blobDirUri = repository.resolve(repositoryLocation, blobDirPath);
    repository.delete(blobDirUri, fileNames, true);
  }

  /**
   * Lists the files in a specific directory of the repository and select them with the provided filter.
   */
  private List<BlobFile> list(URI blobDirUri, Predicate<String> fileFilter) throws IOException {
    String[] fileNames = repository.listAll(blobDirUri);
    List<BlobFile> blobFiles = new ArrayList<>(fileNames.length);
    for (String fileName : fileNames) {
      BlobFile blobFile = new BlobFile(fileName, -1, -1);
      if (fileFilter.test(blobFile.fileName())) {
        blobFiles.add(blobFile);
      }
    }
    return blobFiles;
  }

  private List<Callable<Void>> pullFiles(
      URI blobDirUri,
      Collection<BlobFile> blobFiles,
      IOUtils.IOFunction<BlobFile, IndexOutput> outputSupplier) {
    return blobFiles.stream()
        .map(
            (blobFile) ->
                (Callable<Void>)
                    () -> {
                      try (IndexInput in = repository.openInput(blobDirUri, blobFile.fileName(), IOContext.READ);
                           IndexOutput out = outputSupplier.apply(blobFile)) {
                        copyStream(in, out::writeBytes);
                      }
                      return null;
                    })
        .collect(Collectors.toList());
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Writes bytes to a stream.
   */
  private interface BytesWriter {
    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this writer.
     */
    void write(byte b[], int off, int len) throws IOException;
  }
}
