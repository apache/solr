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

import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Pushes a set of files to Blob, and works with listings.
 */
public class BlobPusher implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PATH_SEPARATOR = "/";
  private static final Pattern PATH_SPLITTER = Pattern.compile(PATH_SEPARATOR);

  /**
   * Size of the buffer used to transfer input-output stream to the {@link BackupRepository}.
   * The appropriate size depends on the {@link BackupRepository}.
   */
  public static final int STREAM_COPY_DEFAULT_BUFFER_SIZE = 16384;

  // WORK IN PROGRESS!!

  private final BackupRepository backupRepository;
  private final URI backupLocation;
  private final int streamCopyBufferSize;
  private final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("blobPusher");

  public BlobPusher(BackupRepository backupRepository, URI backupLocation, int streamCopyBufferSize) {
    this.backupRepository = Objects.requireNonNull(backupRepository);
    this.backupLocation = Objects.requireNonNull(backupLocation);
    this.streamCopyBufferSize = streamCopyBufferSize;
  }

  public void push(
      String blobDirPath,
      Collection<BlobFile> writes,
      IOUtils.IOFunction<BlobFile, InputStream> inputStreamSupplier,
      Collection<String> deletes)
      throws IOException {

    // update "foreign" listings
    //      TODO David

    // Send files to BackupRepository and delete our files too.
    log.debug("Pushing {}", writes);
    executeAll(pushFiles(blobDirPath, writes, inputStreamSupplier));
    log.debug("Deleting {}", deletes);
    deleteFiles(blobDirPath, deletes);

    // update "our" listing
    //      TODO David
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
      IOUtils.IOFunction<BlobFile, InputStream> inputStreamSupplier)
      throws IOException {
    URI blobDirUri = backupRepository.resolve(backupLocation, blobDirPath);
    createDirectories(blobDirUri, blobDirPath);
    return blobFiles.stream()
        .map(
            (blobFile) ->
                (Callable<Void>)
                    () -> {
                      try (InputStream in = inputStreamSupplier.apply(blobFile);
                           OutputStream out = backupRepository.createOutput(backupRepository.resolve(blobDirUri, blobFile.fileName()))) {
                        int bufferSize = (int) Math.min(blobFile.size(), streamCopyBufferSize);
                        org.apache.commons.io.IOUtils.copy(in, out, bufferSize);
                      }
                      return null;
                    })
        .collect(Collectors.toList());
  }

  private void createDirectories(URI blobDirUri, String blobDirPath) throws IOException {
    // Create the parent directories if needed.
    // The goal is to have a minimal number of calls to the backup repository in most cases.
    // Common case: the directory exists or the parent directory exists.
    // Try a direct call to 'createDirectory', avoiding a call to 'exists' in many cases.
    // The API says if the directory already exists, it is a no-op.
    try {
      backupRepository.createDirectory(blobDirUri);
    } catch (IOException e) {
      // Create the parent directories.
      URI pathUri = backupLocation;
      for (String pathElement : PATH_SPLITTER.split(blobDirPath)) {
        pathUri = backupRepository.resolve(pathUri, pathElement);
        backupRepository.createDirectory(pathUri);
      }
    }
  }

  private void deleteFiles(String blobDirPath, Collection<String> fileNames) throws IOException {
    URI blobDirUri = backupRepository.resolve(backupLocation, blobDirPath);
    backupRepository.delete(blobDirUri, fileNames, true);
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
}
