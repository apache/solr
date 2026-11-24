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
package org.apache.solr.core.backup.repository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps another {@link BackupRepository}, retrying its file-copy methods when they fail with a
 * transient {@link IOException}.
 */
public class RetryingBackupRepository extends DelegatingBackupRepository {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @FunctionalInterface
  private interface Copy {
    void run() throws IOException;
  }

  private final int maxRetries;
  private final long initialBackoffMillis;

  /**
   * @param delegate the repository to wrap
   * @param maxRetries number of re-attempts after the first failure; 0 means no retries
   * @param initialBackoffMillis delay before the first retry, doubled for each later retry
   */
  public RetryingBackupRepository(
      BackupRepository delegate, int maxRetries, long initialBackoffMillis) {
    this.delegate = delegate;
    this.maxRetries = maxRetries;
    this.initialBackoffMillis = initialBackoffMillis;
  }

  @Override
  public void copyFileFrom(Directory sourceDir, String fileName, URI dest) throws IOException {
    withRetries(fileName, () -> super.copyFileFrom(sourceDir, fileName, dest));
  }

  @Override
  public void copyFileTo(URI sourceRepo, String fileName, Directory dest) throws IOException {
    withRetries(fileName, () -> super.copyFileTo(sourceRepo, fileName, dest));
  }

  @Override
  public void copyIndexFileFrom(
      Directory sourceDir, String sourceFileName, URI destDir, String destFileName)
      throws IOException {
    withRetries(
        sourceFileName,
        () -> super.copyIndexFileFrom(sourceDir, sourceFileName, destDir, destFileName));
  }

  @Override
  public void copyIndexFileTo(
      URI sourceRepo, String sourceFileName, Directory dest, String destFileName)
      throws IOException {
    withRetries(
        sourceFileName,
        () -> super.copyIndexFileTo(sourceRepo, sourceFileName, dest, destFileName));
  }

  private void withRetries(String fileName, Copy copy) throws IOException {
    long backoff = initialBackoffMillis;
    for (int attempt = 0; ; attempt++) {
      try {
        copy.run();
        return;
      } catch (IOException e) {
        if (attempt >= maxRetries || !isRetryable(e) || Thread.currentThread().isInterrupted()) {
          throw e;
        }
        int attemptNumber = attempt + 1;
        int totalAttempts = maxRetries + 1;
        log.warn(
            "Copy of '{}' failed (attempt {} of {}), retrying in {}ms",
            fileName,
            attemptNumber,
            totalAttempts,
            backoff,
            e);
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          e.addSuppressed(ie);
          throw e;
        }
        backoff *= 2;
      }
    }
  }

  /** Permanent failures (corruption, missing, existing or unreadable files) are not retried. */
  static boolean isRetryable(IOException e) {
    return !(e instanceof CorruptIndexException
        || e instanceof FileNotFoundException
        || e instanceof NoSuchFileException
        || e instanceof FileAlreadyExistsException
        || e instanceof AccessDeniedException);
  }
}
