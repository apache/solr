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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

public class RetryingBackupRepositoryTest extends SolrTestCase {

  private final AtomicInteger attempts = new AtomicInteger();

  /** A repository whose copies throw {@code failure} for the first {@code failures} attempts. */
  private BackupRepository failingRepo(int failures, IOException failure) {
    LocalFileSystemRepository delegate =
        new LocalFileSystemRepository() {
          private void maybeFail() throws IOException {
            if (attempts.incrementAndGet() <= failures) {
              throw failure;
            }
          }

          @Override
          public void copyIndexFileTo(URI src, String srcName, Directory dest, String destName)
              throws IOException {
            maybeFail();
          }

          @Override
          public void copyIndexFileFrom(Directory src, String srcName, URI dest, String destName)
              throws IOException {
            maybeFail();
          }

          @Override
          public void copyFileTo(URI src, String name, Directory dest) throws IOException {
            maybeFail();
          }

          @Override
          public void copyFileFrom(Directory src, String name, URI dest) throws IOException {
            maybeFail();
          }
        };
    return new RetryingBackupRepository(delegate, 2, 1);
  }

  private void copyAll(BackupRepository repo) throws IOException {
    repo.copyIndexFileTo(null, "f", null, "f");
  }

  @Test
  public void testRetriesTransientFailureUntilSuccess() throws Exception {
    copyAll(failingRepo(2, new SocketTimeoutException("Read timed out")));
    assertEquals(3, attempts.get());
  }

  @Test
  public void testGivesUpAfterMaxRetries() {
    BackupRepository repo = failingRepo(10, new SocketTimeoutException("Read timed out"));
    expectThrows(SocketTimeoutException.class, () -> copyAll(repo));
    assertEquals(3, attempts.get());
  }

  @Test
  public void testDoesNotRetryPermanentFailures() {
    BackupRepository corrupt = failingRepo(10, new CorruptIndexException("bad", "test"));
    expectThrows(CorruptIndexException.class, () -> copyAll(corrupt));
    assertEquals(1, attempts.get());

    attempts.set(0);
    BackupRepository missing = failingRepo(10, new NoSuchFileException("f"));
    expectThrows(NoSuchFileException.class, () -> copyAll(missing));
    assertEquals(1, attempts.get());
  }

  @Test
  public void testAllCopyMethodsRetry() throws Exception {
    BackupRepository repo = failingRepo(1, new SocketTimeoutException("Read timed out"));
    repo.copyIndexFileFrom(null, "f", (URI) null, "f");
    repo.copyFileTo(null, "f", null);
    repo.copyFileFrom(null, "f", null);
    // only the very first attempt fails, so the first call retries once and the others don't
    assertEquals(4, attempts.get());
  }

  @Test
  public void testFactoryWrapsByDefaultAndCanBeDisabled() throws Exception {
    try (SolrResourceLoader loader = new SolrResourceLoader(createTempDir())) {
      BackupRepositoryFactory factory = new BackupRepositoryFactory(new PluginInfo[0]);
      try (BackupRepository repo = factory.newInstance(loader)) {
        assertTrue(repo instanceof RetryingBackupRepository);
      }

      System.setProperty(BackupRepositoryFactory.MAX_COPY_RETRIES_PROP, "0");
      try (BackupRepository repo = factory.newInstance(loader)) {
        assertTrue(repo instanceof LocalFileSystemRepository);
      }
    }
  }
}
