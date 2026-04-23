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
package org.apache.solr.core.backup;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.Test;

/** Unit tests for {@link ShardBackupMetadata} */
public class ShardBackupMetadataTest extends SolrTestCase {

  @Test
  public void testStorePreservesDataOnWriteFailure() throws Exception {
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    repo.init(new NamedList<>());

    URI baseDir = createTempDir().toUri();
    repo.createDirectory(baseDir);

    ShardBackupId shardBackupId = new ShardBackupId("shard1", BackupId.zero());

    // Store original metadata
    ShardBackupMetadata original = ShardBackupMetadata.empty();
    original.addBackedFile("unique1", "file1.si", new Checksum(12345L, 100L));
    original.store(repo, baseDir, shardBackupId);

    // Verify original is readable
    ShardBackupMetadata readBack = ShardBackupMetadata.from(repo, baseDir, shardBackupId);
    assertNotNull("Original metadata should be readable", readBack);
    assertTrue(readBack.listOriginalFileNames().contains("file1.si"));

    // Now attempt to overwrite using a repo that fails during createOutput.
    // With the bug (delete-before-write), this destroys the original metadata.
    // With the fix, the original metadata survives because no delete occurs.
    FailingOutputRepository failingRepo = new FailingOutputRepository(repo);
    try {
      ShardBackupMetadata replacement = ShardBackupMetadata.empty();
      replacement.addBackedFile("unique2", "file2.si", new Checksum(67890L, 200L));
      replacement.store(failingRepo, baseDir, shardBackupId);
      fail("Expected IOException from failing repository");
    } catch (IOException expected) {
      // expected
    }

    // The original metadata should still be readable after the failed overwrite
    ShardBackupMetadata afterFailure = ShardBackupMetadata.from(repo, baseDir, shardBackupId);
    assertNotNull("Original metadata should survive a failed overwrite attempt", afterFailure);
    assertTrue(
        "Original file should still be in metadata after failed overwrite",
        afterFailure.listOriginalFileNames().contains("file1.si"));

    repo.close();
  }

  /**
   * A BackupRepository wrapper that delegates all operations to a real repository but throws
   * IOException on createOutput, simulating a write failure.
   */
  private static class FailingOutputRepository extends LocalFileSystemRepository {
    private final BackupRepository delegate;

    FailingOutputRepository(BackupRepository delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean exists(URI path) throws IOException {
      return delegate.exists(path);
    }

    @Override
    public void delete(URI path, java.util.Collection<String> files) throws IOException {
      delegate.delete(path, files);
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
      return delegate.resolve(baseUri, pathComponents);
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
      throw new IOException("Simulated write failure");
    }

    @Override
    public void close() {}
  }
}
