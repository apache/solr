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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.DelegatingBackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ShardBackupMetadata} overwrite behavior. */
public class ShardBackupMetadataTest extends SolrTestCaseJ4 {

  private LocalFileSystemRepository repository;
  private URI folder;
  private ShardBackupId shardBackupId;

  @Before
  public void setUpRepo() throws Exception {
    repository = new LocalFileSystemRepository();
    repository.init(new NamedList<>());
    folder =
        repository.createURI(createTempDir("shard-backup-metadata").toAbsolutePath().toString());
    repository.createDirectory(folder);
    shardBackupId = new ShardBackupId("shard1", BackupId.zero());
  }

  @Test
  public void testStoreOverwritesAndReadsBack() throws Exception {
    metadata("uniq1", "orig1", new Checksum(1L, 10)).store(repository, folder, shardBackupId);
    metadata("uniq2", "orig2", new Checksum(2L, 20)).store(repository, folder, shardBackupId);

    ShardBackupMetadata loaded = ShardBackupMetadata.from(repository, folder, shardBackupId);
    assertNotNull(loaded);
    assertEquals(List.of("uniq2"), loaded.listUniqueFileNames());
    assertTrue(loaded.getFile("orig2").isPresent());
    assertEquals(2L, loaded.getFile("orig2").get().fileChecksum.checksum);
    assertTrue(loaded.getFile("orig1").isEmpty());
  }

  @Test
  public void testStoreDoesNotDeleteExistingMetadata() throws Exception {
    metadata("uniq1", "orig1", new Checksum(1L, 10)).store(repository, folder, shardBackupId);

    RecordingBackupRepository recording = new RecordingBackupRepository(repository);
    metadata("uniq2", "orig2", new Checksum(2L, 20)).store(recording, folder, shardBackupId);

    assertTrue("overwrite must not delete the previous metadata file", recording.deleted.isEmpty());
    assertTrue(
        "LocalFS writeAtomically writes a sibling temp file instead of createOutput",
        recording.created.isEmpty());

    ShardBackupMetadata loaded = ShardBackupMetadata.from(repository, folder, shardBackupId);
    assertEquals(List.of("uniq2"), loaded.listUniqueFileNames());
  }

  @Test
  public void testFailedOverwriteKeepsPreviousMetadata() throws Exception {
    metadata("uniq1", "orig1", new Checksum(1L, 10)).store(repository, folder, shardBackupId);

    FailingWriteRepository failing = new FailingWriteRepository(repository);
    expectThrows(
        IOException.class,
        () ->
            metadata("uniq2", "orig2", new Checksum(2L, 20)).store(failing, folder, shardBackupId));

    URI dest = repository.resolve(folder, shardBackupId.getBackupMetadataFilename());
    assertTrue(repository.exists(dest));
    ShardBackupMetadata loaded = ShardBackupMetadata.from(repository, folder, shardBackupId);
    assertNotNull(loaded);
    assertEquals(List.of("uniq1"), loaded.listUniqueFileNames());
    assertTrue(loaded.getFile("orig1").isPresent());
    assertTrue(loaded.getFile("orig2").isEmpty());
  }

  @Test
  public void testDefaultWriteAtomicallyUsesCreateOutputAndSkipsDelete() throws Exception {
    metadata("uniq1", "orig1", new Checksum(1L, 10)).store(repository, folder, shardBackupId);

    DefaultWriteRecordingRepository recording = new DefaultWriteRecordingRepository(repository);
    metadata("uniq2", "orig2", new Checksum(2L, 20)).store(recording, folder, shardBackupId);

    assertTrue(recording.deleted.isEmpty());
    assertEquals(1, recording.created.size());
    assertEquals(
        repository.resolve(folder, shardBackupId.getBackupMetadataFilename()),
        recording.created.get(0));

    ShardBackupMetadata loaded = ShardBackupMetadata.from(repository, folder, shardBackupId);
    assertEquals(List.of("uniq2"), loaded.listUniqueFileNames());
  }

  private static ShardBackupMetadata metadata(
      String uniqueFileName, String originalFileName, Checksum checksum) {
    ShardBackupMetadata created = ShardBackupMetadata.empty();
    created.addBackedFile(uniqueFileName, originalFileName, checksum);
    return created;
  }

  private static class RecordingBackupRepository extends DelegatingBackupRepository {
    final List<URI> deleted = new ArrayList<>();
    final List<URI> created = new ArrayList<>();

    RecordingBackupRepository(BackupRepository delegate) {
      setDelegate(delegate);
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
      created.add(path);
      return super.createOutput(path);
    }

    @Override
    public void delete(URI path, Collection<String> files) throws IOException {
      for (String file : files) {
        deleted.add(resolve(path, file));
      }
      super.delete(path, files);
    }
  }

  private static class FailingWriteRepository extends DelegatingBackupRepository {
    FailingWriteRepository(BackupRepository delegate) {
      setDelegate(delegate);
    }

    @Override
    public void writeAtomically(URI path, byte[] data) throws IOException {
      throw new IOException("injected write failure");
    }
  }

  /**
   * Uses the interface default {@code writeAtomically} so the test can observe {@code createOutput}
   * instead of the LocalFS temp-file override.
   */
  private static class DefaultWriteRecordingRepository extends RecordingBackupRepository {
    DefaultWriteRecordingRepository(BackupRepository delegate) {
      super(delegate);
    }

    @Override
    public void writeAtomically(URI path, byte[] data) throws IOException {
      try (OutputStream os = createOutput(path)) {
        os.write(data);
      }
    }
  }
}
