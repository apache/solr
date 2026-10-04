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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.DelegatingBackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that overwriting shard backup metadata never deletes the previous metadata file first.
 * This test deliberately uses only the {@link BackupRepository} API that predates the {@code
 * writeBytes} method, so it also compiles and runs against the code from before that change, where
 * the overwrite deleted the existing file and this test fails.
 */
public class ShardBackupMetadataOverwriteTest extends SolrTestCase {

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
  public void testStoreDoesNotDeleteExistingMetadata() throws Exception {
    metadata("uniq1", "orig1", new Checksum(1L, 10)).store(repository, folder, shardBackupId);

    RecordingBackupRepository recording = new RecordingBackupRepository(repository);
    metadata("uniq2", "orig2", new Checksum(2L, 20)).store(recording, folder, shardBackupId);

    assertTrue("overwrite must not delete the previous metadata file", recording.deleted.isEmpty());

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

    RecordingBackupRepository(BackupRepository delegate) {
      setDelegate(delegate);
    }

    @Override
    public void delete(URI path, Collection<String> files) throws IOException {
      for (String file : files) {
        deleted.add(resolve(path, file));
      }
      super.delete(path, files);
    }
  }
}
