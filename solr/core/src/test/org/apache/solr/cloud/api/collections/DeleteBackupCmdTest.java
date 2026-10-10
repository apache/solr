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
package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Set;
import java.util.UUID;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link DeleteBackupCmd}. */
public class DeleteBackupCmdTest extends SolrTestCase {

  private BackupRepository repository;
  private URI backupUri;

  @Before
  public void setUpRepo() throws Exception {
    repository = new LocalFileSystemRepository();
    backupUri =
        repository.createDirectoryURI(
            createTempDir("backup_" + UUID.randomUUID()).toAbsolutePath().toString());
    new BackupFilePaths(repository, backupUri).createIncrementalBackupFolders();
  }

  @Test
  public void testDeleteBackupIdsIgnoresMissingZkStateDir() throws Exception {
    NamedList<Object> results = new NamedList<>();
    new DeleteBackupCmd(null)
        .deleteBackupIds(backupUri, repository, Set.of(BackupId.zero()), results);

    assertNotNull(results.get("deleted"));
    assertFalse(repository.exists(zkStateDir(BackupId.zero())));
  }

  @Test
  public void testDeleteBackupIdsRemovesExistingZkStateDir() throws Exception {
    URI zkStateDir = zkStateDir(BackupId.zero());
    repository.createDirectory(zkStateDir);
    assertTrue(repository.exists(zkStateDir));

    new DeleteBackupCmd(null)
        .deleteBackupIds(backupUri, repository, Set.of(BackupId.zero()), new NamedList<>());

    assertFalse(repository.exists(zkStateDir));
  }

  @Test
  public void testDeleteBackupIdsPropagatesUnexpectedDeleteErrors() {
    BackupRepository failingRepository =
        new LocalFileSystemRepository() {
          @Override
          public void deleteDirectory(URI path) throws IOException {
            throw new IOException("simulated repository failure");
          }
        };

    IOException thrown =
        expectThrows(
            IOException.class,
            () ->
                new DeleteBackupCmd(null)
                    .deleteBackupIds(
                        backupUri, failingRepository, Set.of(BackupId.zero()), new NamedList<>()));
    assertEquals("simulated repository failure", thrown.getMessage());
  }

  @Test
  public void testDeleteBackupIdsIgnoresStagedMetadataTempFile() throws Exception {
    URI metadataDir = new BackupFilePaths(repository, backupUri).getShardBackupMetadataDir();
    ShardBackupId shardBackupId = new ShardBackupId("shard1", BackupId.zero());
    storeMetadata(metadataDir, shardBackupId);
    String stagedFile = createStagedTempFile(metadataDir, shardBackupId);
    URI metadataFile = repository.resolve(metadataDir, shardBackupId.getBackupMetadataFilename());
    assertTrue(repository.exists(metadataFile));
    assertTrue(repository.exists(repository.resolve(metadataDir, stagedFile)));

    NamedList<Object> results = new NamedList<>();
    new DeleteBackupCmd(null)
        .deleteBackupIds(backupUri, repository, Set.of(BackupId.zero()), results);

    assertNotNull(results.get("deleted"));
    assertFalse(repository.exists(metadataFile));
    // The staged file is not any backup point's metadata, so it is left in place.
    assertTrue(repository.exists(repository.resolve(metadataDir, stagedFile)));
  }

  @Test
  public void testKeepNumberOfBackupIgnoresStagedMetadataTempFile() throws Exception {
    URI metadataDir = new BackupFilePaths(repository, backupUri).getShardBackupMetadataDir();
    ShardBackupId oldest = new ShardBackupId("shard1", BackupId.zero());
    ShardBackupId newest = new ShardBackupId("shard1", new BackupId(1));
    storeMetadata(metadataDir, oldest);
    storeMetadata(metadataDir, newest);
    createStagedTempFile(metadataDir, oldest);
    createBackupPropsFile(BackupId.zero());
    createBackupPropsFile(new BackupId(1));

    new DeleteBackupCmd(null).keepNumberOfBackup(repository, backupUri, 1, new NamedList<>());

    assertFalse(
        repository.exists(repository.resolve(metadataDir, oldest.getBackupMetadataFilename())));
    assertTrue(
        repository.exists(repository.resolve(metadataDir, newest.getBackupMetadataFilename())));
  }

  private void storeMetadata(URI metadataDir, ShardBackupId shardBackupId) throws IOException {
    ShardBackupMetadata metadata = ShardBackupMetadata.empty();
    metadata.addBackedFile("uniq_" + shardBackupId.getIdAsString(), "orig", new Checksum(1L, 10));
    metadata.store(repository, metadataDir, shardBackupId);
  }

  private String createStagedTempFile(URI metadataDir, ShardBackupId shardBackupId)
      throws IOException {
    String stagedName = shardBackupId.getBackupMetadataFilename() + ".tmp." + UUID.randomUUID();
    try (OutputStream out = repository.createOutput(repository.resolve(metadataDir, stagedName))) {
      out.write('#');
    }
    return stagedName;
  }

  private void createBackupPropsFile(BackupId backupId) throws IOException {
    try (OutputStream out =
        repository.createOutput(
            repository.resolve(backupUri, BackupFilePaths.getBackupPropsName(backupId)))) {
      out.write('#');
    }
  }

  private URI zkStateDir(BackupId backupId) {
    return repository.resolveDirectory(backupUri, BackupFilePaths.getZkStateDir(backupId));
  }
}
