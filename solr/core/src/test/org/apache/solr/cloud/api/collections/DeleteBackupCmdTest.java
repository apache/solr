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
import java.net.URI;
import java.util.Set;
import java.util.UUID;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
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

  private URI zkStateDir(BackupId backupId) {
    return repository.resolveDirectory(backupUri, BackupFilePaths.getZkStateDir(backupId));
  }
}
