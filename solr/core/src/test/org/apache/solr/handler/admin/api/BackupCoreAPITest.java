/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class BackupCoreAPITest extends SolrTestCaseJ4 {
  @Before // unique core per test
  public void coreInit() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @After // unique core per test
  public void coreDestroy() {
    deleteCore();
  }

  @Test
  public void testIncrementalBackup() throws Exception {
    // See SOLR-11616 to see when this issue can be triggered

    assertU(adoc("id", "1"));
    assertU(commit());

    assertU(adoc("id", "2"));

    assertU(commit("openSearcher", "false"));
    assertQ(req("q", "*:*"), "//result[@numFound='1']");
    assertQ(req("q", "id:1"), "//result[@numFound='1']");
    assertQ(req("q", "id:2"), "//result[@numFound='0']");

    // call backup
    final Path locationPath = createBackupLocation();
    final URI locationUri = bootstrapBackupLocation(locationPath);
    final ShardBackupId shardBackupId = new ShardBackupId("shard1", BackupId.zero());

    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(Paths.get(locationUri));
    SolrQueryResponse resp = new SolrQueryResponse();
    BackupCoreAPI backupCoreAPI =
        new BackupCoreAPI(
            cores,
            req(
                CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                "core",
                DEFAULT_TEST_COLLECTION_NAME,
                "location",
                locationPath.toString(),
                CoreAdminParams.SHARD_BACKUP_ID,
                shardBackupId.getIdAsString()),
            resp);
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    backupCoreRequestBody.location = locationPath.toString();
    backupCoreRequestBody.shardBackupId = shardBackupId.getIdAsString();
    backupCoreRequestBody.incremental = true;
    backupCoreAPI.createBackup(DEFAULT_TEST_COLLECTION_NAME, null, backupCoreRequestBody);
    assertNull("Backup should have succeeded", resp.getException());
    simpleBackupCheck(locationUri, shardBackupId);
  }

  @Test
  public void testMissingRequiredParameterResultIn400ForIncrementalBackup() throws Exception {
    final Path locationPath = createBackupLocation();
    final CoreContainer cores = h.getCoreContainer();
    SolrQueryResponse resp = new SolrQueryResponse();
    BackupCoreAPI backupCoreAPI =
        new BackupCoreAPI(
            cores,
            req(
                CoreAdminParams.ACTION,
                CoreAdminParams.CoreAdminAction.BACKUPCORE.toString(),
                "core",
                DEFAULT_TEST_COLLECTION_NAME,
                "location",
                locationPath.toString()),
            resp);
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    backupCoreRequestBody.location = locationPath.toString();
    backupCoreRequestBody.shardBackupId = null;
    backupCoreRequestBody.incremental = true;
    Exception ex =
        expectThrows(
            Exception.class,
            () ->
                backupCoreAPI.createBackup(
                    DEFAULT_TEST_COLLECTION_NAME, null, backupCoreRequestBody));
    assertTrue(ex.getClass() == SolrException.class);
    assertTrue(((SolrException) ex).code() == 400);
  }

  private static void simpleBackupCheck(
      URI locationURI, ShardBackupId shardBackupId, String... expectedIndexFiles)
      throws IOException {
    try (BackupRepository backupRepository = h.getCoreContainer().newBackupRepository(null)) {
      final BackupFilePaths backupFilePaths = new BackupFilePaths(backupRepository, locationURI);

      // Ensure that the overall file structure looks correct.
      assertTrue(backupRepository.exists(locationURI));
      assertTrue(backupRepository.exists(backupFilePaths.getIndexDir()));
      assertTrue(backupRepository.exists(backupFilePaths.getShardBackupMetadataDir()));
      final String metadataFilename = shardBackupId.getBackupMetadataFilename();
      final URI shardBackupMetadataURI =
          backupRepository.resolve(backupFilePaths.getShardBackupMetadataDir(), metadataFilename);
      assertTrue(backupRepository.exists(shardBackupMetadataURI));

      // Ensure that all files listed in the shard-meta file are stored in the index dir
      final ShardBackupMetadata backupMetadata =
          ShardBackupMetadata.from(
              backupRepository, backupFilePaths.getShardBackupMetadataDir(), shardBackupId);
      for (String indexFileName : backupMetadata.listUniqueFileNames()) {
        final URI indexFileURI =
            backupRepository.resolve(backupFilePaths.getIndexDir(), indexFileName);
        assertTrue(
            "Expected " + indexFileName + " to exist in " + backupFilePaths.getIndexDir(),
            backupRepository.exists(indexFileURI));
      }

      // Ensure that the expected filenames (if any are provided) exist
      for (String expectedIndexFile : expectedIndexFiles) {
        assertTrue(
            "Expected backup to hold a renamed copy of " + expectedIndexFile,
            backupMetadata.listOriginalFileNames().contains(expectedIndexFile));
      }
    }
  }

  private Path createBackupLocation() {
    return createTempDir().toAbsolutePath();
  }

  private URI bootstrapBackupLocation(Path locationPath) throws IOException {
    final String locationPathStr = locationPath.toString();
    h.getCoreContainer().getAllowPaths().add(locationPath);
    try (BackupRepository backupRepo = h.getCoreContainer().newBackupRepository(null)) {
      final URI locationUri = backupRepo.createDirectoryURI(locationPathStr);
      final BackupFilePaths backupFilePaths = new BackupFilePaths(backupRepo, locationUri);
      backupFilePaths.createIncrementalBackupFolders();
      return locationUri;
    }
  }
}
