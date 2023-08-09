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
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.IncrementalShardBackup;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackupCoreAPITest extends SolrTestCaseJ4 {

  private BackupCoreAPI backupCoreAPI;
  private static final String backupName = "my-new-backup";

  @BeforeClass
  public static void initializeCoreAndRequestFactory() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    lrf = h.getRequestFactory("/api", 0, 10);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    SolrQueryRequest solrQueryRequest = req();
    SolrQueryResponse solrQueryResponse = new SolrQueryResponse();
    CoreContainer coreContainer = h.getCoreContainer();

    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        new CoreAdminHandler.CoreAdminAsyncTracker();
    backupCoreAPI =
        new BackupCoreAPI(
            coreContainer, solrQueryRequest, solrQueryResponse, coreAdminAsyncTracker);
  }

  @Test
  public void testCreateNonIncrementalBackupReturnsValidResponse() throws Exception {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody = createBackupCoreRequestBody();
    backupCoreRequestBody.incremental = false;
    backupCoreRequestBody.backupName = backupName;
    SnapShooter.CoreSnapshotResponse response =
        (SnapShooter.CoreSnapshotResponse)
            backupCoreAPI.createBackup(coreName, backupCoreRequestBody);

    assertEquals(backupName, response.snapshotName);
    assertEquals("snapshot." + backupName, response.directoryName);
    assertEquals(1, response.fileCount);
    assertEquals(1, response.indexFileCount);
  }

  @Test
  public void testMissingLocationParameter() throws Exception {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody = createBackupCoreRequestBody();
    backupCoreRequestBody.location = null;
    backupCoreRequestBody.incremental = false;
    backupCoreRequestBody.backupName = backupName;
    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              backupCoreAPI.createBackup(coreName, backupCoreRequestBody);
            });
    assertEquals(500, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException
            .getMessage()
            .contains("'location' parameter is not specified in the request body"));
  }

  @Test
  public void testMissingCoreNameParameter() throws Exception {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody = createBackupCoreRequestBody();
    backupCoreRequestBody.location = null;
    backupCoreRequestBody.incremental = false;
    backupCoreRequestBody.backupName = backupName;

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              backupCoreAPI.createBackup(null, backupCoreRequestBody);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Missing required parameter:"));
  }

  @Test
  public void testNonIncrementalBackupForNonExistentCore() throws Exception {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody = createBackupCoreRequestBody();
    backupCoreRequestBody.location = null;
    backupCoreRequestBody.incremental = false;
    backupCoreRequestBody.backupName = backupName;
    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              backupCoreAPI.createBackup("non-existent-core", backupCoreRequestBody);
            });
    assertEquals(500, solrException.code());
  }

  @Test
  public void testCreateIncrementalBackupReturnsValidResponse() throws Exception {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody = createBackupCoreRequestBody();
    backupCoreRequestBody.incremental = true;
    backupCoreRequestBody.shardBackupId = "md_shard1_0";
    IncrementalShardBackup.IncrementalShardSnapshotResponse response =
        (IncrementalShardBackup.IncrementalShardSnapshotResponse)
            backupCoreAPI.createBackup(coreName, backupCoreRequestBody);

    assertEquals(1, response.indexFileCount);
    assertEquals(1, response.uploadedIndexFileCount);
    assertEquals(backupCoreRequestBody.shardBackupId, response.shardBackupId);
  }

  @AfterClass // unique core per test
  public static void coreDestroy() {
    deleteCore();
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

  private BackupCoreAPI.BackupCoreRequestBody createBackupCoreRequestBody() throws Exception {
    final Path locationPath = createBackupLocation();
    final URI locationUri = bootstrapBackupLocation(locationPath);
    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(Paths.get(locationUri));
    final BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    backupCoreRequestBody.location = locationPath.toString();
    return backupCoreRequestBody;
  }
}
