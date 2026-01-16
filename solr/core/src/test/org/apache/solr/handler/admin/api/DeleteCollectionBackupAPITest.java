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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_PURGE_UNUSED;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.MAX_NUM_BACKUP_POINTS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Map;
import org.apache.solr.client.api.model.PurgeUnusedFilesRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link DeleteCollectionBackup} */
public class DeleteCollectionBackupAPITest extends MockV2APITest {

  private DeleteCollectionBackup api;
  private BackupRepository mockBackupRepository;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    mockBackupRepository = mock(BackupRepository.class);
    when(mockCoreContainer.newBackupRepository(eq("someRepository")))
        .thenReturn(mockBackupRepository);
    when(mockCoreContainer.newBackupRepository(null)).thenReturn(mockBackupRepository);
    when(mockBackupRepository.getBackupLocation(eq("someLocation"))).thenReturn("someLocation");
    URI uri = new URI("someLocation");
    when(mockBackupRepository.createDirectoryURI(eq("someLocation"))).thenReturn(uri);
    when(mockBackupRepository.exists(eq(uri))).thenReturn(true);

    api = new DeleteCollectionBackup(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfBackupNameMissing() {
    // Single delete
    {
      final SolrException thrown =
          expectThrows(
              SolrException.class,
              () ->
                  api.deleteSingleBackupById(
                      null, "someBackupId", "someLocation", "someRepository", "someAsyncId"));

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }

    // Multi delete
    {
      final SolrException thrown =
          expectThrows(
              SolrException.class,
              () ->
                  api.deleteMultipleBackupsByRecency(
                      null, 123, "someLocation", "someRepository", "someAsyncId"));

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }

    // Garbage collect unused files
    {
      final var requestBody = new PurgeUnusedFilesRequestBody();
      requestBody.location = "someLocation";
      requestBody.repositoryName = "someRepository";
      requestBody.async = "someAsyncId";
      final SolrException thrown =
          expectThrows(
              SolrException.class, () -> api.garbageCollectUnusedBackupFiles(null, requestBody));

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }
  }

  @Test
  public void testDeletionByIdReportsErrorIfIdMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () ->
                api.deleteSingleBackupById(
                    "someBackupName", null, "someLocation", "someRepository", "someAsyncId"));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: backupId", thrown.getMessage());
  }

  @Test
  public void testMultiVersionDeletionReportsErrorIfRetainParamMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () ->
                api.deleteMultipleBackupsByRecency(
                    "someBackupName", null, "someLocation", "someRepository", "someAsyncId"));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: retainLatest", thrown.getMessage());
  }

  // The message created in this test isn't valid in practice, since it contains mutually-exclusive
  // parameters, but that doesn't matter for the purposes of this test.
  @Test
  public void testCreateRemoteMessageSingle() throws Exception {
    api.deleteSingleBackupById(
        "someBackupName", "someBackupId", "someLocation", "someRepository", "someAsyncId");
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(4, remoteMessage.size());
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals("someBackupId", remoteMessage.get(BACKUP_ID));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEBACKUP, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCreateRemoteMessageMultiple() throws Exception {
    api.deleteMultipleBackupsByRecency("someBackupName", 2, "someLocation", "someRepository", null);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(4, remoteMessage.size());
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals(2, remoteMessage.get(MAX_NUM_BACKUP_POINTS));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEBACKUP, context.getAction());
    assertNull(context.getAsyncId());
  }

  @Test
  public void testCreateRemoteMessageGarbageCollect() throws Exception {
    PurgeUnusedFilesRequestBody body = new PurgeUnusedFilesRequestBody();
    body.location = "someLocation";
    body.repositoryName = "someRepository";
    api.garbageCollectUnusedBackupFiles("someBackupName", body);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();
    assertEquals(4, remoteMessage.size());
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals(Boolean.TRUE, remoteMessage.get(BACKUP_PURGE_UNUSED));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.DELETEBACKUP, context.getAction());
    assertNull(context.getAsyncId());
  }
}
