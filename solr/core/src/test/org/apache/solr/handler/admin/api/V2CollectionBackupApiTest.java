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

import static org.apache.solr.common.params.CollectionAdminParams.COPY_FILES_STRATEGY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.client.api.model.CreateCollectionBackupRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link CreateCollectionBackup} */
public class V2CollectionBackupApiTest extends MockAPITest {

  private CreateCollectionBackup createCollectionBackup;
  private BackupRepository mockBackupRepository;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    mockBackupRepository = mock(BackupRepository.class);
    when(mockCoreContainer.newBackupRepository("someRepoName")).thenReturn(mockBackupRepository);
    when(mockCoreContainer.newBackupRepository(null)).thenReturn(mockBackupRepository);

    createCollectionBackup =
        new CreateCollectionBackup(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testCreateRemoteMessageWithAllProperties() throws Exception {

    final var requestBody = new CreateCollectionBackupRequestBody();
    requestBody.location = "/some/location";
    requestBody.repository = "someRepoName";
    requestBody.followAliases = true;
    requestBody.backupStrategy = COPY_FILES_STRATEGY;
    requestBody.snapshotName = "someSnapshotName";
    requestBody.incremental = true;
    requestBody.maxNumBackupPoints = 123;
    requestBody.async = "someId";

    when(mockClusterState.hasCollection("someCollectionName")).thenReturn(true);
    when(mockBackupRepository.getBackupLocation(requestBody.location))
        .thenReturn(requestBody.location);
    when(mockBackupRepository.exists(any())).thenReturn(true);

    createCollectionBackup.createCollectionBackup(
        "someCollectionName", "someBackupName", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    var message = messageCapturer.getValue();
    var messageProps = message.getProperties();
    assertEquals(messageProps.toString(), 9, messageProps.size());
    assertEquals("someCollectionName", messageProps.get("collection"));
    assertEquals("/some/location", messageProps.get("location"));
    assertEquals("someRepoName", messageProps.get("repository"));
    assertEquals(true, messageProps.get("followAliases"));
    assertEquals("copy-files", messageProps.get("indexBackup"));
    assertEquals("someSnapshotName", messageProps.get("commitName"));
    assertEquals(true, messageProps.get("incremental"));
    assertEquals(123, messageProps.get("maxNumBackupPoints"));
    assertEquals("someBackupName", messageProps.get("name"));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.BACKUP, context.getAction());
    assertEquals("someId", context.getAsyncId());
  }

  @Test
  public void testCreateRemoteMessageOmitsNullValues() throws Exception {
    final var requestBody = new CreateCollectionBackupRequestBody();
    requestBody.location = "/some/location";

    when(mockClusterState.hasCollection("someCollectionName")).thenReturn(true);
    when(mockBackupRepository.getBackupLocation(requestBody.location))
        .thenReturn(requestBody.location);
    when(mockBackupRepository.exists(any())).thenReturn(true);

    createCollectionBackup.createCollectionBackup(
        "someCollectionName", "someBackupName", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    var message = messageCapturer.getValue();
    var messageProps = message.getProperties();
    assertEquals(5, messageProps.size());
    assertEquals("someCollectionName", messageProps.get("collection"));
    assertEquals("/some/location", messageProps.get("location"));
    assertEquals("someBackupName", messageProps.get("name"));
    assertEquals(true, messageProps.get("incremental"));
    assertEquals("copy-files", messageProps.get("indexBackup"));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.BACKUP, context.getAction());
    assertNull(context.getAsyncId());
  }

  @Test
  public void testCanCreateV2RequestBodyFromV1Params() {
    when(mockClusterState.hasCollection("demoSourceNode")).thenReturn(true);

    final var params = new ModifiableSolrParams();
    params.set("collection", "someCollectionName");
    params.set("location", "/some/location");
    params.set("repository", "someRepoName");
    params.set("followAliases", "true");
    params.set("indexBackup", COPY_FILES_STRATEGY);
    params.set("commitName", "someSnapshotName");
    params.set("incremental", "true");
    params.set("maxNumBackupPoints", "123");
    params.set("async", "someId");

    final var requestBody = CreateCollectionBackup.createRequestBodyFromV1Params(params);

    assertEquals("/some/location", requestBody.location);
    assertEquals("someRepoName", requestBody.repository);
    assertEquals(Boolean.TRUE, requestBody.followAliases);
    assertEquals("copy-files", requestBody.backupStrategy);
    assertEquals("someSnapshotName", requestBody.snapshotName);
    assertEquals(Boolean.TRUE, requestBody.incremental);
    assertEquals(Integer.valueOf(123), requestBody.maxNumBackupPoints);
    assertEquals("someId", requestBody.async);
  }
}
