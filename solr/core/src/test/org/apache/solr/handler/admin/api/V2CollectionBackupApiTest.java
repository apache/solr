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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.client.api.model.CreateCollectionBackupRequestBody;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link CreateCollectionBackup} */
public class V2CollectionBackupApiTest extends MockV2APITest {

  private CreateCollectionBackup api;
  private BackupRepository mockBackupRepository;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    mockBackupRepository = mock(BackupRepository.class);
    when(mockCoreContainer.newBackupRepository("someRepoName")).thenReturn(mockBackupRepository);
    when(mockCoreContainer.newBackupRepository(null)).thenReturn(mockBackupRepository);

    api = new CreateCollectionBackup(mockCoreContainer, mockQueryRequest, queryResponse);
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

    api.createCollectionBackup("someCollectionName", "someBackupName", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.BACKUP,
        requestBody.async,
        message -> {
          assertEquals(message.toString(), 9, message.size());
          assertEquals("someCollectionName", message.get("collection"));
          assertEquals("/some/location", message.get("location"));
          assertEquals("someRepoName", message.get("repository"));
          assertEquals(true, message.get("followAliases"));
          assertEquals("copy-files", message.get("indexBackup"));
          assertEquals("someSnapshotName", message.get("commitName"));
          assertEquals(true, message.get("incremental"));
          assertEquals(123, message.get("maxNumBackupPoints"));
          assertEquals("someBackupName", message.get("name"));
        });
  }

  @Test
  public void testCreateRemoteMessageOmitsNullValues() throws Exception {
    final var requestBody = new CreateCollectionBackupRequestBody();
    requestBody.location = "/some/location";

    when(mockClusterState.hasCollection("someCollectionName")).thenReturn(true);
    when(mockBackupRepository.getBackupLocation(requestBody.location))
        .thenReturn(requestBody.location);
    when(mockBackupRepository.exists(any())).thenReturn(true);

    api.createCollectionBackup("someCollectionName", "someBackupName", requestBody);

    validateRunCommand(
        CollectionParams.CollectionAction.BACKUP,
        message -> {
          assertEquals(5, message.size());
          assertEquals("someCollectionName", message.get("collection"));
          assertEquals("/some/location", message.get("location"));
          assertEquals("someBackupName", message.get("name"));
          assertEquals(true, message.get("incremental"));
          assertEquals("copy-files", message.get("indexBackup"));
        });
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
