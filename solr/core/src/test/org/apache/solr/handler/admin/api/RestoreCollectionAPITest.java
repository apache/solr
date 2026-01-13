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

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.RestoreCollectionRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link RestoreCollection} */
public class RestoreCollectionAPITest extends MockAPITest {

  private static RestoreCollection api;
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

    api = new RestoreCollection(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfBackupNameMissing() {
    final var requestBody = new RestoreCollectionRequestBody();
    requestBody.collection = "someCollection";
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.restoreCollection(null, requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Required parameter 'backupName' missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.restoreCollection("someBackupName", null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required request body", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    // No 'collection' set on the requestBody
    final var requestBody = new RestoreCollectionRequestBody();
    final SolrException thrown =
        expectThrows(
            SolrException.class, () -> api.restoreCollection("someBackupName", requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Required parameter 'collection' missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfProvidedCollectionNameIsInvalid() {
    final var requestBody = new RestoreCollectionRequestBody();
    requestBody.collection = "invalid$collection@name";
    final SolrException thrown =
        expectThrows(
            SolrException.class, () -> api.restoreCollection("someBackupName", requestBody));

    assertEquals(400, thrown.code());
    assertThat(
        thrown.getMessage(), containsString("Invalid collection: [invalid$collection@name]"));
  }

  @Test
  public void testCreatesValidRemoteMessageForExistingCollectionRestore() throws Exception {
    final var requestBody = new RestoreCollectionRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "someLocation";
    requestBody.backupId = 123;
    requestBody.repository = "someRepository";
    requestBody.async = "someAsyncId";

    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    api.restoreCollection("someBackupName", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();

    assertEquals(5, remoteMessage.size());
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals(Integer.valueOf(123), remoteMessage.get(BACKUP_ID));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));
    assertEquals("someBackupName", remoteMessage.get(NAME));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.RESTORE, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCreatesValidRemoteMessageForNewCollectionRestore() throws Exception {
    final var requestBody = new RestoreCollectionRequestBody();
    requestBody.collection = "someCollectionName";
    requestBody.location = "someLocation";
    requestBody.backupId = 123;
    requestBody.repository = "someRepository";
    requestBody.async = "someAsyncId";
    final var createParams = new CreateCollectionRequestBody();
    requestBody.createCollectionParams = createParams;
    createParams.config = "someConfig";
    createParams.nrtReplicas = 123;
    createParams.tlogReplicas = 456;
    createParams.pullReplicas = 789;
    createParams.nodeSet = List.of("node1", "node2");
    createParams.properties = Map.of("foo", "bar");

    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    api.restoreCollection("someBackupName", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();

    assertEquals(12, remoteMessage.size());
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals(Integer.valueOf(123), remoteMessage.get(BACKUP_ID));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals("someConfig", remoteMessage.get(COLL_CONF));
    assertEquals(Integer.valueOf(123), remoteMessage.get(NRT_REPLICAS));
    assertEquals(Integer.valueOf(123), remoteMessage.get(REPLICATION_FACTOR));
    assertEquals(Integer.valueOf(456), remoteMessage.get(TLOG_REPLICAS));
    assertEquals(Integer.valueOf(789), remoteMessage.get(PULL_REPLICAS));
    assertEquals("node1,node2", remoteMessage.get(CREATE_NODE_SET_PARAM));
    assertEquals("bar", remoteMessage.get("property.foo"));

    AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.RESTORE, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCanConvertV1ParamsIntoV2RequestBody() {
    final var v1Params = new ModifiableSolrParams();
    v1Params.add("name", "someBackupName");
    v1Params.add("collection", "someCollectionName");
    v1Params.add("location", "/some/location/str");
    v1Params.add("repository", "someRepositoryName");
    v1Params.add("backupId", "123");
    v1Params.add("async", "someAsyncId");
    // Supported coll-creation params
    v1Params.add("collection.configName", "someConfig");
    v1Params.add("nrtReplicas", "123");
    v1Params.add("property.foo", "bar");
    v1Params.add("createNodeSet", "node1,node2");
    v1Params.add("createNodeSet.shuffle", "false");

    final var requestBody = RestoreCollection.createRequestBodyFromV1Params(v1Params);

    assertEquals("someCollectionName", requestBody.collection);
    assertEquals("/some/location/str", requestBody.location);
    assertEquals("someRepositoryName", requestBody.repository);
    assertEquals(Integer.valueOf(123), requestBody.backupId);
    assertEquals("someAsyncId", requestBody.async);
    // Ensure the nested "collection-creation" object looks as expected
    assertNotNull(requestBody.createCollectionParams);
    final var createParams = requestBody.createCollectionParams;
    assertEquals("someCollectionName", createParams.name);
    assertEquals("someConfig", createParams.config);
    assertEquals(Integer.valueOf(123), createParams.nrtReplicas);
    assertNotNull(createParams.properties);
    assertEquals(1, createParams.properties.size());
    assertEquals("bar", createParams.properties.get("foo"));
    assertEquals(List.of("node1", "node2"), createParams.nodeSet);
    assertEquals(Boolean.FALSE, createParams.shuffleNodes);
  }
}
