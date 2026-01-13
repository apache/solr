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

import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.CreateShardRequestBody;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link CreateShard} */
public class CreateShardAPITest extends MockAPITest {

  private CreateShard api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);

    api = new CreateShard(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.createShard("someCollName", null));

    assertEquals(400, thrown.code());
    assertEquals("Required request-body is missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final var requestBody = new CreateShardRequestBody();
    requestBody.shardName = "someShardName";
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.createShard(null, requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameMissing() {
    final var requestBody = new CreateShardRequestBody();
    requestBody.shardName = null;
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.createShard("someCollectionName", requestBody));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: shard", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameIsInvalid() {
    final var requestBody = new CreateShardRequestBody();
    requestBody.shardName = "invalid$shard@name";
    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.createShard("someCollectionName", requestBody));

    assertEquals(400, thrown.code());
    assertThat(thrown.getMessage(), containsString("Invalid shard: [invalid$shard@name]"));
  }

  @Test
  public void testCreateRemoteMessageAllProperties() throws Exception {
    final var requestBody = new CreateShardRequestBody();
    requestBody.shardName = "someShardName";
    requestBody.replicationFactor = 123;
    requestBody.nrtReplicas = 123;
    requestBody.tlogReplicas = 456;
    requestBody.pullReplicas = 789;
    requestBody.createReplicas = true;
    requestBody.nodeSet = List.of("node1", "node2");
    requestBody.waitForFinalState = true;
    requestBody.followAliases = true;
    requestBody.async = "someAsyncId";
    requestBody.properties = Map.of("propName1", "propVal1", "propName2", "propVal2");

    DocCollection mockCollection = mock(DocCollection.class);
    when(mockClusterState.hasCollection(eq("someCollectionName"))).thenReturn(true);
    when(mockClusterState.getCollection(eq("someCollectionName"))).thenReturn(mockCollection);
    when(mockCollection.get(DocCollection.CollectionStateProps.DOC_ROUTER))
        .thenReturn(Map.of(CommonParams.NAME, ImplicitDocRouter.NAME));

    api.createShard("someCollectionName", requestBody);
    verify(mockCommandRunner)
        .runCollectionCommand(contextCapturer.capture(), messageCapturer.capture(), anyLong());

    final ZkNodeProps createdMessage = messageCapturer.getValue();
    final Map<String, Object> remoteMessage = createdMessage.getProperties();

    assertEquals(11, remoteMessage.size());
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals(123, remoteMessage.get(REPLICATION_FACTOR));
    assertEquals(123, remoteMessage.get(NRT_REPLICAS));
    assertEquals(456, remoteMessage.get(TLOG_REPLICAS));
    assertEquals(789, remoteMessage.get(PULL_REPLICAS));
    assertEquals("node1,node2", remoteMessage.get(CREATE_NODE_SET_PARAM));
    assertEquals(true, remoteMessage.get(WAIT_FOR_FINAL_STATE));
    assertEquals(true, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals("propVal1", remoteMessage.get("property.propName1"));
    assertEquals("propVal2", remoteMessage.get("property.propName2"));

    final AdminCmdContext context = contextCapturer.getValue();
    assertEquals(CollectionParams.CollectionAction.CREATESHARD, context.getAction());
    assertEquals("someAsyncId", context.getAsyncId());
  }

  @Test
  public void testCanConvertV1ParamsToV2RequestBody() {
    final var v1Params = new ModifiableSolrParams();
    v1Params.add(COLLECTION, "someCollectionName");
    v1Params.add(SHARD_ID_PROP, "someShardName");
    v1Params.set(REPLICATION_FACTOR, 123);
    v1Params.set(NRT_REPLICAS, 123);
    v1Params.set(TLOG_REPLICAS, 456);
    v1Params.set(PULL_REPLICAS, 789);
    v1Params.add(CREATE_NODE_SET_PARAM, "node1,node2");
    v1Params.set(WAIT_FOR_FINAL_STATE, true);
    v1Params.set(FOLLOW_ALIASES, true);
    v1Params.add(ASYNC, "someAsyncId");
    v1Params.add("property.propName1", "propVal1");
    v1Params.add("property.propName2", "propVal2");

    final var requestBody = CreateShard.createRequestBodyFromV1Params(v1Params);

    assertEquals("someShardName", requestBody.shardName);
    assertEquals(Integer.valueOf(123), requestBody.replicationFactor);
    assertEquals(Integer.valueOf(123), requestBody.nrtReplicas);
    assertEquals(Integer.valueOf(456), requestBody.tlogReplicas);
    assertEquals(Integer.valueOf(789), requestBody.pullReplicas);
    assertNull(requestBody.createReplicas);
    assertEquals(List.of("node1", "node2"), requestBody.nodeSet);
    assertEquals(Boolean.TRUE, requestBody.waitForFinalState);
    assertEquals(Boolean.TRUE, requestBody.followAliases);
    assertEquals("someAsyncId", requestBody.async);
    assertEquals("propVal1", requestBody.properties.get("propName1"));
    assertEquals("propVal2", requestBody.properties.get("propName2"));
  }
}
