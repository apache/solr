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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

/** Unit tests for {@link CreateShardAPI} */
public class CreateShardAPITest extends SolrTestCaseJ4 {

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateShardAPI(null, null, null);
              api.createShard("someCollName", null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Required request-body is missing", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final var requestBody = new CreateShardAPI.CreateShardRequestBody();
    requestBody.shardName = "someShardName";
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateShardAPI(null, null, null);
              api.createShard(null, requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameMissing() {
    final var requestBody = new CreateShardAPI.CreateShardRequestBody();
    requestBody.shardName = null;
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateShardAPI(null, null, null);
              api.createShard("someCollectionName", requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: shard", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfShardNameIsInvalid() {
    final var requestBody = new CreateShardAPI.CreateShardRequestBody();
    requestBody.shardName = "invalid$shard@name";
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateShardAPI(null, null, null);
              api.createShard("someCollectionName", requestBody);
            });

    assertEquals(400, thrown.code());
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Invalid shard: [invalid$shard@name]"));
  }

  @Test
  public void testCreateRemoteMessageAllProperties() {
    final var requestBody = new CreateShardAPI.CreateShardRequestBody();
    requestBody.shardName = "someShardName";
    requestBody.replicationFactor = 123;
    requestBody.nrtReplicas = 123;
    requestBody.tlogReplicas = 456;
    requestBody.pullReplicas = 789;
    requestBody.createReplicas = true;
    requestBody.nodeSet = List.of("node1", "node2");
    requestBody.waitForFinalState = true;
    requestBody.followAliases = true;
    requestBody.asyncId = "someAsyncId";
    requestBody.properties = Map.of("propName1", "propVal1", "propName2", "propVal2");

    final var remoteMessage =
        CreateShardAPI.createRemoteMessage("someCollectionName", requestBody).getProperties();

    assertEquals(13, remoteMessage.size());
    assertEquals("createshard", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someCollectionName", remoteMessage.get(COLLECTION));
    assertEquals("someShardName", remoteMessage.get(SHARD_ID_PROP));
    assertEquals(123, remoteMessage.get(REPLICATION_FACTOR));
    assertEquals(123, remoteMessage.get(NRT_REPLICAS));
    assertEquals(456, remoteMessage.get(TLOG_REPLICAS));
    assertEquals(789, remoteMessage.get(PULL_REPLICAS));
    assertEquals("node1,node2", remoteMessage.get(CREATE_NODE_SET_PARAM));
    assertEquals(true, remoteMessage.get(WAIT_FOR_FINAL_STATE));
    assertEquals(true, remoteMessage.get(FOLLOW_ALIASES));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
    assertEquals("propVal1", remoteMessage.get("property.propName1"));
    assertEquals("propVal2", remoteMessage.get("property.propName2"));
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

    final var requestBody = CreateShardAPI.CreateShardRequestBody.fromV1Params(v1Params);

    assertEquals("someShardName", requestBody.shardName);
    assertEquals(Integer.valueOf(123), requestBody.replicationFactor);
    assertEquals(Integer.valueOf(123), requestBody.nrtReplicas);
    assertEquals(Integer.valueOf(456), requestBody.tlogReplicas);
    assertEquals(Integer.valueOf(789), requestBody.pullReplicas);
    assertNull(requestBody.createReplicas);
    assertEquals(List.of("node1", "node2"), requestBody.nodeSet);
    assertEquals(Boolean.TRUE, requestBody.waitForFinalState);
    assertEquals(Boolean.TRUE, requestBody.followAliases);
    assertEquals("someAsyncId", requestBody.asyncId);
    assertEquals("propVal1", requestBody.properties.get("propName1"));
    assertEquals("propVal2", requestBody.properties.get("propName2"));
  }
}
