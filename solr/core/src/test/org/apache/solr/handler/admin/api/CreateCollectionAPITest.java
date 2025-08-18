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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.NUM_SLICES;
import static org.apache.solr.common.cloud.DocCollection.CollectionStateProps.SHARDS;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.NUM_SHARDS;
import static org.apache.solr.common.params.CollectionAdminParams.PER_REPLICA_STATE;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CoreAdminParams.NAME;

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.CreateCollectionRouterProperties;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

/** Unit tests for {@link CreateCollection}. */
public class CreateCollectionAPITest extends SolrTestCaseJ4 {

  @Test
  public void testReportsErrorIfRequestBodyMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new CreateCollection(null, null, null);
              api.createCollection(null);
            });

    assertEquals(400, thrown.code());
    assertEquals("Request body is missing but required", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfReplicationFactorAndNrtReplicasConflict() {
    // Valid request body...
    final var requestBody = new CreateCollectionRequestBody();
    requestBody.name = "someName";
    requestBody.config = "someConfig";
    // ...except for a replicationFactor and nrtReplicas conflicting
    requestBody.replicationFactor = 123;
    requestBody.nrtReplicas = 321;

    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              CreateCollection.validateRequestBody(requestBody);
            });

    assertEquals(400, thrown.code());
    assertEquals(
        "Cannot specify both replicationFactor and nrtReplicas as they mean the same thing",
        thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfCollectionNameInvalid() {
    final var requestBody = new CreateCollectionRequestBody();
    requestBody.name = "$invalid@collection+name";
    requestBody.config = "someConfig";

    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              CreateCollection.validateRequestBody(requestBody);
            });

    assertEquals(400, thrown.code());
    assertTrue(
        "Expected invalid collection name to be rejected",
        thrown.getMessage().contains("Invalid collection: [$invalid@collection+name]"));
  }

  @Test
  public void testReportsErrorIfShardNamesInvalid() {
    // Valid request body...
    final var requestBody = new CreateCollectionRequestBody();
    requestBody.name = "someName";
    requestBody.config = "someConfig";
    // ...except for a bad shard name
    requestBody.shardNames = List.of("good-name", "bad;name");

    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              CreateCollection.validateRequestBody(requestBody);
            });

    assertEquals(400, thrown.code());
    assertTrue(
        "Expected invalid shard name to be rejected",
        thrown.getMessage().contains("Invalid shard: [bad;name]"));
  }

  @Test
  public void testCreateRemoteMessageAllProperties() {
    final var requestBody = new CreateCollectionRequestBody();
    requestBody.name = "someName";
    requestBody.replicationFactor = 123;
    requestBody.config = "someConfig";
    requestBody.numShards = 456;
    requestBody.shardNames = List.of("shard1", "shard2");
    requestBody.pullReplicas = 789;
    requestBody.tlogReplicas = 987;
    requestBody.waitForFinalState = false;
    requestBody.perReplicaState = true;
    requestBody.alias = "someAliasName";
    requestBody.properties = Map.of("propName", "propValue");
    requestBody.async = "someAsyncId";
    requestBody.router = new CreateCollectionRouterProperties();
    requestBody.router.name = "someRouterName";
    requestBody.router.field = "someField";
    requestBody.nodeSet = List.of("node1", "node2");
    requestBody.shuffleNodes = false;

    final var remoteMessage = CreateCollection.createRemoteMessage(requestBody).getProperties();

    assertEquals("create", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("true", remoteMessage.get("fromApi"));
    assertEquals("someName", remoteMessage.get(NAME));
    assertEquals(123, remoteMessage.get(REPLICATION_FACTOR));
    assertEquals("someConfig", remoteMessage.get(COLL_CONF));
    assertEquals(456, remoteMessage.get(NUM_SLICES));
    assertEquals("shard1,shard2", remoteMessage.get(SHARDS));
    assertEquals(789, remoteMessage.get(PULL_REPLICAS));
    assertEquals(987, remoteMessage.get(TLOG_REPLICAS));
    assertEquals(123, remoteMessage.get(NRT_REPLICAS)); // replicationFactor value used
    assertEquals(false, remoteMessage.get(WAIT_FOR_FINAL_STATE));
    assertEquals(true, remoteMessage.get(PER_REPLICA_STATE));
    assertEquals("someAliasName", remoteMessage.get(ALIAS));
    assertEquals("propValue", remoteMessage.get("property.propName"));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
    assertEquals("someRouterName", remoteMessage.get("router.name"));
    assertEquals("someField", remoteMessage.get("router.field"));
    assertEquals("node1,node2", remoteMessage.get(CREATE_NODE_SET));
    assertEquals(false, remoteMessage.get(CREATE_NODE_SET_SHUFFLE_PARAM));
  }

  @Test
  public void testNoReplicaCreationMessage() {
    final var requestBody = new CreateCollectionRequestBody();
    requestBody.name = "someName";
    requestBody.createReplicas = false;

    final var remoteMessage = CreateCollection.createRemoteMessage(requestBody).getProperties();

    assertEquals("create", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someName", remoteMessage.get(NAME));
    assertEquals("EMPTY", remoteMessage.get(CREATE_NODE_SET));
  }

  @Test
  public void testV1ParamsCanBeConvertedIntoV2RequestBody() {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(NAME, "someName");
    solrParams.set(NUM_SHARDS, 123);
    solrParams.set(SHARDS, "shard1,shard2");
    solrParams.set(REPLICATION_FACTOR, 123);
    solrParams.set(TLOG_REPLICAS, 456);
    solrParams.set(PULL_REPLICAS, 789);
    solrParams.set(CREATE_NODE_SET, "node1,node2");
    solrParams.set(CREATE_NODE_SET_SHUFFLE_PARAM, true);
    solrParams.set(COLL_CONF, "someConfig");
    solrParams.set(PER_REPLICA_STATE, true);
    solrParams.set("router.name", "someRouterName");
    solrParams.set("router.field", "someField");
    solrParams.set("property.somePropName", "somePropValue");

    final var v2RequestBody = CreateCollection.createRequestBodyFromV1Params(solrParams, true);

    assertEquals("someName", v2RequestBody.name);
    assertEquals(Integer.valueOf(123), v2RequestBody.numShards);
    assertEquals(List.of("shard1", "shard2"), v2RequestBody.shardNames);
    assertEquals(Integer.valueOf(123), v2RequestBody.replicationFactor);
    assertEquals(Integer.valueOf(456), v2RequestBody.tlogReplicas);
    assertEquals(Integer.valueOf(789), v2RequestBody.pullReplicas);
    assertEquals(List.of("node1", "node2"), v2RequestBody.nodeSet);
    assertNull(v2RequestBody.createReplicas);
    assertEquals(Boolean.TRUE, v2RequestBody.shuffleNodes);
    assertEquals("someConfig", v2RequestBody.config);
    assertEquals(Boolean.TRUE, v2RequestBody.perReplicaState);
    assertNotNull(v2RequestBody.router);
    assertEquals("someRouterName", v2RequestBody.router.name);
    assertEquals("someField", v2RequestBody.router.field);
    assertNotNull(v2RequestBody.properties);
    assertEquals("somePropValue", v2RequestBody.properties.get("somePropName"));
  }

  @Test
  public void testV1ParamsWithEmptyNodeSetCanBeConvertedIntoV2RequestBody() {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(NAME, "someName");
    solrParams.set(CREATE_NODE_SET, "EMPTY");

    final var v2RequestBody = CreateCollection.createRequestBodyFromV1Params(solrParams, true);

    assertEquals("someName", v2RequestBody.name);
    assertEquals(Boolean.FALSE, v2RequestBody.createReplicas);
    assertNull(v2RequestBody.nodeSet);
  }
}
