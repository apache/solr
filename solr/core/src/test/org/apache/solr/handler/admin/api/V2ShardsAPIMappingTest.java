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

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_IF_DOWN;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.NAME;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;

import java.util.Locale;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.V2ApiMappingTest;
import org.junit.Test;

/**
 * Unit tests for the V2 APIs that use the /c/{collection}/shards or /c/{collection}/shards/{shard}
 * paths.
 *
 * <p>Note that the V2 requests made by these tests are not necessarily semantically valid. They
 * shouldn't be taken as examples. In several instances, mutually exclusive JSON parameters are
 * provided. This is done to exercise conversion of all parameters, even if particular combinations
 * are never expected in the same request.
 */
public class V2ShardsAPIMappingTest extends V2ApiMappingTest<CollectionsHandler> {

  @Override
  public void populateApiBag() {
    final CollectionsHandler collectionsHandler = getRequestHandler();
    apiBag.registerObject(new SplitShardAPI(collectionsHandler));
    apiBag.registerObject(new CreateShardAPI(collectionsHandler));
    apiBag.registerObject(new AddReplicaAPI(collectionsHandler));
    apiBag.registerObject(new DeleteShardAPI(collectionsHandler));
    apiBag.registerObject(new SyncShardAPI(collectionsHandler));
    apiBag.registerObject(new ForceLeaderAPI(collectionsHandler));
    apiBag.registerObject(new DeleteReplicaAPI(collectionsHandler));
  }

  @Override
  public CollectionsHandler createUnderlyingRequestHandler() {
    return createMock(CollectionsHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return false;
  }

  @Test
  public void testForceLeaderAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards/shardName", "POST", "{ 'force-leader': {}}");

    assertEquals(CollectionParams.CollectionAction.FORCELEADER.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shardName", v1Params.get(SHARD));
  }

  @Test
  public void testSyncShardAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards/shardName", "POST", "{ 'sync-shard': {}}");

    assertEquals(CollectionParams.CollectionAction.SYNCSHARD.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shardName", v1Params.get(SHARD));
  }

  @Test
  public void testDeleteShardAllProperties() throws Exception {
    final ModifiableSolrParams v2QueryParams = new ModifiableSolrParams();
    v2QueryParams.add("deleteIndex", "true");
    v2QueryParams.add("deleteDataDir", "true");
    v2QueryParams.add("deleteInstanceDir", "true");
    v2QueryParams.add("followAliases", "true");
    final SolrParams v1Params =
        captureConvertedV1Params("/collections/collName/shards/shardName", "DELETE", v2QueryParams);

    assertEquals(CollectionParams.CollectionAction.DELETESHARD.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shardName", v1Params.get(SHARD));
    assertEquals("true", v1Params.get("deleteIndex"));
    assertEquals("true", v1Params.get("deleteDataDir"));
    assertEquals("true", v1Params.get("deleteInstanceDir"));
    assertEquals("true", v1Params.get("followAliases"));
  }

  @Test
  public void testSplitShardAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards",
            "POST",
            "{ 'split': {"
                + "'shard': 'shard1', "
                + "'ranges': 'someRangeValues', "
                + "'splitKey': 'someSplitKey', "
                + "'numSubShards': 123, "
                + "'splitFuzz': 'some_fuzz_value', "
                + "'timing': true, "
                + "'splitByPrefix': true, "
                + "'followAliases': true, "
                + "'splitMethod': 'rewrite', "
                + "'async': 'some_async_id', "
                + "'waitForFinalState': true, "
                + "'coreProperties': {"
                + "    'foo': 'foo1', "
                + "    'bar': 'bar1', "
                + "}}}");

    assertEquals(CollectionParams.CollectionAction.SPLITSHARD.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shard1", v1Params.get(SHARD_ID_PROP));
    assertEquals("someRangeValues", v1Params.get(CoreAdminParams.RANGES));
    assertEquals("someSplitKey", v1Params.get(SPLIT_KEY));
    assertEquals(123, v1Params.getPrimitiveInt(NUM_SUB_SHARDS));
    assertEquals("some_fuzz_value", v1Params.get(SPLIT_FUZZ));
    assertTrue(v1Params.getPrimitiveBool(TIMING));
    assertTrue(v1Params.getPrimitiveBool(SPLIT_BY_PREFIX));
    assertTrue(v1Params.getPrimitiveBool(FOLLOW_ALIASES));
    assertEquals("rewrite", v1Params.get(SPLIT_METHOD));
    assertEquals("some_async_id", v1Params.get(ASYNC));
    assertTrue(v1Params.getPrimitiveBool(WAIT_FOR_FINAL_STATE));
    assertEquals("foo1", v1Params.get("property.foo"));
    assertEquals("bar1", v1Params.get("property.bar"));
  }

  @Test
  public void testCreateShardAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards",
            "POST",
            "{ 'create': {"
                + "'shard': 'shard1', "
                + "'nodeSet': ['foo', 'bar', 'baz'], "
                + "'followAliases': true, "
                + "'async': 'some_async_id', "
                + "'waitForFinalState': true, "
                + "'replicationFactor': 123, "
                + "'nrtReplicas': 456, "
                + "'tlogReplicas': 789, "
                + "'pullReplicas': 101, "
                + "'coreProperties': {"
                + "    'foo': 'foo1', "
                + "    'bar': 'bar1', "
                + "}}}");

    assertEquals(CollectionParams.CollectionAction.CREATESHARD.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shard1", v1Params.get(SHARD_ID_PROP));
    assertEquals("foo,bar,baz", v1Params.get(CREATE_NODE_SET_PARAM));
    assertTrue(v1Params.getPrimitiveBool(FOLLOW_ALIASES));
    assertEquals("some_async_id", v1Params.get(ASYNC));
    assertTrue(v1Params.getPrimitiveBool(WAIT_FOR_FINAL_STATE));
    assertEquals(123, v1Params.getPrimitiveInt(REPLICATION_FACTOR));
    assertEquals(456, v1Params.getPrimitiveInt(NRT_REPLICAS));
    assertEquals(789, v1Params.getPrimitiveInt(TLOG_REPLICAS));
    assertEquals(101, v1Params.getPrimitiveInt(PULL_REPLICAS));
    assertEquals("foo1", v1Params.get("property.foo"));
    assertEquals("bar1", v1Params.get("property.bar"));
  }

  @Test
  public void testAddReplicaAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards",
            "POST",
            "{ 'add-replica': {"
                + "'shard': 'shard1', "
                + "'_route_': 'someRouteValue', "
                + "'node': 'someNodeValue', "
                + "'name': 'someName', "
                + "'instanceDir': 'dir1', "
                + "'dataDir': 'dir2', "
                + "'ulogDir': 'dir3', "
                + "'createNodeSet': ['foo', 'bar', 'baz'], "
                + "'followAliases': true, "
                + "'async': 'some_async_id', "
                + "'waitForFinalState': true, "
                + "'skipNodeAssignment': true, "
                + "'type': 'tlog', "
                + "'coreProperties': {"
                + "    'foo': 'foo1', "
                + "    'bar': 'bar1', "
                + "}}}");

    assertEquals(CollectionParams.CollectionAction.ADDREPLICA.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shard1", v1Params.get(SHARD_ID_PROP));
    assertEquals("someRouteValue", v1Params.get("_route_"));
    assertEquals("someNodeValue", v1Params.get("node"));
    assertEquals("foo,bar,baz", v1Params.get(CREATE_NODE_SET_PARAM));
    assertEquals("someName", v1Params.get(NAME));
    assertEquals("dir1", v1Params.get("instanceDir"));
    assertEquals("dir2", v1Params.get("dataDir"));
    assertEquals("dir3", v1Params.get("ulogDir"));
    assertTrue(v1Params.getPrimitiveBool(FOLLOW_ALIASES));
    assertEquals("some_async_id", v1Params.get(ASYNC));
    assertTrue(v1Params.getPrimitiveBool(WAIT_FOR_FINAL_STATE));
    assertTrue(v1Params.getPrimitiveBool("skipNodeAssignment"));
    assertEquals("tlog", v1Params.get("type"));
    assertEquals("foo1", v1Params.get("property.foo"));
    assertEquals("bar1", v1Params.get("property.bar"));
  }

  // really this is a replica API, but since there's only 1 API on the replica path, it's included
  // here for simplicity.
  @Test
  public void testDeleteReplicaAllProperties() throws Exception {
    final ModifiableSolrParams v2QueryParams = new ModifiableSolrParams();
    v2QueryParams.add("deleteIndex", "true");
    v2QueryParams.add("deleteDataDir", "true");
    v2QueryParams.add("deleteInstanceDir", "true");
    v2QueryParams.add("followAliases", "true");
    v2QueryParams.add("count", "4");
    v2QueryParams.add("onlyIfDown", "true");
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName/shards/shard1/someReplica", "DELETE", v2QueryParams);

    assertEquals(
        CollectionParams.CollectionAction.DELETEREPLICA.lowerName,
        v1Params.get(ACTION).toLowerCase(Locale.ROOT));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shard1", v1Params.get(SHARD_ID_PROP));
    assertEquals("someReplica", v1Params.get(REPLICA));
    assertEquals("true", v1Params.get(DELETE_INDEX));
    assertEquals("true", v1Params.get(DELETE_DATA_DIR));
    assertEquals("true", v1Params.get(DELETE_INSTANCE_DIR));
    assertEquals("true", v1Params.get(FOLLOW_ALIASES));
    assertEquals("4", v1Params.get(COUNT_PROP));
    assertEquals("true", v1Params.get(ONLY_IF_DOWN));
  }
}
