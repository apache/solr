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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;

import java.util.Map;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.TestCollectionAPIs;
import org.apache.solr.handler.admin.V2ApiMappingTest;
import org.junit.Test;

/**
 * Unit tests for the V2 APIs found in {@link org.apache.solr.handler.admin.api} that use the
 * /c/{collection} path.
 *
 * <p>This test bears many similarities to {@link TestCollectionAPIs} which appears to test the
 * mappings indirectly by checking message sent to the ZK overseer (which is similar, but not
 * identical to the v1 param list). If there's no particular benefit to testing the mappings in this
 * way (there very well may be), then we should combine these two test classes at some point in the
 * future using the simpler approach here.
 *
 * <p>Note that the V2 requests made by these tests are not necessarily semantically valid. They
 * shouldn't be taken as examples. In several instances, mutually exclusive JSON parameters are
 * provided. This is done to exercise conversion of all parameters, even if particular combinations
 * are never expected in the same request.
 */
public class V2CollectionAPIMappingTest extends V2ApiMappingTest<CollectionsHandler> {

  @Override
  public void populateApiBag() {
    final CollectionsHandler collectionsHandler = getRequestHandler();
    apiBag.registerObject(new MigrateDocsAPI(collectionsHandler));
    apiBag.registerObject(new ModifyCollectionAPI(collectionsHandler));
    apiBag.registerObject(new MoveReplicaAPI(collectionsHandler));
    apiBag.registerObject(new RebalanceLeadersAPI(collectionsHandler));
    apiBag.registerObject(new CollectionStatusAPI(collectionsHandler));
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
  public void testGetCollectionStatus() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName", "GET", Map.of(SHARD, new String[] {"shard2"}));

    assertEquals(CollectionParams.CollectionAction.CLUSTERSTATUS.toString(), v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("shard2", v1Params.get(SHARD));
  }

  @Test
  public void testModifyCollectionAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName",
            "POST",
            "{ 'modify': {"
                + "'replicationFactor': 123, "
                + "'readOnly': true, "
                + "'config': 'techproducts_config', "
                + "'async': 'requestTrackingId', "
                + "'properties': {"
                + "     'foo': 'bar', "
                + "     'baz': 456 "
                + "}"
                + "}}");

    assertEquals(
        CollectionParams.CollectionAction.MODIFYCOLLECTION.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals(123, v1Params.getPrimitiveInt(ZkStateReader.REPLICATION_FACTOR));
    assertTrue(v1Params.getPrimitiveBool(ZkStateReader.READ_ONLY));
    assertEquals("techproducts_config", v1Params.get(COLL_CONF));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
    assertEquals("bar", v1Params.get("property.foo"));
    assertEquals(456, v1Params.getPrimitiveInt("property.baz"));
  }

  @Test
  public void testMoveReplicaAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName",
            "POST",
            "{ 'move-replica': {"
                + "'sourceNode': 'someSourceNode', "
                + "'targetNode': 'someTargetNode', "
                + "'replica': 'someReplica', "
                + "'shard': 'someShard', "
                + "'waitForFinalState': true, "
                + "'timeout': 123, "
                + "'inPlaceMove': true, "
                + "'followAliases': true "
                + "}}");

    assertEquals(CollectionParams.CollectionAction.MOVEREPLICA.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("someSourceNode", v1Params.get("sourceNode"));
    assertEquals("someTargetNode", v1Params.get("targetNode"));
    assertEquals("someReplica", v1Params.get("replica"));
    assertEquals("someShard", v1Params.get("shard"));
    assertTrue(v1Params.getPrimitiveBool("waitForFinalState"));
    assertEquals(123, v1Params.getPrimitiveInt("timeout"));
    assertTrue(v1Params.getPrimitiveBool("inPlaceMove"));
    assertTrue(v1Params.getPrimitiveBool("followAliases"));
  }

  @Test
  public void testMigrateDocsAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName",
            "POST",
            "{ 'migrate-docs': {"
                + "'target': 'someTargetCollection', "
                + "'splitKey': 'someSplitKey', "
                + "'forwardTimeout': 123, "
                + "'followAliases': true, "
                + "'async': 'requestTrackingId' "
                + "}}");

    assertEquals(CollectionParams.CollectionAction.MIGRATE.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("someTargetCollection", v1Params.get("target.collection"));
    assertEquals("someSplitKey", v1Params.get("split.key"));
    assertEquals(123, v1Params.getPrimitiveInt("forward.timeout"));
    assertTrue(v1Params.getPrimitiveBool("followAliases"));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
  }

  @Test
  public void testRebalanceLeadersAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/collName",
            "POST",
            "{ 'rebalance-leaders': {" + "'maxAtOnce': 123, " + "'maxWaitSeconds': 456" + "}}");

    assertEquals(
        CollectionParams.CollectionAction.REBALANCELEADERS.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals(123, v1Params.getPrimitiveInt("maxAtOnce"));
    assertEquals(456, v1Params.getPrimitiveInt("maxWaitSeconds"));
  }
}
