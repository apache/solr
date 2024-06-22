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

import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.TIMING;

import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
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
}
