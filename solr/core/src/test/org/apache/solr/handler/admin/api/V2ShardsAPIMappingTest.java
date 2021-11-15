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

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.api.ApiRegistrar;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the V2 APIs found in {@link org.apache.solr.handler.admin.api} that use the /c/{collection}/shards/{shard} path.
 *
 * Note that the V2 requests made by these tests are not necessarily semantically valid.  They shouldn't be taken as
 * examples. In several instances, mutually exclusive JSON parameters are provided.  This is done to exercise conversion
 * of all parameters, even if particular combinations are never expected in the same request.
 */
public class V2ShardsAPIMappingTest {
    private ApiBag apiBag;

    private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
    private CollectionsHandler mockCollectionsHandler;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Before
    public void setupApiBag() throws Exception {
        mockCollectionsHandler = mock(CollectionsHandler.class);
        queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

        apiBag = new ApiBag(false);
        ApiRegistrar.registerShardApis(apiBag, mockCollectionsHandler);
    }

    @Test
    public void testForceLeaderAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections/collName/shards/shardName", "POST",
                "{ 'force-leader': {}}");

        assertEquals(CollectionParams.CollectionAction.FORCELEADER.lowerName, v1Params.get(ACTION));
        assertEquals("collName", v1Params.get(COLLECTION));
        assertEquals("shardName", v1Params.get(SHARD));
    }

    @Test
    public void testSyncShardAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/collections/collName/shards/shardName", "POST",
                "{ 'sync-shard': {}}");

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
        final SolrParams v1Params = captureConvertedV1Params("/collections/collName/shards/shardName", "DELETE", v2QueryParams);

        assertEquals(CollectionParams.CollectionAction.DELETESHARD.lowerName, v1Params.get(ACTION));
        assertEquals("collName", v1Params.get(COLLECTION));
        assertEquals("shardName", v1Params.get(SHARD));
        assertEquals("true", v1Params.get("deleteIndex"));
        assertEquals("true", v1Params.get("deleteDataDir"));
        assertEquals("true", v1Params.get("deleteInstanceDir"));
        assertEquals("true", v1Params.get("followAliases"));
    }

    private SolrParams captureConvertedV1Params(String path, String method, SolrParams queryParams) throws Exception {
        return captureConvertedV1Params(path, method, queryParams, null);
    }

    private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody) throws Exception {
        return captureConvertedV1Params(path, method, new ModifiableSolrParams(), v2RequestBody);
    }

    private SolrParams captureConvertedV1Params(String path, String method, SolrParams queryParams, String v2RequestBody) throws Exception {
        final HashMap<String, String> parts = new HashMap<>();
        final Api api = apiBag.lookup(path, method, parts);
        final SolrQueryResponse rsp = new SolrQueryResponse();
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, queryParams) {
            @Override
            public List<CommandOperation> getCommands(boolean validateInput) {
                if (v2RequestBody == null) return Collections.emptyList();
                return ApiBag.getCommandOperations(new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
            }

            @Override
            public Map<String, String> getPathTemplateValues() {
                return parts;
            }

            @Override
            public String getHttpMethod() {
                return method;
            }
        };

        api.call(req, rsp);
        verify(mockCollectionsHandler).handleRequestBody(queryRequestCaptor.capture(), any());
        return queryRequestCaptor.getValue().getParams();
    }
}
