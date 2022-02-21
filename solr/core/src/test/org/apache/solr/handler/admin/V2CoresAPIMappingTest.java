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

package org.apache.solr.handler.admin;

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.admin.api.AllCoresStatusAPI;
import org.apache.solr.handler.admin.api.CreateCoreAPI;
import org.apache.solr.handler.admin.api.SingleCoreStatusAPI;
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
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.common.params.CollectionAdminParams.NUM_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.*;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.STATUS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the /cores APIs.
 *
 * Note that the V2 requests made by these tests are not necessarily semantically valid.  They shouldn't be taken as
 * examples. In several instances, mutually exclusive JSON parameters are provided.  This is done to exercise conversion
 * of all parameters, even if particular combinations are never expected in the same request.
 */
public class V2CoresAPIMappingTest extends SolrTestCaseJ4 {
    private ApiBag apiBag;
    private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
    private CoreAdminHandler mockCoreAdminHandler;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Before
    public void setUpMocks() {
        mockCoreAdminHandler = mock(CoreAdminHandler.class);
        queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

        apiBag = new ApiBag(false);
        apiBag.registerObject(new CreateCoreAPI(mockCoreAdminHandler));
        apiBag.registerObject(new SingleCoreStatusAPI(mockCoreAdminHandler));
        apiBag.registerObject(new AllCoresStatusAPI(mockCoreAdminHandler));
    }

    @Test
    public void testCreateCoreAllProperties() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores", "POST",
                "{'create': {" +
                        "'name': 'someCoreName', " +
                        "'instanceDir': 'someInstanceDir', " +
                        "'dataDir': 'someDataDir', " +
                        "'ulogDir': 'someUpdateLogDirectory', " +
                        "'schema': 'some-schema-file-name', " +
                        "'config': 'some-config-file-name', " +
                        "'configSet': 'someConfigSetName', " +
                        "'loadOnStartup': true, " +
                        "'isTransient': true, " +
                        "'shard': 'someShardName', " +
                        "'collection': 'someCollectionName', " +
                        "'replicaType': 'TLOG', " +
                        "'coreNodeName': 'someNodeName', " +
                        "'numShards': 123, " +
                        "'roles': ['role1', 'role2'], " +
                        "'properties': {'prop1': 'val1', 'prop2': 'val2'}, " +
                        "'newCollection': true, " +
                        "'async': 'requestTrackingId' " +
                        "}}");

        assertEquals(CREATE.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
        assertEquals("someCoreName", v1Params.get(NAME));
        assertEquals("someInstanceDir", v1Params.get(INSTANCE_DIR));
        assertEquals("someDataDir", v1Params.get(DATA_DIR));
        assertEquals("someUpdateLogDirectory", v1Params.get(ULOG_DIR));
        assertEquals("some-schema-file-name", v1Params.get(SCHEMA));
        assertEquals("some-config-file-name", v1Params.get(CONFIG));
        assertEquals("someConfigSetName", v1Params.get(CONFIGSET));
        assertEquals(true, v1Params.getPrimitiveBool(LOAD_ON_STARTUP));
        assertEquals(true, v1Params.getPrimitiveBool(TRANSIENT));
        assertEquals("someShardName", v1Params.get(SHARD));
        assertEquals("someCollectionName", v1Params.get(COLLECTION));
        assertEquals("TLOG", v1Params.get(REPLICA_TYPE));
        assertEquals("someNodeName", v1Params.get(CORE_NODE_NAME));
        assertEquals(123, v1Params.getPrimitiveInt(NUM_SHARDS));
        assertEquals("role1,role2", v1Params.get(ROLES));
        assertEquals("val1", v1Params.get("property.prop1"));
        assertEquals("val2", v1Params.get("property.prop2"));
        assertEquals(true, v1Params.getPrimitiveBool(NEW_COLLECTION));
        assertEquals("requestTrackingId", v1Params.get(ASYNC));
    }

    @Test
    public void testSpecificCoreStatusApiAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/someCore", "GET",
                Map.of(INDEX_INFO, new String[] { "true" }));

        assertEquals(STATUS.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
        assertEquals("someCore", v1Params.get(CORE));
        assertEquals(true, v1Params.getPrimitiveBool(INDEX_INFO));
    }

    @Test
    public void testAllCoreStatusApiAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores", "GET",
                Map.of(INDEX_INFO, new String[] { "true" }));

        assertEquals(STATUS.name().toLowerCase(Locale.ROOT), v1Params.get(ACTION));
        assertNull("Expected 'core' parameter to be null", v1Params.get(CORE));
        assertEquals(true, v1Params.getPrimitiveBool(INDEX_INFO));
    }

    private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody) throws Exception {
        final HashMap<String, String> parts = new HashMap<>();
        final Api api = apiBag.lookup(path, method, parts);
        final SolrQueryResponse rsp = new SolrQueryResponse();
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, Maps.newHashMap()) {
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
        verify(mockCoreAdminHandler).handleRequestBody(queryRequestCaptor.capture(), any());
        return queryRequestCaptor.getValue().getParams();
    }

    private SolrParams captureConvertedV1Params(String path, String method, Map<String, String[]> queryParams) throws Exception {
        final HashMap<String, String> parts = new HashMap<>();
        final Api api = apiBag.lookup(path, method, parts);
        final SolrQueryResponse rsp = new SolrQueryResponse();
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, queryParams) {
            @Override
            public List<CommandOperation> getCommands(boolean validateInput) {
                return Collections.emptyList();
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
        verify(mockCoreAdminHandler).handleRequestBody(queryRequestCaptor.capture(), any());
        return queryRequestCaptor.getValue().getParams();
    }
}
