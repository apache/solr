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

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the V2 APIs found in {@link org.apache.solr.handler.admin.api} that use the /cores/{core} path.
 *
 * Note that the V2 requests made by these tests are not necessarily semantically valid.  They shouldn't be taken as
 * examples. In several instances, mutually exclusive JSON parameters are provided.  This is done to exercise conversion
 * of all parameters, even if particular combinations are never expected in the same request.
 */
public class V2CoreAPIMappingTest extends SolrTestCaseJ4 {
    private ApiBag apiBag;

    private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;

    private CoreAdminHandler mockCoreHandler;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Before
    public void setupApiBag() throws Exception {
        mockCoreHandler = mock(CoreAdminHandler.class);
        queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

        apiBag = new ApiBag(false);
        apiBag.registerObject(new ReloadCoreAPI(mockCoreHandler));
        apiBag.registerObject(new SwapCoresAPI(mockCoreHandler));
        apiBag.registerObject(new RenameCoreAPI(mockCoreHandler));
        apiBag.registerObject(new UnloadCoreAPI(mockCoreHandler));
        apiBag.registerObject(new MergeIndexesAPI(mockCoreHandler));
        apiBag.registerObject(new SplitCoreAPI(mockCoreHandler));
    }

    @Test
    public void testReloadCoreAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST", "{\"reload\": {}}");

        assertEquals("reload", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
    }

    @Test
    public void testSwapCoresAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST", "{\"swap\": {\"with\": \"otherCore\"}}");

        assertEquals("swap", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
        assertEquals("otherCore", v1Params.get(OTHER));
    }

    @Test
    public void testRenameCoreAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST", "{\"rename\": {\"to\": \"otherCore\"}}");

        assertEquals("rename", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
        assertEquals("otherCore", v1Params.get(OTHER));
    }

    @Test
    public void testUnloadCoreAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST",
                "{" +
                "\"unload\": {" +
                        "\"deleteIndex\": true, " +
                        "\"deleteDataDir\": true, " +
                        "\"deleteInstanceDir\": true, " +
                        "\"async\": \"someRequestId\"}}");

        assertEquals("unload", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
        assertEquals(true, v1Params.getBool(DELETE_INDEX));
        assertEquals(true, v1Params.getBool(DELETE_DATA_DIR));
        assertEquals(true, v1Params.getBool(DELETE_INSTANCE_DIR));
        assertEquals("someRequestId", v1Params.get(ASYNC));
    }

    @Test
    public void testMergeIndexesAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST",
                "{" +
                        "\"merge-indexes\": {" +
                          "\"indexDir\": [\"dir1\", \"dir2\"], " +
                          "\"srcCore\": [\"core1\", \"core2\"], " +
                          "\"updateChain\": \"someUpdateChain\", " +
                          "\"async\": \"someRequestId\"}}");

        assertEquals("mergeindexes", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
        assertEquals("someUpdateChain", v1Params.get(UpdateParams.UPDATE_CHAIN));
        assertEquals("someRequestId", v1Params.get(ASYNC));
        final List<String> indexDirs = Arrays.asList(v1Params.getParams("indexDir"));
        assertEquals(2, indexDirs.size());
        assertTrue(indexDirs.containsAll(List.of("dir1", "dir2")));
        final List<String> srcCores = Arrays.asList(v1Params.getParams("srcCore"));
        assertEquals(2, srcCores.size());
        assertTrue(srcCores.containsAll(List.of("core1", "core2")));
    }

    @Test
    public void testSplitCoreAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params("/cores/coreName", "POST",
                "{" +
                        "\"split\": {" +
                        "\"path\": [\"path1\", \"path2\"], " +
                        "\"targetCore\": [\"core1\", \"core2\"], " +
                        "\"splitKey\": \"someSplitKey\", " +
                        "\"getRanges\": true, " +
                        "\"ranges\": \"range1,range2\", " +
                        "\"async\": \"someRequestId\"}}");

        assertEquals("split", v1Params.get(ACTION));
        assertEquals("coreName", v1Params.get(CORE));
        assertEquals("someSplitKey", v1Params.get(SPLIT_KEY));
        assertEquals("range1,range2", v1Params.get(RANGES));
        assertEquals("someRequestId", v1Params.get(ASYNC));
        final List<String> pathEntries = Arrays.asList(v1Params.getParams(PATH));
        assertEquals(2, pathEntries.size());
        assertTrue(pathEntries.containsAll(List.of("path1", "path2")));
        final List<String> targetCoreEntries = Arrays.asList(v1Params.getParams(TARGET_CORE));
        assertEquals(2, targetCoreEntries.size());
        assertTrue(targetCoreEntries.containsAll(List.of("core1", "core2")));
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
        verify(mockCoreHandler).handleRequestBody(queryRequestCaptor.capture(), any());
        return queryRequestCaptor.getValue().getParams();
    }
}
