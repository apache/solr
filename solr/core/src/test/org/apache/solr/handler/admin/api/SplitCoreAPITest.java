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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.nio.file.Path;
import java.util.HashMap;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SplitCoreRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the JAX-RS {@link SplitCoreAPI} V2 endpoint. */
public class SplitCoreAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private static final String CORE_NAME = DEFAULT_TEST_CORENAME;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path configSet = createTempDir("configSet");
    copyMinConf(configSet);
    EnvUtils.setProperty(ALLOW_PATHS_SYSPROP, configSet.toAbsolutePath().toString());
    solrTestRule.startSolr(LuceneTestCase.createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(configSet).create();
  }

  private SplitCoreAPI createApi() {
    CoreContainer coreContainer = solrTestRule.getCoreContainer();
    CoreAdminHandler.CoreAdminAsyncTracker asyncTracker =
        coreContainer.getMultiCoreHandler().getCoreAdminAsyncTracker();
    SolrCore solrCore = coreContainer.getCore(CORE_NAME);
    SolrQueryRequestBase req =
        new SolrQueryRequestBase(solrCore, new MapSolrParams(new HashMap<>())) {};
    solrCore.close(); // decrement ref count; CoreContainer still holds the core
    SolrQueryResponse rsp = new SolrQueryResponse();
    return new SplitCoreAPI(coreContainer, asyncTracker, req, rsp);
  }

  @Test
  public void testSplitWithNoTargetCoreOrPathReturnsError() throws Exception {
    SplitCoreAPI api = createApi();
    SplitCoreRequestBody requestBody = new SplitCoreRequestBody();

    SolrException ex =
        assertThrows(SolrException.class, () -> api.splitCore(CORE_NAME, requestBody));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Expected error about missing targetCore or path",
        ex.getMessage().contains("path") || ex.getMessage().contains("targetCore"));
  }

  @Test
  public void testSplitWithNullRequestBodyReturnsError() throws Exception {
    SplitCoreAPI api = createApi();

    SolrException ex = assertThrows(SolrException.class, () -> api.splitCore(CORE_NAME, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Expected error about missing targetCore or path",
        ex.getMessage().contains("path") || ex.getMessage().contains("targetCore"));
  }
}
