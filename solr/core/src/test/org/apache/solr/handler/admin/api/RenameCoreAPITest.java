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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.RenameCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class RenameCoreAPITest extends SolrTestCaseJ4 {

  private static final String CORE_NAME = DEFAULT_TEST_COLLECTION_NAME;

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private RenameCore api;

  @BeforeClass
  public static void beforeTest() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    CoreContainer coreContainer = solrTestRule.getCoreContainer();
    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        new CoreAdminHandler.CoreAdminAsyncTracker();
    api =
        new RenameCore(
            coreContainer,
            coreAdminAsyncTracker,
            new SolrQueryRequestBase(null, new ModifiableSolrParams()),
            new SolrQueryResponse());
  }

  @Test
  public void testRenameCoreToSameNameSucceeds() throws Exception {
    RenameCoreRequestBody requestBody = new RenameCoreRequestBody();
    requestBody.to = CORE_NAME;
    SolrJerseyResponse response = api.renameCore(CORE_NAME, requestBody);
    assertEquals(0, response.responseHeader.status);
  }

  @Test
  public void testMissingRequestBodyThrowsError() {
    final SolrException solrException =
        expectThrows(SolrException.class, () -> api.renameCore(CORE_NAME, null));
    assertEquals(400, solrException.code());
    assertTrue(solrException.getMessage().contains("Required request-body is missing"));
  }

  @Test
  public void testMissingToParameterThrowsError() {
    RenameCoreRequestBody requestBody = new RenameCoreRequestBody();
    final SolrException solrException =
        expectThrows(SolrException.class, () -> api.renameCore(CORE_NAME, requestBody));
    assertEquals(400, solrException.code());
    assertTrue(solrException.getMessage().contains("Missing required parameter: to"));
  }
}
