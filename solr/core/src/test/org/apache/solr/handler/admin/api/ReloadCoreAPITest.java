/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin.api;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReloadCoreAPITest extends SolrTestCaseJ4 {

  private ReloadCoreAPI reloadCoreAPI;
  private static final String NON_EXISTENT_CORE = "non_existent_core";

  @BeforeClass
  public static void initializeCoreAndRequestFactory() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    lrf = h.getRequestFactory("/api", 0, 10);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    SolrQueryRequest solrQueryRequest = req();
    SolrQueryResponse solrQueryResponse = new SolrQueryResponse();
    CoreContainer coreContainer = h.getCoreContainer();
    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        new CoreAdminHandler.CoreAdminAsyncTracker();
    reloadCoreAPI =
        new ReloadCoreAPI(
            solrQueryRequest, solrQueryResponse, coreContainer, coreAdminAsyncTracker);
  }

  @Test
  public void testValidReloadCoreAPIResponse() throws Exception {
    SolrJerseyResponse response =
        reloadCoreAPI.reloadCore(coreName, new ReloadCoreAPI.ReloadCoreRequestBody());
    assertEquals(0, response.responseHeader.status);
    assertNotNull(response.responseHeader.qTime);
  }

  @Test
  public void testNonExistentCoreExceptionResponse() {
    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              reloadCoreAPI.reloadCore(
                  NON_EXISTENT_CORE, new ReloadCoreAPI.ReloadCoreRequestBody());
            });
    assertEquals(400, solrException.code());
    assertTrue(solrException.getMessage().contains("No such core: " + NON_EXISTENT_CORE));
  }

  @AfterClass // unique core per test
  public static void coreDestroy() {
    deleteCore();
  }
}
