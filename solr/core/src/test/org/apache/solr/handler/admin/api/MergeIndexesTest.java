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

import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.endpoint.MergeIndexesApi;
import org.apache.solr.client.api.model.MergeIndexesRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class MergeIndexesTest extends SolrTestCaseJ4 {
  private MergeIndexesApi mergeIndexesApi;
  private CoreContainer coreContainer;

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
    coreContainer = h.getCoreContainer();

    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        new CoreAdminHandler.CoreAdminAsyncTracker();
    mergeIndexesApi =
        new MergeIndexes(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
    assumeWorkingMockito();
  }

  @Test
  public void testReportsErrorIfBothIndexDirAndSrcCoreAreEmpty() throws Exception {
    var requestBody = new MergeIndexesRequestBody();
    requestBody.indexDir = null;
    requestBody.srcCore = null;
    var ex =
        assertThrows(
            SolrException.class, () -> mergeIndexesApi.mergeIndexes(coreName, requestBody));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("At least one indexDir or srcCore must be specified"));
  }

  @Test
  public void testReportsErrorIfSrcCoreMissing() throws Exception {
    final var INVALID_CORE = "INVALID_CORE";
    var requestBody = new MergeIndexesRequestBody();
    requestBody.srcCore = Arrays.asList(INVALID_CORE);
    var ex =
        assertThrows(
            SolrException.class, () -> mergeIndexesApi.mergeIndexes(coreName, requestBody));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("Core: " + INVALID_CORE + " does not exist"));
  }

  @Test
  public void testReportsErrorIfPathNotAllowed() throws Exception {
    CoreContainer mockCoreContainer = Mockito.mock(CoreContainer.class);
    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        Mockito.mock(CoreAdminHandler.CoreAdminAsyncTracker.class);
    SolrCore mockSolrCore = Mockito.mock(SolrCore.class);

    final var requestBody = new MergeIndexesRequestBody();
    final var path_not_allowed = "INVALID_PATH";
    requestBody.indexDir = Arrays.asList(path_not_allowed);

    var mergeIndexes =
        new MergeIndexes(mockCoreContainer, coreAdminAsyncTracker, req(), new SolrQueryResponse());

    Mockito.when(mockCoreContainer.getCore(coreName)).thenReturn(mockSolrCore);
    Mockito.when(mockSolrCore.getCoreContainer()).thenReturn(mockCoreContainer);
    Mockito.doThrow(IndexOutOfBoundsException.class)
        .when(mockCoreContainer)
        .assertPathAllowed(Paths.get(path_not_allowed));
    var exp =
        assertThrows(SolrException.class, () -> mergeIndexes.mergeIndexes(coreName, requestBody));
  }

  @AfterClass // unique core per test
  public static void coreDestroy() {
    deleteCore();
  }
}
