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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link ReplaceNodeAPI} */
public class ReplaceNodeAPITest extends SolrTestCaseJ4 {

  private CoreContainer mockCoreContainer;
  private static final String sourceNodeName = "demoSourceNode";
  private static final String targetNodeName = "demoTargetNode";
  private static final boolean waitForFinalState = false;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;
  private ReplaceNodeAPI replaceNodeApi;
  private DistributedCollectionConfigSetCommandRunner mockCommandRunner;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    mockCoreContainer = mock(CoreContainer.class);
    mockCommandRunner = mock(DistributedCollectionConfigSetCommandRunner.class);
    when(mockCoreContainer.getDistributedCollectionCommandRunner())
        .thenReturn(Optional.of(mockCommandRunner));
    when(mockCommandRunner.runCollectionCommand(any(), any(), anyLong()))
        .thenReturn(new OverseerSolrResponse(new NamedList<>()));
    mockQueryRequest = mock(SolrQueryRequest.class);
    queryResponse = new SolrQueryResponse();
    replaceNodeApi = new ReplaceNodeAPI(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testSuccessfulReplaceNodeCommand() throws Exception {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    final String expectedJson = "{\"responseHeader\":{\"status\":0,\"QTime\":0}}";
    final CollectionsHandler collectionsHandler = mock(CollectionsHandler.class);
    when(mockCoreContainer.getCollectionsHandler()).thenReturn(collectionsHandler);
    ReplaceNodeAPI.ReplaceNodeRequestBody requestBody =
        new ReplaceNodeAPI.ReplaceNodeRequestBody(targetNodeName, waitForFinalState);
    final SolrJerseyResponse response = replaceNodeApi.replaceNode(sourceNodeName, requestBody);

    assertEquals(0, response.responseHeader.status);
    assertEquals(0, response.responseHeader.qTime);
  }
}
