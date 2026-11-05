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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.RebalanceLeadersRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for rebalance-leaders request parameters and top-level response fields. */
public class RebalanceLeadersAPITest extends MockV2APITest {

  private RebalanceLeaders api;
  private ModifiableSolrParams originalParams;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    final var handler = mock(CollectionsHandler.class);
    when(mockCoreContainer.getCollectionsHandler()).thenReturn(handler);
    when(handler.getCoreContainer()).thenReturn(mockCoreContainer);
    final var collection = mock(DocCollection.class);
    when(collection.getSlices()).thenReturn(List.of());
    when(mockClusterState.getCollection("collection")).thenReturn(collection);
    originalParams = new ModifiableSolrParams();
    originalParams.set("collection", "queryCollection");
    originalParams.add("otherParam", "first", "second");
    when(mockQueryRequest.getParams()).thenReturn(originalParams);
    doAnswer(
            invocation -> {
              when(mockQueryRequest.getParams()).thenReturn(invocation.getArgument(0));
              return null;
            })
        .when(mockQueryRequest)
        .setParams(any());
    api = new RebalanceLeaders(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testBodyParametersOverrideQueryParameters() throws Exception {
    originalParams.set("maxAtOnce", 99);
    originalParams.set("maxWaitSeconds", 999);
    final var body = new RebalanceLeadersRequestBody();
    body.maxAtOnce = 2;
    body.maxWaitSeconds = 30;

    final var response = api.rebalanceLeaders("collection", body);

    final var params = mockQueryRequest.getParams();
    assertEquals("collection", params.get("collection"));
    assertEquals(Integer.valueOf(2), params.getInt("maxAtOnce"));
    assertEquals(Integer.valueOf(30), params.getInt("maxWaitSeconds"));
    assertArrayEquals(new String[] {"first", "second"}, params.getParams("otherParam"));
    assertEquals("queryCollection", originalParams.get("collection"));
    verify(mockZkStateReader).forceUpdateCollection("collection");
    assertEquals(
        Map.of("Success", "All active replicas with the preferredLeader property set are leaders"),
        response.unknownProperties().get("Summary"));
  }

  @Test
  public void testEmptyBodyDefaults() throws Exception {
    api.rebalanceLeaders("collection", new RebalanceLeadersRequestBody());

    assertNull(mockQueryRequest.getParams().get("maxAtOnce"));
    assertEquals(Integer.valueOf(60), mockQueryRequest.getParams().getInt("maxWaitSeconds"));
  }

  @Test
  public void testNullBodyDefaults() throws Exception {
    final var response = api.rebalanceLeaders("collection", null);

    assertNull(mockQueryRequest.getParams().get("maxAtOnce"));
    assertEquals(Integer.valueOf(60), mockQueryRequest.getParams().getInt("maxWaitSeconds"));
    assertNotNull(response.unknownProperties().get("Summary"));
  }

  @Test
  public void testRejectsMissingCollection() {
    final var exception =
        expectThrows(
            SolrException.class,
            () -> api.rebalanceLeaders(null, new RebalanceLeadersRequestBody()));
    assertEquals(400, exception.code());
    assertEquals("Missing required parameter: collection", exception.getMessage());
  }

  @Test
  public void testRejectsStandaloneMode() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);
    final var exception =
        expectThrows(
            SolrException.class,
            () -> api.rebalanceLeaders("collection", new RebalanceLeadersRequestBody()));
    assertEquals(400, exception.code());
    assertEquals("Solr instance is not running in SolrCloud mode.", exception.getMessage());
  }
}
