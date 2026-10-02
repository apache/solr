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
package org.apache.solr.handler.admin.proxy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link V2SolrRequestBasedProxy} */
public class V2SolrRequestBasedProxyTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  private ModifiableSolrParams solrParams;
  private SolrRequest<Object> mockSolrRequest;
  private CoreContainer mockStandalone;
  private CoreContainer mockCloud;

  @Before
  @SuppressWarnings("unchecked")
  public void setUpMocks() {
    solrParams = new ModifiableSolrParams();
    mockSolrRequest = mock(SolrRequest.class);
    mockStandalone = mock(CoreContainer.class);
    mockCloud = mock(CoreContainer.class);
    when(mockSolrRequest.getParams()).thenReturn(solrParams);
    // WrappedSolrRequest's constructor calls these, so stub them to avoid NPEs
    when(mockSolrRequest.getMethod()).thenReturn(SolrRequest.METHOD.GET);
    when(mockSolrRequest.getPath()).thenReturn("/api/node/properties");
    when(mockSolrRequest.getRequestType()).thenReturn(SolrRequest.SolrRequestType.ADMIN);
    when(mockStandalone.isZooKeeperAware()).thenReturn(false);
    when(mockStandalone.getZkController()).thenReturn(null);
    when(mockCloud.isZooKeeperAware()).thenReturn(true);
  }

  /** Minimal concrete subclass for testing. */
  private class TestProxy extends V2SolrRequestBasedProxy<Object> {
    TestProxy() {
      this(null);
    }

    TestProxy(CoreContainer coreContainer) {
      super(coreContainer, mockSolrRequest);
    }

    @Override
    protected void processTypedProxiedResponse(String nodeName, Object proxiedResponse) {}

    // Override the live-node validation that usually happens here.
    @Override
    protected Set<String> validateNodeNames(String nodeNames) {
      return new HashSet<>(Arrays.asList(nodeNames.split(",")));
    }
  }

  @Test
  public void shouldProxyIsFalseWhenNodesParamAbsent() {
    var proxy = new TestProxy(mockStandalone);
    assertFalse(
        "Expected 'shouldProxy' to return false when 'nodes' param is absent", proxy.shouldProxy());
  }

  @Test
  public void shouldProxyIsFalseWhenStandaloneAndNodesAll() {
    solrParams.add("nodes", "all");
    var proxy = new TestProxy(mockStandalone);
    assertFalse("Expected standalone nodes=all to be handled locally", proxy.shouldProxy());
  }

  @Test
  public void shouldProxyIsTrueWhenStandaloneAndExplicitNodes() {
    solrParams.add("nodes", "localhost:7574_solr");
    var proxy = new TestProxy(mockStandalone);
    assertTrue(
        "standalone nodes=<host> still proxies so validateNodeNames can reject it",
        proxy.shouldProxy());
  }

  @Test
  public void destinationNodesThrowsWhenStandaloneAndExplicitNodes() {
    solrParams.add("nodes", "localhost:7574_solr");
    V2SolrRequestBasedProxy<Object> proxy =
        new V2SolrRequestBasedProxy<>(mockStandalone, mockSolrRequest) {
          @Override
          protected void processTypedProxiedResponse(String nodeName, Object proxiedResponse) {}
        };
    SolrException ex = expectThrows(SolrException.class, proxy::getDestinationNodes);
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("nodes"));
    assertTrue(ex.getMessage().contains("SolrCloud"));
    // This proxy opts in to standaloneAllMeansLocal(), so the message names the 'all' exception
    assertTrue(ex.getMessage().contains("treated as this node"));
  }

  @Test
  public void shouldProxyIsTrueWhenCloudAndNodesPresent() {
    solrParams.add("nodes", "localhost:7574_solr");
    var proxy = new TestProxy(mockCloud);
    assertTrue(
        "Expected 'shouldProxy' to return true when 'nodes' param is present in SolrCloud",
        proxy.shouldProxy());
  }

  @Test
  public void testDestinationNodesExtractsValueFromNodes() {
    solrParams.add("nodes", "somehost:8983_solr");

    // Stub out live-node validation for tests.
    final var proxy = new TestProxy();

    Collection<String> nodes = proxy.getDestinationNodes();
    assertEquals(Set.of("somehost:8983_solr"), new HashSet<>(nodes));
  }

  @Test
  public void testPreparedRequestMirrorsSolrRequestVals() {
    solrParams.set("nodes", "somehost:8983_solr");
    solrParams.set("wt", "json");

    SolrRequest<Object> prepared = new TestProxy().prepareProxiedRequest();

    assertNull(
        "'nodes' param should be stripped from proxied request", prepared.getParams().get("nodes"));
    assertEquals("json", prepared.getParams().get("wt"));
    assertEquals("/api/node/properties", prepared.getPath());
  }
}
