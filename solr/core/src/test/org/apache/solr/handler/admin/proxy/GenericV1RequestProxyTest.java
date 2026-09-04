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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link GenericV1RequestProxy} */
public class GenericV1RequestProxyTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  private ModifiableSolrParams solrParams;
  private SolrQueryRequest mockRequest;

  @Before
  public void setUpMocks() {
    solrParams = new ModifiableSolrParams();
    mockRequest = mock(SolrQueryRequest.class);

    when(mockRequest.getParams()).thenReturn(solrParams);
  }

  @Test
  public void shouldProxyReflectsPresenceOfNodesParam() {
    var proxy = new GenericV1RequestProxy(null, mockRequest, new SolrQueryResponse());
    assertFalse(
        "Expected 'shouldProxy' to return false when 'nodes' param is absent", proxy.shouldProxy());

    solrParams.add("nodes", "localhost:7574_solr");
    proxy = new GenericV1RequestProxy(null, mockRequest, new SolrQueryResponse());
    assertTrue(
        "Expected 'shouldProxy' to return true when 'nodes' param is present", proxy.shouldProxy());
  }

  // ---- getDestinationNodes() tests ----
  //
  // validateNodeNames() requires a live ZkController, so these tests use an anonymous subclass
  // that stubs it out, keeping the focus on getDestinationNodes()'s own param-reading logic.

  @Test
  public void testDestinationNodesExtractsValueFromNodes() {
    solrParams.add("nodes", "somehost:8983_solr");

    // Stub out live-node validation for tests.
    GenericV1RequestProxy proxy =
        new GenericV1RequestProxy(null, mockRequest, new SolrQueryResponse()) {
          @Override
          protected Set<String> validateNodeNames(String nodeNames) {
            return new HashSet<>(Arrays.asList(nodeNames.split(",")));
          }
        };

    Collection<String> nodes = proxy.getDestinationNodes();
    assertEquals(Set.of("somehost:8983_solr"), new HashSet<>(nodes));
  }

  @Test
  public void testPreparedRequestMirrorsSolrQueryRequestVals() {
    solrParams.set("nodes", "somehost:8983_solr");
    solrParams.set("wt", "json");
    when(mockRequest.getPath()).thenReturn("/admin/info/system");

    GenericV1RequestProxy proxy =
        new GenericV1RequestProxy(null, mockRequest, new SolrQueryResponse());
    SolrRequest<?> prepared = proxy.prepareProxiedRequest();

    assertNull(
        "'nodes' param should be stripped from proxied request", prepared.getParams().get("nodes"));
    assertEquals("json", prepared.getParams().get("wt"));
    assertEquals("/admin/info/system", prepared.getPath());
  }
}
