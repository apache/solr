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
package org.apache.solr.security;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.Principal;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.CryptoKeys;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class TestPKIAuthenticationPlugin extends SolrTestCaseJ4 {

  private static class MockPKIAuthenticationPlugin extends PKIAuthenticationPlugin {
    SolrRequestInfo solrRequestInfo;

    final PublicKey myKey;

    public MockPKIAuthenticationPlugin(String node) {
      super(null, node, new PublicKeyHandler());
      myKey = CryptoKeys.deserializeX509PublicKey(getPublicKey());
    }

    @Override
    SolrRequestInfo getRequestInfo() {
      return solrRequestInfo;
    }

    @Override
    PublicKey fetchPublicKeyFromRemote(String ignored) {
      return myKey;
    }

    @Override
    boolean isSolrThread() {
      return true;
    }
  }

  final AtomicReference<Principal> principal = new AtomicReference<>();
  final AtomicReference<String> headerValue = new AtomicReference<>();
  final AtomicReference<ServletRequest> wrappedRequestByFilter = new AtomicReference<>();

  final FilterChain filterChain =
      (servletRequest, servletResponse) -> wrappedRequestByFilter.set(servletRequest);
  final String nodeName = "node_x_233";

  final SolrQueryRequestBase solrQueryRequestBase =
      new SolrQueryRequestBase(null, new ModifiableSolrParams());

  String headerKey;
  HttpServletRequest mockReq;
  MockPKIAuthenticationPlugin mock;
  Map<String, String> headers;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeWorkingMockito();

    principal.set(null);
    headerValue.set(null);
    wrappedRequestByFilter.set(null);

    headerKey = PKIAuthenticationPlugin.HEADER_V2;

    mockReq = createMockRequest(headerValue);
    mock = new MockPKIAuthenticationPlugin(nodeName);
    mockMetrics(mock);
    headers = new HashMap<>();
  }

  private static void mockMetrics(MockPKIAuthenticationPlugin mock) {
    SolrMetricsContext smcMock = mock(SolrMetricsContext.class);
    when(smcMock.getChildContext(any())).thenReturn(smcMock);
    LongCounter longCounterMock = mock(LongCounter.class);
    LongHistogram longHistogramMock = mock(LongHistogram.class);
    when(smcMock.longCounter(any(), any())).thenReturn(longCounterMock);
    when(smcMock.longHistogram(any(), any(), any())).thenReturn(longHistogramMock);
    mock.initializeMetrics(smcMock, Attributes.empty());
  }

  @Override
  public void tearDown() throws Exception {
    if (mock != null) {
      mock.close();
    }
    super.tearDown();
  }

  public void testBasicRequest() throws Exception {
    String username = "solr user"; // with spaces
    solrQueryRequestBase.setUserPrincipalName(username);
    mock.solrRequestInfo = new SolrRequestInfo(solrQueryRequestBase, new SolrQueryResponse());
    mockSetHeaderOnRequest();
    headerValue.set(headers.get(headerKey));
    assertNotNull(headerValue.get());
    assertTrue(headerValue.get().startsWith(nodeName));
    assertTrue(mock.authenticate(mockReq, null, filterChain));

    assertNotNull(wrappedRequestByFilter.get());
    assertNotNull(((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal());
    assertEquals(
        username, ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());
  }

  private void mockSetHeaderOnRequest() {
    mock.setHeader((k, v) -> headers.put(k, v));
  }

  public void testSuperUser() throws Exception {
    // Simulate the restart of a node, this will return a different key on subsequent invocations.
    // Create it in advance because it can take some time and should be done before header is set
    MockPKIAuthenticationPlugin mock1 =
        new MockPKIAuthenticationPlugin(nodeName) {
          boolean firstCall = true;

          @Override
          PublicKey fetchPublicKeyFromRemote(String ignored) {
            try {
              return firstCall ? myKey : mock.myKey;
            } finally {
              firstCall = false;
            }
          }
        };
    mockMetrics(mock1);

    // Setup regular superuser request
    mock.solrRequestInfo = null;
    mockSetHeaderOnRequest();
    headerValue.set(headers.get(headerKey));
    assertNotNull(headerValue.get());
    assertTrue(headerValue.get().startsWith(nodeName));

    assertTrue(mock.authenticate(mockReq, null, filterChain));
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals(
        "$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());

    // With the simulated restart
    assertTrue(mock1.authenticate(mockReq, null, filterChain));
    assertNotNull(wrappedRequestByFilter.get());
    assertEquals(
        "$", ((HttpServletRequest) wrappedRequestByFilter.get()).getUserPrincipal().getName());
    mock1.close();
  }

  @Test
  public void testLegacyV1HeaderRejected() throws Exception {
    // A request with only the legacy SolrAuth (v1) header and no SolrAuthV2 header should be
    // rejected, since PKI v1 support has been removed. Verify that the plugin never even consults
    // the SolrAuth header — the rejection must be due to the absence of SolrAuthV2, not due to
    // the v1 token being malformed.
    HttpServletRequest legacyReq = mock(HttpServletRequest.class);
    when(legacyReq.getHeader(PKIAuthenticationPlugin.HEADER_V2)).thenReturn(null);
    when(legacyReq.getRequestURI()).thenReturn("/collection1/select");

    HttpServletResponse response = mock(HttpServletResponse.class);
    assertFalse(
        "Should have rejected request with only a legacy v1 SolrAuth header",
        mock.authenticate(legacyReq, response, filterChain));

    verify(legacyReq, never()).getHeader("SolrAuth");
    verify(response)
        .setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), PKIAuthenticationPlugin.HEADER_V2);
    verify(response).sendError(ArgumentMatchers.eq(401), anyString());

    assertNull(
        "Should not have proceeded after authentication failure", wrappedRequestByFilter.get());
  }

  private HttpServletRequest createMockRequest(final AtomicReference<String> headerValue) {
    HttpServletRequest mockReq = mock(HttpServletRequest.class);
    when(mockReq.getHeader(any(String.class)))
        .then(
            invocation -> {
              if (headerKey.equals(invocation.getArgument(0))) {
                return headerValue.get();
              } else return null;
            });
    when(mockReq.getRequestURI()).thenReturn("/collection1/select");
    return mockReq;
  }
}
