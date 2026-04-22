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
package org.apache.solr.client.solrj;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.StreamingResponseCallback;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link WrappedSolrRequest}; focused on ensuring that the "wrapped" and "wrapper"
 * SolrRequest instances line up on retvals, etc.
 *
 * <p>Getters are tested by modifying the "wrapped" value and ensuring the "wrapper" reflects the
 * change. Setters are tested by modifying the "wrapper" value and ensuring the change propagates to
 * the "wrapped" instance.
 */
public class WrappedSolrRequestTest extends SolrTestCase {

  private GenericSolrRequest inner;
  private WrappedSolrRequest<SimpleSolrResponse> wrapper;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    inner = new GenericSolrRequest(SolrRequest.METHOD.GET, "/test");
    wrapper = new WrappedSolrRequest<>(inner);
  }

  @Test
  public void testGetMethod() {
    inner.setMethod(SolrRequest.METHOD.POST);
    assertEquals(inner.getMethod(), wrapper.getMethod());
  }

  @Test
  public void testSetMethod() {
    wrapper.setMethod(SolrRequest.METHOD.DELETE);
    assertEquals(inner.getMethod(), wrapper.getMethod());
  }

  @Test
  public void testGetPath() {
    inner.setPath("/updated");
    assertEquals(inner.getPath(), wrapper.getPath());
  }

  @Test
  public void testSetPath() {
    wrapper.setPath("/updated");
    assertEquals(inner.getPath(), wrapper.getPath());
  }

  @Test
  public void testGetResponseParser() {
    inner.setResponseParser(new XMLResponseParser());
    assertSame(inner.getResponseParser(), wrapper.getResponseParser());
  }

  @Test
  public void testSetResponseParser() {
    XMLResponseParser rp = new XMLResponseParser();
    wrapper.setResponseParser(rp);
    assertSame(inner.getResponseParser(), wrapper.getResponseParser());
  }

  @Test
  public void testGetStreamingResponseCallback() {
    StreamingResponseCallback cb = noopCallback();
    inner.setStreamingResponseCallback(cb);
    assertSame(inner.getStreamingResponseCallback(), wrapper.getStreamingResponseCallback());
  }

  @Test
  public void testSetStreamingResponseCallback() {
    wrapper.setStreamingResponseCallback(noopCallback());
    assertSame(inner.getStreamingResponseCallback(), wrapper.getStreamingResponseCallback());
  }

  @Test
  public void testGetQueryParams() {
    inner.setQueryParams(Set.of("q", "rows"));
    assertEquals(inner.getQueryParams(), wrapper.getQueryParams());
  }

  @Test
  public void testSetQueryParams() {
    wrapper.setQueryParams(Set.of("q", "rows"));
    assertEquals(inner.getQueryParams(), wrapper.getQueryParams());
  }

  @Test
  public void testGetRequestType() {
    inner.setRequestType(SolrRequest.SolrRequestType.ADMIN);
    assertEquals(inner.getRequestType(), wrapper.getRequestType());
  }

  @Test
  public void testSetRequestType() {
    wrapper.setRequestType(SolrRequest.SolrRequestType.QUERY);
    assertEquals(inner.getRequestType(), wrapper.getRequestType());
  }

  @Test
  public void testGetPreferredNodes() {
    inner.setPreferredNodes(List.of("node1:8983_solr", "node2:8983_solr"));
    assertEquals(inner.getPreferredNodes(), wrapper.getPreferredNodes());
  }

  @Test
  public void testSetPreferredNodes() {
    wrapper.setPreferredNodes(List.of("node1:8983_solr", "node2:8983_solr"));
    assertEquals(inner.getPreferredNodes(), wrapper.getPreferredNodes());
  }

  @Test
  public void testGetUserPrincipal() {
    Principal principal = () -> "test-user";
    inner.setUserPrincipal(principal);
    assertEquals(inner.getUserPrincipal(), wrapper.getUserPrincipal());
  }

  @Test
  public void testSetUserPrincipal() {
    wrapper.setUserPrincipal(() -> "test-user");
    assertEquals(inner.getUserPrincipal(), wrapper.getUserPrincipal());
  }

  @Test
  public void testGetBasicAuthUser() {
    inner.setBasicAuthCredentials("alice", "secret");
    assertEquals(inner.getBasicAuthUser(), wrapper.getBasicAuthUser());
  }

  @Test
  public void testGetBasicAuthPassword() {
    inner.setBasicAuthCredentials("alice", "secret");
    assertEquals(inner.getBasicAuthPassword(), wrapper.getBasicAuthPassword());
  }

  @Test
  public void testSetBasicAuthCredentials() {
    wrapper.setBasicAuthCredentials("alice", "secret");
    assertEquals(inner.getBasicAuthUser(), wrapper.getBasicAuthUser());
    assertEquals(inner.getBasicAuthPassword(), wrapper.getBasicAuthPassword());
  }

  @Test
  public void testRequiresCollection() {
    inner.setRequiresCollection(true);
    assertEquals(inner.requiresCollection(), wrapper.requiresCollection());
  }

  @Test
  public void testGetApiVersion() {
    assertEquals(inner.getApiVersion(), wrapper.getApiVersion());
  }

  @Test
  @SuppressWarnings("UndefinedEquals") // Reference-check equality here is fine.
  public void testGetContentStreams() throws Exception {
    assertEquals(inner.getContentStreams(), wrapper.getContentStreams());
  }

  @Test
  public void testGetContentWriter() {
    RequestWriter.ContentWriter cw =
        inner.withContent(new byte[] {1, 2, 3}, "application/octet-stream").contentWriter;
    assertEquals(
        inner.getContentWriter("application/octet-stream"),
        wrapper.getContentWriter("application/octet-stream"));
    assertSame(cw, wrapper.getContentWriter("application/octet-stream"));
  }

  @Test
  public void testGetCollection() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", "myCollection");
    GenericSolrRequest innerWithCollection =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/test", params);
    final var wrapperWithCollection = new WrappedSolrRequest<>(innerWithCollection);
    assertEquals(innerWithCollection.getCollection(), wrapperWithCollection.getCollection());
  }

  @Test
  public void testGetHeaders() {
    inner.addHeader("X-Custom", "value");
    assertEquals(inner.getHeaders(), wrapper.getHeaders());
  }

  @Test
  public void testAddHeader() {
    wrapper.addHeader("X-Custom", "value");
    assertEquals(inner.getHeaders(), wrapper.getHeaders());
  }

  @Test
  public void testAddHeaders() {
    wrapper.addHeaders(Map.of("X-Foo", "foo", "X-Bar", "bar"));
    assertEquals(inner.getHeaders(), wrapper.getHeaders());
  }

  @Test
  public void testGetParams() {
    SolrParams params = inner.getParams();
    assertSame(params, wrapper.getParams());
  }

  private static StreamingResponseCallback noopCallback() {
    return new StreamingResponseCallback() {
      @Override
      public void streamSolrDocument(SolrDocument doc) {}

      @Override
      public void streamDocListInfo(long numFound, long start, Float maxScore) {}
    };
  }
}
