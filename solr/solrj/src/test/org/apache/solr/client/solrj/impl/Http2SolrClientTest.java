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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link Http2SolrClient}.
 *
 * <p>This test runs the client against a plain embedded Jetty server (via {@link
 * SolrJettyTestBase}) hosting {@link DebugServlet}, which records the last request it received
 * (method, headers, parameters, query string, cookies). It does NOT use a mock-response server
 * such as WireMock: WireMock 2.x embeds Jetty 9.4 and is binary-incompatible with this fork's
 * Jetty 10, and WireMock 3.x jumped to Jetty 11/jakarta. The DebugServlet harness is the
 * canonical upstream Apache Solr approach for these client unit tests and is fully portable.
 */
@SolrTestCaseJ4.SuppressSSL
public class Http2SolrClientTest extends SolrJettyTestBase {

  private static final String EXPECTED_USER_AGENT =
      "Solr[" + Http2SolrClient.class.getName() + "] 2.0";

  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig =
        JettyConfig.builder()
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .withServlet(new ServletHolder(SlowServlet.class), "/slow/*")
            .withSSLConfig(sslConfig.buildServerSSLConfig())
            .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  /**
   * Sleeps long enough for any reasonable client idle timeout to fire, so {@code testTimeout}
   * actually exercises the timeout path. Without this servlet {@code /slow/*} would 404
   * immediately and the test would never observe a "Timeout" error.
   */
  public static class SlowServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      doGet(req, resp);
    }
  }

  @AfterClass
  public static void cleanup() {
    DebugServlet.clear();
  }

  private Http2SolrClient getHttp2SolrClient(String url, int connectionTimeOut, int socketTimeout) {
    return new Http2SolrClient.Builder(url)
        .connectionTimeout(connectionTimeOut)
        .idleTimeout(socketTimeout)
        .build();
  }

  private Http2SolrClient getHttp2SolrClient(String url) {
    return new Http2SolrClient.Builder(url).build();
  }

  @Test
  @LuceneTestCase.Nightly // works but is slow due to timeout
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        getHttp2SolrClient(jetty.getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 2000)) {
      // The fork's Http2SolrClient surfaces every idle/request-timeout as a
      // SolrServerException wrapping the underlying TimeoutException (see Http2SolrClient
      // request paths), matching the throws clause of SolrClient.query.
      SolrServerException e =
          LuceneTestCase.expectThrows(
              SolrServerException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertTrue(e.getMessage().contains("Timeout"));
    }
  }

  @Test
  public void test0IdleTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        getHttp2SolrClient(
            jetty.getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, 0)) {
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
  }

  /**
   * test that SolrExceptions thrown by Http2SolrClient can correctly encapsulate http status codes
   * even when not on the list of ErrorCodes solr may return.
   */
  @Test
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(
        status
            + " didn't generate an UNKNOWN error code, someone modified the list of valid "
            + "ErrorCode's w/o changing this test to work a different way",
        SolrException.ErrorCode.UNKNOWN,
        SolrException.ErrorCode.getErrorCode(status));

    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      DebugServlet.setErrorCode(status);
      SolrQuery q = new SolrQuery("foo");
      SolrException e =
          LuceneTestCase.expectThrows(
              SolrException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertEquals("Unexpected exception status code", status, e.code());
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "ሴ");

      // GET
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.GET));
      assertEquals("get", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertNull(DebugServlet.headers.get("content-type"));

      // POST
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.POST));
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "application/x-www-form-urlencoded",
          DebugServlet.headers.get("content-type"));

      // PUT
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.PUT));
      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "application/x-www-form-urlencoded",
          DebugServlet.headers.get("content-type"));
    }

    // XML/GET
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      client.setParser(new XMLResponseParser());
      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "ሴ");

      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.GET));
      assertEquals("get", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertNull(DebugServlet.headers.get("content-type"));

      // XML/POST
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.POST));
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "application/x-www-form-urlencoded",
          DebugServlet.headers.get("content-type"));

      // XML/PUT
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class,
          () -> client.query(q, SolrRequest.METHOD.PUT));
      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "application/x-www-form-urlencoded",
          DebugServlet.headers.get("content-type"));
    }
  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.deleteById("id"));
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
    }

    // XML response and writer
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.deleteByQuery("*:*"));
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      Collection<String> ids = Collections.singletonList("a");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("a"));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById(ids, null));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", "a"));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", ids, null));
    }
  }

  @Test
  public void testUpdate() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.add(new SolrInputDocument());
      ureq.setParam("a", "ሴ");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
    }

    // XML response and writer
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      client.setParser(new XMLResponseParser());
      client.setRequestWriter(new RequestWriter());
      DebugServlet.clear();
      UpdateRequest ureq = new UpdateRequest();
      ureq.add(new SolrInputDocument());
      ureq.setParam("a", "ሴ");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("content-type"));
    }

    // javabin request and response
    try (Http2SolrClient client = getHttp2SolrClient(jetty.getBaseUrl() + "/debug/foo")) {
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());
      DebugServlet.clear();
      UpdateRequest ureq = new UpdateRequest();
      ureq.add(new SolrInputDocument());
      ureq.setParam("a", "ሴ");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("ሴ", DebugServlet.parameters.get("a")[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
    }
  }

  @Test
  public void testGetDefaultSslContextFactory() {
    // getDefaultSslContextFactory() only returns a (non-null) factory when at least one
    // javax.net.ssl.* property is set; set one here so this test is deterministic regardless of
    // whether the surrounding test run randomized SSL on or off.
    final String prevTrustStore = System.getProperty("javax.net.ssl.trustStore");
    System.setProperty("javax.net.ssl.trustStore", "dummy-truststore");
    System.clearProperty("solr.jetty.ssl.verifyClientHostName");
    try {
      assertNull(
          Http2SolrClient.getDefaultSslContextFactory().getEndpointIdentificationAlgorithm());

      System.setProperty("solr.jetty.ssl.verifyClientHostName", "HTTPS");
      SslContextFactory.Client sslContextFactory = Http2SolrClient.getDefaultSslContextFactory();
      assertEquals("HTTPS", sslContextFactory.getEndpointIdentificationAlgorithm());
    } finally {
      System.clearProperty("solr.jetty.ssl.verifyClientHostName");
      if (prevTrustStore == null) {
        System.clearProperty("javax.net.ssl.trustStore");
      } else {
        System.setProperty("javax.net.ssl.trustStore", prevTrustStore);
      }
    }
  }

  private Set<String> setOf(String... keys) {
    Set<String> set = new TreeSet<>();
    if (keys != null) {
      Collections.addAll(set, keys);
    }
    return set;
  }

  private void setReqParamsOf(UpdateRequest req, String... keys) {
    if (keys != null) {
      for (String k : keys) {
        req.setParam(k, k + "Value");
      }
    }
  }

  private void verifyServletState(Http2SolrClient client, SolrRequest<?> request) {
    // check query String
    Iterator<String> paramNames = request.getParams().getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String name = paramNames.next();
      String[] values = request.getParams().getParams(name);
      if (values != null) {
        for (String value : values) {
          boolean shouldBeInQueryString =
              client.getQueryParams().contains(name)
                  || (request.getQueryParams() != null && request.getQueryParams().contains(name));
          // in either case, it should be in the parameters
          if (shouldBeInQueryString) {
            assertTrue(
                "param " + name + " should be in query string",
                DebugServlet.queryString.contains(name + "=" + value));
          } else {
            assertFalse(
                "param " + name + " should NOT be in query string",
                DebugServlet.queryString != null && DebugServlet.queryString.contains(name + "="));
          }
        }
      }
    }
  }

  @Test
  public void testQueryString() throws Exception {
    final String clientUrl = jetty.getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client = getHttp2SolrClient(clientUrl)) {
      // test without request query params
      DebugServlet.clear();
      client.setQueryParams(setOf("serverOnly"));
      UpdateRequest req = new UpdateRequest();
      setReqParamsOf(req, "serverOnly", "notServer");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test without server query params
      DebugServlet.clear();
      client.setQueryParams(setOf());
      req = new UpdateRequest();
      req.setQueryParams(setOf("requestOnly"));
      setReqParamsOf(req, "requestOnly", "notRequest");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test with both request and server query params
      DebugServlet.clear();
      req = new UpdateRequest();
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test with both request and server query params with single stream
      DebugServlet.clear();
      req = new UpdateRequest();
      req.add(new SolrInputDocument());
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request
      // so it passes the verification step.
      req.setQueryParams(setOf("requestOnly", "both", "neither"));
      verifyServletState(client, req);
    }
  }

  /**
   * Missed tests : see BasicHttpSolrClientTest
   *
   * <ul>
   *   <li>set cookies via interceptor
   *   <li>invariant params
   *   <li>compression
   *   <li>get raw stream
   * </ul>
   */
}
