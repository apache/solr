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

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.embedded.JettyConfig;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Http2SolrClientTest extends SolrJettyTestBase {

  private static final String EXPECTED_USER_AGENT =
      "Solr[" + Http2SolrClient.class.getName() + "] 2.0";

  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      queryString = null;
      cookies = null;
      responseHeaders = null;
    }

    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String, String> headers = null;
    public static Map<String, String[]> parameters = null;
    public static String queryString = null;
    public static javax.servlet.http.Cookie[] cookies = null;
    public static List<String[]> responseHeaders = null;

    public static void setErrorCode(Integer code) {
      errorCode = code;
    }

    public static void addResponseHeader(String headerName, String headerValue) {
      if (responseHeaders == null) {
        responseHeaders = new ArrayList<>();
      }
      responseHeaders.add(new String[] {headerName, headerValue});
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "delete";
      recordRequest(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "get";
      recordRequest(req, resp);
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "head";
      recordRequest(req, resp);
    }

    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name.toLowerCase(Locale.getDefault()), req.getHeader(name));
      }
    }

    @SuppressForbidden(reason = "fake servlet only")
    private void setParameters(HttpServletRequest req) {
      parameters = req.getParameterMap();
    }

    private void setQueryString(HttpServletRequest req) {
      queryString = req.getQueryString();
    }

    private void setCookies(HttpServletRequest req) {
      javax.servlet.http.Cookie[] ck = req.getCookies();
      cookies = req.getCookies();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "post";
      recordRequest(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "put";
      recordRequest(req, resp);
    }

    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      setParameters(req);
      setQueryString(req);
      setCookies(req);
      if (responseHeaders != null) {
        for (String[] h : responseHeaders) {
          resp.addHeader(h[0], h[1]);
        }
      }
      if (null != errorCode) {
        try {
          resp.sendError(errorCode);
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in DebugServlet", e);
        }
      }
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig =
        JettyConfig.builder()
            .withServlet(
                new ServletHolder(BasicHttpSolrClientTest.RedirectServlet.class), "/redirect/*")
            .withServlet(new ServletHolder(BasicHttpSolrClientTest.SlowServlet.class), "/slow/*")
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .withSSLConfig(sslConfig.buildServerSSLConfig())
            .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("basicauth");
    System.clearProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY);
    DebugServlet.clear();
    super.tearDown();
  }

  private Http2SolrClient.Builder getHttp2SolrClientBuilder(
      String url, int connectionTimeout, int socketTimeout) {
    return new Http2SolrClient.Builder(url)
        .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
        .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        getHttp2SolrClientBuilder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 2000)
            .build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
    }
  }

  @Test
  public void test0IdleTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        getHttp2SolrClientBuilder(getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, 0)
            .build()) {
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
  }

  @Test
  public void testRequestTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        getHttp2SolrClientBuilder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 0)
            .withRequestTimeout(500, TimeUnit.MILLISECONDS)
            .build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
    }
  }

  /**
   * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
   * even when not on the list of ErrorCodes solr may return.
   */
  @Test
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(
        status
            + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
        SolrException.ErrorCode.UNKNOWN,
        SolrException.ErrorCode.getErrorCode(status));

    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build()) {
      DebugServlet.setErrorCode(status);
      try {
        SolrQuery q = new SolrQuery("foo");
        client.query(q, SolrRequest.METHOD.GET);
        fail("Didn't get excepted exception from oversided request");
      } catch (SolrException e) {
        assertEquals("Unexpected exception status code", status, e.code());
      }
    } finally {
      DebugServlet.clear();
    }
  }

  /**
   * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
   * even when not on the list of ErrorCodes solr may return.
   */
  @Test
  public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(
        status
            + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
        SolrException.ErrorCode.UNKNOWN,
        SolrException.ErrorCode.getErrorCode(status));

    try (Http2SolrClient client = new Http2SolrClient.Builder(null).build()) {
      DebugServlet.setErrorCode(status);
      try {
        // if client base url is null, request url will be used in exception message
        SolrPing ping = new SolrPing();
        ping.setBasePath(getBaseUrl() + "/debug/foo");
        client.request(ping);

        fail("Didn't get excepted exception from oversided request");
      } catch (SolrException e) {
        assertEquals("Unexpected exception status code", status, e.code());
        assertTrue(e.getMessage().contains(getBaseUrl()));
      }
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    try (Http2SolrClient client = new Http2SolrClient.Builder(url).build()) {

      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      // default method
      assertEquals("get", DebugServlet.lastMethod);
      // agent
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // agent
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      // content-type
      assertNull(DebugServlet.headers.get("content-type"));
      // param encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      // POST
      DebugServlet.clear();
      try {
        client.query(q, SolrRequest.METHOD.POST);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));

      // PUT
      DebugServlet.clear();
      try {
        client.query(q, SolrRequest.METHOD.PUT);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }
    // XML/GET
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url).withResponseParser(new XMLResponseParser()).build()) {

      DebugServlet.clear();
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("get", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));

      // XML/POST
      DebugServlet.clear();
      try {
        client.query(q, SolrRequest.METHOD.POST);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));

      DebugServlet.clear();
      try {
        client.query(q, SolrRequest.METHOD.PUT);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }
  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client = new Http2SolrClient.Builder(url).build()) {
      try {
        client.deleteById("id");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      // default method
      assertEquals("post", DebugServlet.lastMethod);
      // agent
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // agent
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
    }
    // XML
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url).withResponseParser(new XMLResponseParser()).build()) {

      try {
        client.deleteByQuery("*:*");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build()) {
      Collection<String> ids = Collections.singletonList("a");
      try {
        client.getById("a");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      try {
        client.getById(ids, null);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      try {
        client.getById("foo", "a");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      try {
        client.getById("foo", ids, null);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
  }

  @Test
  public void testUpdate() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    UpdateRequest req = new UpdateRequest();
    req.add(new SolrInputDocument());
    req.setParam("a", "\u1234");
    try (Http2SolrClient client = new Http2SolrClient.Builder(url).build()) {

      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      // default method
      assertEquals("post", DebugServlet.lastMethod);
      // agent
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // content type
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
      // parameter encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withRequestWriter(new RequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build()) {

      // XML response and writer
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("content-type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }

    // javabin request
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withRequestWriter(new BinaryRequestWriter())
            .withResponseParser(new BinaryResponseParser())
            .build()) {

      DebugServlet.clear();
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(EXPECTED_USER_AGENT, DebugServlet.headers.get("user-agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("content-type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }
  }

  @Test
  public void testFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + "/redirect/foo";
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withFollowRedirects(true).build()) {
      SolrQuery q = new SolrQuery("*:*");
      client.query(q);
    }
  }

  @Test
  public void testDoNotFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + "/redirect/foo";
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withFollowRedirects(false).build()) {
      SolrQuery q = new SolrQuery("*:*");

      SolrServerException thrown = assertThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(thrown.getMessage().contains("redirect"));
    }
  }

  @Test
  public void testRedirectSwapping() throws Exception {
    final String clientUrl = getBaseUrl() + "/redirect/foo";
    SolrQuery q = new SolrQuery("*:*");

    // default for follow redirects is false
    try (Http2SolrClient client = new Http2SolrClient.Builder(clientUrl).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withFollowRedirects(true).build()) {
      // shouldn't throw an exception
      client.query(q);
    }

    // set explicit false for following redirects
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withFollowRedirects(false).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    try (Http2SolrClient client = new Http2SolrClient.Builder(getBaseUrl()).build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      client.add("collection1", doc);
      client.commit("collection1");

      assertEquals(
          1,
          client.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());
    }

    final String collection1Url = getCoreUrl();
    try (Http2SolrClient client = new Http2SolrClient.Builder(collection1Url).build()) {
      assertEquals(1, client.query(new SolrQuery("id:collection")).getResults().getNumFound());
    }
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
              client.getUrlParamNames().contains(name)
                  || (request.getQueryParams() != null && request.getQueryParams().contains(name));
          assertEquals(
              shouldBeInQueryString, DebugServlet.queryString.contains(name + "=" + value));
          // in either case, it should be in the parameters
          assertNotNull(DebugServlet.parameters.get(name));
          assertEquals(1, DebugServlet.parameters.get(name).length);
          assertEquals(value, DebugServlet.parameters.get(name)[0]);
        }
      }
    }
  }

  @Test
  public void testQueryString() throws Exception {

    final String clientUrl = getBaseUrl() + "/debug/foo";
    UpdateRequest req = new UpdateRequest();

    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withTheseParamNamesInTheUrl(Set.of("serverOnly"))
            .build()) {
      // test without request query params
      DebugServlet.clear();
      setReqParamsOf(req, "serverOnly", "notServer");

      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test without server query params
      DebugServlet.clear();
    }
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withTheseParamNamesInTheUrl(Set.of()).build()) {
      req = new UpdateRequest();
      req.setQueryParams(Set.of("requestOnly"));
      setReqParamsOf(req, "requestOnly", "notRequest");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test with both request and server query params
      DebugServlet.clear();
    }
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withTheseParamNamesInTheUrl(Set.of("serverOnly", "both"))
            .build()) {
      req = new UpdateRequest();
      req.setQueryParams(Set.of("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);
    }
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withTheseParamNamesInTheUrl(Set.of("serverOnly", "both"))
            .build()) {

      // test with both request and server query params with single stream
      DebugServlet.clear();
      req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setQueryParams(Set.of("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request
      // so it passes the verification step.
      req.setQueryParams(Set.of("requestOnly", "both", "neither"));
      verifyServletState(client, req);
    }
  }

  @Test
  public void testGetDefaultSslContextFactory() {
    assertEquals(
        "HTTPS",
        Http2SolrClient.getDefaultSslContextFactory().getEndpointIdentificationAlgorithm());

    System.setProperty("javax.net.ssl.keyStoreType", "foo");
    System.setProperty("javax.net.ssl.trustStoreType", "bar");
    SslContextFactory.Client sslContextFactory = Http2SolrClient.getDefaultSslContextFactory();
    assertEquals("HTTPS", sslContextFactory.getEndpointIdentificationAlgorithm());
    assertEquals("foo", sslContextFactory.getKeyStoreType());
    assertEquals("bar", sslContextFactory.getTrustStoreType());
    System.clearProperty("javax.net.ssl.keyStoreType");
    System.clearProperty("javax.net.ssl.trustStoreType");

    System.setProperty("solr.ssl.checkPeerName", "true");
    System.setProperty("javax.net.ssl.keyStoreType", "foo");
    System.setProperty("javax.net.ssl.trustStoreType", "bar");
    SslContextFactory.Client sslContextFactory2 = Http2SolrClient.getDefaultSslContextFactory();
    assertEquals("HTTPS", sslContextFactory2.getEndpointIdentificationAlgorithm());
    assertEquals("foo", sslContextFactory2.getKeyStoreType());
    assertEquals("bar", sslContextFactory2.getTrustStoreType());
    System.clearProperty("solr.ssl.checkPeerName");
    System.clearProperty("javax.net.ssl.keyStoreType");
    System.clearProperty("javax.net.ssl.trustStoreType");

    System.setProperty("solr.ssl.checkPeerName", "false");
    System.setProperty("javax.net.ssl.keyStoreType", "foo");
    System.setProperty("javax.net.ssl.trustStoreType", "bar");
    SslContextFactory.Client sslContextFactory3 = Http2SolrClient.getDefaultSslContextFactory();
    assertNull(sslContextFactory3.getEndpointIdentificationAlgorithm());
    assertEquals("foo", sslContextFactory3.getKeyStoreType());
    assertEquals("bar", sslContextFactory3.getTrustStoreType());
    System.clearProperty("solr.ssl.checkPeerName");
    System.clearProperty("javax.net.ssl.keyStoreType");
    System.clearProperty("javax.net.ssl.trustStoreType");
  }

  protected void expectThrowsAndMessage(
      Class<? extends Exception> expectedType,
      ThrowingRunnable executable,
      String expectedMessage) {
    Exception e = expectThrows(expectedType, executable);
    assertTrue(
        "Expecting message to contain \"" + expectedMessage + "\" but was: " + e.getMessage(),
        e.getMessage().contains(expectedMessage));
  }

  @Test
  public void testBadExplicitCredentials() {
    expectThrowsAndMessage(
        IllegalStateException.class,
        () -> new Http2SolrClient.Builder().withBasicAuthCredentials("foo", null),
        "Invalid Authentication credentials");
    expectThrowsAndMessage(
        IllegalStateException.class,
        () -> new Http2SolrClient.Builder().withBasicAuthCredentials(null, "foo"),
        "Invalid Authentication credentials");
  }

  @Test
  public void testSetCredentialsExplicitly() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
            .withBasicAuthCredentials("foo", "explicit")
            .build(); ) {
      QueryRequest r = new QueryRequest(new SolrQuery("quick brown fox"));
      try {
        ignoreException("Error from server");
        client.request(r);
      } catch (Exception e) {
        // expected
      }
      unIgnoreException("Error from server");
      assertTrue(DebugServlet.headers.size() > 0);
      String authorizationHeader = DebugServlet.headers.get("authorization");
      assertNotNull(
          "No authorization information in headers found. Headers: " + DebugServlet.headers,
          authorizationHeader);
      assertEquals(
          "Basic "
              + Base64.getEncoder().encodeToString("foo:explicit".getBytes(StandardCharsets.UTF_8)),
          authorizationHeader);
    }
  }

  @Test
  public void testSetCredentialsWithSysProps() throws IOException, SolrServerException {
    System.setProperty(
        PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS, "foo:bar");
    System.setProperty(
        HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY,
        PreemptiveBasicAuthClientBuilderFactory.class.getName());
    // Hack to ensure we get a new set of parameters for this test
    PreemptiveBasicAuthClientBuilderFactory.setDefaultSolrParams(
        new PreemptiveBasicAuthClientBuilderFactory.CredentialsResolver().defaultParams);
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build(); ) {
      QueryRequest r = new QueryRequest(new SolrQuery("quick brown fox"));
      DebugServlet.addResponseHeader(
          WWWAuthenticationProtocolHandler.NAME, "Basic realm=\"Debug Servlet\"");
      DebugServlet.setErrorCode(HttpStatus.UNAUTHORIZED_401);
      try {
        client.request(r);
      } catch (Exception e) {
        // expected
      }
      assertTrue(DebugServlet.headers.size() > 0);
      String authorizationHeader = DebugServlet.headers.get("authorization");
      assertNotNull(
          "No authorization information in headers found. Headers: " + DebugServlet.headers,
          authorizationHeader);
      assertEquals(
          "Basic " + Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8)),
          authorizationHeader);
    } finally {
      System.clearProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS);
      System.clearProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY);
      PreemptiveBasicAuthClientBuilderFactory.setDefaultSolrParams(
          new MapSolrParams(new HashMap<>()));
    }
  }

  @Test
  public void testPerRequestCredentialsWin() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
            .withBasicAuthCredentials("foo2", "explicit")
            .build(); ) {
      QueryRequest r = new QueryRequest(new SolrQuery("quick brown fox"));
      r.setBasicAuthCredentials("foo3", "per-request");
      try {
        ignoreException("Error from server");
        client.request(r);
      } catch (Exception e) {
        // expected
      }
      unIgnoreException("Error from server");
      assertTrue(DebugServlet.headers.size() > 0);
      String authorizationHeader = DebugServlet.headers.get("authorization");
      assertNotNull(
          "No authorization information in headers found. Headers: " + DebugServlet.headers,
          authorizationHeader);
      assertEquals(
          "Basic "
              + Base64.getEncoder()
                  .encodeToString("foo3:per-request".getBytes(StandardCharsets.UTF_8)),
          authorizationHeader);
    } finally {
      System.clearProperty("basicauth");
    }
  }

  @Test
  public void testNoCredentials() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build(); ) {
      QueryRequest r = new QueryRequest(new SolrQuery("quick brown fox"));
      try {
        ignoreException("Error from server");
        client.request(r);
      } catch (Exception e) {
        // expected
      }
      unIgnoreException("Error from server");
      assertFalse(
          "Expecting no authorization header but got: " + DebugServlet.headers,
          DebugServlet.headers.containsKey("authorization"));
    }
  }

  @Test
  public void testBadHttpFactory() {
    System.setProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY, "FakeClassName");
    try {
      SolrClient client = new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build();
      fail("Expecting exception");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unable to instantiate"));
    }
  }

  @Test
  public void testGetRawStream() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client =
        getHttp2SolrClientBuilder(
                getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT)
            .build()) {
      GenericSolrRequest req =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/select", params("q", "*:*"));
      req.setResponseParser(new InputStreamResponseParser("xml"));
      SimpleSolrResponse rsp = req.process(client);
      Object stream = rsp.getResponse().get("stream");
      assertNotNull(stream);
      MatcherAssert.assertThat(stream, instanceOf(InputStream.class));
      org.apache.solr.common.util.IOUtils.closeQuietly((InputStream) stream);
    }
  }

  @Test
  public void testBuilder() {
    try (Http2SolrClient seed =
        new Http2SolrClient.Builder("baseSolrUrl")
            .withBasicAuthCredentials("testu", "testp")
            .build()) {

      try (Http2SolrClient clone1 =
          new Http2SolrClient.Builder("baseSolrUrl").withHttpClient(seed).build()) {
        String expected1 =
            Http2SolrClient.basicAuthCredentialsToAuthorizationString("testu", "testp");
        assertEquals(expected1, clone1.basicAuthAuthorizationStr);
      }

      // test overwrite seed value
      try (Http2SolrClient clone2 =
          new Http2SolrClient.Builder("baseSolrUrl")
              .withHttpClient(seed)
              .withBasicAuthCredentials("testu2", "testp2")
              .build()) {
        String expected2 =
            Http2SolrClient.basicAuthCredentialsToAuthorizationString("testu2", "testp2");
        assertEquals(expected2, clone2.basicAuthAuthorizationStr);
      }

      // test overwrite seed value, order of builder method calls reversed
      try (Http2SolrClient clone3 =
          new Http2SolrClient.Builder("baseSolrUrl")
              .withBasicAuthCredentials("testu3", "testp3")
              .withHttpClient(seed)
              .build()) {
        String expected3 =
            Http2SolrClient.basicAuthCredentialsToAuthorizationString("testu3", "testp3");
        assertEquals(expected3, clone3.basicAuthAuthorizationStr);
      }
    }
  }

  /* Missed tests : - set cookies via interceptor - invariant params - compression */
}
