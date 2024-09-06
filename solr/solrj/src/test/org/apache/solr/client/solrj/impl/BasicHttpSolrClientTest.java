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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.embedded.JettyConfig;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicHttpSolrClientTest extends SolrJettyTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class RedirectServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.sendRedirect("/solr/collection1/select?" + req.getQueryString());
    }
  }

  public static class SlowServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignored) {
      }
    }
  }

  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      queryString = null;
      cookies = null;
    }

    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String, String> headers = null;
    public static Map<String, String[]> parameters = null;
    public static String queryString = null;
    public static javax.servlet.http.Cookie[] cookies = null;

    public static void setErrorCode(Integer code) {
      errorCode = code;
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
        headers.put(name, req.getHeader(name));
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
            .withServlet(new ServletHolder(RedirectServlet.class), "/redirect/*")
            .withServlet(new ServletHolder(SlowServlet.class), "/slow/*")
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    final var queryRequest = new QueryRequest(q);
    queryRequest.setPath("/slow/foo" + queryRequest.getPath());
    try (SolrClient client =
        new HttpSolrClient.Builder(getBaseUrl())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(2000, TimeUnit.MILLISECONDS)
            .build()) {
      SolrServerException e =
          expectThrows(SolrServerException.class, () -> queryRequest.process(client));
      assertTrue(e.getMessage().contains("Timeout"));
    }
  }

  /**
   * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
   * even when not on the list of ErrorCodes solr may return.
   */
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(
        status
            + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
        ErrorCode.UNKNOWN,
        ErrorCode.getErrorCode(status));

    try (SolrClient client = getHttpSolrClient(getBaseUrl())) {
      DebugServlet.setErrorCode(status);
      SolrQuery q = new SolrQuery("foo");
      final var queryRequest = new QueryRequest(q);
      queryRequest.setPath("/debug/foo" + queryRequest.getPath());
      SolrException e = expectThrows(SolrException.class, () -> queryRequest.process(client));
      assertEquals("Unexpected exception status code", status, e.code());
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery() throws Exception {
    DebugServlet.clear();
    final String debugPath = "/debug/foo";
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    final var queryRequest = new QueryRequest(q);
    queryRequest.setPath(debugPath);
    try (HttpSolrClient client = getHttpSolrClient(getBaseUrl())) {

      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      // default method
      assertEquals("get", DebugServlet.lastMethod);
      // agent
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // agent
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      // keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      // content-type
      assertNull(DebugServlet.headers.get("Content-Type"));
      // param encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      // POST
      DebugServlet.clear();
      queryRequest.setMethod(METHOD.POST);
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals(
          "application/x-www-form-urlencoded; charset=UTF-8",
          DebugServlet.headers.get("Content-Type"));

      // PUT
      DebugServlet.clear();
      queryRequest.setMethod(METHOD.PUT);
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals(
          "application/x-www-form-urlencoded; charset=UTF-8",
          DebugServlet.headers.get("Content-Type"));
    }

    // XML
    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      // XML/GET
      DebugServlet.clear();
      queryRequest.setMethod(METHOD.GET); // Reset to the default here after using 'PUT' above
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      assertEquals("get", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      // XML/POST
      DebugServlet.clear();
      queryRequest.setMethod(METHOD.POST);
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals(
          "application/x-www-form-urlencoded; charset=UTF-8",
          DebugServlet.headers.get("Content-Type"));

      DebugServlet.clear();
      queryRequest.setMethod(METHOD.PUT);
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals(
          "application/x-www-form-urlencoded; charset=UTF-8",
          DebugServlet.headers.get("Content-Type"));
    }
  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    final String debugPath = "/debug/foo";

    try (HttpSolrClient client = getHttpSolrClient(getBaseUrl())) {
      final UpdateRequest deleteById = new UpdateRequest();
      deleteById.deleteById("id");
      deleteById.setPath(debugPath + deleteById.getPath());
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> deleteById.process(client));

      // default method
      assertEquals("post", DebugServlet.lastMethod);
      // agent
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // agent
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      // keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    }

    // XML
    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      final var deleteByQueryRequest = new UpdateRequest();
      deleteByQueryRequest.setPath(debugPath + deleteByQueryRequest.getPath());
      deleteByQueryRequest.deleteByQuery("*:*");
      deleteByQueryRequest.setCommitWithin(-1);
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> deleteByQueryRequest.process(client));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (SolrClient client = getHttpSolrClient(getBaseUrl() + "/debug/foo")) {
      Collection<String> ids = Collections.singletonList("a");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("a"));
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById(ids, null));
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", "a"));
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", ids, null));
    }
  }

  @Test
  public void testUpdate() throws Exception {
    DebugServlet.clear();
    final String debugPath = "/debug/foo";

    try (HttpSolrClient client = getHttpSolrClient(getBaseUrl())) {
      UpdateRequest req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setPath(debugPath + req.getPath());
      req.setParam("a", "\u1234");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> req.process(client));

      // default method
      assertEquals("post", DebugServlet.lastMethod);
      // agent
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      // default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      // default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      // content type
      assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
      // parameter encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }
    DebugServlet.clear();
    // XML response and writer
    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl())
            .withRequestWriter(new RequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      UpdateRequest req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setPath(debugPath + req.getPath());
      req.setParam("a", "\u1234");

      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }
    DebugServlet.clear();
    // javabin request
    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl())
            .withRequestWriter(new BinaryRequestWriter())
            .withResponseParser(new BinaryResponseParser())
            .build()) {
      UpdateRequest req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setPath(debugPath + req.getPath());
      req.setParam("a", "\u1234");

      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals(
          "Solr[" + HttpSolrClient.class.getName() + "] 1.0",
          DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }
  }

  @Test
  public void testRedirect() throws Exception {
    final String redirectPath = "/redirect/foo";
    SolrQuery q = new SolrQuery("*:*");
    final var queryRequest = new QueryRequest(q);
    queryRequest.setPath(redirectPath + queryRequest.getPath());

    // default for redirect is false.
    try (HttpSolrClient client = new HttpSolrClient.Builder(getBaseUrl()).build()) {
      SolrServerException e =
          expectThrows(SolrServerException.class, () -> queryRequest.process(client));
      assertTrue(e.getMessage().contains("redirect"));
    }

    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl()).withFollowRedirects(true).build()) {
      // No exception expected
      queryRequest.process(client);
    }

    // And with explicit false:
    try (HttpSolrClient client =
        new HttpSolrClient.Builder(getBaseUrl()).withFollowRedirects(false).build()) {
      SolrServerException e =
          expectThrows(SolrServerException.class, () -> queryRequest.process(client));
      assertTrue(e.getMessage().contains("redirect"));
    }
  }

  @Test
  public void testCompression() throws Exception {
    final String debugPath = "/debug/foo";
    final SolrQuery q = new SolrQuery("*:*");
    final var queryRequest = new QueryRequest(q);
    queryRequest.setPath(debugPath + queryRequest.getPath());

    try (SolrClient client = getHttpSolrClient(getBaseUrl())) {
      // verify request header gets set
      DebugServlet.clear();
      expectThrows(
          BaseHttpSolrClient.RemoteSolrException.class, () -> queryRequest.process(client));
      assertNull(DebugServlet.headers.toString(), DebugServlet.headers.get("Accept-Encoding"));
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(getBaseUrl()).allowCompression(true).build()) {
      try {
        queryRequest.process(client);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      assertNotNull(DebugServlet.headers.get("Accept-Encoding"));
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(getBaseUrl()).allowCompression(false).build()) {
      try {
        queryRequest.process(client);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
    assertNull(DebugServlet.headers.get("Accept-Encoding"));

    // verify server compresses output
    HttpGet get = new HttpGet(getCoreUrl() + "/select?q=foo&wt=xml");
    get.setHeader("Accept-Encoding", "gzip");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, true);

    RequestConfig config = RequestConfig.custom().setDecompressionEnabled(false).build();
    get.setConfig(config);

    CloseableHttpClient httpclient = HttpClientUtil.createClient(params);
    HttpEntity entity = null;
    try {
      HttpResponse response =
          httpclient.execute(get, HttpClientUtil.createNewHttpClientRequestContext());
      entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      assertNotNull(Arrays.asList(response.getAllHeaders()).toString(), ceheader);
      assertEquals("gzip", ceheader.getValue());
    } finally {
      if (entity != null) {
        entity.getContent().close();
      }
      HttpClientUtil.close(httpclient);
    }

    // verify compressed response can be handled
    try (SolrClient client = getHttpSolrClient(getBaseUrl(), DEFAULT_TEST_COLLECTION_NAME)) {
      QueryResponse response = client.query(new SolrQuery("foo"));
      assertEquals(0, response.getStatus());
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    try (SolrClient client = getHttpSolrClient(getBaseUrl())) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      client.add("collection1", doc);
      client.commit("collection1");

      assertEquals(
          1,
          client.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());
    }

    try (SolrClient client = getHttpSolrClient(getBaseUrl(), DEFAULT_TEST_CORENAME)) {
      assertEquals(1, client.query(new SolrQuery("id:collection")).getResults().getNumFound());
    }
  }

  @Test
  public void testGetRawStream() throws SolrServerException, IOException {
    CloseableHttpClient httpClient = HttpClientUtil.createClient(null);
    try (SolrClient solrClient =
        new HttpSolrClient.Builder(getBaseUrl())
            .withDefaultCollection(DEFAULT_TEST_CORENAME)
            .withHttpClient(httpClient)
            .withResponseParser(null)
            .build(); ) {

      QueryRequest req = new QueryRequest();
      NamedList<?> response = solrClient.request(req);
      InputStream stream = (InputStream) response.get("stream");
      assertNotNull(stream);
      stream.close();
    } finally {
      HttpClientUtil.close(httpClient);
    }
  }

  /** An interceptor changing the request */
  HttpRequestInterceptor changeRequestInterceptor =
      new HttpRequestInterceptor() {

        @Override
        public void process(HttpRequest request, HttpContext context)
            throws HttpException, IOException {
          log.info("Intercepted params: {}", context);

          HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
          URIBuilder uribuilder = new URIBuilder(wrapper.getURI());
          uribuilder.addParameter("b", "\u4321");
          try {
            wrapper.setURI(uribuilder.build());
          } catch (URISyntaxException ex) {
            throw new HttpException("Invalid request URI", ex);
          }
        }
      };

  public static final String cookieName = "cookieName";
  public static final String cookieValue = "cookieValue";

  /** An interceptor setting a cookie */
  HttpRequestInterceptor cookieSettingRequestInterceptor =
      new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest request, HttpContext context)
            throws HttpException, IOException {
          BasicClientCookie cookie = new BasicClientCookie(cookieName, cookieValue);
          cookie.setVersion(0);
          cookie.setPath("/");
          cookie.setDomain(getJetty().getBaseUrl().getHost());

          CookieStore cookieStore = new BasicCookieStore();
          CookieSpec cookieSpec = new SolrPortAwareCookieSpecFactory().create(context);
          // CookieSpec cookieSpec = registry.lookup(policy).create(context);
          // Add the cookies to the request
          List<Header> headers = cookieSpec.formatCookies(Collections.singletonList(cookie));
          for (Header header : headers) {
            request.addHeader(header);
          }
          context.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
        }
      };

  /**
   * Set cookies via interceptor Change the request via an interceptor Ensure cookies are actually
   * set and that request is actually changed
   */
  @Test
  public void testInterceptors() {
    DebugServlet.clear();
    HttpClientUtil.addRequestInterceptor(changeRequestInterceptor);
    HttpClientUtil.addRequestInterceptor(cookieSettingRequestInterceptor);

    final String debugPath = "/debug/foo";
    try (SolrClient server = getHttpSolrClient(getBaseUrl())) {

      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");
      final var queryRequest = new QueryRequest(q);
      queryRequest.setPath(debugPath + queryRequest.getPath());
      expectThrows(
          Exception.class,
          () -> {
            queryRequest.setMethod(random().nextBoolean() ? METHOD.POST : METHOD.GET);
            queryRequest.process(server);
          });

      // Assert cookies from UseContextCallback
      assertNotNull(DebugServlet.cookies);
      boolean foundCookie = false;
      for (javax.servlet.http.Cookie cookie : DebugServlet.cookies) {
        if (cookieName.equals(cookie.getName()) && cookieValue.equals(cookie.getValue())) {
          foundCookie = true;
          break;
        }
      }
      assertTrue(foundCookie);

      // Assert request changes by ChangeRequestCallback
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("\u4321", DebugServlet.parameters.get("b")[0]);

    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      HttpClientUtil.removeRequestInterceptor(changeRequestInterceptor);
      HttpClientUtil.removeRequestInterceptor(cookieSettingRequestInterceptor);
    }
  }

  private void setReqParamsOf(UpdateRequest req, String... keys) {
    if (keys != null) {
      for (String k : keys) {
        req.setParam(k, k + "Value");
      }
    }
  }

  private void verifyServletState(HttpSolrClient client, SolrRequest<?> request) {
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
    final String debugPath = "/debug/foo";
    HttpSolrClient.Builder builder = new HttpSolrClient.Builder(getBaseUrl());
    try (HttpSolrClient client =
        builder.withTheseParamNamesInTheUrl(Set.of("serverOnly")).build()) {
      // test without request query params
      DebugServlet.clear();
      UpdateRequest req = new UpdateRequest();
      req.setPath(debugPath + req.getPath());
      setReqParamsOf(req, "serverOnly", "notServer");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req));
      verifyServletState(client, req);
    }
    try (HttpSolrClient client = builder.withTheseParamNamesInTheUrl(Set.of()).build()) {
      // test without server query params
      DebugServlet.clear();
      UpdateRequest req2 = new UpdateRequest();
      req2.setPath(debugPath + req2.getPath());
      req2.setQueryParams(Set.of("requestOnly"));
      setReqParamsOf(req2, "requestOnly", "notRequest");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req2));
      verifyServletState(client, req2);
    }
    try (HttpSolrClient client =
        builder.withTheseParamNamesInTheUrl(Set.of("serverOnly", "both")).build()) {
      // test with both request and server query params
      DebugServlet.clear();
      UpdateRequest req3 = new UpdateRequest();
      req3.setPath(debugPath + req3.getPath());
      req3.setQueryParams(Set.of("requestOnly", "both"));
      setReqParamsOf(req3, "serverOnly", "requestOnly", "both", "neither");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req3));
      verifyServletState(client, req3);
    }
    try (HttpSolrClient client =
        builder.withTheseParamNamesInTheUrl(Set.of("serverOnly", "both")).build()) {
      // test with both request and server query params with single stream
      DebugServlet.clear();
      UpdateRequest req4 = new UpdateRequest();
      req4.setPath(debugPath + req4.getPath());
      req4.add(new SolrInputDocument());
      req4.setQueryParams(Set.of("requestOnly", "both"));
      setReqParamsOf(req4, "serverOnly", "requestOnly", "both", "neither");
      expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(req4));
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request,
      // so it passes the verification step.
      req4.setQueryParams(Set.of("requestOnly", "both", "neither"));
      verifyServletState(client, req4);
    }
  }

  @Test
  @SuppressWarnings({"try"})
  public void testInvariantParams() throws IOException {
    try (HttpSolrClient createdClient =
        new HttpSolrClient.Builder()
            .withBaseSolrUrl(getBaseUrl())
            .withInvariantParams(SolrTestCaseJ4.params("param", "value"))
            .build()) {
      assertEquals("value", createdClient.getInvariantParams().get("param"));
    }

    try (HttpSolrClient createdClient =
        new HttpSolrClient.Builder()
            .withBaseSolrUrl(getBaseUrl())
            .withInvariantParams(SolrTestCaseJ4.params("fq", "fq1", "fq", "fq2"))
            .build()) {
      assertEquals(2, createdClient.getInvariantParams().getParams("fq").length);
    }

    try (SolrClient createdClient =
        new HttpSolrClient.Builder()
            .withBaseSolrUrl(getBaseUrl())
            .withKerberosDelegationToken("mydt")
            .withInvariantParams(
                SolrTestCaseJ4.params(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM, "mydt"))
            .build()) {
      fail();
    } catch (Exception ex) {
      if (!ex.getMessage()
          .equals(
              "parameter "
                  + DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM
                  + " is redefined.")) {
        throw ex;
      }
    }
  }
}
