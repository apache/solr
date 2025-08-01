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

import static org.apache.solr.handler.admin.api.ReplicationAPIBase.FILE_STREAM;
import static org.hamcrest.core.StringContains.containsStringIgnoringCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

public class Http2SolrClientTest extends HttpSolrClientTestBase {

  @Override
  protected String expectedUserAgent() {
    return "Solr[" + Http2SolrClient.class.getName() + "] " + SolrVersion.LATEST_STRING;
  }

  @Override
  @SuppressWarnings(value = "unchecked")
  protected <B extends HttpSolrClientBuilderBase<?, ?>> B builder(
      String url, int connectionTimeout, int socketTimeout) {
    Http2SolrClient.Builder b =
        new Http2SolrClient.Builder(url)
            .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
            .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    return (B) b;
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        (Http2SolrClient)
            builder(getBaseUrl() + SLOW_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 2000)
                .withDefaultCollection(DEFAULT_CORE)
                .build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertIsTimeout(e);
    }
  }

  @Test
  public void test0IdleTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        (Http2SolrClient)
            builder(getBaseUrl() + DEBUG_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 0)
                .withDefaultCollection(DEFAULT_CORE)
                .build()) {
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (SolrClient.RemoteSolrException ignored) {
      }
    }
  }

  @Test
  public void testRequestTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        (Http2SolrClient)
            builder(getBaseUrl() + SLOW_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 0)
                .withDefaultCollection(DEFAULT_CORE)
                .withRequestTimeout(500, TimeUnit.MILLISECONDS)
                .build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertIsTimeout(e);
    }
  }

  /**
   * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
   * even when not on the list of ErrorCodes solr may return.
   */
  @Test
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .build()) {
      super.testSolrExceptionCodeNotFromSolr(client);
    } finally {
      DebugServlet.clear();
    }
  }

  /** test that temporary baseURL overrides are reflected in error messages */
  @Test
  public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
    final int status = 527;
    DebugServlet.setErrorCode(status);

    try (Http2SolrClient client = new Http2SolrClient.Builder(null).build()) {
      try {
        // if client base url is null, request url will be used in exception message
        client.requestWithBaseUrl(getBaseUrl() + DEBUG_SERVLET_PATH, DEFAULT_CORE, new SolrPing());

        fail("Didn't get excepted exception from oversided request");
      } catch (SolrException e) {
        assertEquals("Unexpected exception status code", status, e.code());
        assertTrue(e.getMessage().contains(getBaseUrl()));
      }
    } finally {
      DebugServlet.clear();
    }
  }

  @Override
  protected void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", MUST_ENCODE);
    q.setParam("case_sensitive_param", "lowercase");
    q.setParam("CASE_SENSITIVE_PARAM", "uppercase");
    Http2SolrClient.Builder b =
        new Http2SolrClient.Builder(url).withDefaultCollection(DEFAULT_CORE);
    if (rp != null) {
      b.withResponseParser(rp);
    }
    try (Http2SolrClient client = b.build()) {
      client.query(q, method);
      assertEquals(
          client.getParser().getWriterType(), DebugServlet.parameters.get(CommonParams.WT)[0]);
    } catch (SolrClient.RemoteSolrException ignored) {
    }
  }

  @Test
  @Override
  public void testQueryGet() throws Exception {
    super.testQueryGet();
  }

  @Test
  @Override
  public void testQueryPost() throws Exception {
    super.testQueryPost();
  }

  @Test
  @Override
  public void testQueryPut() throws Exception {
    super.testQueryPut();
  }

  @Test
  @Override
  public void testQueryXmlGet() throws Exception {
    super.testQueryXmlGet();
  }

  @Test
  @Override
  public void testQueryXmlPost() throws Exception {
    super.testQueryXmlPost();
  }

  @Test
  @Override
  public void testQueryXmlPut() throws Exception {
    super.testQueryXmlPut();
  }

  @Test
  public void testOverrideBaseUrl() throws Exception {
    DebugServlet.clear();
    final var defaultUrl =
        "http://not-a-real-url:8983/solr"; // Would result in an exception if used
    final var urlToUse = getBaseUrl() + DEBUG_SERVLET_PATH;
    final var queryParams = new ModifiableSolrParams();
    queryParams.add("q", "*:*");

    // Ensure the correct URL is used by the lambda-based requestWithBaseUrl method
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(defaultUrl).withDefaultCollection(DEFAULT_CORE).build()) {
      try {
        client.requestWithBaseUrl(urlToUse, (c) -> c.query(queryParams));
      } catch (SolrClient.RemoteSolrException rse) {
      }

      assertEquals(urlToUse + "/select", DebugServlet.url);
    }

    // Ensure the correct URL is used by the SolrRequest-based requestWithBaseUrl method
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(defaultUrl).withDefaultCollection(DEFAULT_CORE).build()) {
      try {
        client.requestWithBaseUrl(urlToUse, null, new QueryRequest(queryParams));
      } catch (SolrClient.RemoteSolrException rse) {
      }

      assertEquals(urlToUse + "/select", DebugServlet.url);
    }
  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url).withDefaultCollection(DEFAULT_CORE).build()) {
      try {
        client.deleteById("id");
      } catch (SolrClient.RemoteSolrException ignored) {
      }
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      validateDelete();
    }
  }

  @Test
  public void testDeleteXml() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withDefaultCollection(DEFAULT_CORE)
            .withResponseParser(new XMLResponseParser())
            .build()) {
      try {
        client.deleteByQuery("*:*");
      } catch (SolrClient.RemoteSolrException ignored) {
      }
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      validateDelete();
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .build()) {
      super.testGetById(client);
    }
  }

  @Test
  public void testUpdateDefault() throws Exception {
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url).withDefaultCollection(DEFAULT_CORE).build()) {
      testUpdate(client, WT.JAVABIN, "application/javabin", MUST_ENCODE);
    }
  }

  @Test
  public void testUpdateXml() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withDefaultCollection(DEFAULT_CORE)
            .withRequestWriter(new XMLRequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      testUpdate(client, WT.XML, "application/xml; charset=UTF-8", MUST_ENCODE);
    }
  }

  @Test
  public void testUpdateJavabin() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withDefaultCollection(DEFAULT_CORE)
            .withRequestWriter(new JavaBinRequestWriter())
            .withResponseParser(new JavaBinResponseParser())
            .build()) {
      testUpdate(client, WT.JAVABIN, "application/javabin", MUST_ENCODE);
    }
  }

  @Test
  public void testAsyncGet() throws Exception {
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    ResponseParser rp = new XMLResponseParser();
    HttpSolrClientBuilderBase<?, ?> b =
        builder(url, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT).withResponseParser(rp);
    super.testQueryAsync(b);
  }

  @Test
  public void testAsyncPost() throws Exception {
    super.testUpdateAsync();
  }

  @Test
  public void testAsyncException() throws Exception {
    super.testAsyncExceptionBase();
  }

  @Test
  public void testAsyncQueryWithSharedClient() throws Exception {
    DebugServlet.clear();
    final var url = getBaseUrl() + DEBUG_SERVLET_PATH;
    ResponseParser rp = new XMLResponseParser();
    final var queryParams = new MapSolrParams(Collections.singletonMap("q", "*:*"));
    final var builder =
        new Http2SolrClient.Builder(url).withDefaultCollection(DEFAULT_CORE).withResponseParser(rp);
    try (Http2SolrClient originalClient = builder.build()) {
      final var derivedBuilder = builder.withHttpClient(originalClient);
      super.testQueryAsync(derivedBuilder);
    }
  }

  @Test
  public void testFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + REDIRECT_SERVLET_PATH;
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withDefaultCollection(DEFAULT_CORE)
            .withFollowRedirects(true)
            .build()) {
      SolrQuery q = new SolrQuery("*:*");
      client.query(q);
    }
  }

  @Test
  public void testDoNotFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + REDIRECT_SERVLET_PATH;
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withDefaultCollection(DEFAULT_CORE)
            .withFollowRedirects(false)
            .build()) {
      SolrQuery q = new SolrQuery("*:*");

      SolrServerException thrown = assertThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(thrown.getMessage().contains("redirect"));
    }
  }

  @Test
  public void testRedirectSwapping() throws Exception {
    final String clientUrl = getBaseUrl() + REDIRECT_SERVLET_PATH;
    SolrQuery q = new SolrQuery("*:*");

    // default for follow redirects is false
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl).withDefaultCollection(DEFAULT_CORE).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withDefaultCollection(DEFAULT_CORE)
            .withFollowRedirects(true)
            .build()) {
      // shouldn't throw an exception
      client.query(q);
    }

    // set explicit false for following redirects
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(clientUrl)
            .withDefaultCollection(DEFAULT_CORE)
            .withFollowRedirects(false)
            .build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {
    Http2SolrClient baseUrlClient = new Http2SolrClient.Builder(getBaseUrl()).build();
    Http2SolrClient collection1UrlClient = new Http2SolrClient.Builder(getCoreUrl()).build();
    testCollectionParameters(baseUrlClient, collection1UrlClient);
  }

  @Test
  @Override
  public void testQueryString() throws Exception {
    super.testQueryString();
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
  public void testSetCredentialsExplicitly() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .withBasicAuthCredentials("foo", "explicit")
            .build(); ) {
      super.testSetCredentialsExplicitly(client);
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
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .build()) {
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
      PreemptiveBasicAuthClientBuilderFactory.setDefaultSolrParams(SolrParams.of());
    }
  }

  @Test
  public void testPerRequestCredentials() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .withBasicAuthCredentials("foo2", "explicit")
            .build(); ) {
      super.testPerRequestCredentials(client);
    }
  }

  @Test
  public void testNoCredentials() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .build(); ) {
      super.testNoCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentials() {
    // username foo, password with embedded colon separator is "expli:cit".
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .withOptionalBasicAuthCredentials("foo:expli:cit")
            .build(); ) {
      super.testUseOptionalCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentialsWithNull() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withDefaultCollection(DEFAULT_CORE)
            .withOptionalBasicAuthCredentials(null)
            .build(); ) {
      super.testUseOptionalCredentialsWithNull(client);
    }
  }

  @Test
  public void testMalformedOptionalCredentials() {

    expectThrowsAndMessage(
        IllegalStateException.class,
        () -> new Http2SolrClient.Builder().withOptionalBasicAuthCredentials("usernamepassword"),
        "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");

    expectThrowsAndMessage(
        IllegalStateException.class,
        () -> new Http2SolrClient.Builder().withOptionalBasicAuthCredentials("username password"),
        "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");
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
  public void testBadHttpFactory() {
    System.setProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY, "FakeClassName");
    try {
      SolrClient client =
          new Http2SolrClient.Builder(getBaseUrl() + DEBUG_SERVLET_PATH)
              .withDefaultCollection(DEFAULT_CORE)
              .build();
      fail("Expecting exception");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unable to instantiate"));
    }
  }

  @Test
  public void testGetRawStream() throws Exception {
    try (Http2SolrClient client =
        (Http2SolrClient)
            builder(
                    getBaseUrl() + DEBUG_SERVLET_PATH,
                    DEFAULT_CONNECTION_TIMEOUT,
                    DEFAULT_CONNECTION_TIMEOUT)
                .withDefaultCollection(DEFAULT_CORE)
                .build()) {
      super.testGetRawStream(client);
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

  @Test
  public void testIdleTimeoutWithHttpClient() throws Exception {
    String url = getBaseUrl() + SLOW_STREAM_SERVLET_PATH;
    try (Http2SolrClient oldClient =
        new Http2SolrClient.Builder(url)
            .withRequestTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .withIdleTimeout(100, TimeUnit.MILLISECONDS)
            .build()) {

      try (Http2SolrClient onlyBaseUrlChangedClient =
          new Http2SolrClient.Builder(url).withHttpClient(oldClient).build()) {
        assertEquals(oldClient.getIdleTimeout(), onlyBaseUrlChangedClient.getIdleTimeout());
        assertEquals(oldClient.getHttpClient(), onlyBaseUrlChangedClient.getHttpClient());
      }

      // too little time to succeed
      QueryRequest req = new QueryRequest();
      req.setResponseParser(new InputStreamResponseParser(FILE_STREAM));
      assertIsTimeout(expectThrows(SolrServerException.class, () -> oldClient.request(req)));

      int newIdleTimeoutMs = 10 * 1000; // enough time to succeed
      try (Http2SolrClient idleTimeoutChangedClient =
          new Http2SolrClient.Builder(url)
              .withHttpClient(oldClient)
              .withIdleTimeout(newIdleTimeoutMs, TimeUnit.MILLISECONDS)
              .build()) {
        assertNotEquals(oldClient.getIdleTimeout(), idleTimeoutChangedClient.getIdleTimeout());
        assertEquals(newIdleTimeoutMs, idleTimeoutChangedClient.getIdleTimeout());
        NamedList<Object> response = idleTimeoutChangedClient.request(req);
        try (InputStream is = (InputStream) response.get("stream")) {
          assertEquals("0123456789", new String(is.readAllBytes(), StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  public void testRequestTimeoutWithHttpClient() throws Exception {
    String url = getBaseUrl() + SLOW_STREAM_SERVLET_PATH;
    try (Http2SolrClient oldClient =
        new Http2SolrClient.Builder(url)
            .withIdleTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .withRequestTimeout(100, TimeUnit.MILLISECONDS)
            .build()) {

      try (Http2SolrClient onlyBaseUrlChangedClient =
          new Http2SolrClient.Builder(url).withHttpClient(oldClient).build()) {
        // Client created with the same HTTP client should have the same behavior
        assertEquals(oldClient.getHttpClient(), onlyBaseUrlChangedClient.getHttpClient());
      }

      // too little time to succeed
      QueryRequest req = new QueryRequest();
      req.setResponseParser(new InputStreamResponseParser(FILE_STREAM));
      assertIsTimeout(expectThrows(SolrServerException.class, () -> oldClient.request(req)));

      int newRequestTimeoutMs = 10 * 1000; // enough time to succeed
      try (Http2SolrClient requestTimeoutChangedClient =
          new Http2SolrClient.Builder(url)
              .withHttpClient(oldClient)
              .withRequestTimeout(newRequestTimeoutMs, TimeUnit.MILLISECONDS)
              .build()) {
        NamedList<Object> response = requestTimeoutChangedClient.request(req);
        try (InputStream is = (InputStream) response.get("stream")) {
          assertEquals("0123456789", new String(is.readAllBytes(), StandardCharsets.UTF_8));
        }
      }
    }
  }

  private static void assertIsTimeout(Throwable t) {
    assertThat(t.getMessage(), containsStringIgnoringCase("Timeout"));
  }

  /* Missed tests : - set cookies via interceptor - invariant params - compression */
}
