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

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Http2SolrClientTest extends Http2SolrClientTestBase<Http2SolrClient.Builder> {

  @Override
  protected String expectedUserAgent() {
    return "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  }

  @Override
  protected <B extends HttpSolrClientBuilderBase> B builder(
      String url, int connectionTimeout, int socketTimeout, Class<B> type) {
    Http2SolrClient.Builder b =  new Http2SolrClient.Builder(url)
        .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
        .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    return type.cast(b);
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client =
        builder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 2000, Http2SolrClient.Builder.class)
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
        builder(getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, 0, Http2SolrClient.Builder.class)
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
        builder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 0, Http2SolrClient.Builder.class)
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
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build()) {
      super.testSolrExceptionCodeNotFromSolr(client);
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
   try (Http2SolrClient client = new Http2SolrClient.Builder(null).build()) {
      super.testSolrExceptionWithNullBaseurl(client);
    } finally {
      DebugServlet.clear();
    }
  }

  protected void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    Http2SolrClient.Builder b = new Http2SolrClient.Builder(url);
    if(rp != null) {
      b.withResponseParser(rp);
    }
    try (Http2SolrClient client = b.build()) {
        client.query(q, method);
      assertEquals(
              client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
  }


  @Test
  public void testQueryGet() throws Exception {
    super.testQueryGet();
  }
  @Test
  public void testQueryPost() throws Exception {
    super.testQueryPost();
  }
  @Test
  public void testQueryPut() throws Exception {
    super.testQueryPut();
  }
  @Test
  public void testQueryXmlGet() throws Exception {
    super.testQueryXmlGet();
  }

  @Test
  public void testQueryXmlPost() throws Exception {
    super.testQueryXmlPost();
  }

  @Test
  public void testQueryXmlPut() throws Exception {
    super.testQueryXmlPut();
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
      assertEquals(
              client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      validateDelete();
    }
  }
  @Test
  public void testDeleteXml() throws Exception {
    DebugServlet.clear();
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client =
                 new Http2SolrClient.Builder(url).withResponseParser(new XMLResponseParser()).build()) {
      try {
        client.deleteByQuery("*:*");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      assertEquals(
              client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      validateDelete();
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build()) {
      super.testGetById(client);
    }
  }

  @Test
  public void testUpdateDefault() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client = new Http2SolrClient.Builder(url).build()) {
      testUpdate(client, "javabin", "application/javabin");
    }
  }

  @Test
  public void testUpdateXml() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (Http2SolrClient client =
                 new Http2SolrClient.Builder(url)
                         .withRequestWriter(new RequestWriter())
                         .withResponseParser(new XMLResponseParser())
                         .build()) {
      testUpdate(client, "xml", "application/xml; charset=UTF-8");
    }
  }
    @Test
    public void testUpdateJavabin() throws Exception {
      String url = getBaseUrl() + "/debug/foo";
       try (Http2SolrClient client =
        new Http2SolrClient.Builder(url)
            .withRequestWriter(new BinaryRequestWriter())
            .withResponseParser(new BinaryResponseParser())
            .build()) {
      testUpdate(client, "javabin", "application/javabin");
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
    Http2SolrClient baseUrlClient = new Http2SolrClient.Builder(getBaseUrl()).build();
    Http2SolrClient collection1UrlClient = new Http2SolrClient.Builder(getCoreUrl()).build();
    testCollectionParameters(baseUrlClient, collection1UrlClient);
  }

  @Test
  public void testQueryString() throws Exception {
    testQueryString(Http2SolrClient.class, Http2SolrClient.Builder.class);
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
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
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
  public void testPerRequestCredentials() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
            .withBasicAuthCredentials("foo2", "explicit")
            .build(); ) {
      super.testPerRequestCredentials(client);
    }
  }

  @Test
  public void testNoCredentials() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build(); ) {
      super.testNoCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentials() {
    // username foo, password with embedded colon separator is "expli:cit".
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
            .withOptionalBasicAuthCredentials("foo:expli:cit")
            .build(); ) {
      super.testUseOptionalCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentialsWithNull() {
    try (Http2SolrClient client =
        new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo")
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
      SolrClient client = new Http2SolrClient.Builder(getBaseUrl() + "/debug/foo").build();
      fail("Expecting exception");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unable to instantiate"));
    }
  }



  @Test
  public void testGetRawStream() throws Exception {
    try(Http2SolrClient client = builder(
            getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, Http2SolrClient.Builder.class)
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

  /* Missed tests : - set cookies via interceptor - invariant params - compression */
}
