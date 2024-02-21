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
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.SSLTestConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class HttpSolrJdkClientTest extends Http2SolrClientTestBase {

  private static SSLContext allTrustingSslContext;

  @BeforeClass
  public static void beforeClass() {
    try {
      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      SSLTestConfig stc = SolrTestCaseJ4.sslConfig;
      keyManagerFactory.init(stc.defaultKeyStore(), stc.defaultKeyStorePassword().toCharArray());

      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(
          keyManagerFactory.getKeyManagers(),
          new TrustManager[] {MOCK_TRUST_MANAGER},
          stc.notSecureSecureRandom());
      allTrustingSslContext = sslContext;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void workaroundToReleaseThreads_noClosableUntilJava21() {
    System.gc();
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
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (HttpSolrJdkClient client = builder(url).build()) {
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
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (HttpSolrJdkClient client =
        builder(url).withResponseParser(new XMLResponseParser()).build()) {
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

  @Override
  protected void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception {
    DebugServlet.clear();
    if (rp instanceof XMLResponseParser) {
      DebugServlet.addResponseHeader("Content-Type", "application/xml; charset=UTF-8");
      DebugServlet.responseBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<response />";
    } else {
      DebugServlet.addResponseHeader("Content-Type", "application/octet-stream");
      DebugServlet.responseBody = javabinResponse();
    }
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    HttpSolrJdkClient.Builder b = builder(url);
    if (rp != null) {
      b.withResponseParser(rp);
    }
    try (HttpSolrJdkClient client = b.build()) {
      client.query(q, method);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (HttpSolrJdkClient client = builder(getBaseUrl() + DEBUG_SERVLET_PATH).build()) {
      super.testGetById(client);
    }
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (HttpSolrJdkClient client =
        (HttpSolrJdkClient)
            builder(getBaseUrl() + SLOW_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 2000).build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
    }
  }

  @Test
  public void test0IdleTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (HttpSolrJdkClient client =
        (HttpSolrJdkClient)
            builder(getBaseUrl() + DEBUG_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 0).build()) {
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
  }

  @Test
  public void testRequestTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (HttpSolrJdkClient client =
        (HttpSolrJdkClient)
            builder(getBaseUrl() + SLOW_SERVLET_PATH, DEFAULT_CONNECTION_TIMEOUT, 0)
                .withRequestTimeout(500, TimeUnit.MILLISECONDS)
                .build()) {
      client.query(q, SolrRequest.METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
    }
  }

  @Test
  public void testFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + REDIRECT_SERVLET_PATH;
    try (HttpSolrJdkClient client = builder(clientUrl).withFollowRedirects(true).build()) {
      SolrQuery q = new SolrQuery("*:*");
      client.query(q);
    }
  }

  @Test
  public void testDoNotFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + REDIRECT_SERVLET_PATH;
    try (HttpSolrJdkClient client = builder(clientUrl).withFollowRedirects(false).build()) {
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
    try (HttpSolrJdkClient client = builder(clientUrl).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

    try (HttpSolrJdkClient client = builder(clientUrl).withFollowRedirects(true).build()) {
      // shouldn't throw an exception
      client.query(q);
    }

    // set explicit false for following redirects
    try (HttpSolrJdkClient client = builder(clientUrl).withFollowRedirects(false).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }
  }

  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    try (HttpSolrJdkClient client = builder(getBaseUrl() + DEBUG_SERVLET_PATH).build()) {
      super.testSolrExceptionCodeNotFromSolr(client);
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
    try (HttpSolrJdkClient client = builder(null).build()) {
      super.testSolrExceptionWithNullBaseurl(client);
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testUpdateDefault() throws Exception {
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (HttpSolrJdkClient client = builder(url).build()) {
      testUpdate(client, WT.JAVABIN, "application/javabin", "\u1234");
    }
  }

  @Test
  public void testUpdateXml() throws Exception {
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;

    // 64k+ post body, just to be sure we are using the [in|out]put streams correctly.
    StringBuilder sb = new StringBuilder();
    for(int i=0 ; i<65535 ; i++) {
      sb.append("A");
    }
    String value = sb.toString();

    try (HttpSolrJdkClient client =
        builder(url)
            .withRequestWriter(new RequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      testUpdate(client, WT.XML, "application/xml; charset=UTF-8", value);
    }
  }

  @Test
  public void testUpdateJavabin() throws Exception {
    String url = getBaseUrl() + DEBUG_SERVLET_PATH;
    try (HttpSolrJdkClient client =
        builder(url)
            .withRequestWriter(new BinaryRequestWriter())
            .withResponseParser(new BinaryResponseParser())
            .build()) {
      testUpdate(client, WT.JAVABIN, "application/javabin", "\u1234");
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {
    HttpSolrJdkClient baseUrlClient = builder(getBaseUrl()).withDefaultCollection(null).build();
    HttpSolrJdkClient collection1UrlClient =
        builder(getCoreUrl()).withDefaultCollection(null).build();
    testCollectionParameters(baseUrlClient, collection1UrlClient);
  }

  @Test
  public void testQueryString() throws Exception {
    super.testQueryString();
  }

  @Test
  public void testGetRawStream() throws Exception {
    try (HttpSolrJdkClient client =
        (HttpSolrJdkClient)
            builder(
                    getBaseUrl() + DEBUG_SERVLET_PATH,
                    DEFAULT_CONNECTION_TIMEOUT,
                    DEFAULT_CONNECTION_TIMEOUT)
                .build()) {
      super.testGetRawStream(client);
    }
  }

  @Test
  public void testSetCredentialsExplicitly() throws Exception {
    try (HttpSolrJdkClient client =
        builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withBasicAuthCredentials("foo", "explicit")
            .build(); ) {
      super.testSetCredentialsExplicitly(client);
    }
  }

  @Test
  public void testPerRequestCredentials() throws Exception {
    try (HttpSolrJdkClient client =
        builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withBasicAuthCredentials("foo2", "explicit")
            .build(); ) {
      super.testPerRequestCredentials(client);
    }
  }

  @Test
  public void testNoCredentials() throws Exception {
    try (HttpSolrJdkClient client = builder(getBaseUrl() + DEBUG_SERVLET_PATH).build(); ) {
      super.testNoCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentials() throws Exception {
    // username foo, password with embedded colon separator is "expli:cit".
    try (HttpSolrJdkClient client =
        builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withOptionalBasicAuthCredentials("foo:expli:cit")
            .build(); ) {
      super.testUseOptionalCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentialsWithNull() throws Exception {
    try (HttpSolrJdkClient client =
        builder(getBaseUrl() + DEBUG_SERVLET_PATH)
            .withOptionalBasicAuthCredentials(null)
            .build(); ) {
      super.testUseOptionalCredentialsWithNull(client);
    }
  }

  @Test
  public void testProcessorMimeTypes() throws Exception {
    ResponseParser rp = new XMLResponseParser();

    try (HttpSolrJdkClient client = builder(getBaseUrl()).withResponseParser(rp).build()) {
      assertTrue(client.processorAcceptsMimeType(rp.getContentTypes(), "application/xml"));
      assertFalse(client.processorAcceptsMimeType(rp.getContentTypes(), "application/json"));
    }

    rp = new BinaryResponseParser();
    try (HttpSolrJdkClient client = builder(getBaseUrl()).withResponseParser(rp).build()) {
      assertTrue(
          client.processorAcceptsMimeType(
              rp.getContentTypes(), "application/vnd.apache.solr.javabin"));
      assertTrue(client.processorAcceptsMimeType(rp.getContentTypes(), "application/octet-stream"));
      assertFalse(client.processorAcceptsMimeType(rp.getContentTypes(), "application/xml"));
    }
  }

  @Test
  public void testContentTypeToEncoding() throws Exception {
    try (HttpSolrJdkClient client = builder(getBaseUrl()).build()) {
      assertEquals("UTF-8", client.contentTypeToEncoding("application/xml; charset=UTF-8"));
      assertNull(client.contentTypeToEncoding("application/vnd.apache.solr.javabin"));
      assertNull(client.contentTypeToEncoding("application/octet-stream"));
      assertNull(client.contentTypeToEncoding("multipart/form-data; boundary=something"));
    }
  }

  @Override
  protected String expectedUserAgent() {
    return "Solr[" + HttpSolrJdkClient.class.getName() + "] 1.0";
  }

  @Override
  @SuppressWarnings(value = "unchecked")
  protected <B extends HttpSolrClientBuilderBase<?, ?>> B builder(
      String url, int connectionTimeout, int socketTimeout) {
    HttpSolrJdkClient.Builder b =
        new HttpSolrJdkClient.Builder(url)
            .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
            .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS)
            .withDefaultCollection(DEFAULT_CORE)
            .withSSLContext(allTrustingSslContext);
    return (B) b;
  }

  private HttpSolrJdkClient.Builder builder(String url) {
    return builder(url, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
  }

  private byte[] javabinResponse() {
    String[] str = JAVABIN_STR.split(" ");
    byte[] bytes = new byte[str.length];
    for (int i = 0; i < str.length; i++) {
      int asInt = 0;
      bytes[i] = (byte) Integer.decode("#" + str[i]).intValue();
    }
    return bytes;
  }

  private static final String JAVABIN_STR =
      "02 A2 e0 2e 72 65 73 70 6f "
          + "6e 73 65 48 65 61 64 65 72 "
          + "a4 e0 2b 7a 6b 43 6f 6e 6e "
          + "65 63 74 65 64 01 e0 26 73 "
          + "74 61 74 75 73 06 00 00 00 "
          + "00 e0 25 51 54 69 6d 65 06 "
          + "00 00 00 00 e0 26 70 61 72 "
          + "61 6d 73 a4 e0 21 71 21 7a "
          + "e0 24 72 6f 77 73 21 30 e0 "
          + "22 77 74 27 6a 61 76 61 62 "
          + "69 6e e0 27 76 65 72 73 69 "
          + "6f 6e 21 32 e0 28 72 65 73 "
          + "70 6f 6e 73 65 0c 84 60 60 "
          + "00 01 80";

  /**
   * Taken from: https://www.baeldung.com/java-httpclient-ssl sec 4.1, 2024/02/12. This is an
   * all-trusting Trust Manager. Works with self-signed certificates.
   */
  private static final TrustManager MOCK_TRUST_MANAGER =
      new X509ExtendedTrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
          // no-op
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return new java.security.cert.X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType)
            throws CertificateException {
          // no-op
        }
      };
}
