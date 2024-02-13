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

public class HttpSolrClientJdkImplTest
    extends Http2SolrClientTestBase<HttpSolrClientJdkImpl.Builder> {

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
    String url = getBaseUrl() + "/debug/foo";
    try (HttpSolrClientJdkImpl client = builder(url).build()) {
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
    try (HttpSolrClientJdkImpl client =
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
      byte[] javabin = {
        (byte) 0x02,
        (byte) 0xa2,
        (byte) 0xe0,
        (byte) 0x2e,
        (byte) 0x72,
        (byte) 0x65,
        (byte) 0x73,
        (byte) 0x70,
        (byte) 0x6f,
        (byte) 0x6e,
        (byte) 0x73,
        (byte) 0x65,
        (byte) 0x48,
        (byte) 0x65,
        (byte) 0x61,
        (byte) 0x64,
        (byte) 0x65,
        (byte) 0x72,
        (byte) 0xa4,
        (byte) 0xe0,
        (byte) 0x2b,
        (byte) 0x7a,
        (byte) 0x6b,
        (byte) 0x43,
        (byte) 0x6f,
        (byte) 0x6e,
        (byte) 0x6e,
        (byte) 0x65,
        (byte) 0x63,
        (byte) 0x74,
        (byte) 0x65,
        (byte) 0x64,
        (byte) 0x01,
        (byte) 0xe0,
        (byte) 0x26,
        (byte) 0x73,
        (byte) 0x74,
        (byte) 0x61,
        (byte) 0x74,
        (byte) 0x75,
        (byte) 0x73,
        (byte) 0x06,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0xe0,
        (byte) 0x25,
        (byte) 0x51,
        (byte) 0x54,
        (byte) 0x69,
        (byte) 0x6d,
        (byte) 0x65,
        (byte) 0x06,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0x00,
        (byte) 0xe0,
        (byte) 0x26,
        (byte) 0x70,
        (byte) 0x61,
        (byte) 0x72,
        (byte) 0x61,
        (byte) 0x6d,
        (byte) 0x73,
        (byte) 0xa4,
        (byte) 0xe0,
        (byte) 0x21,
        (byte) 0x71,
        (byte) 0x21,
        (byte) 0x7a,
        (byte) 0xe0,
        (byte) 0x24,
        (byte) 0x72,
        (byte) 0x6f,
        (byte) 0x77,
        (byte) 0x73,
        (byte) 0x21,
        (byte) 0x30,
        (byte) 0xe0,
        (byte) 0x22,
        (byte) 0x77,
        (byte) 0x74,
        (byte) 0x27,
        (byte) 0x6a,
        (byte) 0x61,
        (byte) 0x76,
        (byte) 0x61,
        (byte) 0x62,
        (byte) 0x69,
        (byte) 0x6e,
        (byte) 0xe0,
        (byte) 0x27,
        (byte) 0x76,
        (byte) 0x65,
        (byte) 0x72,
        (byte) 0x73,
        (byte) 0x69,
        (byte) 0x6f,
        (byte) 0x6e,
        (byte) 0x21,
        (byte) 0x32,
        (byte) 0xe0,
        (byte) 0x28,
        (byte) 0x72,
        (byte) 0x65,
        (byte) 0x73,
        (byte) 0x70,
        (byte) 0x6f,
        (byte) 0x6e,
        (byte) 0x73,
        (byte) 0x65,
        (byte) 0x0c,
        (byte) 0x84,
        (byte) 0x60,
        (byte) 0x60,
        (byte) 0x00,
        (byte) 0x01,
        (byte) 0x80
      };
      DebugServlet.responseBody = javabin;
    }
    String url = getBaseUrl() + "/debug/foo";
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    HttpSolrClientJdkImpl.Builder b = builder(url);
    if (rp != null) {
      b.withResponseParser(rp);
    }
    try (HttpSolrClientJdkImpl client = b.build()) {
      client.query(q, method);
      assertEquals(
          client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    }
  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (HttpSolrClientJdkImpl client = builder(getBaseUrl() + "/debug/foo").build()) {
      super.testGetById(client);
    }
  }

  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try (HttpSolrClientJdkImpl client =
        builder(
                getBaseUrl() + "/slow/foo",
                DEFAULT_CONNECTION_TIMEOUT,
                2000,
                HttpSolrClientJdkImpl.Builder.class)
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
    try (HttpSolrClientJdkImpl client =
        builder(
                getBaseUrl() + "/debug/foo",
                DEFAULT_CONNECTION_TIMEOUT,
                0,
                HttpSolrClientJdkImpl.Builder.class)
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
    try (HttpSolrClientJdkImpl client =
        builder(
                getBaseUrl() + "/slow/foo",
                DEFAULT_CONNECTION_TIMEOUT,
                0,
                HttpSolrClientJdkImpl.Builder.class)
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
    final String clientUrl = getBaseUrl() + "/redirect/foo";
    try (HttpSolrClientJdkImpl client = builder(clientUrl).withFollowRedirects(true).build()) {
      SolrQuery q = new SolrQuery("*:*");
      client.query(q);
    }
  }

  @Test
  public void testDoNotFollowRedirect() throws Exception {
    final String clientUrl = getBaseUrl() + "/redirect/foo";
    try (HttpSolrClientJdkImpl client = builder(clientUrl).withFollowRedirects(false).build()) {
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
    try (HttpSolrClientJdkImpl client = builder(clientUrl).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

    try (HttpSolrClientJdkImpl client = builder(clientUrl).withFollowRedirects(true).build()) {
      // shouldn't throw an exception
      client.query(q);
    }

    // set explicit false for following redirects
    try (HttpSolrClientJdkImpl client = builder(clientUrl).withFollowRedirects(false).build()) {

      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }
  }

  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    try (HttpSolrClientJdkImpl client = builder(getBaseUrl() + "/debug/foo").build()) {
      super.testSolrExceptionCodeNotFromSolr(client);
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
    try (HttpSolrClientJdkImpl client = builder(null).build()) {
      super.testSolrExceptionWithNullBaseurl(client);
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testUpdateDefault() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (HttpSolrClientJdkImpl client = builder(url).build()) {
      testUpdate(client, "javabin", "application/javabin");
    }
  }

  @Test
  public void testUpdateXml() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (HttpSolrClientJdkImpl client =
        builder(url)
            .withRequestWriter(new RequestWriter())
            .withResponseParser(new XMLResponseParser())
            .build()) {
      testUpdate(client, "xml", "application/xml; charset=UTF-8");
    }
  }

  @Test
  public void testUpdateJavabin() throws Exception {
    String url = getBaseUrl() + "/debug/foo";
    try (HttpSolrClientJdkImpl client =
        builder(url)
            .withRequestWriter(new BinaryRequestWriter())
            .withResponseParser(new BinaryResponseParser())
            .build()) {
      testUpdate(client, "javabin", "application/javabin");
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {
    HttpSolrClientJdkImpl baseUrlClient = builder(getBaseUrl()).build();
    HttpSolrClientJdkImpl collection1UrlClient = builder(getCoreUrl()).build();
    testCollectionParameters(baseUrlClient, collection1UrlClient);
  }

  @Test
  public void testQueryString() throws Exception {
    testQueryString(HttpSolrClientJdkImpl.class, HttpSolrClientJdkImpl.Builder.class);
  }

  @Test
  public void testGetRawStream() throws Exception {
    try (HttpSolrClientJdkImpl client =
        builder(
                getBaseUrl() + "/debug/foo",
                DEFAULT_CONNECTION_TIMEOUT,
                DEFAULT_CONNECTION_TIMEOUT,
                HttpSolrClientJdkImpl.Builder.class)
            .build()) {
      super.testGetRawStream(client);
    }
  }

  @Test
  public void testSetCredentialsExplicitly() throws Exception {
    try (HttpSolrClientJdkImpl client =
        builder(getBaseUrl() + "/debug/foo")
            .withBasicAuthCredentials("foo", "explicit")
            .build(); ) {
      super.testSetCredentialsExplicitly(client);
    }
  }

  @Test
  public void testPerRequestCredentials() throws Exception {
    try (HttpSolrClientJdkImpl client =
        builder(getBaseUrl() + "/debug/foo")
            .withBasicAuthCredentials("foo2", "explicit")
            .build(); ) {
      super.testPerRequestCredentials(client);
    }
  }

  @Test
  public void testNoCredentials() throws Exception {
    try (HttpSolrClientJdkImpl client = builder(getBaseUrl() + "/debug/foo").build(); ) {
      super.testNoCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentials() throws Exception {
    // username foo, password with embedded colon separator is "expli:cit".
    try (HttpSolrClientJdkImpl client =
        builder(getBaseUrl() + "/debug/foo")
            .withOptionalBasicAuthCredentials("foo:expli:cit")
            .build(); ) {
      super.testUseOptionalCredentials(client);
    }
  }

  @Test
  public void testUseOptionalCredentialsWithNull() throws Exception {
    try (HttpSolrClientJdkImpl client =
        builder(getBaseUrl() + "/debug/foo").withOptionalBasicAuthCredentials(null).build(); ) {
      super.testUseOptionalCredentialsWithNull(client);
    }
  }

  @Override
  protected String expectedUserAgent() {
    return "Solr[" + HttpSolrClientJdkImpl.class.getName() + "] 1.0";
  }

  @Override
  protected <B extends HttpSolrClientBuilderBase> B builder(
      String url, int connectionTimeout, int socketTimeout, Class<B> type) {
    HttpSolrClientJdkImpl.Builder b =
        new HttpSolrClientJdkImpl.Builder(url)
            .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
            .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS)
            .withSSLContext(allTrustingSslContext);
    return type.cast(b);
  }

  private HttpSolrClientJdkImpl.Builder builder(String url) {
    return builder(
        url,
        DEFAULT_CONNECTION_TIMEOUT,
        DEFAULT_CONNECTION_TIMEOUT,
        HttpSolrClientJdkImpl.Builder.class);
  }

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
