package org.apache.solr.client.solrj.impl;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.SSLTestConfig;

/**
 * Taken from: https://www.baeldung.com/java-httpclient-ssl sec 4.1, 2024/02/12. This is an
 * all-trusting Trust Manager. Works with self-signed certificates.
 */
public class MockTrustManager extends X509ExtendedTrustManager {

  public static final SSLContext ALL_TRUSTING_SSL_CONTEXT;

  private static final MockTrustManager INSTANCE = new MockTrustManager();

  static {
    try {
      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      SSLTestConfig stc = SolrTestCaseJ4.sslConfig;
      keyManagerFactory.init(stc.defaultKeyStore(), stc.defaultKeyStorePassword().toCharArray());

      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(
          keyManagerFactory.getKeyManagers(),
          new TrustManager[] {INSTANCE},
          stc.notSecureSecureRandom());
      ALL_TRUSTING_SSL_CONTEXT = sslContext;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private MockTrustManager() {}

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
}
