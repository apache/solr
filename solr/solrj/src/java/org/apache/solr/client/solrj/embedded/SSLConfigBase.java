package org.apache.solr.client.solrj.embedded;

public class SSLConfigBase {
  private boolean useSsl;
  private boolean clientAuth;
  private String keyStore;
  private String keyStorePassword;
  private String trustStore;
  private String trustStorePassword;

  public SSLConfigBase(
      boolean useSsl,
      boolean clientAuth,
      String keyStore,
      String keyStorePassword,
      String trustStore,
      String trustStorePassword) {
    this.useSsl = useSsl;
    this.clientAuth = clientAuth;
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
  }

  public void setUseSSL(boolean useSsl) {
    this.useSsl = useSsl;
  }

  public void setClientAuth(boolean clientAuth) {
    this.clientAuth = clientAuth;
  }

  /** All other settings on this object are ignored unless this is true */
  public boolean isSSLMode() {
    return useSsl;
  }

  public boolean isClientAuthMode() {
    return clientAuth;
  }

  public String getKeyStore() {
    return keyStore;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStore() {
    return trustStore;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }
}
