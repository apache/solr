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

package org.apache.solr.client.solrj.embedded;

/** Encapsulates settings related to SSL/TLS Configuration. */
public class SSLConfigBase {
  private boolean useSsl;
  private boolean clientAuth;
  private String keyStore;
  private String keyStorePassword;
  private String trustStore;
  private String trustStorePassword;

  /**
   * NOTE: all other settings are ignored if useSsl is false; trustStore settings are ignored if
   * clientAuth is false.
   *
   * @param useSsl - enable/disable SSL/TLS
   * @param clientAuth - whether client authentication should be requested.
   * @param keyStore - the Keystore to use
   * @param keyStorePassword - the Keystore password
   * @param trustStore - the Truststore to use
   * @param trustStorePassword - The Truststore password
   */
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

  /**
   * If set to false, all other settings are ignored.
   *
   * @param useSsl boolean
   */
  public void setUseSSL(boolean useSsl) {
    this.useSsl = useSsl;
  }

  /**
   * Set whether client authentication should be requested. If set to false, truststore settings are
   * ignored.
   *
   * @param clientAuth boolean
   */
  public void setClientAuth(boolean clientAuth) {
    this.clientAuth = clientAuth;
  }

  /**
   * All other settings on this object are ignored unless this is true
   *
   * @return boolean
   */
  public boolean isSSLMode() {
    return useSsl;
  }

  /**
   * Whether client authentication should be requested. If false, truststore settings are ignored.
   *
   * @return
   */
  public boolean isClientAuthMode() {
    return clientAuth;
  }

  /**
   * The keystore to use.
   *
   * @return the keystore
   */
  public String getKeyStore() {
    return keyStore;
  }

  /**
   * The keystore password.
   *
   * @return the password
   */
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  /**
   * The trustore to use. Ignored if client auth is false.
   *
   * @return the truststore
   */
  public String getTrustStore() {
    return trustStore;
  }

  /**
   * The truststore password.
   *
   * @return the password.
   */
  public String getTrustStorePassword() {
    return trustStorePassword;
  }
}
