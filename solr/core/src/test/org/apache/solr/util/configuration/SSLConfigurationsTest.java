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

package org.apache.solr.util.configuration;

import static org.hamcrest.CoreMatchers.is;

import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.tests.util.TestRuleRestoreSystemProperties;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.configuration.providers.EnvSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.SysPropSSLCredentialProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class SSLConfigurationsTest extends SolrTestCase {
  private Map<String, String> envs;
  private SSLConfigurations sut;

  public static final String SAMPLE_PW1 = "pw123";
  public static final String SAMPLE_PW2 = "pw456";
  public static final String SAMPLE_PW3 = "pw789";
  public static final String KEY_STORE_PASSWORD = SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD;
  public static final String TRUST_STORE_PASSWORD =
      SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD;
  public static final String CLIENT_KEY_STORE_PASSWORD =
      SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD;
  public static final String CLIENT_TRUST_STORE_PASSWORD =
      SSLConfigurations.SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD;

  @Rule
  public TestRule syspropRestore =
      new TestRuleRestoreSystemProperties(
          SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD,
          SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD,
          SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD,
          SSLConfigurations.SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    envs = new HashMap<>();
  }

  private SSLConfigurations createSut() {
    EnvSSLCredentialProvider envSSLCredentialProvider = new EnvSSLCredentialProvider();
    envSSLCredentialProvider.setEnvVars(envs);
    sut =
        new SSLConfigurations(
            Arrays.asList(envSSLCredentialProvider, new SysPropSSLCredentialProvider()));
    return sut;
  }

  @Test
  public void testSslConfigKeystorePwFromKeystoreEnvVar() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    createSut().init();
    assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW1));
  }

  @Test
  public void testSslConfigKeystorePwFromClientKeystoreEnvVar() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigKeystorePwFromBothEnvVars() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigKeystorePwNotOverwrittenIfExists() {
    System.setProperty(CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW3);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW3)); // unchanged
  }

  @Test
  public void testSslConfigTruststorePwFromKeystoreEnvVar() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    createSut().init();
    assertThat(System.getProperty(CLIENT_TRUST_STORE_PASSWORD), is(SAMPLE_PW1));
  }

  @Test
  public void testSslConfigTruststorePwFromClientKeystoreEnvVar() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_TRUST_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigTruststorePwFromBothEnvVars() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_TRUST_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigTruststorePwNotOverwrittenIfExists() {
    System.setProperty(CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW3);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(CLIENT_TRUST_STORE_PASSWORD), is(SAMPLE_PW3)); // unchanged
  }

  @Test
  public void testGetKeyStorePasswordFromProperty() {
    System.setProperty(KEY_STORE_PASSWORD, SAMPLE_PW1);
    assertThat(createSut().getKeyStorePassword(), is(SAMPLE_PW1));
  }

  @Test
  public void testGetKeyStorePasswordFromEnv() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW2);
    assertThat(createSut().getKeyStorePassword(), is(SAMPLE_PW2));
  }

  @Test
  public void testGetTrustStorePasswordFromProperty() {
    System.setProperty(TRUST_STORE_PASSWORD, SAMPLE_PW1);
    assertThat(createSut().getTrustStorePassword(), is(SAMPLE_PW1));
  }

  @Test
  public void testGetTrustStorePasswordFromEnv() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    assertThat(createSut().getTrustStorePassword(), is(SAMPLE_PW2));
  }

  @Test
  public void testGetClientKeyStorePasswordFromProperty() {
    System.setProperty(CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW1);
    assertThat(createSut().getClientKeyStorePassword(), is(SAMPLE_PW1));
  }

  @Test
  public void testGetClientKeyStorePasswordFromEnv() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    assertThat(createSut().getClientKeyStorePassword(), is(SAMPLE_PW2));
  }

  @Test
  public void testGetClientTrustStorePasswordFromProperty() {
    System.setProperty(CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    assertThat(createSut().getClientTrustStorePassword(), is(SAMPLE_PW1));
  }

  @Test
  public void testGetClientTrustStorePasswordFromEnv() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    assertThat(createSut().getClientTrustStorePassword(), is(SAMPLE_PW2));
  }

  @Test
  public void testSystemPropertyPriorityOverEnvVar() {
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW2);
    assertThat(createSut().getKeyStorePassword(), is(SAMPLE_PW2));
    System.setProperty(KEY_STORE_PASSWORD, SAMPLE_PW1);
    assertThat(createSut().getKeyStorePassword(), is(SAMPLE_PW2));
  }

  @Test
  public void testZooKeeperTlsRequiresExplicitClientSwitch() {
    System.setProperty("javax.net.ssl.trustStore", "/client/truststore.p12");
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    createSut().initZooKeeper();
    assertNull(System.getProperty("zookeeper.client.secure"));
    assertNull(System.getProperty("zookeeper.ssl.trustStore.location"));
  }

  @Test
  public void testZooKeeperTlsUsesClientSslSettings() {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("javax.net.ssl.keyStore", "/client/keystore.p12");
    System.setProperty("javax.net.ssl.keyStoreType", "PKCS12");
    System.setProperty("javax.net.ssl.trustStore", "/client/truststore.p12");
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    System.setProperty("solr.ssl.check.peer.name.enabled", "false");
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().initZooKeeper();
    assertEquals(
        "org.apache.zookeeper.ClientCnxnSocketNetty",
        System.getProperty("zookeeper.clientCnxnSocket"));
    assertEquals("/client/keystore.p12", System.getProperty("zookeeper.ssl.keyStore.location"));
    assertEquals("PKCS12", System.getProperty("zookeeper.ssl.keyStore.type"));
    assertEquals(SAMPLE_PW1, System.getProperty("zookeeper.ssl.keyStore.password"));
    assertEquals("/client/truststore.p12", System.getProperty("zookeeper.ssl.trustStore.location"));
    assertEquals("PKCS12", System.getProperty("zookeeper.ssl.trustStore.type"));
    assertEquals(SAMPLE_PW2, System.getProperty("zookeeper.ssl.trustStore.password"));
    assertEquals("false", System.getProperty("zookeeper.ssl.hostnameVerification"));
  }

  @Test
  public void testZooKeeperExplicitSettingsTakePrecedence() {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("zookeeper.ssl.trustStore.location", "/zk/custom.jks");
    System.setProperty("zookeeper.ssl.trustStore.passwordPath", "/zk/password");
    System.setProperty("zookeeper.ssl.hostnameVerification", "true");
    System.setProperty("javax.net.ssl.trustStore", "/solr/truststore.p12");
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    System.setProperty("solr.ssl.check.peer.name.enabled", "false");
    envs.put(EnvSSLCredentialProvider.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    createSut().initZooKeeper();
    assertEquals("/zk/custom.jks", System.getProperty("zookeeper.ssl.trustStore.location"));
    assertEquals("/zk/password", System.getProperty("zookeeper.ssl.trustStore.passwordPath"));
    assertNull(System.getProperty("zookeeper.ssl.trustStore.password"));
    assertNull(System.getProperty("zookeeper.ssl.trustStore.type"));
    assertEquals("true", System.getProperty("zookeeper.ssl.hostnameVerification"));
  }

  @Test
  public void testZooKeeperDerivedStoresUseJsseDefaultType() {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("javax.net.ssl.keyStore", "/client/keystore");
    System.setProperty("javax.net.ssl.trustStore", "/client/truststore");
    createSut().initZooKeeper();
    assertEquals(KeyStore.getDefaultType(), System.getProperty("zookeeper.ssl.keyStore.type"));
    assertEquals(KeyStore.getDefaultType(), System.getProperty("zookeeper.ssl.trustStore.type"));
    assertEquals(
        ZookeeperSSLContextSupplier.class.getName(),
        System.getProperty("zookeeper.ssl.context.supplier.class"));
  }

  @Test
  public void testZooKeeperExplicitContextSupplierPreserved() {
    assertContextSupplier(
        "zookeeper.ssl.context.supplier.class", "custom.Supplier", "custom.Supplier");
  }

  @Test
  public void testZooKeeperNativeProviderPreserved() {
    assertContextSupplier("zookeeper.ssl.sslProvider", "OPENSSL", null);
  }

  @Test
  public void testZooKeeperReverseDnsVerificationPreserved() {
    assertContextSupplier("zookeeper.ssl.allowReverseDnsLookup", "true", null);
  }

  /**
   * Enables a secure ZK client with a Solr truststore, sets one extra ZooKeeper property, and
   * asserts the resulting context supplier class ({@code null} means Solr's supplier was not
   * installed).
   */
  private void assertContextSupplier(String key, String value, String expectedSupplier) {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("javax.net.ssl.trustStore", "/client/truststore.p12");
    System.setProperty(key, value);
    createSut().initZooKeeper();
    assertEquals(expectedSupplier, System.getProperty("zookeeper.ssl.context.supplier.class"));
  }
}
