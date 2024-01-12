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

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.tests.util.TestRuleRestoreSystemProperties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.configuration.providers.EnvSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.SysPropSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.hadoop.HadoopSSLCredentialProvider;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

public class HadoopSSLConfigurationsTest extends SolrTestCaseJ4 {
  private Map<String, String> envs;
  private SSLConfigurations sut;

  public static final String SAMPLE_PW1 = "pw123";
  public static final String SAMPLE_PW2 = "pw456";
  public static final String SAMPLE_PW3 = "pw789";
  public static final String KEY_STORE_PASSWORD = SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD;
  public static final String CLIENT_KEY_STORE_PASSWORD =
      SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD;

  @Rule
  public TestRule syspropRestore =
      new TestRuleRestoreSystemProperties(
          SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD,
          SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD,
          CREDENTIAL_PROVIDER_PATH);

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    envs = new HashMap<>();
  }

  private SSLConfigurations createSut(Configuration mockHadoopConfiguration) {
    EnvSSLCredentialProvider envSSLCredentialProvider = new EnvSSLCredentialProvider();
    envSSLCredentialProvider.setEnvVars(envs);
    sut =
        new SSLConfigurations(
            Arrays.asList(
                new HadoopSSLCredentialProvider(mockHadoopConfiguration),
                envSSLCredentialProvider,
                new SysPropSSLCredentialProvider()));
    return sut;
  }

  @Test
  public void testSslConfigKeystorePwFromKeystoreHadoopCredentialProvider() throws IOException {
    getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD, SAMPLE_PW1)
        .init();
    MatcherAssert.assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW1));
  }

  @Test
  public void testSslConfigKeystorePwFromClientKeystoreHadoopCredentialProvider()
      throws IOException {
    getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2)
        .init();
    MatcherAssert.assertThat(System.getProperty(CLIENT_KEY_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testGetKeyStorePasswordFromHadoopCredentialProvider() throws IOException {
    SSLConfigurations sut =
        getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD, SAMPLE_PW3);
    MatcherAssert.assertThat(sut.getKeyStorePassword(), is(SAMPLE_PW3));
  }

  @Test
  public void testGetTruststorePasswordFromHadoopCredentialProvider() throws IOException {
    SSLConfigurations sut =
        getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD, SAMPLE_PW3);
    MatcherAssert.assertThat(sut.getTrustStorePassword(), is(SAMPLE_PW3));
  }

  @Test
  public void testGetClientKeyStorePasswordFromHadoopCredentialProvider() throws IOException {
    SSLConfigurations sut =
        getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW3);
    MatcherAssert.assertThat(sut.getClientKeyStorePassword(), is(SAMPLE_PW3));
  }

  @Test
  public void testGetClientTruststorePasswordFromHadoopCredentialProvider() throws IOException {
    SSLConfigurations sut =
        getSutWithMockedHadoopCredentialProvider(
            SSLConfigurations.SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW3);
    MatcherAssert.assertThat(sut.getClientTrustStorePassword(), is(SAMPLE_PW3));
  }

  @Test
  public void testHadoopCredentialProviderPrioritySysPropAndEnvVars() throws IOException {
    SSLConfigurations sut =
        getSutWithMockedHadoopCredentialProvider(KEY_STORE_PASSWORD, SAMPLE_PW3);
    MatcherAssert.assertThat(sut.getKeyStorePassword(), is(SAMPLE_PW3));
  }

  private SSLConfigurations getSutWithMockedHadoopCredentialProvider(String key, String pw)
      throws IOException {
    Configuration mockHadoopConfiguration = getMockHadoopConfiguration();
    when(mockHadoopConfiguration.getPassword(key))
        .then(
            invocationOnMock ->
                invocationOnMock.getArguments()[0].equals(key) ? pw.toCharArray() : null);
    System.setProperty(CREDENTIAL_PROVIDER_PATH, "/some/path"); // enables HCP
    return createSut(mockHadoopConfiguration);
  }

  private Configuration getMockHadoopConfiguration() {
    SolrTestCaseJ4.assumeWorkingMockito();
    return Mockito.mock(Configuration.class);
  }
}
