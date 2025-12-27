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
package org.apache.solr.bootstrap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for SslCertificateGenerator SSL configuration logic.
 *
 * <p>Tests the decision logic for when to enable SSL, auto-generate certificates, or use
 * user-provided keystores.
 */
public class SslCertificateGeneratorTest extends SolrTestCaseJ4 {

  private Path tempDir;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tempDir = Files.createTempDirectory("solr-ssl-test");
  }

  @After
  public void tearDown() throws Exception {
    // Clean up temp directory
    deleteDirectory(tempDir.toFile());
    super.tearDown();
  }

  private void deleteDirectory(File dir) {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }

  @Test
  public void testDefaultBehavior_NoSslVariables_HttpMode() throws Exception {
    // When no SSL variables are set, should default to HTTP mode (SSL disabled)
    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("Default behavior should be HTTP mode (null config)", config, is(nullValue()));
  }

  @Test
  public void testExplicitSslDisabled() throws Exception {
    // Explicitly disable SSL
    System.setProperty("solr.ssl.enabled", "false");

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("Explicitly disabled SSL should return null", config, is(nullValue()));
  }

  @Test
  public void testSslEnabled_NoKeystores_AutoGenerate() throws Exception {
    // SSL enabled with no keystores should auto-generate (SOLR_SSL_GENERATE defaults to true)
    System.setProperty("solr.ssl.enabled", "true");
    System.setProperty("solr.pid.dir", tempDir.toString());

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("SSL should be configured", config, is(notNullValue()));
    assertThat("Certificate should be auto-generated", config.isGenerated(), is(true));
    assertThat(
        "Generated keystore path should be set",
        config.getGeneratedKeystorePath(),
        is(notNullValue()));
    assertThat(
        "Generated password should be set", config.getGeneratedPassword(), is(notNullValue()));
    assertThat(
        "Keystore file should be created",
        Files.exists(config.getGeneratedKeystorePath()),
        is(true));
  }

  @Test
  public void testSslEnabled_WithKeyStore_NoGeneration() throws Exception {
    // SSL enabled with user-provided keystore should not generate
    System.setProperty("solr.ssl.enabled", "true");
    System.setProperty("solr.ssl.key.store", "/path/to/keystore.p12");

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("SSL should be configured", config, is(notNullValue()));
    assertThat("Certificate should not be generated", config.isGenerated(), is(false));
    assertThat("Should indicate keystore is provided", config.hasKeyStore(), is(true));
    assertThat("No generated keystore path", config.getGeneratedKeystorePath(), is(nullValue()));
    assertThat("No generated password", config.getGeneratedPassword(), is(nullValue()));
  }

  @Test
  public void testSslEnabled_WithTrustStore_NoGeneration() throws Exception {
    // SSL enabled with user-provided truststore should not generate
    System.setProperty("solr.ssl.enabled", "true");
    System.setProperty("solr.ssl.trust.store", "/path/to/truststore.p12");

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("SSL should be configured", config, is(notNullValue()));
    assertThat("Certificate should not be generated", config.isGenerated(), is(false));
    assertThat("Should indicate truststore is provided", config.hasTrustStore(), is(true));
    assertThat("No generated keystore path", config.getGeneratedKeystorePath(), is(nullValue()));
  }

  @Test(expected = IllegalStateException.class)
  public void testSslEnabled_GenerateDisabled_NoKeystores_ThrowsException() throws Exception {
    // SSL enabled but generation disabled and no keystores should throw error
    System.setProperty("solr.ssl.enabled", "true");
    System.setProperty("solr.ssl.generate", "false");

    SslCertificateGenerator.configureSsl();
  }

  @Test
  public void testAutoEnableSsl_KeyStoreProvided() throws Exception {
    // Providing keystore should auto-enable SSL (backward compatibility)
    System.setProperty("solr.ssl.key.store", "/path/to/keystore.p12");
    // Note: solr.ssl.enabled not set, should auto-enable

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("SSL should be auto-enabled when keystore provided", config, is(notNullValue()));
    assertThat("Certificate should not be generated", config.isGenerated(), is(false));
    assertThat("Should indicate keystore is provided", config.hasKeyStore(), is(true));
  }

  @Test
  public void testAutoEnableSsl_TrustStoreProvided() throws Exception {
    // Providing truststore should auto-enable SSL (backward compatibility)
    System.setProperty("solr.ssl.trust.store", "/path/to/truststore.p12");
    // Note: solr.ssl.enabled not set, should auto-enable

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat("SSL should be auto-enabled when truststore provided", config, is(notNullValue()));
    assertThat("Certificate should not be generated", config.isGenerated(), is(false));
    assertThat("Should indicate truststore is provided", config.hasTrustStore(), is(true));
  }

  @Test
  public void testSslEnabled_ExistingKeystore_Reuse() throws Exception {
    // If keystore already exists, should reuse it instead of regenerating
    System.setProperty("solr.ssl.enabled", "true");
    System.setProperty("solr.pid.dir", tempDir.toString());

    // First generation
    SslCertificateGenerator.SslConfiguration config1 = SslCertificateGenerator.configureSsl();
    assertThat(config1, is(notNullValue()));
    Path keystorePath = config1.getGeneratedKeystorePath();
    String password1 = config1.getGeneratedPassword();

    // Second call should reuse the same keystore
    SslCertificateGenerator.SslConfiguration config2 = SslCertificateGenerator.configureSsl();
    assertThat(config2, is(notNullValue()));

    assertThat("Should reuse same keystore", config2.getGeneratedKeystorePath(), is(keystorePath));
    assertThat("Should reuse same password", config2.getGeneratedPassword(), is(password1));
  }

  @Test
  public void testSslConfiguration_BothKeystoreAndTruststore() throws Exception {
    // Both keystore and truststore provided
    System.setProperty("solr.ssl.key.store", "/path/to/keystore.p12");
    System.setProperty("solr.ssl.trust.store", "/path/to/truststore.p12");

    SslCertificateGenerator.SslConfiguration config = SslCertificateGenerator.configureSsl();

    assertThat(config, is(notNullValue()));
    assertThat("Should have keystore", config.hasKeyStore(), is(true));
    assertThat("Should have truststore", config.hasTrustStore(), is(true));
    assertThat("Should not be generated", config.isGenerated(), is(false));
  }
}
