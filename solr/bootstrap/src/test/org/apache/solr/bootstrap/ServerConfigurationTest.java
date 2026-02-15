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

import static org.hamcrest.Matchers.is;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for ServerConfiguration.
 *
 * <p>Tests configuration parsing from system properties and default values.
 */
public class ServerConfigurationTest extends SolrTestCaseJ4 {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Set required properties for ServerConfiguration
    System.setProperty("solr.solr.home", "/tmp/solr-home");
    System.setProperty("solr.install.dir", "/tmp/solr-install");
    System.setProperty("solr.logs.dir", "/tmp/solr-logs");
    System.setProperty("solr.jetty.home", "/tmp/jetty-home");
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testDefaultConfiguration() {
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    // Test default values
    assertThat("Default port should be 8983", config.getPort(), is(8983));
    assertThat("Default host should be 127.0.0.1", config.getHost(), is("127.0.0.1"));
    assertThat("HTTPS should be disabled by default", config.isHttpsEnabled(), is(false));
    assertThat("Default min threads should be 10", config.getMinThreads(), is(10));
    assertThat("Default max threads should be 10000", config.getMaxThreads(), is(10000));
    assertThat("GZIP should be enabled by default", config.isGzipEnabled(), is(true));
  }

  @Test
  public void testCustomPort() {
    System.setProperty("solr.port.listen", "9000");
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("Port should be set from system property", config.getPort(), is(9000));
  }

  @Test
  public void testCustomHost() {
    System.setProperty("solr.host.bind", "0.0.0.0");
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("Host should be set from system property", config.getHost(), is("0.0.0.0"));
  }

  @Test
  public void testHttpsEnabled() {
    System.setProperty("solr.jetty.https.port", "8983");
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("HTTPS should be enabled when https port is set", config.isHttpsEnabled(), is(true));
    assertThat("HTTPS port should be set", config.getHttpsPort(), is(8983));
  }

  @Test
  public void testSslConfiguration() {
    System.setProperty("solr.ssl.key.store", "/path/to/keystore.p12");
    System.setProperty("solr.ssl.key.store.password", "secret");
    System.setProperty("solr.ssl.trust.store", "/path/to/truststore.p12");
    System.setProperty("solr.ssl.trust.store.password", "secret2");

    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat(
        "Keystore path should be set", config.getKeyStorePath(), is("/path/to/keystore.p12"));
    assertThat("Keystore password should be set", config.getKeyStorePassword(), is("secret"));
    assertThat(
        "Truststore path should be set", config.getTrustStorePath(), is("/path/to/truststore.p12"));
    assertThat("Truststore password should be set", config.getTrustStorePassword(), is("secret2"));
  }

  @Test
  public void testSslCheckPeerNameDefault() {
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat(
        "SSL check peer name should default to true for backward compatibility",
        config.isSslCheckPeerName(),
        is(true));
  }

  @Test
  public void testSslCheckPeerNameCustom() {
    System.setProperty("solr.ssl.check.peer.name.enabled", "false");
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat(
        "SSL check peer name should be set from system property",
        config.isSslCheckPeerName(),
        is(false));
  }

  @Test
  public void testThreadPoolConfiguration() {
    System.setProperty("solr.jetty.threads.min", "20");
    System.setProperty("solr.jetty.threads.max", "500");

    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("Min threads should be set from system property", config.getMinThreads(), is(20));
    assertThat("Max threads should be set from system property", config.getMaxThreads(), is(500));
  }

  @Test
  public void testHttpConfiguration() {
    System.setProperty("solr.jetty.output.buffer.size", "65536");
    System.setProperty("solr.jetty.output.aggregation.size", "16384");
    System.setProperty("solr.jetty.request.header.size", "16384");
    System.setProperty("solr.jetty.response.header.size", "16384");

    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat(
        "Output buffer size should be set from system property",
        config.getOutputBufferSize(),
        is(65536));
    assertThat(
        "Output aggregation size should be set from system property",
        config.getOutputAggregationSize(),
        is(16384));
    assertThat(
        "Request header size should be set from system property",
        config.getRequestHeaderSize(),
        is(16384));
    assertThat(
        "Response header size should be set from system property",
        config.getResponseHeaderSize(),
        is(16384));
  }

  @Test
  public void testGzipConfiguration() {
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("GZIP should be enabled by default", config.isGzipEnabled(), is(true));
    assertThat("Default GZIP min size should be 2048", config.getGzipMinSize(), is(2048));
    assertThat(
        "Default GZIP methods should be GET,POST", config.getGzipIncludedMethods(), is("GET,POST"));
  }

  @Test
  public void testSecurePortConfiguration() {
    System.setProperty("solr.jetty.secure.port", "8443");

    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("Secure port should be set from system property", config.getSecurePort(), is(8443));
  }

  @Test
  public void testStopConfiguration() {
    System.setProperty("STOP.PORT", "7983");
    System.setProperty("STOP.KEY", "secret-key");
    System.setProperty("solr.jetty.stop.timeout", "30000");

    ServerConfiguration config = ServerConfiguration.fromSystemProperties();

    assertThat("Stop port should be set from system property", config.getStopPort(), is(7983));
    assertThat(
        "Stop key should be set from system property", config.getStopKey(), is("secret-key"));
    assertThat(
        "Stop timeout should be set from system property", config.getStopTimeout(), is(30000));
  }
}
