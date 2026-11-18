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
package org.apache.solr.cloud;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Tests embedded ZooKeeper listener selection and automatic TLS configuration. */
public class SolrZkServerTest extends SolrTestCase {
  private SolrZkServer server(String configuration) throws Exception {
    Path home = createTempDir();
    Path cfg = home.resolve("zoo.cfg");
    Files.writeString(cfg, "tickTime=2000\n" + configuration);
    SolrZkServer server = new SolrZkServer(true, null, home.resolve("data"), home.toString(), 8983);
    server.parseConfig();
    // TLS settings are applied in memory only; the zoo.cfg on disk is left untouched.
    assertEquals("tickTime=2000\n" + configuration, Files.readString(cfg));
    return server;
  }

  private void enableHttps() throws Exception {
    Path store = createTempDir().resolve("keystore.jks");
    try (var in = getClass().getResourceAsStream("/SSLTestConfig.testing.keystore")) {
      Files.copy(in, store);
    }
    System.setProperty("solr.jetty.https.port", "8983");
    System.setProperty("solr.jetty.keystore", store.toString());
    System.setProperty("solr.jetty.keystore.type", "JKS");
    System.setProperty("solr.jetty.keystore.password", "secret");
    System.setProperty("solr.jetty.truststore", store.toString());
    System.setProperty("solr.jetty.truststore.type", "JKS");
    System.setProperty("solr.jetty.truststore.password", "secret");
  }

  @Test
  public void testPlaintextDefault() throws Exception {
    SolrZkServer server = server("");
    assertEquals("127.0.0.1:9983", server.getClientString());
    assertNull(server.zkProps.getSecureClientPortAddress());
  }

  @Test
  public void testHttpsAutomaticallyUsesTls() throws Exception {
    enableHttps();
    SolrZkServer server = server("");
    assertNull(server.zkProps.getClientPortAddress());
    assertEquals(9983, server.zkProps.getSecureClientPortAddress().getPort());
    assertEquals("127.0.0.1:9983", server.getClientString());
    assertEquals("none", System.getProperty("zookeeper.ssl.clientAuth"));
    assertEquals("false", System.getProperty("zookeeper.client.portUnification"));
    assertEquals("secret", System.getProperty("zookeeper.ssl.keyStore.password"));
  }

  @Test
  public void testHttpsUpgradesExistingClientPort() throws Exception {
    enableHttps();
    SolrZkServer server = server("clientPort=2181\n");
    assertNull(server.zkProps.getClientPortAddress());
    assertEquals("127.0.0.1:2181", server.getClientString());
  }

  @Test
  public void testSecureClientSelectsSecurePortWhenBothExist() throws Exception {
    System.setProperty("zookeeper.client.secure", "true");
    SolrZkServer server = server("clientPort=2181\nsecureClientPort=2281\n");
    assertEquals("127.0.0.1:2281", server.getClientString());
  }

  @Test
  public void testPlaintextClientSelectsPlaintextPortWhenBothExist() throws Exception {
    SolrZkServer server = server("clientPort=2181\nsecureClientPort=2281\n");
    assertEquals("127.0.0.1:2181", server.getClientString());
  }

  @Test
  public void testSecureOnlyConfiguration() throws Exception {
    SolrZkServer server = server("secureClientPort=2281\n");
    assertNull(server.zkProps.getClientPortAddress());
    assertEquals("127.0.0.1:2281", server.getClientString());
  }

  @Test
  public void testExplicitZooKeeperSettingsPreserved() throws Exception {
    enableHttps();
    System.setProperty("solr.ssl.check.peer.name.enabled", "false");
    SolrZkServer server =
        server("secureClientPort=2281\nssl.clientAuth=need\nssl.hostnameVerification=true\n");
    assertEquals("127.0.0.1:2281", server.getClientString());
    assertEquals("need", System.getProperty("zookeeper.ssl.clientAuth"));
    assertEquals("true", System.getProperty("zookeeper.ssl.hostnameVerification"));
  }

  @Test
  public void testHttpsSelectsSecurePortWhenBothExist() throws Exception {
    enableHttps();
    SolrZkServer server = server("clientPort=2181\nsecureClientPort=2281\n");
    assertNotNull(server.zkProps.getClientPortAddress());
    assertEquals("127.0.0.1:2281", server.getClientString());
  }

  @Test
  public void testHttpsHonorsSolrPeerNameVerification() throws Exception {
    enableHttps();
    System.setProperty("solr.ssl.check.peer.name.enabled", "false");
    server("");
    assertEquals("false", System.getProperty("zookeeper.ssl.hostnameVerification"));
  }
}
