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

import java.nio.file.Path;
import java.util.List;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

public class ClusterStateProviderUrlSchemeTest extends SolrCloudTestCase {
  private static final String SOLR_SSL_ENABLED = "solr.ssl.enabled";

  @Test
  public void testHttpClusterStateProviderUrlScheme() throws Exception {
    List<String> solrUrls = List.of("http://localhost:8983/solr");
    try (var httpClient = new HttpJettySolrClient.Builder().build();
        ClusterStateProvider clusterStateProvider =
            new HttpClusterStateProvider<>(solrUrls, httpClient)) {
      assertEquals("http", clusterStateProvider.getUrlScheme());
    }
  }

  @Test
  public void testHttpClusterStateProviderUrlSchemeWithMixedProtocol() {
    List<String> solrUrls = List.of("http://localhost:8983/solr", "https://localhost:8984/solr");
    try (var httpClient = new HttpJettySolrClient.Builder().build()) {
      expectThrows(
          IllegalArgumentException.class,
          () -> new HttpClusterStateProvider<>(solrUrls, httpClient));
    }
  }

  @Test
  public void testZkClusterStateProviderUrlScheme() throws Exception {
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      SolrZkClient client = new SolrZkClient.Builder().withUrl(server.getZkAddress()).build();
      ZkController.createClusterZkNodes(client);
      testUrlSchemeWithClusterProperties(client);
      testUrlSchemeDefault(client);
      testUrlSchemeWithSystemProperties(client);
      client.close();
    } finally {
      server.shutdown();
    }
  }

  private void testUrlSchemeDefault(SolrZkClient client) throws Exception {
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      assertEquals("http", clusterStateProvider.getUrlScheme());
    }
  }

  private void testUrlSchemeWithSystemProperties(SolrZkClient client) throws Exception {
    System.setProperty(SOLR_SSL_ENABLED, "true");
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      assertEquals("https", clusterStateProvider.getUrlScheme());
    } finally {
      System.clearProperty(SOLR_SSL_ENABLED);
      assertNull(System.getProperty(SOLR_SSL_ENABLED));
    }
  }

  private void testUrlSchemeWithClusterProperties(SolrZkClient client) throws Exception {
    ClusterProperties cp = new ClusterProperties(client);
    cp.setClusterProperty("urlScheme", "ftp");
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      zkStateReader.createClusterStateWatchersAndUpdate();
      assertEquals("ftp", clusterStateProvider.getUrlScheme());
    }
  }
}
