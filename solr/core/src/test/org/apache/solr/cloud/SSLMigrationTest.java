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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.SSLTestConfig;
import org.junit.Test;

/**
 * We want to make sure that when migrating between http and https modes the replicas will not be
 * rejoined as new nodes, but rather take off where it left off in the cluster.
 */
@SuppressSSL
@AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12028") // 17-Mar-2018
public class SSLMigrationTest extends AbstractFullDistribZkTestBase {

  @Test
  public void test() throws Exception {
    // Migrate from HTTP -> HTTPS -> HTTP
    assertReplicaInformation("http");
    testMigrateSSL(new SSLTestConfig(true, false));
    testMigrateSSL(new SSLTestConfig(false, false));
  }

  public void testMigrateSSL(SSLTestConfig sslConfig) throws Exception {
    String urlScheme = sslConfig.isSSLMode() ? "https" : "http";
    setUrlScheme(urlScheme);

    for (JettySolrRunner runner : jettys) {
      runner.stop();
    }

    HttpClientUtil.setSocketFactoryRegistryProvider(
        sslConfig.buildClientSocketFactoryRegistryProvider());
    for (int i = 0; i < this.jettys.size(); i++) {
      JettySolrRunner runner = jettys.get(i);
      JettyConfig config =
          JettyConfig.builder()
              .setContext(context)
              .setPort(runner.getLocalPort())
              .stopAtShutdown(false)
              .withServlets(getExtraServlets())
              .withFilters(getExtraRequestFilters())
              .withSSLConfig(sslConfig.buildServerSSLConfig())
              .build();

      Properties props = new Properties();
      if (getSolrConfigFile() != null) props.setProperty("solrconfig", getSolrConfigFile());
      if (getSchemaFile() != null) props.setProperty("schema", getSchemaFile());
      props.setProperty("solr.data.dir", getDataDir(testDir + "/shard" + i + "/data"));

      JettySolrRunner newRunner = new JettySolrRunner(runner.getSolrHome(), props, config);
      newRunner.start();
      jettys.set(i, newRunner);
    }

    assertReplicaInformation(urlScheme);
  }

  private void assertReplicaInformation(String urlScheme) {
    List<Replica> replicas = getReplicas();
    assertEquals("Wrong number of replicas found", 4, replicas.size());
    for (Replica replica : replicas) {
      assertTrue(
          "Replica didn't have the proper urlScheme in the ClusterState",
          replica.getStr(ZkStateReader.BASE_URL_PROP).startsWith(urlScheme));
    }
  }

  private List<Replica> getReplicas() {
    List<Replica> replicas = new ArrayList<>();

    DocCollection collection = cloudClient.getClusterState().getCollection(DEFAULT_COLLECTION);
    for (Slice slice : collection.getSlices()) {
      replicas.addAll(slice.getReplicas());
    }
    return replicas;
  }

  private void setUrlScheme(String value) throws Exception {
    Map<String, String> m =
        Map.of(
            "action",
            CollectionAction.CLUSTERPROP.toString().toLowerCase(Locale.ROOT),
            "name",
            "urlScheme",
            "val",
            value);
    SolrParams params = new MapSolrParams(m);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String[] urls =
        getReplicas().stream()
            .map(r -> r.getStr(ZkStateReader.BASE_URL_PROP))
            .toArray(String[]::new);
    // Create new SolrServer to configure new HttpClient w/ SSL config
    try (SolrClient client = new LBHttpSolrClient.Builder().withBaseSolrUrls(urls).build()) {
      client.request(request);
    }
  }
}
