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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class Http2SolrClientProxyTest extends SolrJettyTestBase {

  // TODO add SSL test

  @BeforeClass
  public static void beforeTest() throws Exception {
    RandomizedTest.assumeFalse(sslConfig.isSSLMode());

    JettyConfig jettyConfig =
        JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build();
    createAndStartJettyWithProxy(legacyExampleCollection1SolrHome(), new Properties(), jettyConfig);
  }

  public static JettySolrRunner createAndStartJettyWithProxy(
      String solrHome, Properties nodeProperties, JettyConfig jettyConfig) throws Exception {

    initCore(null, null, solrHome);

    Path coresDir = createTempDir().resolve("cores");

    Properties props = new Properties();
    props.setProperty("name", DEFAULT_TEST_CORENAME);
    props.setProperty("configSet", "collection1");
    props.setProperty("config", "${solrconfig:solrconfig.xml}");
    props.setProperty("schema", "${schema:schema.xml}");

    writeCoreProperties(coresDir.resolve("core"), props, "RestTestBase");

    Properties nodeProps = new Properties(nodeProperties);
    nodeProps.setProperty("coreRootDirectory", coresDir.toString());
    nodeProps.setProperty("configSetBaseDir", solrHome);

    jetty = new JettySolrRunner(solrHome, nodeProps, jettyConfig, true);
    jetty.start();
    port = jetty.getLocalPort();
    return jetty;
  }

  /** Setup a simple http proxy and verify a request works */
  @Test
  public void testProxy() throws Exception {
    var proxy = jetty.getProxy();
    assertNotNull(proxy);

    String host = proxy.getUrl().getHost();
    String url = "http://" + host + ":" + (proxy.getUrl().getPort() + 10) + "/solr";

    var builder =
        new Http2SolrClient.Builder(url)
            .withProxyConfiguration(host, proxy.getListenPort(), false, false);

    try (Http2SolrClient client = builder.build()) {
      String id = "1234";
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", id);
      client.add(DEFAULT_TEST_COLLECTION_NAME, doc);
      client.commit(DEFAULT_TEST_COLLECTION_NAME);
      assertEquals(
          1,
          client
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("id:" + id))
              .getResults()
              .getNumFound());
    }
  }
}
