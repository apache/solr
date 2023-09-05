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
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class Http2SolrClientProxyTest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();

  // TODO add SSL test

  @BeforeClass
  public static void beforeTest() throws Exception {
    RandomizedTest.assumeFalse(sslConfig.isSSLMode());

    solrClientTestRule.enableProxy();
    solrClientTestRule.startSolr(
        createTempDir(),
        new Properties(),
        JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build());
    // Actually only need extremely minimal configSet but just use the default
    solrClientTestRule
        .newCollection()
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET) // TODO should be default for empty home
        .create();
  }

  /** Setup a simple http proxy and verify a request works */
  @Test
  public void testProxy() throws Exception {
    var proxy = solrClientTestRule.getJetty().getProxy();
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
