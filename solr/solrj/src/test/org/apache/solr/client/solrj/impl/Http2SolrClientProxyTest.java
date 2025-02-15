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
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
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
    solrClientTestRule.startSolr(createTempDir(), new Properties(), JettyConfig.builder().build());
    // Actually only need extremely minimal configSet but just use the default
    solrClientTestRule
        .newCollection()
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET) // TODO should be default for empty home
        .create();
  }

  private SocketProxy proxy;

  private String host;

  private String url;

  @Before
  public void before() {
    this.proxy = solrClientTestRule.getJetty().getProxy();
    this.host = proxy.getUrl().getHost();
    this.url = "http://" + host + ":" + (proxy.getUrl().getPort() + 10) + "/solr";
  }

  @Test
  public void testProxyWithHttp2SolrClient() throws Exception {
    assertNotNull(proxy);
    var builder =
        new Http2SolrClient.Builder(url)
            .withProxyConfiguration(host, proxy.getListenPort(), false, false);

    try (Http2SolrClient client = builder.build()) {
      testProxy(client);
    }
  }

  @Test
  public void testProxyWithHttpSolrClientJdkImpl() throws Exception {
    assertNotNull(proxy);
    var builder =
        new HttpJdkSolrClient.Builder(url)
            .withProxyConfiguration(host, proxy.getListenPort(), false, false);
    try (HttpJdkSolrClient client = builder.build()) {
      testProxy(client);
    }
    // This is a workaround for java.net.http.HttpClient not implementing closeable/autocloseable
    // until Java 21.
    Thread[] threads = new Thread[Thread.currentThread().getThreadGroup().activeCount()];
    Thread.currentThread().getThreadGroup().enumerate(threads);
    Set<Thread> tSet =
        Arrays.stream(threads)
            .filter(Objects::nonNull)
            .filter(t -> t.getName().startsWith("HttpClient-"))
            .collect(Collectors.toSet());
    for (Thread t : tSet) {
      t.interrupt();
    }
    System.gc();
  }

  /** Set up a simple http proxy and verify a request works */
  public void testProxy(HttpSolrClientBase client) throws Exception {
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

    client.deleteByQuery(DEFAULT_TEST_COLLECTION_NAME, "*:*");
    client.commit(DEFAULT_TEST_COLLECTION_NAME);
    assertEquals(
        0,
        client
            .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("*:*"))
            .getResults()
            .getNumFound());
  }
}
