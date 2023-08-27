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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class HttpSolrClientConPoolTest extends SolrJettyTestBase {

  @ClassRule public static SolrJettyTestRule secondJetty = new SolrJettyTestRule();
  private static String fooUrl; // first Jetty URL
  private static String barUrl; // second Jetty URL

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
    fooUrl = getBaseUrl() + "/" + "collection1";

    secondJetty.startSolr(Path.of(legacyExampleCollection1SolrHome()));
    barUrl = secondJetty.getBaseUrl() + "/" + "collection1";
  }

  public void testPoolSize() throws SolrServerException, IOException {
    PoolingHttpClientConnectionManager pool = HttpClientUtil.createPoolingConnectionManager();

    CloseableHttpClient httpClient =
        HttpClientUtil.createClient(
            new ModifiableSolrParams(), pool, false /* let client shutdown it*/);
    final HttpSolrClient clientFoo =
        new HttpSolrClient.Builder(fooUrl).withHttpClient(httpClient).build();
    final HttpSolrClient clientBar =
        new HttpSolrClient.Builder(barUrl).withHttpClient(httpClient).build();

    clientFoo.deleteByQuery("*:*");
    clientBar.deleteByQuery("*:*");

    List<String> urls = new ArrayList<>();
    for (int i = 0; i < 17; i++) {
      urls.add(fooUrl);
    }
    for (int i = 0; i < 31; i++) {
      urls.add(barUrl);
    }

    Collections.shuffle(urls, random());

    try {
      int i = 0;
      for (String url : urls) {
        if (clientFoo.getBaseURL().equals(url)) {
          clientFoo.add(new SolrInputDocument("id", "" + (i++)));
        } else {
          clientBar.add(new SolrInputDocument("id", "" + (i++)));
        }
      }

      clientFoo.commit();
      clientBar.commit();

      assertEquals(17, clientFoo.query(new SolrQuery("*:*")).getResults().getNumFound());
      assertEquals(31, clientBar.query(new SolrQuery("*:*")).getResults().getNumFound());

      PoolStats stats = pool.getTotalStats();
      assertEquals("oh " + stats, 2, stats.getAvailable());
    } finally {
      for (HttpSolrClient c : new HttpSolrClient[] {clientFoo, clientBar}) {
        HttpClientUtil.close(c.getHttpClient());
        c.close();
      }
    }
  }

  public void testLBClient() throws IOException, SolrServerException {

    PoolingHttpClientConnectionManager pool = HttpClientUtil.createPoolingConnectionManager();
    final SolrClient client1;
    int threadCount = atLeast(2);
    final ExecutorService threads =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            threadCount, new SolrNamedThreadFactory(getClass().getSimpleName() + "TestScheduler"));
    CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams(), pool);
    try {
      final LBHttpSolrClient roundRobin =
          new LBHttpSolrClient.Builder()
              .withBaseSolrUrl(fooUrl)
              .withBaseSolrUrl(barUrl)
              .withHttpClient(httpClient)
              .build();

      List<ConcurrentUpdateSolrClient> concurrentClients =
          Arrays.asList(
              new ConcurrentUpdateSolrClient.Builder(fooUrl)
                  .withHttpClient(httpClient)
                  .withThreadCount(threadCount)
                  .withQueueSize(10)
                  .withExecutorService(threads)
                  .build(),
              new ConcurrentUpdateSolrClient.Builder(barUrl)
                  .withHttpClient(httpClient)
                  .withThreadCount(threadCount)
                  .withQueueSize(10)
                  .withExecutorService(threads)
                  .build());

      for (int i = 0; i < 2; i++) {
        roundRobin.deleteByQuery("*:*");
      }

      for (int i = 0; i < 57; i++) {
        final SolrInputDocument doc = new SolrInputDocument("id", "" + i);
        if (random().nextBoolean()) {
          final ConcurrentUpdateSolrClient concurrentClient =
              concurrentClients.get(random().nextInt(concurrentClients.size()));
          concurrentClient.add(doc); // here we are testing that CUSC and plain clients reuse pool
          concurrentClient.blockUntilFinished();
        } else {
          if (random().nextBoolean()) {
            roundRobin.add(doc);
          } else {
            final UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.add(doc); // here we mimic CloudSolrClient impl
            final List<String> urls = Arrays.asList(fooUrl, barUrl);
            Collections.shuffle(urls, random());
            LBSolrClient.Req req = new LBSolrClient.Req(updateRequest, urls);
            roundRobin.request(req);
          }
        }
      }

      for (int i = 0; i < 2; i++) {
        roundRobin.commit();
      }
      long total = 0;
      for (int i = 0; i < 2; i++) {
        total += roundRobin.query(new SolrQuery("*:*")).getResults().getNumFound();
      }
      assertEquals(57, total);
      PoolStats stats = pool.getTotalStats();
      // System.out.println("\n"+stats);
      assertEquals(
          "expected number of connections shouldn't exceed number of endpoints" + stats,
          2,
          stats.getAvailable());
    } finally {
      threads.shutdown();
      HttpClientUtil.close(httpClient);
    }
  }
}
