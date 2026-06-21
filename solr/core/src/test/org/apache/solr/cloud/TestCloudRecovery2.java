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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.Replica;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.SuppressCodecs({"MockRandom", "Direct", "SimpleText"})
@LuceneTestCase.Nightly
public class TestCloudRecovery2 extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void beforeTestCloudRecovery2() throws Exception {
    useFactory(null);
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(3)
        .addConfig("config", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 3);

    // 2 replicas will not ensure we don't lose an update here, so 3
    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1,3)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());
  }

  @AfterClass
  public static void afterTestCloudRecovery2() throws Exception {
    shutdownCluster();
  }

  @Test
  public void test() throws Exception {
    JettySolrRunner node1 = cluster.getJettySolrRunner(0);
    JettySolrRunner node2 = cluster.getJettySolrRunner(1);

    cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 10, TimeUnit.SECONDS, false, 1, 3, true, true);


    try (Http2SolrClient client1 = SolrTestCaseJ4.getHttpSolrClient(node1.getBaseUrl())) {

      node2.stop().await(15, TimeUnit.SECONDS);

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 10, TimeUnit.SECONDS, false, 1, 2, true, true);

      UpdateRequest req = new UpdateRequest();
      for (int i = 0; i < 100; i++) {
        req = req.add("id", i+"", "num", i+"");
      }

      try {
        req.commit(client1, COLLECTION);
      } catch (BaseHttpSolrClient.RemoteSolrException e) {
        Thread.sleep(250);
        try {
          req.commit(client1, COLLECTION);
        } catch (BaseHttpSolrClient.RemoteSolrException e2) {
          Thread.sleep(500);
        }
      }

      node2.start().await(15, TimeUnit.SECONDS);

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 3);

      log.info("wait for active collection before query");
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 15, TimeUnit.SECONDS, false, 1, 3, true, true);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
        long numFound = client.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
        assertEquals(100, numFound);
      }
      long numFound = client1.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
      assertEquals(100, numFound);

      new UpdateRequest().add("id", "1", "num", "10")
          .commit(client1, COLLECTION);

      // can be stale (eventually consistent) but should catch up
      for (int i = 0; i < 20; i++) {
        try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
          Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "true")).getResults().get(0).get("num");
          try {
            assertEquals("10  i="+ i, "10", v.toString());
            break;
          } catch (AssertionError error) {
            if (i == 29) {
              throw error;
            }
            Thread.sleep(100);
          }
        }
      }

      Object v = client1.query(COLLECTION, new SolrQuery("q", "id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("10", v.toString());

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
        client.commit(COLLECTION, true, true);
      }

      // can be stale (eventually consistent) but should catch up
      for (int i = 0; i < 30; i ++) {
        try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
          v = client.query(COLLECTION, new SolrQuery("q", "id:1", "distrib", "true")).getResults().get(0).get("num");
          try {
            assertEquals("node requested=" + node2.getBaseUrl() + " 10  i="+ i, "10", v.toString());
            break;
          } catch (AssertionError error) {
            if (i == 29) {
              throw error;
            }
            Thread.sleep(250);
          }
        }
      }

      //
      node2.stop().await(5, TimeUnit.SECONDS);


      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);

      log.info("wait for active collection before query");
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 10, TimeUnit.SECONDS, false, 1, 2, true, true);

      new UpdateRequest().add("id", "1", "num", "20")
          .commit(client1, COLLECTION);

      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("20", v.toString());

      node2.start().await(5, TimeUnit.SECONDS);

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 3);
      log.info("wait for active collection before query");
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 10, TimeUnit.SECONDS, false, 1, 3, true, true);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
        client.commit(COLLECTION, true, true);
      }

      // can be stale (eventually consistent) but should catch up
      for (int i = 0; i < 30; i ++) {
        try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
          v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
          try {
            assertEquals("20", v.toString());
            break;
          } catch (AssertionError error) {
            if (i == 29) {
              throw error;
            }
            Thread.sleep(200);
            try (Http2SolrClient client2 = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
              client2.commit(COLLECTION, true, true);
            }
          }
        }
      }

      node2.stop().await(5, TimeUnit.SECONDS);

     // cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);
      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);
      log.info("wait for active collection before query");
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 10, TimeUnit.SECONDS, false, 1, 2, true, false);

      new UpdateRequest().add("id", "1", "num", "30")
          .commit(client1, COLLECTION);

      for (int i = 0; i < 30; i ++) {
        try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node1.getBaseUrl())) {
          try {
            v = client.query(COLLECTION, new SolrQuery("q", "id:1", "distrib", "false")).getResults().get(0).get("num");
          } catch (Exception e) {
            if (i == 29) {
              throw e;
            }
            Thread.sleep(250);
            continue;
          }

          try {
            SolrTestCaseJ4.assertEquals("30", v.toString());
            break;
          } catch (AssertionError error) {
            if (i == 29) {
              throw error;
            }
            Thread.sleep(250);
          }
        }
      }


      node2.start().await(5, TimeUnit.SECONDS);

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 3);

      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 5, TimeUnit.SECONDS, false, 1, 3, true, false);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
        client.commit(COLLECTION, true, true);
      }

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("30", v.toString());
      }
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }
    Replica oldLeader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(), COLLECTION,"s1", 5000, false);

    node1.stop().await(5, TimeUnit.SECONDS);

    cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);

   // cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 5, TimeUnit.SECONDS, false, 1, 2, true, false);

    AtomicReference<Replica> newLeader = new AtomicReference<>(
        cluster.getSolrClient().getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(), COLLECTION, "s1", 5000, true));

    if (oldLeader.getNodeName().equals(node1.getNodeName())) {
      waitForState("", COLLECTION, (liveNodes, collectionState) -> {
        try {
          newLeader.set(cluster.getSolrClient().getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(), COLLECTION, "s1", 5000, false));
        } catch (Exception e) {
          log.error("failed", e);
        }
        return newLeader.get() != null && !newLeader.get().getName().equals(oldLeader.getName());
      });
    }

    node1.start().await(5, TimeUnit.SECONDS);

    cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 3);

    cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), COLLECTION, 30, TimeUnit.SECONDS, false, 1, 3, true, false);

    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node1.getBaseUrl())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }

  }

}
