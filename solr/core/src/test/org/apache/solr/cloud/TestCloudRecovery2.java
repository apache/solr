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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloudRecovery2 extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "config", 1, 2)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        DEFAULT_TEST_COLLECTION_NAME, cluster.getZkStateReader(), false, true, 30);
  }

  @Test
  public void test() throws Exception {
    JettySolrRunner node1 = cluster.getJettySolrRunner(0);
    JettySolrRunner node2 = cluster.getJettySolrRunner(1);
    try (SolrClient client1 = getHttpSolrClient(node1.getBaseUrl().toString())) {

      node2.stop();
      waitForState(
          "", DEFAULT_TEST_COLLECTION_NAME, (liveNodes, collectionState) -> liveNodes.size() == 1);

      UpdateRequest req = new UpdateRequest();
      for (int i = 0; i < 100; i++) {
        req = req.add("id", i + "", "num", i + "");
      }
      req.commit(client1, DEFAULT_TEST_COLLECTION_NAME);

      node2.start();
      waitForState("", DEFAULT_TEST_COLLECTION_NAME, clusterShape(1, 2));

      try (SolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        long numFound =
            client
                .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "*:*", "distrib", "false"))
                .getResults()
                .getNumFound();
        assertEquals(100, numFound);
      }
      long numFound =
          client1
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "*:*", "distrib", "false"))
              .getResults()
              .getNumFound();
      assertEquals(100, numFound);

      new UpdateRequest().add("id", "1", "num", "10").commit(client1, DEFAULT_TEST_COLLECTION_NAME);

      try (SolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        Object v =
            client
                .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
                .getResults()
                .get(0)
                .get("num");
        assertEquals("10", v.toString());
      }
      Object v =
          client1
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("10", v.toString());

      //
      node2.stop();
      waitForState(
          "", DEFAULT_TEST_COLLECTION_NAME, (liveNodes, collectionState) -> liveNodes.size() == 1);

      new UpdateRequest().add("id", "1", "num", "20").commit(client1, DEFAULT_TEST_COLLECTION_NAME);
      v =
          client1
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("20", v.toString());

      node2.start();
      waitForState("", DEFAULT_TEST_COLLECTION_NAME, clusterShape(1, 2));
      try (SolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        v =
            client
                .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
                .getResults()
                .get(0)
                .get("num");
        assertEquals("20", v.toString());
      }

      node2.stop();
      waitForState(
          "", DEFAULT_TEST_COLLECTION_NAME, (liveNodes, collectionState) -> liveNodes.size() == 1);

      new UpdateRequest().add("id", "1", "num", "30").commit(client1, DEFAULT_TEST_COLLECTION_NAME);
      v =
          client1
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("30", v.toString());

      node2.start();
      waitForState("", DEFAULT_TEST_COLLECTION_NAME, clusterShape(1, 2));

      try (SolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        v =
            client
                .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
                .getResults()
                .get(0)
                .get("num");
        assertEquals("30", v.toString());
      }
      v =
          client1
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("30", v.toString());
    }

    node1.stop();
    waitForState(
        "",
        DEFAULT_TEST_COLLECTION_NAME,
        (liveNodes, collectionState) -> {
          Replica leader = collectionState.getLeader("shard1");
          return leader != null && leader.getNodeName().equals(node2.getNodeName());
        });

    node1.start();
    waitForState("", DEFAULT_TEST_COLLECTION_NAME, clusterShape(1, 2));
    try (SolrClient client = getHttpSolrClient(node1.getBaseUrl().toString())) {
      Object v =
          client
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("30", v.toString());
    }
    try (SolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
      Object v =
          client
              .query(DEFAULT_TEST_COLLECTION_NAME, new SolrQuery("q", "id:1", "distrib", "false"))
              .getResults()
              .get(0)
              .get("num");
      assertEquals("30", v.toString());
    }
  }
}
