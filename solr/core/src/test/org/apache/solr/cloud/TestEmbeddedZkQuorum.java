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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test embedded ZooKeeper running in quorum mode within Solr nodes.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Multiple Solr nodes can start with embedded ZK in quorum mode
 *   <li>The ZK quorum forms correctly
 *   <li>Collections can be created and used
 *   <li>Documents can be indexed and queried
 *   <li>All resources are properly closed on shutdown
 * </ul>
 */
@SolrTestCaseJ4.SuppressSSL
public class TestEmbeddedZkQuorum extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "test_quorum_collection";
  private static final int NUM_NODES = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Disable ZooKeeper JMX to avoid MBean registration conflicts during beasting
    System.setProperty("zookeeper.jmx.log4j.disable", "true");

    // Get path to a test config
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");

    // Configure cluster with 3 nodes, each running embedded ZK
    cluster =
        configureCluster(NUM_NODES).addConfig("conf1", configPath).withEmbeddedZkQuorum().build();
    cluster.waitForAllNodes(60);
  }

  @Test
  public void testBasicQuorumFunctionality()
      throws IOException, InterruptedException, TimeoutException {
    for (int i = 0; i < NUM_NODES; i++) {
      JettySolrRunner node = cluster.getJettySolrRunner(i);
      assertTrue("Node " + i + " should be running", node.isRunning());
      assertNotNull("Node " + i + " should have a NodeName", node.getNodeName());
    }
  }

  @Test
  public void testCollectionIndexing() throws Exception {
    try (CloudSolrClient client = cluster.getSolrClient(COLLECTION_NAME)) {
      CollectionAdminRequest.Create createCmd =
          CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 1, 3);
      createCmd.process(client);
      cluster.waitForActiveCollection(COLLECTION_NAME, 1, 3);

      // Index some documents
      for (int i = 0; i < 10; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("title_s", "Test Document " + i);
        doc.addField("content_t", "This is test content for document " + i);
        client.add(doc);
      }
      client.commit();

      // Query the documents
      SolrQuery query = new SolrQuery("*:*");
      query.setRows(100);
      QueryResponse response = client.query(query);
      SolrDocumentList results = response.getResults();

      // Verify results
      assertEquals("Should have 10 documents", 10, results.getNumFound());

      CollectionAdminRequest.Delete deleteCmd =
          CollectionAdminRequest.deleteCollection(COLLECTION_NAME);
      deleteCmd.process(client);
    }
  }

  /**
   * Tests ZK quorum resilience when a single node fails and recovers.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>A 3-node ZK quorum can lose 1 node and maintain quorum (2/3)
   *   <li>The cluster continues to accept writes with 2 nodes
   *   <li>A failed node can rejoin the quorum using the same ports
   *   <li>All data is preserved after node recovery
   * </ul>
   *
   * <p>This test creates its own private cluster to avoid interfering with other tests.
   */
  @Test
  public void testQuorumResilienceWithNodeFailure() throws Exception {
    final String collectionName = "quorum_resilience";
    final int initialDocs = 5;
    final int docsWhileDown = 5;
    final int docsAfterRecovery = 5;

    // Create a private cluster for this test
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");
    MiniSolrCloudCluster privateCluster =
        configureCluster(NUM_NODES).addConfig("conf1", configPath).withEmbeddedZkQuorum().build();

    try {
      privateCluster.waitForAllNodes(60);

      // Create collection with replica on each node
      CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 3)
          .process(privateCluster.getSolrClient());
      privateCluster.waitForActiveCollection(collectionName, 1, 3);

      try (CloudSolrClient client = privateCluster.getSolrClient(collectionName)) {
        // Index initial documents and verify
        indexDocuments(client, 0, initialDocs, "initial");
        privateCluster.waitForDocCount(
            collectionName, initialDocs, "initial documents", 120, TimeUnit.SECONDS);

        // Stop one node (quorum maintained with 2/3 nodes)
        JettySolrRunner stoppedNode = privateCluster.getJettySolrRunner(2);
        String stoppedNodeName = stoppedNode.getNodeName();
        if (log.isInfoEnabled()) {
          log.info("Stopping node to test quorum resilience: {}", stoppedNodeName);
        }
        privateCluster.stopJettySolrRunner(stoppedNode);

        // Wait for ZK to detect node loss and verify cluster still operational
        privateCluster.waitForLiveNodes(2, 120);
        indexDocuments(client, initialDocs, docsWhileDown, "during_failure");
        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown,
            "documents while node down",
            120,
            TimeUnit.SECONDS);
        if (log.isInfoEnabled()) {
          log.info("Starting node {} again and testing functionality", stoppedNodeName);
        }

        privateCluster.startJettySolrRunner(stoppedNode, true);
        privateCluster.waitForNode(stoppedNode, 120);

        // Wait for cluster to stabilize and verify all nodes running
        privateCluster.waitForLiveNodes(3, 120);

        // CRITICAL: Wait for collection to become active (replicas up, leader elected)
        // before attempting to index documents
        privateCluster.waitForActiveCollection(collectionName, 120, TimeUnit.SECONDS, 1, 3);

        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown,
            "documents after recovery",
            120,
            TimeUnit.SECONDS);

        // Verify full cluster functionality by adding more documents
        indexDocuments(client, initialDocs + docsWhileDown, docsAfterRecovery, "after_recovery");
        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown + docsAfterRecovery,
            "all documents",
            120,
            TimeUnit.SECONDS);
      }
    } finally {
      CollectionAdminRequest.deleteCollection(collectionName)
          .process(privateCluster.getSolrClient());
      privateCluster.shutdown();
    }
  }

  /**
   * Tests ZK quorum resilience when the node that fails happens to be the current ZK
   * ensemble LEADER, as opposed to an arbitrary follower (which {@link
   * #testQuorumResilienceWithNodeFailure} already covers). Losing the leader forces a real
   * leader election among the 2 survivors, not just a membership-count change.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>A new ZK leader is elected among the 2 surviving nodes
   *   <li>The cluster continues to accept writes with 2 nodes while the former leader is down
   *   <li>The former leader can rejoin the ensemble under its original identity (exercising the
   *       same "reconnect as an already-configured member" path tested by {@link
   *       #testQuorumResilienceWithNodeFailure}, but now rejoining an ensemble it used to lead)
   * </ul>
   *
   * <p>This test creates its own private cluster to avoid interfering with other tests.
   */
  @Test
  public void testZkLeaderFailover() throws Exception {
    final String collectionName = "zk_leader_failover";
    final int initialDocs = 5;
    final int docsWhileDown = 5;
    final int docsAfterRecovery = 5;

    // Create a private cluster for this test
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");
    MiniSolrCloudCluster privateCluster =
        configureCluster(NUM_NODES).addConfig("conf1", configPath).withEmbeddedZkQuorum().build();

    try {
      privateCluster.waitForAllNodes(60);

      CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 3)
          .process(privateCluster.getSolrClient());
      privateCluster.waitForActiveCollection(collectionName, 1, 3);

      try (CloudSolrClient client = privateCluster.getSolrClient(collectionName)) {
        indexDocuments(client, 0, initialDocs, "initial");
        privateCluster.waitForDocCount(
            collectionName, initialDocs, "initial documents", 120, TimeUnit.SECONDS);

        List<JettySolrRunner> jettys = privateCluster.getJettySolrRunners();
        int leaderIndex = findZkLeaderIndex(jettys, 30);
        assertTrue("Could not find a ZK leader among the 3 nodes", leaderIndex >= 0);
        JettySolrRunner leaderJetty = jettys.get(leaderIndex);
        String leaderNodeName = leaderJetty.getNodeName();
        if (log.isInfoEnabled()) {
          log.info("Stopping the current ZK leader ({}) to test leader failover", leaderNodeName);
        }

        privateCluster.stopJettySolrRunner(leaderJetty);

        // The 2 survivors must elect a NEW leader among themselves to stay serving.
        List<JettySolrRunner> survivors = new ArrayList<>();
        for (JettySolrRunner jetty : jettys) {
          if (jetty != leaderJetty) {
            survivors.add(jetty);
          }
        }
        int newLeaderIndex = findZkLeaderIndex(survivors, 60);
        assertTrue(
            "No new ZK leader was elected among the 2 surviving nodes after the old leader was"
                + " stopped",
            newLeaderIndex >= 0);

        privateCluster.waitForLiveNodes(2, 120);
        indexDocuments(client, initialDocs, docsWhileDown, "during_failover");
        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown,
            "documents while leader down",
            120,
            TimeUnit.SECONDS);

        if (log.isInfoEnabled()) {
          log.info("Restarting the former leader {} and verifying it rejoins", leaderNodeName);
        }
        privateCluster.startJettySolrRunner(leaderJetty, true);
        privateCluster.waitForNode(leaderJetty, 120);
        privateCluster.waitForLiveNodes(3, 120);
        privateCluster.waitForActiveCollection(collectionName, 120, TimeUnit.SECONDS, 1, 3);

        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown,
            "documents after former leader rejoins",
            120,
            TimeUnit.SECONDS);

        indexDocuments(client, initialDocs + docsWhileDown, docsAfterRecovery, "after_recovery");
        privateCluster.waitForDocCount(
            collectionName,
            initialDocs + docsWhileDown + docsAfterRecovery,
            "all documents",
            120,
            TimeUnit.SECONDS);
      }
    } finally {
      CollectionAdminRequest.deleteCollection(collectionName)
          .process(privateCluster.getSolrClient());
      privateCluster.shutdown();
    }
  }

  /**
   * Stress-tests the race between a node deciding to cold-bootstrap alongside its peers versus
   * joining an already-running ensemble (see {@code ZkContainer#tryJoinExistingEnsemble}): every
   * node in a fresh quorum starts concurrently, and under unlucky timing one node could mistake
   * a sibling's barely-open port for an already-serving ensemble to join, instead of
   * bootstrapping together with it -- which would fragment the 3 nodes across two different
   * ensemble views instead of one.
   *
   * <p>Repeats a full cold start from scratch many times (rather than relying on any single run's
   * scheduling luck) and asserts every single one converges to exactly one 3-member quorum: a
   * leader with 2 synced followers, never a node stuck alone or a split ensemble.
   */
  @Test
  public void testRepeatedColdBootstrapConvergesConsistently() throws Exception {
    final int iterations = 8;
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");

    for (int iter = 0; iter < iterations; iter++) {
      MiniSolrCloudCluster privateCluster =
          configureCluster(NUM_NODES)
              .addConfig("conf1", configPath)
              .withEmbeddedZkQuorum()
              .build();
      try {
        privateCluster.waitForAllNodes(60);

        List<JettySolrRunner> jettys = privateCluster.getJettySolrRunners();
        assertEquals(
            "iteration " + iter + ": expected " + NUM_NODES + " nodes",
            NUM_NODES,
            jettys.size());

        int leaderIndex = findZkLeaderIndex(jettys, 30);
        assertTrue(
            "iteration "
                + iter
                + ": no ZK leader found among "
                + NUM_NODES
                + " freshly cold-started nodes",
            leaderIndex >= 0);

        int syncedFollowers = zkSyncedFollowerCount(jettys.get(leaderIndex).getLocalPort() + 1000);
        assertEquals(
            "iteration "
                + iter
                + ": leader should have "
                + (NUM_NODES - 1)
                + " synced followers in a single "
                + NUM_NODES
                + "-member quorum, not a split ensemble or a node left stuck alone",
            NUM_NODES - 1,
            syncedFollowers);
      } finally {
        privateCluster.shutdown();
      }
    }
  }

  /**
   * Tests ZK quorum loss and recovery when majority of nodes fail.
   *
   * <p>This test verifies that:
   *
   * <ul>
   *   <li>A 3-node ZK quorum loses quorum when 2 nodes are down (1/3 remaining)
   *   <li>The surviving node maintains its replica but cannot process updates without quorum
   *   <li>Both failed nodes can be restarted to restore quorum
   *   <li>The cluster becomes operational again (can query and index documents)
   *   <li>Note: After catastrophic failure, some replicas may need time or manual intervention to
   *       fully recover
   * </ul>
   *
   * <p>This test creates its own private cluster to avoid interfering with other tests. <b>Hard to
   * make this test pass</b>
   */
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18094")
  @Test
  public void testQuorumLossAndRecovery() throws Exception {
    final String collectionName = "quorum_loss";

    // Create a private cluster for this test
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");
    MiniSolrCloudCluster privateCluster =
        configureCluster(NUM_NODES).addConfig("conf1", configPath).withEmbeddedZkQuorum().build();

    try {
      privateCluster.waitForAllNodes(60);

      // Create collection with 3 replicas (one on each node) to ensure at least
      // one replica survives when we stop 2 nodes
      CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 3)
          .process(privateCluster.getSolrClient());
      privateCluster.waitForActiveCollection(collectionName, 1, 3);

      try (CloudSolrClient client = privateCluster.getSolrClient(collectionName)) {
        indexDocuments(client, 0, 1, "before_loss");
        privateCluster.waitForDocCount(
            collectionName, 1, "initial document", 120, TimeUnit.SECONDS);

        // Stop 2 out of 3 nodes to lose quorum
        JettySolrRunner node1 = privateCluster.getJettySolrRunner(1);
        JettySolrRunner node2 = privateCluster.getJettySolrRunner(2);
        String node1Name = node1.getNodeName();
        String node2Name = node2.getNodeName();

        if (log.isInfoEnabled()) {
          log.info("Stopping 2 nodes to lose quorum: {}, {}", node1Name, node2Name);
        }
        privateCluster.stopJettySolrRunner(node1);
        privateCluster.stopJettySolrRunner(node2);

        // Wait for ZK to detect quorum loss
        privateCluster.waitForLiveNodes(1, 120);

        // Restart both nodes to restore quorum
        if (log.isInfoEnabled()) {
          log.info("Restarting nodes to restore quorum");
        }
        privateCluster.startJettySolrRunner(node1, true);
        privateCluster.startJettySolrRunner(node2, true);

        // Wait for both nodes to register with ZK (they should appear in live_nodes)
        // but we don't require them to be fully recovered immediately
        privateCluster.waitForNode(node1, 120);
        privateCluster.waitForNode(node2, 120);
        privateCluster.waitForLiveNodes(3, 120);

        // CRITICAL: Wait for collection to become active (replicas up, leader elected)
        // After catastrophic failure, we need to ensure at least one replica is active
        // before attempting operations
        privateCluster.waitForActiveCollection(collectionName, 120, TimeUnit.SECONDS, 1, 1);

        // After catastrophic failure, the cluster should be operational with quorum restored
        // even if not all replicas are immediately active
        try {
          privateCluster.waitForDocCount(
              collectionName, 1, "document after recovery", 120, TimeUnit.SECONDS);

          // Verify cluster accepts writes
          indexDocuments(client, 1, 1, "after_recovery");
          privateCluster.waitForDocCount(
              collectionName, 2, "all documents after recovery", 120, TimeUnit.SECONDS);

        } catch (Exception e) {
          if (log.isErrorEnabled()) {
            log.error("Cluster failed to become operational after quorum restoration");
          }
          throw e;
        }
      }
    } finally {
      // Clean up collection and cluster
      CollectionAdminRequest.deleteCollection(collectionName)
          .process(privateCluster.getSolrClient());
      privateCluster.shutdown();
    }
  }

  // Helper methods for improved test clarity and reusability

  /**
   * Index a batch of documents with a specific phase tag.
   *
   * @param client the CloudSolrClient to use
   * @param startId starting document ID
   * @param count number of documents to index
   * @param phase phase tag to add to documents
   */
  private void indexDocuments(CloudSolrClient client, int startId, int count, String phase)
      throws Exception {
    for (int i = 0; i < count; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", startId + i);
      doc.addField("phase_s", phase);
      doc.addField(
          "content_t", String.format(Locale.ROOT, "Document %d in phase %s", startId + i, phase));
      client.add(doc);
    }
    client.commit();
  }

  /**
   * Finds the index (into {@code jettys}) of whichever node is currently the embedded ZK
   * ensemble leader, by probing each running node's ZK client port with the "mntr" four-letter
   * word. Polls up to {@code timeoutSeconds} since a leader may still be mid-election (e.g. right
   * after a prior leader was stopped).
   *
   * @return the leader's index, or -1 if no leader is found within the timeout
   */
  private static int findZkLeaderIndex(List<JettySolrRunner> jettys, int timeoutSeconds)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (System.nanoTime() < deadlineNanos) {
      for (int i = 0; i < jettys.size(); i++) {
        JettySolrRunner jetty = jettys.get(i);
        if (!jetty.isRunning()) {
          continue;
        }
        // Mirrors MiniSolrCloudCluster's own embedded-ZK-quorum constructor, which always uses
        // the default client-port offset (Solr port + 1000) when reserving ports for each node.
        if ("leader".equals(zkServerState(jetty.getLocalPort() + 1000))) {
          return i;
        }
      }
      Thread.sleep(200);
    }
    return -1;
  }

  /**
   * Issues the ZK "mntr" four-letter word against {@code zkPort} and returns the value of
   * zk_server_state, or {@code null} if the port isn't reachable/serving yet.
   */
  private static String zkServerState(int zkPort) {
    return mntr(zkPort).get("zk_server_state");
  }

  /**
   * Issues the ZK "mntr" four-letter word against {@code zkPort} and returns the value of
   * zk_synced_followers (only meaningful on the leader), or -1 if absent/unparseable.
   */
  private static int zkSyncedFollowerCount(int zkPort) {
    String value = mntr(zkPort).get("zk_synced_followers");
    if (value == null) {
      return -1;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Issues the ZK "mntr" four-letter word against {@code zkPort} and returns its key/value pairs,
   * or an empty map if the port isn't reachable/serving yet.
   */
  private static Map<String, String> mntr(int zkPort) {
    Map<String, String> values = new HashMap<>();
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", zkPort), 1000);
      socket.getOutputStream().write("mntr\n".getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          int tab = line.indexOf('\t');
          if (tab > 0) {
            values.put(line.substring(0, tab), line.substring(tab + 1).trim());
          }
        }
      }
    } catch (IOException e) {
      // Not reachable / not serving yet -- treated the same as "no answer" by the caller.
    }
    return values;
  }
}
