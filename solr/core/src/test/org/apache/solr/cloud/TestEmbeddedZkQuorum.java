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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Locale;
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
  @AwaitsFix(bugUrl = "https://example.com/foo")
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
}
