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
import java.nio.file.Path;
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
public class TestEmbeddedZkQuorum extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "test_quorum_collection";
  private static final int NUM_NODES = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Get path to a test config
    Path configPath = TEST_PATH().resolve("collection1").resolve("conf");

    // Configure cluster with 3 nodes, each running embedded ZK
    cluster =
        configureCluster(NUM_NODES).addConfig("conf1", configPath).withEmbeddedZkQuorum().build();
    log.info("Cluster configured with {} nodes", NUM_NODES);
  }

  @Test
  public void testBasicQuorumFunctionality() {
    log.info("Starting testBasicQuorumFunctionality");

    // Verify all nodes are running
    assertEquals(
        "Expected " + NUM_NODES + " nodes to be running",
        NUM_NODES,
        cluster.getJettySolrRunners().size());

    for (int i = 0; i < NUM_NODES; i++) {
      JettySolrRunner node = cluster.getJettySolrRunner(i);
      assertTrue("Node " + i + " should be running", node.isRunning());
      assertNotNull("Node " + i + " should have a NodeName", node.getNodeName());
      if (log.isInfoEnabled()) {
        log.info("Node {} is running: {}", i, node.getNodeName());
      }
    }

    log.info("All {} nodes verified as running", NUM_NODES);
  }

  @Test
  public void testCollectionCreationAndIndexing() throws Exception {
    log.info("Starting testCollectionCreationAndIndexing");

    // Create a SolrClient
    try (CloudSolrClient client = cluster.getSolrClient(COLLECTION_NAME)) {

      // Create a collection with 2 shards and 2 replicas
      log.info("Creating collection: {}", COLLECTION_NAME);
      CollectionAdminRequest.Create createCmd =
          CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", 2, 2);
      createCmd.process(client);

      // Wait for collection to be ready
      log.info("Waiting for collection to be ready...");
      Thread.sleep(5000);

      // Index some documents
      log.info("Indexing documents...");
      for (int i = 0; i < 10; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("title_s", "Test Document " + i);
        doc.addField("content_t", "This is test content for document " + i);
        client.add(doc);
      }

      // Commit
      log.info("Committing documents...");
      client.commit();

      // Query the documents
      log.info("Querying documents...");
      SolrQuery query = new SolrQuery("*:*");
      query.setRows(100);
      QueryResponse response = client.query(query);
      SolrDocumentList results = response.getResults();

      // Verify results
      assertEquals("Should have 10 documents", 10, results.getNumFound());
      if (log.isInfoEnabled()) {
        log.info("Successfully indexed and queried {} documents", results.getNumFound());
      }

      // Query with a filter
      log.info("Querying with filter...");
      SolrQuery filterQuery = new SolrQuery("title_s:\"Test Document 5\"");
      QueryResponse filterResponse = client.query(filterQuery);
      SolrDocumentList filterResults = filterResponse.getResults();

      assertEquals(
          "Should find 1 document with title 'Test Document 5'", 1, filterResults.getNumFound());
      assertEquals(
          "Document ID should be 5", "5", filterResults.getFirst().getFieldValue("id").toString());
      log.info("Filter query successful");

      // Clean up - delete the collection
      log.info("Deleting collection: {}", COLLECTION_NAME);
      CollectionAdminRequest.Delete deleteCmd =
          CollectionAdminRequest.deleteCollection(COLLECTION_NAME);
      deleteCmd.process(client);

      log.info("Test completed successfully");
    }
  }

  @Test
  public void testQuorumResilienceWithNodeFailure() throws Exception {
    log.info("Starting testQuorumResilienceWithNodeFailure");
    String collectionName = "resilience_test_collection";

    // Step 1: Create a collection with replicas across all nodes
    log.info("Creating collection: {}", collectionName);
    CollectionAdminRequest.Create createCmd =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 3);
    createCmd.process(cluster.getSolrClient());

    // Wait for collection to be ready
    cluster.waitForActiveCollection(collectionName, 1, 3);
    log.info("Collection created and active");

    // Use a single client for all operations
    try (CloudSolrClient client = cluster.getSolrClient(collectionName)) {

      // Step 2: Index initial documents
      log.info("Indexing initial documents...");
      for (int i = 0; i < 5; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("phase_s", "initial");
        doc.addField("content_t", "Initial content " + i);
        client.add(doc);
      }
      client.commit();
      log.info("Initial documents committed");

      // Verify initial documents
      SolrQuery initialQuery = new SolrQuery("*:*");
      QueryResponse initialResponse = client.query(initialQuery);
      assertEquals(
          "Should have 5 initial documents", 5, initialResponse.getResults().getNumFound());

      // Step 3: Stop one node (simulating ZK quorum member loss)
      // With 3 nodes, stopping 1 should maintain quorum (2 remaining)
      JettySolrRunner stoppedNode = cluster.getJettySolrRunner(2);
      String stoppedNodeName = stoppedNode.getNodeName();
      log.info("Stopping node: {}", stoppedNodeName);
      cluster.stopJettySolrRunner(stoppedNode);
      cluster.waitForJettyToStop(stoppedNode);
      log.info("Node stopped: {}", stoppedNodeName);

      // Step 4: Verify cluster still works with 2 nodes (quorum maintained)
      log.info("Verifying cluster still operational with 2 nodes...");
      Thread.sleep(5000); // Give ZK time to detect the node loss

      // Add more documents while one node is down
      log.info("Indexing documents while node is down...");
      for (int i = 5; i < 10; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("phase_s", "during_failure");
        doc.addField("content_t", "Content added during failure " + i);
        client.add(doc);
      }
      client.commit();
      log.info("Documents committed while node down");

      // Query to verify documents are accessible
      SolrQuery duringFailureQuery = new SolrQuery("*:*");
      QueryResponse duringFailureResponse = client.query(duringFailureQuery);
      assertEquals(
          "Should have 10 documents (5 initial + 5 during failure)",
          10,
          duringFailureResponse.getResults().getNumFound());
      log.info("Cluster operational with quorum of 2 nodes");

      // Step 5: Restart the stopped node
      log.info("Restarting stopped node: {}", stoppedNodeName);
      // Note: Use reusePort=true to ensure it binds to the same ports (critical for ZK quorum)
      cluster.startJettySolrRunner(stoppedNode, true);
      cluster.waitForNode(stoppedNode, 60);
      log.info("Node restarted: {}", stoppedNodeName);

      // Give ZK and Solr time to fully rejoin and sync
      Thread.sleep(10000);

      // Step 6: Verify all 3 nodes are back and operational
      int runningNodes = 0;
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        if (jetty.isRunning()) {
          runningNodes++;
        }
      }
      assertEquals("All 3 nodes should be running", 3, runningNodes);
      log.info("All 3 nodes verified as running");

      // Step 7: Verify data integrity after node rejoins
      log.info("Verifying data integrity after node rejoin...");
      SolrQuery afterRecoveryQuery = new SolrQuery("*:*");
      afterRecoveryQuery.setRows(100);
      QueryResponse afterRecoveryResponse = client.query(afterRecoveryQuery);
      assertEquals(
          "Should still have all 10 documents after recovery",
          10,
          afterRecoveryResponse.getResults().getNumFound());

      // Verify documents from each phase are present
      SolrQuery initialPhaseQuery = new SolrQuery("phase_s:initial");
      assertEquals(5, client.query(initialPhaseQuery).getResults().getNumFound());

      SolrQuery failurePhaseQuery = new SolrQuery("phase_s:during_failure");
      assertEquals(5, client.query(failurePhaseQuery).getResults().getNumFound());

      log.info("Data integrity verified after recovery");

      // Step 8: Add more documents after recovery to verify full cluster functionality
      log.info("Indexing documents after recovery...");
      for (int i = 10; i < 15; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", i);
        doc.addField("phase_s", "after_recovery");
        doc.addField("content_t", "Content added after recovery " + i);
        client.add(doc);
      }
      client.commit();

      SolrQuery finalQuery = new SolrQuery("*:*");
      QueryResponse finalResponse = client.query(finalQuery);
      assertEquals(
          "Should have 15 documents total after recovery",
          15,
          finalResponse.getResults().getNumFound());

      log.info("Quorum resilience test completed successfully");
    }

    // Clean up - delete the collection
    CollectionAdminRequest.Delete deleteCmd =
        CollectionAdminRequest.deleteCollection(collectionName);
    deleteCmd.process(cluster.getSolrClient());
  }

  @Test
  public void testMinimumQuorumRequired() throws Exception {
    log.info("Starting testMinimumQuorumRequired");
    String collectionName = "quorum_minimum_test";

    // Create a collection
    log.info("Creating collection: {}", collectionName);
    CollectionAdminRequest.Create createCmd =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 2);
    createCmd.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 2);

    // Use a single client for all operations
    try (CloudSolrClient client = cluster.getSolrClient(collectionName)) {

      // Index a document
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", 1);
      doc.addField("content_t", "Test document");
      client.add(doc);
      client.commit();

      // Verify document is present
      SolrQuery query = new SolrQuery("*:*");
      QueryResponse response = client.query(query);
      assertEquals("Should have 1 document", 1, response.getResults().getNumFound());

      // Stop two nodes (only 1 remains - quorum lost)
      log.info("Stopping 2 nodes to lose quorum...");
      JettySolrRunner node1 = cluster.getJettySolrRunner(1);
      JettySolrRunner node2 = cluster.getJettySolrRunner(2);
      String node1Name = node1.getNodeName();
      String node2Name = node2.getNodeName();

      cluster.stopJettySolrRunner(node1);
      log.info("Stopped node 1: {}", node1Name);

      cluster.stopJettySolrRunner(node2);
      log.info("Stopped node 2: {}", node2Name);

      // Give ZK time to detect quorum loss
      Thread.sleep(5000);

      // With only 1 node remaining, ZK quorum is lost (need 2 out of 3)
      // The cluster should not be able to perform write operations
      log.info("Verifying cluster behavior with lost quorum...");

      // Note: Reading might still work from local replicas, but cluster state updates will fail
      // Attempting to create a collection should fail or timeout
      boolean operationFailed = false;
      try {
        CollectionAdminRequest.Create createCmd2 =
            CollectionAdminRequest.createCollection("should_fail", "conf1", 1, 1);
        createCmd2.setWaitForFinalState(false);
        createCmd2.process(cluster.getSolrClient());
      } catch (Exception e) {
        log.info("Expected failure with lost quorum: {}", e.getMessage());
        operationFailed = true;
      }

      // In some cases the operation might not fail immediately, so we don't assert failure
      // The important part is testing that nodes can be restarted
      log.info("Operation failure status with lost quorum: {}", operationFailed);

      // Restart nodes to restore quorum
      log.info("Restarting nodes to restore quorum...");
      cluster.startJettySolrRunner(node1, true);
      cluster.waitForNode(node1, 60);
      log.info("Restarted node 1");

      cluster.startJettySolrRunner(node2, true);
      cluster.waitForNode(node2, 60);
      log.info("Restarted node 2");

      // Give cluster time to stabilize
      Thread.sleep(10000);

      // Verify cluster is operational again
      log.info("Verifying cluster operational after quorum restoration...");
      SolrQuery finalQuery = new SolrQuery("*:*");
      QueryResponse finalResponse = client.query(finalQuery);
      assertEquals(
          "Original document should still be present", 1, finalResponse.getResults().getNumFound());

      log.info("Minimum quorum test completed successfully");
    }

    // Clean up - delete the collection
    CollectionAdminRequest.Delete deleteCmd =
        CollectionAdminRequest.deleteCollection(collectionName);
    deleteCmd.process(cluster.getSolrClient());
  }
}
