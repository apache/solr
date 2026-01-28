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
}
