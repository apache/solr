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
package org.apache.solr.zero.cloudapicollections;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.process.ZeroStoreSolrCloudTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Tests related to Zero store based collections, i.e. collections having only replicas of type
 * {@link Type#ZERO}.
 */
public class ZeroStoreSimpleCollectionTest extends ZeroStoreSolrCloudTestCase {

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that verifies that a basic collection creation command for a "Zero" type collection
   * completes successfully if the cluster is enabled with Zero store
   */
  @Test
  public void testCreateCollection() throws Exception {
    setupCluster(3);
    String collectionName = "ZeroBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, 1, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);
    create.process(cloudClient).getResponse();

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
  }

  /**
   * Test that verifies that a collection creation command for a "Zero" type collection fails if the
   * cluster was not enabled with Zero store
   */
  @Test
  public void testCreateCollectionZeroDisabled() throws Exception {
    setupClusterZeroDisable(1);
    String collectionName = "ZeroBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, 1, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);
    try {
      create.process(cloudClient);
      fail("Request should have failed");
    } catch (SolrException ex) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(ex.getMessage().contains("Zero store is not enabled"));
    } catch (Exception ex) {
      fail("Unexpected exception thrown " + ex.getMessage());
    }
  }

  /**
   * Test that verifies that common collection api commands that create new shards will initiate the
   * metadataSuffix node in ZooKeeper
   */
  @Test
  public void testShardCreationInitiatesMetadataNode() throws Exception {
    setupCluster(1);
    String collectionName = "ZeroBasedCollectionName1";
    String shardName = "shard1";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                collectionName, null, shardName, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);
    create.process(cloudClient).getResponse();

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    // assert metadata node exists after collection creation
    String metadataNodePath = getMetadataNodePath(collectionName, "shard1");
    assertTrue(cluster.getZkStateReader().getZkClient().exists(metadataNodePath, false));

    // assert metadata node exists after shard creation
    CollectionAdminRequest.CreateShard createShard =
        CollectionAdminRequest.createShard(collectionName, "shard2");
    createShard.process(cloudClient).getResponse();
    waitForState("Timed-out wait for shard to be created", collectionName, clusterShape(2, 2));

    metadataNodePath = getMetadataNodePath(collectionName, "shard2");
    assertTrue(cluster.getZkStateReader().getZkClient().exists(metadataNodePath, false));

    // assert metadata node exists after shard split
    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collectionName).setShardName("shard1");
    splitShard.process(cloudClient).getResponse();

    metadataNodePath = getMetadataNodePath(collectionName, "shard1_0");
    assertTrue(cluster.getZkStateReader().getZkClient().exists(metadataNodePath, false));

    metadataNodePath = getMetadataNodePath(collectionName, "shard1_1");
    assertTrue(cluster.getZkStateReader().getZkClient().exists(metadataNodePath, false));
  }

  /**
   * Test that verifies that adding a NRT replica to a Zero collection fails but adding a ZERO
   * replica to a Zero collection completes successfully
   */
  @Test
  public void testAddReplica() throws Exception {
    setupCluster(3);
    String collectionName = "ZeroBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, 1, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);

    // Create the collection
    create.process(cloudClient).getResponse();
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    try {
      // Let the request fail cleanly just in case, but in reality it fails with an exception since
      // we throw a Runtime from down below
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Type.NRT)
          .process(cloudClient);
      fail();
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("Can't add a NRT replica to a collection backed by Zero store"));
    }

    // Adding a ZERO replica is expected to work ok
    assertTrue(
        CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Type.ZERO)
            .process(cloudClient)
            .isSuccess());
  }

  private String getMetadataNodePath(String collectionName, String shardName) {
    return ZkStateReader.COLLECTIONS_ZKNODE
        + "/"
        + collectionName
        + "/"
        + ZkStateReader.SHARD_LEADERS_ZKNODE
        + "/"
        + shardName
        + "/"
        + ZeroMetadataController.SUFFIX_NODE_NAME;
  }
}
