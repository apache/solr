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
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.zero.process.ZeroStoreSolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests related to Zero store based collections (collection having only replicas of type {@link
 * Type#ZERO}). and ensuring the expected Zero collection specific metadata exists when creating
 * collections, modifying collections, creating shards, or modifying shards for collections.
 */
public class ZeroStoreShardMetadataTest extends ZeroStoreSolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster(3);
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    shutdownCluster();
  }

  /** Creates a Zero store based collection and verify it is indeed a Zero collection */
  @Test
  public void testCreateCollection() throws Exception {
    String collectionName = "ZeroBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, 1, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);

    create.process(cloudClient);

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);

    assertCollectionIsZero(collection);
  }

  /**
   * Test that creating a shard via CREATESHARD works correctly. Note, only collections using
   * implicit routers can be issued CREATESHARD via the collections API.
   */
  @Test
  public void testCreateShardOnImplicitRouterCollection() throws Exception {
    String collectionName = "ZeroBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // create the collection w/ implicit router
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                collectionName, "conf", "shard1", 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);

    create.process(cloudClient);

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    // sending a CREATESHARD should work correctly
    CollectionAdminRequest.CreateShard createShard =
        CollectionAdminRequest.createShard(collectionName, "shard2");
    createShard.process(cloudClient);

    waitForState("Timed-out wait for createshard to finish", collectionName, clusterShape(2, 2));
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    assertCollectionIsZero(collection);
  }

  /**
   * Test that splitting a shard creates two shards correctly storing their respective ZeroStoreName
   */
  @Test
  public void testSplitShard() throws Exception {
    String collectionName = "ZeroBasedCollectionName3";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, 1, 0)
            .setZeroIndex(true)
            .setZeroReplicas(1);

    create.process(cloudClient);

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(
        cluster
            .getZkStateReader()
            .getZkClient()
            .exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    // sending a split shard command should generate two createshard commands that create two new
    // sub-shards in zookeeper
    CollectionAdminRequest.SplitShard split =
        CollectionAdminRequest.splitShard(collectionName).setShardName("shard1");
    split.process(cloudClient);

    // we haven't deleted the old shard so we should expect 3 total shards and 2 active ones
    waitForState("Timed-out wait for split to finish", collectionName, activeClusterShape(2, 2));
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(2, collection.getActiveSlices().size());
    assertEquals(3, collection.getSlices().size());

    // single shard sanity check; <shardname>_<counter> is the nomenclature for sub shards
    Slice slice = collection.getActiveSlicesMap().get("shard1_1");
    assertNotNull(slice);

    // check them all
    assertCollectionIsZero(collection);
  }

  private void assertCollectionIsZero(DocCollection collection) {
    // verify that we've stored in ZooKeeper the zeroIndex field indicating the collection is a
    // Zero store based one
    assertTrue(
        "Zero collection was created but field "
            + ZkStateReader.ZERO_INDEX
            + " was missing from ZooKeeper metadata or not set",
        collection.isZeroIndex());
  }
}
