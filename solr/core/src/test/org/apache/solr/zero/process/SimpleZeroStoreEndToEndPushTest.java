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
package org.apache.solr.zero.process;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.junit.After;
import org.junit.Test;

/** A simple end-to-end push tests for collections using a Zero store */
public class SimpleZeroStoreEndToEndPushTest extends ZeroStoreSolrCloudTestCase {

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Verify that doing a single update to a Zero collection with commit=true pushes to the Zero
   * store.
   */
  @Test
  public void testUpdatePushesToZeroWithCommit() throws Exception {
    testUpdatePushesToZero(true);
  }

  /**
   * Verify that doing a single update to a Zero collection even without commit pushes to the Zero
   * store. This will work because Solr will implicitly add commit=true for a Zero collection
   */
  @Test
  public void testUpdatePushesToZeroWithoutCommit() throws Exception {
    testUpdatePushesToZero(false);
  }

  public void testUpdatePushesToZero(boolean withCommit) throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // send an update to the cluster
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    if (withCommit) {
      updateReq.commit(cloudClient, collectionName);
    } else {
      updateReq.process(cloudClient, collectionName);
    }

    // verify we can find the document
    QueryRequest queryReq = new QueryRequest(new SolrQuery("*:*"));
    QueryResponse queryRes = queryReq.process(cluster.getSolrClient(), collectionName);

    assertEquals(1, queryRes.getResults().size());

    // get the replica's core and metadata controller
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");
    CoreContainer leaderCC = getCoreContainer(shardLeaderReplica.getNodeName());
    ZeroMetadataController metadataController =
        leaderCC.getZeroStoreManager().getZeroMetadataController();
    try (SolrCore leaderCore = leaderCC.getCore(shardLeaderReplica.getCoreName())) {
      ZeroMetadataVersion metadataVersion =
          metadataController.readMetadataValue(collectionName, "shard1");

      ZeroStoreClient zeroStoreClient = leaderCC.getZeroStoreManager().getZeroStoreClient();
      ZeroFile.WithLocal metadataFile =
          metadataController.newShardMetadataZeroFile(
              collectionName, shardNames, metadataVersion.getMetadataSuffix());
      // verify that we pushed the core to Zero store
      assertTrue(zeroStoreClient.shardMetadataExists(metadataFile));

      // verify the number of index files locally is equal to tghe number on Zero store
      ZeroStoreShardMetadata shardMetadata = zeroStoreClient.pullShardMetadata(metadataFile);
      assertEquals(
          leaderCore.getDeletionPolicy().getLatestCommit().getFileNames().size(),
          shardMetadata.getZeroFiles().size());
    }
  }
}
