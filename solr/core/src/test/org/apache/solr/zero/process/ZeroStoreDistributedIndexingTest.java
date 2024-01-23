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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig.ZeroSystemProperty;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.DistributedZkUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.ZeroStoreUpdateProcessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that indexing requests for {@link Type#ZERO} are distributed correctly through {@link
 * DistributedZkUpdateProcessor}.
 */
public class ZeroStoreDistributedIndexingTest extends ZeroStoreSolrCloudTestCase {
  private static Map<String, List<ZeroCoreIndexingBatchProcessor>> indexingBatchProcessors =
      new HashMap<>();

  private static final String COLLECTION_NAME = "zeroCollection";
  private static final String SHARD1_NAME = "shard1";
  private static final String SHARD2_NAME = "shard2";

  private Replica shard1LeaderReplica;
  private Replica shard2LeaderReplica;
  private Replica shard1FollowerReplica;
  private Replica shard2FollowerReplica;

  private SolrInputDocument[] shard1Docs = new SolrInputDocument[2];
  private SolrInputDocument[] shard2Docs = new SolrInputDocument[2];

  @BeforeClass
  public static void setupCluster() throws Exception {
    assumeWorkingMockito();
    // cleared by ZeroStoreSolrCloudTestCase after test
    System.setProperty(ZeroSystemProperty.ZeroStoreEnabled.getPropertyName(), "true");

    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replaceFirst(
            "<solr>",
            "<solr>"
                + "  <zero>"
                + "    <repositories>"
                + "      <repository\n"
                + "        name=\"local\""
                + "        class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\""
                + "        default=\"true\">"
                + "        <str name=\"location\">"
                + zeroStoreDir
                + "</str>"
                + "      </repository>"
                + "    </repositories>"
                + "  </zero>");

    configureCluster(1)
        .withSolrXml(solrXml)
        .addConfig("conf", configset("zero-distrib-indexing"))
        .configure();
  }

  @Before
  public void setupTest() throws Exception {
    assertEquals("wrong number of nodes", 1, cluster.getJettySolrRunners().size());

    String shardNames = SHARD1_NAME + "," + SHARD2_NAME;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                COLLECTION_NAME, "conf", shardNames, 0)
            .setRouterField("shardName")
            .setZeroIndex(true)
            .setZeroReplicas(2);
    create.process(cluster.getSolrClient());
    // Verify that collection was created
    waitForState(
        "Timed-out wait for collection to be created", COLLECTION_NAME, clusterShape(2, 4));
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
    Slice shard1 = collection.getSlice(SHARD1_NAME);
    assertEquals("wrong number of replicas", 2, shard1.getReplicas().size());
    Slice shard2 = collection.getSlice(SHARD2_NAME);
    assertEquals("wrong number of replicas", 2, shard2.getReplicas().size());

    shard1LeaderReplica = shard1.getLeader();
    shard2LeaderReplica = shard2.getLeader();
    shard1FollowerReplica =
        shard1.getReplicas(r -> !r.getName().equals(shard1.getLeader().getName())).get(0);
    shard2FollowerReplica =
        shard2.getReplicas(r -> !r.getName().equals(shard2.getLeader().getName())).get(0);

    generateDocsForShard(SHARD1_NAME, shard1Docs);
    generateDocsForShard(SHARD2_NAME, shard2Docs);

    indexingBatchProcessors.clear();
  }

  private void generateDocsForShard(String shardName, SolrInputDocument[] docArray) {
    for (int i = 1; i <= docArray.length; i++) {
      SolrInputDocument document = new SolrInputDocument();
      String id = i + "." + shardName;
      document.addField("id", id);
      document.addField("shardName", shardName);
      docArray[i - 1] = document;
    }
  }

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.deleteAllCollections();
    }
  }

  @AfterClass
  public static void afterClass() {
    if (indexingBatchProcessors != null) {
      indexingBatchProcessors = null;
    }
  }

  /** Tests that an update request sent to the leader is handled correctly. */
  @Test
  public void testIndexingBatchSentToLeader() throws Exception {

    SolrClient shard1LeaderDirectClient =
        getHttpSolrClient(
            shard1LeaderReplica.getBaseUrl() + "/" + shard1LeaderReplica.getCoreName());
    try {

      UpdateRequest req = new UpdateRequest();
      req.add(shard1Docs[0]);
      req.add(shard1Docs[1]);
      req.deleteById("dummyId");
      req.process(shard1LeaderDirectClient, null);

      assertNotNull(
          "shard1 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard1LeaderReplica.getCoreName()).size());
      assertNull(
          "shard2 leader should not have indexed",
          indexingBatchProcessors.get(shard2LeaderReplica.getCoreName()));
      assertNull(
          "shard1 follower should not have indexed",
          indexingBatchProcessors.get(shard1FollowerReplica.getCoreName()));
      assertNull(
          "shard2 follower should not have indexed",
          indexingBatchProcessors.get(shard2FollowerReplica.getCoreName()));

      // two adds, one delete and a commit for shard1 leader
      ZeroCoreIndexingBatchProcessor processor =
          getZeroCoreIndexingBatchProcessor(shard1LeaderReplica);
      verify(processor, times(3)).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
    } finally {
      shard1LeaderDirectClient.close();
    }
  }

  /** Tests that an update request sent to a follower is handled correctly. */
  @Test
  public void testIndexingBatchSentToFollower() throws Exception {

    SolrClient shard1FollowerDirectClient =
        getHttpSolrClient(
            shard1FollowerReplica.getBaseUrl() + "/" + shard1FollowerReplica.getCoreName());
    try {

      UpdateRequest req = new UpdateRequest();
      req.add(shard1Docs[0]);
      req.add(shard1Docs[1]);
      req.process(shard1FollowerDirectClient, null);

      assertNotNull(
          "shard1 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard1LeaderReplica.getCoreName()).size());
      assertNull(
          "shard2 leader should not have indexed",
          indexingBatchProcessors.get(shard2LeaderReplica.getCoreName()));
      assertNotNull(
          "shard1 follower should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard1FollowerReplica.getCoreName()).size());
      assertNull(
          "shard2 follower should not have indexed",
          indexingBatchProcessors.get(shard2FollowerReplica.getCoreName()));

      // two adds and a commit for shard1 leader
      ZeroCoreIndexingBatchProcessor processor =
          getZeroCoreIndexingBatchProcessor(shard1LeaderReplica);
      verify(processor, times(2)).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
      // isolated commit for shard1 follower
      processor = getZeroCoreIndexingBatchProcessor(shard1FollowerReplica);
      verify(processor, never()).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
    } finally {
      shard1FollowerDirectClient.close();
    }
  }

  /** Tests that an update request sent to other shard's leader is handled correctly. */
  @Test
  public void testIndexingBatchSentToOtherShardsLeader() throws Exception {

    SolrClient shard2LeaderDirectClient =
        getHttpSolrClient(
            shard2LeaderReplica.getBaseUrl() + "/" + shard2LeaderReplica.getCoreName());
    try {
      UpdateRequest req = new UpdateRequest();
      req.add(shard1Docs[0]);
      req.add(shard1Docs[1]);
      req.process(shard2LeaderDirectClient, null);

      assertNotNull(
          "shard1 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard1LeaderReplica.getCoreName()).size());
      assertNotNull(
          "shard2 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard2LeaderReplica.getCoreName()).size());
      assertNull(
          "shard1 follower should not have indexed",
          indexingBatchProcessors.get(shard1FollowerReplica.getCoreName()));
      assertNull(
          "shard2 follower should not have indexed",
          indexingBatchProcessors.get(shard2FollowerReplica.getCoreName()));

      // two adds and a commit for shard1 leader
      ZeroCoreIndexingBatchProcessor processor =
          getZeroCoreIndexingBatchProcessor(shard1LeaderReplica);
      verify(processor, times(2)).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
      // isolated commit for shard2 leader
      processor = getZeroCoreIndexingBatchProcessor(shard2LeaderReplica);
      verify(processor, never()).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
    } finally {
      shard2LeaderDirectClient.close();
    }
  }

  /**
   * Tests that in a single request documents belonging to two different shards are handled
   * correctly.
   */
  @Test
  public void testMultiShardIndexingBatch() throws Exception {

    SolrClient shard1LeaderDirectClient =
        getHttpSolrClient(
            shard1LeaderReplica.getBaseUrl() + "/" + shard1LeaderReplica.getCoreName());
    try {

      UpdateRequest req = new UpdateRequest();
      req.add(shard1Docs[0]);
      req.add(shard2Docs[0]);
      req.add(shard1Docs[1]);
      req.add(shard2Docs[1]);
      req.process(shard1LeaderDirectClient, null);

      assertNotNull(
          "shard1 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard1LeaderReplica.getCoreName()).size());
      // DistributedZkUpdateProcessor forwards the documents to other nodes through
      // StreamingSolrClients#getSolrClient
      // such that, they are streamed through a single update request
      assertNotNull(
          "shard2 leader should have indexed and pulled from Zero store",
          indexingBatchProcessors.get(shard2LeaderReplica.getCoreName()).size());
      assertNull(
          "shard1 follower should not have indexed",
          indexingBatchProcessors.get(shard1FollowerReplica.getCoreName()));
      assertNull(
          "shard2 follower should not have indexed",
          indexingBatchProcessors.get(shard2FollowerReplica.getCoreName()));

      // two adds and a commit for shard1
      ZeroCoreIndexingBatchProcessor processor =
          getZeroCoreIndexingBatchProcessor(shard1LeaderReplica);
      verify(processor, times(2)).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
      // two adds and a commit for shard2
      processor = getZeroCoreIndexingBatchProcessor(shard2LeaderReplica);
      verify(processor, times(2)).addOrDeleteGoingToBeIndexedLocally();
      verify(processor, times(1)).hardCommitCompletedLocally();
    } finally {
      shard1LeaderDirectClient.close();
    }
  }

  /**
   * The purpose of this test {@link DistributedUpdateProcessorFactory} is to produce a {@link
   * DistributedZkUpdateProcessor} that can be spied upon. The spied instance's implementation is
   * not changed as such.
   */
  public static class TestDistributedUpdateProcessorFactory
      extends DistributedUpdateProcessorFactory {
    @Override
    public UpdateRequestProcessor getInstance(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      ZeroStoreUpdateProcessor zkUpdateProcessor =
          (ZeroStoreUpdateProcessor) super.getInstance(req, rsp, next);

      ClusterState clusterState = req.getCoreContainer().getZkController().getClusterState();
      SolrCore core = req.getCore();
      ZeroCoreIndexingBatchProcessor spyZeroCoreIndexingBatchProcessor =
          spy(new ZeroCoreIndexingBatchProcessor(core, clusterState, rsp));
      zkUpdateProcessor.updateCoreIndexingBatchProcessor(spyZeroCoreIndexingBatchProcessor);

      indexingBatchProcessors
          .computeIfAbsent(core.getName(), k -> new ArrayList<>())
          .add(spyZeroCoreIndexingBatchProcessor);
      return zkUpdateProcessor;
    }
  }

  private ZeroCoreIndexingBatchProcessor getZeroCoreIndexingBatchProcessor(Replica replica) {
    return indexingBatchProcessors.get(replica.getCoreName()).get(0);
  }
}
