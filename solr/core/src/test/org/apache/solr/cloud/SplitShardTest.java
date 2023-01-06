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

import static org.apache.solr.handler.admin.CollectionsHandler.AUTO_PREFERRED_LEADERS;
import static org.hamcrest.core.StringContains.containsString;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitShardTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String COLLECTION_NAME = "splitshardtest-collection";

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
    shutdownCluster();
  }

  @Test
  public void testSplitOneHostTwoSubShardsTwoReplicas() throws Exception {
    setupCluster(1);
    innerTestSplitTwoSubShardsTwoReplicas();
  }

  @Test
  public void testSplitTwoHostsTwoSubShardsTwoReplicas() throws Exception {
    setupCluster(2);
    innerTestSplitTwoSubShardsTwoReplicas();
  }

  @Test
  public void testSplitThreeHostsTwoSubShardsTwoReplicas() throws Exception {
    setupCluster(3);
    innerTestSplitTwoSubShardsTwoReplicas();
  }

  private void innerTestSplitTwoSubShardsTwoReplicas() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION_NAME, 1, 2);

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(COLLECTION_NAME)
            .setNumSubShards(2)
            .setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState(
        "Timed out waiting for sub shards to be active", COLLECTION_NAME, activeClusterShape(2, 6));
  }

  @Test
  public void testSplitOneHostFiveSubShardsOneReplica() throws Exception {
    setupCluster(1);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(COLLECTION_NAME)
            .setNumSubShards(5)
            .setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState(
        "Timed out waiting for sub shards to be active. Number of active shards="
            + cluster
                .getSolrClient()
                .getClusterState()
                .getCollection(COLLECTION_NAME)
                .getActiveSlices()
                .size(),
        COLLECTION_NAME,
        activeClusterShape(6, 7));

    try {
      splitShard =
          CollectionAdminRequest.splitShard(COLLECTION_NAME)
              .setShardName("shard2")
              .setNumSubShards(10);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards > 8");
    } catch (BaseHttpSolrClient.RemoteSolrException ex) {
      assertTrue(
          ex.getMessage()
              .contains("A shard can only be split into 2 to 8 subshards in one split request."));
    }

    try {
      splitShard =
          CollectionAdminRequest.splitShard(COLLECTION_NAME)
              .setShardName("shard2")
              .setNumSubShards(1);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards < 2");
    } catch (BaseHttpSolrClient.RemoteSolrException ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  "A shard can only be split into 2 to 8 subshards in one split request. Provided numSubShards=1"));
    }
  }

  @Test
  public void multipleOptionsSplitTest() throws Exception {
    setupCluster(1);
    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(COLLECTION_NAME)
            .setNumSubShards(5)
            .setRanges("0-c,d-7fffffff")
            .setShardName("shard1");
    SolrException thrown =
        assertThrows(SolrException.class, () -> splitShard.process(cluster.getSolrClient()));
    MatcherAssert.assertThat(
        thrown.getMessage(),
        containsString("numSubShards can not be specified with split.key or ranges parameters"));
  }

  @Test
  public void testSplitFuzz() throws Exception {
    setupCluster(1);
    String collectionName = "splitFuzzCollection";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 2);

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collectionName).setSplitFuzz(0.5f).setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState(
        "Timed out waiting for sub shards to be active. Number of active shards="
            + cluster
                .getSolrClient()
                .getClusterState()
                .getCollection(collectionName)
                .getActiveSlices()
                .size(),
        collectionName,
        activeClusterShape(3, 4));
    DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(collectionName);
    Slice s1_0 = coll.getSlice("shard1_0");
    Slice s1_1 = coll.getSlice("shard1_1");
    long fuzz = ((long) Integer.MAX_VALUE >> 3) + 1L;
    long delta0 = (long) s1_0.getRange().max - s1_0.getRange().min;
    long delta1 = (long) s1_1.getRange().max - s1_1.getRange().min;
    long expected0 = (Integer.MAX_VALUE >> 1) + fuzz;
    long expected1 = (Integer.MAX_VALUE >> 1) - fuzz;
    assertEquals("wrong range in s1_0", expected0, delta0);
    assertEquals("wrong range in s1_1", expected1, delta1);
  }

  private void setupCluster(int nodeCount) throws Exception {
    configureCluster(nodeCount).addConfig("conf", configset("cloud-minimal")).configure();
  }

  private CloudSolrClient createCollection(String collectionName, int repFactor) throws Exception {

    CollectionAdminRequest.createCollection(collectionName, "conf", 1, repFactor)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, repFactor);

    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);
    return client;
  }

  private long getNumDocs(CloudSolrClient client) throws Exception {
    String collectionName = client.getDefaultCollection();
    DocCollection collection = client.getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();

    long totCount = 0;
    for (Slice slice : slices) {
      if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
      long lastReplicaCount = -1;
      for (Replica replica : slice.getReplicas()) {
        SolrClient replicaClient =
            getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
        long numFound = 0;
        try {
          numFound =
              replicaClient
                  .query(params("q", "*:*", "distrib", "false"))
                  .getResults()
                  .getNumFound();
          log.info("Replica count={} for {}", numFound, replica);
        } finally {
          replicaClient.close();
        }
        if (lastReplicaCount >= 0) {
          assertEquals("Replica doc count for " + replica, lastReplicaCount, numFound);
        }
        lastReplicaCount = numFound;
      }
      totCount += lastReplicaCount;
    }

    long cloudClientDocs = client.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(
        "Sum of shard count should equal distrib query doc count", totCount, cloudClientDocs);
    return totCount;
  }

  @Test
  public void testConcurrentSplitOneHostRepFactorOne() throws Exception {
    setupCluster(1);
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to
    // 1 and looping the test until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the
    // cluster state for the collection, etc. Using more threads increases the chance to hit a
    // concurrency bug, but too many threads can overwhelm single-threaded buffering replay after
    // the low level index split and result in subShard leaders that can't catch up and become
    // active (a known issue that still needs to be resolved.)
    splitWithConcurrentUpdates("livesplit-1-1", 1, 4, false);
  }

  @Test
  public void testConcurrentSplitThreeHostsRepFactorTwo() throws Exception {
    setupCluster(3);
    splitWithConcurrentUpdates("livesplit-3-2", 2, 4, true);
  }

  private void splitWithConcurrentUpdates(
      String collectionName, int repFactor, int nThreads, boolean setPreferredLeaders)
      throws Exception {
    final CloudSolrClient client = createCollection(collectionName, repFactor);

    final ConcurrentHashMap<String, Long> model =
        new ConcurrentHashMap<>(); // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger numDocsAdded = new AtomicInteger();
    final AtomicInteger numDocsDeleted = new AtomicInteger();
    Thread[] indexThreads = new Thread[nThreads];
    try {

      for (int i = 0; i < nThreads; i++) {
        indexThreads[i] =
            new Thread(
                () -> {
                  Random random = random();
                  List<Integer> threadDocIndexes = new ArrayList<>();
                  while (doIndex.get()) {
                    try {
                      // Thread.sleep(10);  // cap indexing rate at 100 docs per second, per thread
                      int currDoc = numDocsAdded.incrementAndGet();
                      String docId = "doc_" + currDoc;

                      // Try all docs in the same update request
                      UpdateRequest addReq = new UpdateRequest();
                      addReq.add(sdoc("id", docId));
                      // UpdateResponse ursp = updateReq.commit(client, collectionName);
                      // uncomment this if you want a commit each time
                      UpdateResponse ursp = addReq.process(client, collectionName);
                      assertEquals(0, ursp.getStatus()); // for now, don't accept any failures
                      if (ursp.getStatus() == 0) {
                        // in the future, keep track of a version per document and reuse ids to keep
                        // index from growing too large
                        model.put(docId, 1L);
                        threadDocIndexes.add(currDoc);
                      }

                      if (currDoc % 20 == 0) {
                        int docIndex =
                            threadDocIndexes.remove(random.nextInt(threadDocIndexes.size()));
                        docId = "doc_" + docIndex;
                        UpdateRequest deleteReq = new UpdateRequest();
                        deleteReq.deleteById(docId);
                        ursp = deleteReq.process(client, collectionName);
                        assertEquals(0, ursp.getStatus()); // for now, don't accept any failures
                        model.remove(docId);
                        numDocsDeleted.incrementAndGet();
                      }
                    } catch (Exception e) {
                      fail(e.getMessage());
                      break;
                    }
                  }
                });
      }

      for (Thread thread : indexThreads) {
        thread.start();
      }

      Thread.sleep(100); // wait for a few docs to be indexed before invoking split
      int docCount = model.size();

      CollectionAdminRequest.SplitShard splitShard =
          CollectionAdminRequest.splitShard(collectionName).setShardName("shard1");
      // Set the preferred leaders param either with a request param, or with a system property.
      if (random().nextBoolean()) {
        splitShard.shouldSetPreferredLeaders(setPreferredLeaders);
      } else {
        System.setProperty(AUTO_PREFERRED_LEADERS, Boolean.toString(setPreferredLeaders));
      }
      try {
        splitShard.process(client);
        waitForState(
            "Timed out waiting for sub shards to be active.",
            collectionName,
            // 2 repFactor for the new split shards, 1 repFactor for old replicas
            activeClusterShape(2, 3 * repFactor));
      } finally {
        System.clearProperty(AUTO_PREFERRED_LEADERS);
      }

      // make sure that docs were indexed during the split
      assertTrue(model.size() > docCount);

      Thread.sleep(100); // wait for a few more docs to be indexed after split

    } finally {
      // shut down the indexers
      doIndex.set(false);
      for (Thread thread : indexThreads) {
        thread.join();
      }
    }

    client.commit(); // final commit is needed for visibility

    long numDocs = getNumDocs(client);
    if (numDocs != model.size()) {
      SolrDocumentList results =
          client
              .query(new SolrQuery("q", "*:*", "fl", "id", "rows", Integer.toString(model.size())))
              .getResults();
      Map<String, Long> leftover = new HashMap<>(model);
      for (SolrDocument doc : results) {
        String id = (String) doc.get("id");
        leftover.remove(id);
      }
      log.error("MISSING DOCUMENTS: {}", leftover);
    }

    assertEquals(
        "Documents are missing!"
            + " numDocsAdded="
            + numDocsAdded.get()
            + " numDocsDeleted="
            + numDocsDeleted.get()
            + " numDocs="
            + numDocs,
        numDocsAdded.get() - numDocsDeleted.get(),
        numDocs);
    if (log.isInfoEnabled()) {
      log.info("{} docs added, {} docs deleted", numDocsAdded.get(), numDocsDeleted.get());
    }

    if (setPreferredLeaders) {
      DocCollection collection =
          cluster.getSolrClient().getClusterState().getCollection(collectionName);
      for (Slice slice : collection.getSlices()) {
        if (!slice.getState().equals(Slice.State.ACTIVE)) {
          continue;
        }
        boolean preferredLeaderFound = false;
        for (Replica replica : slice.getReplicas()) {
          if (replica.getBool(CollectionAdminParams.PROPERTY_PREFIX + "preferredleader", false)) {
            preferredLeaderFound = true;
            assertEquals(
                "Replica "
                    + replica.getName()
                    + " is the preferred leader but not the leader of shard "
                    + slice.getName(),
                slice.getLeader(),
                replica);
          }
        }
        assertTrue("No preferred leader found for shard " + slice.getName(), preferredLeaderFound);
      }
    }
  }

  public void testShardSplitWithNodeset() throws Exception {
    setupCluster(1);
    String COLL = "shard_split_nodeset";

    CollectionAdminRequest.createCollection(COLL, "conf", 2, 2).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLL, 2, 4);

    JettySolrRunner jetty = cluster.startJettySolrRunner();

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(COLL)
            .setCreateNodeSet(jetty.getNodeName())
            .setShardName("shard1");
    NamedList<Object> response = splitShard.process(cluster.getSolrClient()).getResponse();
    assertNotNull(response.get("success"));

    cluster
        .getZkStateReader()
        .waitForState(
            COLL,
            10,
            TimeUnit.SECONDS,
            (liveNodes, collectionState) ->
                testColl(jetty, collectionState, List.of("shard1_0", "shard1_1")));

    JettySolrRunner randomJetty = cluster.getRandomJetty(random());
    splitShard =
        CollectionAdminRequest.splitShard(COLL)
            .setCreateNodeSet(randomJetty.getNodeName())
            .setShardName("shard2");
    response = splitShard.process(cluster.getSolrClient()).getResponse();
    assertNotNull(response.get("success"));

    cluster
        .getZkStateReader()
        .waitForState(
            COLL,
            10,
            TimeUnit.SECONDS,
            (liveNodes, collectionState) ->
                testColl(randomJetty, collectionState, List.of("shard2_0", "shard2_1")));
  }

  private boolean testColl(
      JettySolrRunner jetty, DocCollection collectionState, Collection<String> sh) {
    Collection<String> set = new HashSet<>(sh);
    collectionState.forEachReplica(
        (s, replica) -> {
          if (replica.getNodeName().equals(jetty.getNodeName())
              && !replica.isLeader()
              && set.contains(replica.shard)) {
            set.remove(replica.shard);
          }
        });

    return set.isEmpty();
  }
}
