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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Slice.State;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.update.SolrIndexSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for shard splitting in conjunction with Zero store */
public class ZeroStoreSplitTest extends ZeroStoreSolrCloudTestCase {

  private static String node1;
  private static String node2;

  static Map<String, Map<String, CountDownLatch>> solrProcessesTaskTracker = new HashMap<>();

  @Before
  public void setupCluster() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(2);

    ClusterState initClusterState = cluster.getZkStateReader().getClusterState();
    List<String> liveNodes = new ArrayList<>(2);
    liveNodes.addAll(initClusterState.getLiveNodes());

    node1 = liveNodes.get(0);
    node2 = liveNodes.get(1);

    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      solrProcessesTaskTracker.put(runner.getNodeName(), configureTestZeroProcessForNode(runner));
    }
  }

  @After
  public void tearDownTest() throws Exception {
    shutdownCluster();
  }

  CloudSolrClient createCollection(String collectionName, int repFactor) throws Exception {

    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 0, 0, 0)
        .setZeroIndex(true)
        .setZeroReplicas(repFactor)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, repFactor);

    return cluster.getSolrClient(collectionName);
  }

  private void indexPrefixDocs(
      CloudSolrClient client,
      String collectionName,
      int nPrefixes,
      int nDocsPerPrefix,
      int docOffset)
      throws Exception {
    if (random().nextBoolean()) {
      // index docs separately
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          client.add(sdoc("id", prefix + "!doc" + (j + docOffset)));
        }
      }
      if (random().nextBoolean()) {
        client.commit(collectionName, true, true, false);
      }
    } else {
      // Try all docs in the same update request
      UpdateRequest updateReq = new UpdateRequest();
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          updateReq.add(sdoc("id", prefix + "!doc" + (j + docOffset)));
        }
      }
      UpdateResponse ursp;
      if (random().nextBoolean()) {
        ursp = updateReq.commit(client, collectionName);
      } else {
        ursp = updateReq.process(client, collectionName);
      }
      assertEquals(0, ursp.getStatus());
    }
  }

  private void doSplitShard(String collectionName, int repFactor, int nPrefixes, int nDocsPerPrefix)
      throws Exception {
    CloudSolrClient client = createCollection(collectionName, repFactor);

    if (random().nextBoolean()) {
      // start off with a commit
      client.commit(collectionName, true, true, false);
    }

    indexPrefixDocs(client, collectionName, nPrefixes, nDocsPerPrefix, 0);

    // Can't assert this because might not have committed (openSearcher) above
    //  and let's not commit any way so that we test uncommitted docs aren't lost on split.
    // assertEquals(nPrefixes * nDocsPerPrefix, getNumDocs(client));

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collectionName)
            .setSplitByPrefix(true)
            .setShardName("shard1");
    if (rarely()) {
      splitShard.setSplitMethod(SolrIndexSplitter.SplitMethod.LINK.toLower());
    }
    splitShard.process(client);
    waitForState(
        "Timed out waiting for sub shards to be active.",
        collectionName,
        activeClusterShape(2, 2 * repFactor)); // 2 repFactor for the new split shards
    // splits do a commit, by the way

    // now index another batch of docs into the new shards
    indexPrefixDocs(client, collectionName, nPrefixes, nDocsPerPrefix, nDocsPerPrefix);
    client.commit(collectionName);
    assertEquals(nPrefixes * nDocsPerPrefix * 2, getNumDocs(client));
  }

  /** Returns num docs. Ensures is not under-counted due to "query freshness" problem. */
  private long getNumDocs(CloudSolrClient client) throws Exception {
    final SolrDocumentList results =
        client.query(new SolrQuery("q", "*:*", "shards", getLeaderShardUrls(client))).getResults();
    return results.getNumFound();
  }

  private String getLeaderShardUrls(CloudSolrClient client) {
    String collectionName = client.getDefaultCollection();
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();
    // query only leaders to avoid query freshness
    return slices.stream()
        .filter(s -> s.getState() == State.ACTIVE)
        .map(Slice::getLeader)
        .map(Replica::getCoreUrl)
        .map(url -> url.replace("http://", ""))
        .collect(Collectors.joining(","));
  }

  @Test
  public void testSplit() throws Exception {
    doSplitShard("c1", 1, 2, 2);
    doSplitShard("c2", 2, 2, 2);
  }

  private void doLiveSplitShard(String collectionName, int repFactor, int nThreads)
      throws Exception {
    final boolean doSplit =
        true; // test debugging aid: set to false if you want to check that the test passes if we
    // don't do a split
    final boolean updateFailureOK =
        false; // we should not expect any of our updates to fail without a node failure
    final CloudSolrClient client = createCollection(collectionName, repFactor);

    final Set<String> indexedDocs =
        Collections.synchronizedSet(new HashSet<>()); // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docs = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
    // allows waiting for a given number of updates
    final AtomicReference<CountDownLatch> updateLatch =
        new AtomicReference<>(new CountDownLatch(random().nextInt(4)));
    Thread[] indexThreads = new Thread[nThreads];
    try {

      for (int i = 0; i < nThreads; i++) {
        indexThreads[i] =
            new Thread(
                () -> {
                  while (doIndex.get()) {
                    try {
                      // Thread.sleep(10);  // cap indexing rate at 100 docs per second per thread
                      int currDoc = docs.incrementAndGet();
                      String docId = "doc_" + currDoc;

                      // Try all docs in the same update request
                      UpdateRequest updateReq = new UpdateRequest();
                      updateReq.add(sdoc("id", docId));

                      UpdateResponse ursp;
                      if (random().nextInt(4) == 0) { // add commit 25% of the time
                        ursp = updateReq.commit(client, collectionName);
                      } else {
                        ursp = updateReq.process(client, collectionName);
                      }

                      updateLatch.get().countDown();
                      if (ursp.getStatus() == 0) {
                        indexedDocs.add(docId);
                      } else {
                        failures.incrementAndGet();
                        if (!updateFailureOK) {
                          assertEquals(0, ursp.getStatus());
                        }
                      }
                    } catch (Exception e) {
                      updateLatch
                          .get()
                          .countDown(); // do this on exception as well so we don't get stuck
                      failures.incrementAndGet();

                      // confirm that ZERO replicas reject updates while the shard is splitting
                      assertTrue(e.getMessage().contains("Rejecting"));
                    }
                  }
                });
      }

      for (Thread thread : indexThreads) {
        thread.start();
      }

      updateLatch.get().await(); // wait for some documents to be indexed

      if (doSplit) {
        CollectionAdminRequest.SplitShard splitShard =
            CollectionAdminRequest.splitShard(collectionName).setShardName("shard1");
        splitShard.process(client);
        waitForState(
            "Timed out waiting for sub shards to be active.",
            collectionName,
            activeClusterShape(2, 2 * repFactor)); // 2 repFactor for the new split shards
      } else {
        // The sleep here is fine since this code is only executed manually while debugging
        // this test.
        Thread.sleep(10 * 1000);
      }

      // wait for a few more docs to be indexed after split
      updateLatch.set(new CountDownLatch(random().nextInt(4)));
      updateLatch.get().await();

    } finally {
      // shut down the indexers
      doIndex.set(false);
      for (Thread thread : indexThreads) {
        thread.join();
      }
    }

    client.commit(); // final commit is needed for visibility

    // Count the number of docs that are in the index, and compare that to the number of docs that
    // the test believes were indexed successfully
    long numDocs = getNumDocs(client);
    assertEquals(
        "Number of query results does not match number of successfully indexed docs.",
        numDocs,
        indexedDocs.size());

    // Now verify that the correct number of failures were observed; one for each document that was
    // not indexed successfully
    assertEquals(
        "The total number of indexed docs and failures should equal the overall number of docs",
        docs.get() - indexedDocs.size(),
        failures.get());

    // Verify that the indexed documents actually match what we expected
    final SolrDocumentList results =
        client
            .query(
                new SolrQuery(
                    "q",
                    "*:*",
                    "shards",
                    getLeaderShardUrls(client),
                    "rows",
                    String.valueOf(numDocs)))
            .getResults();
    results.forEach(
        d -> {
          assertTrue(
              "Did not find " + d.getFieldValue("id") + " that we thought was indexed",
              indexedDocs.remove(d.getFieldValue("id")));
        });

    assertTrue(
        "There were some documents in the index that we thought had failed to index successfully",
        indexedDocs.isEmpty());
  }

  // TODO: this test is adapted from SplitShardTest.testLiveSplit and could perhaps
  // be unified.
  @Test
  public void testLiveSplit() throws Exception {
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to
    // 1 and looping the test until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the
    // cluster state for the collection, etc.
    // Using more threads increases the chance to hit a concurrency bug, but too many threads can
    // overwhelm single-threaded buffering replay after the low level index split and result in
    // subShard leaders that can't catch up and become active (a known issue that still needs
    // to be resolved.)
    doLiveSplitShard("livesplit1", 1, 8);
  }

  /*
   * Work of running sync with Zero store before split test.
   * Set up cluster, index to leader, reelect leader, trigger split.
   * Verify document files available on new leader and children subshards.
   */
  private void runTestSyncFromZeroBeforeSplit(boolean splitByPrefix) throws Exception {
    SolrCore followerCore = null;
    SolrCore firstLeaderCore = null;
    SolrCore newLeaderCore = null;
    SolrCore ssReplica1Core = null;
    SolrCore ssReplica2Core = null;

    try {
      // Create collection with a single shard and two replicas on separate nodes
      String collectionName = "testSyncSplitCollection";
      String shardName = "shard1";
      int numReplicas = 2;
      CollectionAdminRequest.Create createCollection =
          CollectionAdminRequest.createCollection(collectionName, 1, 0)
              .setZeroIndex(true)
              .setZeroReplicas(numReplicas);
      cluster.getSolrClient().request(createCollection);

      // Wait for collection to be created
      cluster.waitForActiveCollection(collectionName, 1, 2);

      ClusterState clusterState = cluster.getZkStateReader().getClusterState();
      DocCollection collection = clusterState.getCollection(collectionName);

      // Verify that there is 1 replica on node1 and 1 replica on node2
      assertEquals(1, collection.getReplicas(node1).size());
      assertEquals(1, collection.getReplicas(node2).size());

      // Add a document [to leader replica]
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "1");
      doc.setField("cat", "cat123");
      UpdateRequest req = new UpdateRequest();
      req.add(doc);
      req.commit(cluster.getSolrClient(), collectionName);

      // get the follower replica
      Replica leaderReplica = collection.getLeader(shardName);
      Replica follower = null;
      for (Replica replica : collection.getReplicas()) {
        if (!replica.getName().equals(leaderReplica.getName())) {
          follower = replica;
          break;
        }
      }

      // verify this document update didn't happen on the follower, it should only have its default
      // segment file
      CoreContainer followerCC = getCoreContainer(follower.getNodeName());
      followerCore = followerCC.getCore(follower.getCoreName());
      assertEquals(1, followerCore.getDeletionPolicy().getLatestCommit().getFileNames().size());

      // leader size
      CoreContainer firstLeaderCC = getCoreContainer(leaderReplica.getNodeName());
      firstLeaderCore = firstLeaderCC.getCore(leaderReplica.getCoreName());
      int indexSize = firstLeaderCore.getDeletionPolicy().getLatestCommit().getFileNames().size();
      assertTrue(indexSize > 1);

      // trigger reelection so follower replica becomes leader - does not trigger a sync with Zero
      // store.
      // Set follower replica as preferred leader
      final Replica followerReplica = follower;
      CollectionAdminResponse setLeaderReplicaPropertyResponse =
          CollectionAdminRequest.addReplicaProperty(
                  collection.getName(),
                  collection.getShardId(follower.getNodeName(), follower.getCoreName()),
                  follower.getName(),
                  "preferredLeader",
                  "true")
              .process(cluster.getSolrClient(collectionName));

      assertEquals(0, setLeaderReplicaPropertyResponse.getStatus());
      waitForState(
          "Expecting property 'preferredleader' to appear on follower replica "
              + followerReplica.getName(),
          collectionName,
          (n, c) ->
              "true"
                  .equals(c.getReplica(followerReplica.getName()).getProperty("preferredleader")));

      clusterState = cluster.getZkStateReader().getClusterState();
      collection = clusterState.getCollection(collectionName);

      CollectionAdminRequest.RebalanceLeaders rebalanceLeaderRequest =
          CollectionAdminRequest.rebalanceLeaders(collection.getName());
      rebalanceLeaderRequest.setMaxWaitSeconds(5);
      rebalanceLeaderRequest.process(cluster.getSolrClient(collectionName));

      // verify that follower is now the leader:
      clusterState = cluster.getZkStateReader().getClusterState();
      collection = clusterState.getCollection(collectionName);
      Replica newLeader = collection.getLeader(shardName);
      assertEquals(newLeader.getNodeName(), follower.getNodeName());
      assertEquals(newLeader.getCoreName(), follower.getCoreName());

      CoreContainer newLeaderCC = getCoreContainer(newLeader.getNodeName());
      newLeaderCore = newLeaderCC.getCore(newLeader.getCoreName());
      assertEquals(1, newLeaderCore.getDeletionPolicy().getLatestCommit().getFileNames().size());

      // trigger shard split
      if (splitByPrefix) {
        CollectionAdminRequest.splitShard(collectionName)
            .setShardName(shardName)
            .setSplitByPrefix(true)
            .process(cluster.getSolrClient(collectionName));
      } else {
        CollectionAdminRequest.splitShard(collectionName)
            .setShardName(shardName)
            .process(cluster.getSolrClient(collectionName));
      }
      waitForState(
          "Waiting for subshards created from split",
          collectionName,
          SolrCloudTestCase.activeClusterShape(2, 4));
      // second replica should be elected leader so split will be kicked off from here
      // validate that new leader syncs with Zero store prior to split (contains expected files)
      assertEquals(
          indexSize, newLeaderCore.getDeletionPolicy().getLatestCommit().getFileNames().size());

      // validate that subshards combined contain expected files
      clusterState = cluster.getZkStateReader().getClusterState();
      collection = clusterState.getCollection(collectionName);
      String childShard1 = shardName + "_0";
      String childShard2 = shardName + "_1";
      Collection<Replica> subShard1Replicas = collection.getSlice(childShard1).getReplicas();
      Collection<Replica> subShard2Replicas = collection.getSlice(childShard2).getReplicas();
      Replica ssReplica1 = subShard1Replicas.iterator().next();
      Replica ssReplica2 = subShard2Replicas.iterator().next();

      CoreContainer ssReplica1CC = getCoreContainer(ssReplica1.getNodeName());
      CoreContainer ssReplica2CC = getCoreContainer(ssReplica2.getNodeName());
      ssReplica1Core = ssReplica1CC.getCore(ssReplica1.getCoreName());
      ssReplica2Core = ssReplica2CC.getCore(ssReplica2.getCoreName());

      int ss1FileSize = ssReplica1Core.getDeletionPolicy().getLatestCommit().getFileNames().size();
      int ss2FileSize = ssReplica2Core.getDeletionPolicy().getLatestCommit().getFileNames().size();
      assertEquals(
          indexSize,
          ss1FileSize + ss2FileSize - 1); // subtract 1 for additional default segment file

    } catch (Exception e) {
      fail(
          "testSyncFromZeroBeforeSplit, splitByPrefix is: "
              + splitByPrefix
              + ". Failed to sync before split with exception: "
              + e.getMessage());
    } finally {
      if (followerCore != null) followerCore.close();
      if (firstLeaderCore != null) firstLeaderCore.close();
      if (newLeaderCore != null) newLeaderCore.close();
      if (ssReplica1Core != null) ssReplica1Core.close();
      if (ssReplica2Core != null) ssReplica2Core.close();
      cluster.deleteAllCollections();
    }
  }

  @Test
  public void testDeleteDocumentWhenSplitting() throws Exception {

    final boolean deleteByQuery = random().nextBoolean();
    final Set<Integer> deletedDocs =
        Collections.synchronizedSet(
            new HashSet<>()); // Documents that the test believes to be deleted from the index
    final Set<Integer> indexedDocs =
        Collections.synchronizedSet(
            new HashSet<>()); // Documents that the test believes to be in the index
    final Set<Integer> failures = Collections.synchronizedSet(new HashSet<>());
    final CountDownLatch updateLatch = new CountDownLatch(random().nextInt(4));

    final String collectionName = "testDeleteDocumentWhenSplitting";
    final CloudSolrClient client = createCollection(collectionName, 2);

    // Add 100 docs
    int first = 1, last = 100;
    IntStream.range(first, last + 1)
        .forEach(
            i -> { // last is exclusive
              assertEquals(0, indexDoc(i, false, collectionName).getStatus());
              indexedDocs.add(i);
            });

    client.commit();
    long numDocs = getNumDocs(client);
    assertEquals(last, numDocs);

    Thread thread =
        new Thread(
            () ->
                IntStream.range(first, last + 1)
                    .forEach(
                        i -> {
                          UpdateRequest deleteReq = new UpdateRequest();
                          if (deleteByQuery) {
                            deleteReq.deleteByQuery("id:" + i);
                          } else {
                            deleteReq.deleteById(String.valueOf(i));
                          }

                          try {
                            UpdateResponse response = deleteReq.process(client, collectionName);
                            // Expect success or exception
                            assertEquals(0, response.getStatus());
                            indexedDocs.remove(i);
                            deletedDocs.add(i);
                          } catch (Exception e) {
                            failures.add(i);
                            assertTrue(e.getMessage().contains("Rejecting"));

                            // If there are less than 10 docs left, then wait for the split, because
                            // we want to verify that we
                            // are able to delete some successfully once the split is completed
                            if (last - i <= 10) {
                              waitForState(
                                  "Timed out waiting for sub shards to be active.",
                                  collectionName,
                                  activeClusterShape(2, 4));
                            }
                          } finally {
                            updateLatch.countDown();
                          }
                        }));

    try {
      thread.start();

      updateLatch.await(); // wait for some documents to be deleted

      CollectionAdminRequest.splitShard(collectionName).setShardName("shard1").process(client);
      waitForState(
          "Timed out waiting for sub shards to be active.",
          collectionName,
          activeClusterShape(2, 4));

    } finally {
      thread.join();
    }

    numDocs = getNumDocs(client);
    assertEquals(
        "The number of docs in the index does not match the number we expected to remain",
        numDocs,
        indexedDocs.size());
    assertEquals(
        "Expected to see a failure for each document that remains in the index",
        numDocs,
        failures.size());

    // The thread waited on the split once there were 10 docs remaining
    assertTrue("At least 10 docs should have been deleted successfully", numDocs < last - 10);
  }

  private UpdateResponse indexDoc(int id, boolean commit, String collectionName) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", String.valueOf(id));
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    try {
      return commit
          ? req.commit(cluster.getSolrClient(), collectionName)
          : req.process(cluster.getSolrClient(), collectionName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Test sync from Zero store before split (using splitByPrefix == false)
   */
  @Test
  public void testSyncFromZeroBeforeSplit() throws Exception {
    runTestSyncFromZeroBeforeSplit(false);
  }

  /*
   * Test sync from Zero store before split (using splitByPrefix == true)
   */
  @Test
  public void testSyncFromZeroBeforeSplitByPrefix() throws Exception {
    runTestSyncFromZeroBeforeSplit(true);
  }
}
