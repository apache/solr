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

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for missing Zero core. Missing core refers to a case in which shard index exists on the
 * source-of-truth for Zero collections, the Zero store, but is missing locally on the solr node.
 * The metadata for the index shard and replica exist on ZK which indicates Solr Cloud is aware of
 * the shard and replica.
 */
public class ZeroStoreMissingCoreTest extends ZeroStoreSolrCloudTestCase {

  @After
  public void teardownTest() throws Exception {
    shutdownCluster();
  }

  /**
   * Tests that after a missing core is discovered and created, a query is able to successfully pull
   * into it from Zero store. Also makes sure an indexing on top of pulled contents is also
   * successful.
   */
  @Test
  public void testMissingCorePullAndIndexingSucceeds() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);
    Replica replica =
        cluster
            .getZkStateReader()
            .getClusterState()
            .getCollection(collectionName)
            .getLeader("shard1");
    Map<String, CountDownLatch> asyncPullLatches =
        stopSolrRemoveCoreRestartSolr(
            cluster.getJettySolrRunner(0), replica, collectionName, false);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    // verify the documents are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1"));

    // send another update to the collection
    updateReq = new UpdateRequest();
    updateReq.add("id", "2");
    updateReq.commit(cloudClient, collectionName);

    // verify the documents are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1", "2"));

    // verify that they made it to Zero store by a clean pull
    asyncPullLatches =
        stopSolrRemoveCoreRestartSolr(
            cluster.getJettySolrRunner(0), replica, collectionName, false);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1", "2"));
  }

  /**
   * Tests that after a missing core is discovered and created, an indexing is able to successfully
   * pull into it from Zero store and is itself successful.
   */
  @Test
  public void testMissingCoreIndexingSucceeds() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 1;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    Replica replica =
        cluster
            .getZkStateReader()
            .getClusterState()
            .getCollection(collectionName)
            .getLeader("shard1");
    stopSolrRemoveCoreRestartSolr(cluster.getJettySolrRunner(0), replica, collectionName, false);

    // send another update to the collection
    updateReq = new UpdateRequest();
    updateReq.add("id", "2");
    updateReq.commit(cloudClient, collectionName);

    // verify that previous and new document both are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1", "2"));

    // verify that new state made it to Zero store by doing a clean pull
    Map<String, CountDownLatch> asyncPullLatches =
        stopSolrRemoveCoreRestartSolr(
            cluster.getJettySolrRunner(0), replica, collectionName, false);
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1", "2"));
  }

  /**
   * Tests that a newly created shard (create collection) does not encounter "never synced" error.
   */
  @Test
  public void testNewlyCreatedShardIsNotConsideredNeverSynced() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(1);
    // Stop the background pulls so that they do not interfere with the outcome of the query.
    stopAsyncCorePulling(cluster.getJettySolrRunner(0));
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 1;
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // this query should not fail
    cloudClient.query(collectionName, new ModifiableSolrParams().set("q", "*:*"));
  }

  /**
   * Tests that a core that has never synced with the Zero store does fail with "never synced"
   * error. This test verifies behavior that is not the desired end state for Zero replicas: a query
   * to a never synced core should sync that core and respond correctly rather than fail. But
   * currently this is not yet implemented (the overall topic is called "query freshness").
   */
  @Test
  public void testQueryFailureForNeverSyncedCore() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 1;
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // we need to initialize Zero store with some indexing
    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    Replica replica =
        cluster
            .getZkStateReader()
            .getClusterState()
            .getCollection(collectionName)
            .getLeader("shard1");
    Map<String, CountDownLatch> asyncPullLatches =
        stopSolrRemoveCoreRestartSolr(
            cluster.getJettySolrRunner(0), replica, collectionName, false);
    // since the node was restarted and core has not synced with the Zero store,
    // the query should fail with "never synced" error
    assertQueryFailsWithNeverSyncedError(
        cloudClient, collectionName, new ModifiableSolrParams().set("q", "*:*"));

    // start background pull, so that local core can be updated from the Zero store and contents
    // can be verified.
    queryAndWaitForPullToFinish(cloudClient, collectionName, asyncPullLatches);
    // verify the documents are present
    assertQueryReturnsAllDocs(cloudClient, collectionName, Arrays.asList("1"));
  }

  /**
   * Tests that if a shard has two replicas and one of them is not synced with the Zero store then
   * SolrClient does not fail the query with "never synced" error.
   *
   * <p>Unless you are not directly querying a replica(core), SolrClient makes use of all the
   * available replicas one-by-one until it succeeds. Since the test is querying a collection (not
   * directly a replica) the query should pass (even though during the execution Solr will hit the
   * never-synced replica failure but that would not propagate to client because Solr was able to
   * get successful response from the second replica).
   *
   * <p>In this test, follower replica will be made a "never synced" replica but the outcome should
   * remain same if we would have to make the leader replica "never synced".
   */
  @Test
  public void testTwoReplicaQueryDoesNotFailIfOnlyOneHasNeverSynced() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(2);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "zeroCollection";
    int numReplicas = 2;
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // get the leader and follower replicas
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica leaderReplica = collection.getLeader("shard1");
    Replica followerReplica = null;
    for (Replica repl : collection.getSlice("shard1").getReplicas()) {
      if (!repl.getName().equals(leaderReplica.getName())) {
        followerReplica = repl;
        break;
      }
    }

    // get the leader and follower runners
    JettySolrRunner leaderRunner;
    JettySolrRunner followerRunner;
    if (cluster.getJettySolrRunner(0).getNodeName().equals(leaderReplica.getNodeName())) {
      leaderRunner = cluster.getJettySolrRunner(0);
      followerRunner = cluster.getJettySolrRunner(1);
    } else {
      leaderRunner = cluster.getJettySolrRunner(1);
      followerRunner = cluster.getJettySolrRunner(0);
    }

    // Blocks pulls on leader so that they do not interfere with the outcome of the queries.
    stopAsyncCorePulling(leaderRunner);

    // we need to initialize Zero store with some indexing
    // send an update to the collection
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    // Restarts and blocks follower pulls so that they do not interfere with the outcome of the
    // queries.
    stopSolrRemoveCoreRestartSolr(followerRunner, followerReplica, collectionName, true);

    try (SolrClient followerDirectClient =
        getHttpSolrClient(followerReplica.getBaseUrl() + "/" + followerReplica.getCoreName())) {
      // sanity:
      // since the follower node was restarted and core has not synced with the Zero store,
      // a direct query to follower replica should fail with "never synced" error
      assertQueryFailsWithNeverSyncedError(
          followerDirectClient, new ModifiableSolrParams().set("q", "*:*").set("distrib", "false"));
    }

    // But since the leaderReplica is not in "never synced" state therefore a query to collection
    // should succeed.
    // Do it few times so that we know that follower replica was indeed tried as a first replica and
    // failed, and then the query was served from the leader replica.
    // (By default, for each query request CloudSolrClient randomizes which replica to try first)
    for (int i = 0; i < 10; i++) {
      QueryResponse response =
          cloudClient.query(collectionName, new ModifiableSolrParams().set("q", "*:*"));
      assertQueryResponseHasAllDocs(response, Arrays.asList("1"));
    }
  }

  private void assertQueryFailsWithNeverSyncedError(SolrClient replicaClient, SolrParams params) {
    assertQueryFailsWithNeverSyncedError(replicaClient, null, params);
  }

  private void assertQueryFailsWithNeverSyncedError(
      SolrClient client, String collectionName, SolrParams params) {
    try {
      client.query(collectionName, params);
      fail("A core never synced with the Zero store did not fail the query");
    } catch (Exception ex) {
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      assertTrue(
          "A core never synced with the Zero store failed for unexpected reason " + sw,
          sw.toString()
              .contains("is not fresh enough because it has never synced with the Zero store"));
    }
  }

  private void queryAndWaitForPullToFinish(
      CloudSolrClient cloudClient,
      String collectionName,
      Map<String, CountDownLatch> asyncPullLatches)
      throws Exception {
    // do a query to trigger core pull
    String leaderCoreName =
        cluster
            .getZkStateReader()
            .getClusterState()
            .getCollection(collectionName)
            .getLeader("shard1")
            .getCoreName();
    CountDownLatch latch = new CountDownLatch(1);
    asyncPullLatches.put(leaderCoreName, latch);

    primingPullQuery(cloudClient, collectionName, new ModifiableSolrParams().set("q", "*:*"));

    // wait until pull is finished
    assertTrue("Timed-out waiting for pull to finish", latch.await(120, TimeUnit.SECONDS));
  }

  /**
   * This method assumes that there is only one replica running on the node. It stops the node,
   * deletes the core directory and restarts the node. Also takes an optional CorePullTracker and
   * returns a map of CountDownLatch that can be used to control the background pull machinery.
   */
  private Map<String, CountDownLatch> stopSolrRemoveCoreRestartSolr(
      JettySolrRunner runner, Replica replica, String collectionName, boolean blockBackgroundPulls)
      throws Exception {
    CoreContainer cc = getCoreContainer(replica.getNodeName());

    // sanity: core exists
    try (SolrCore core = cc.getCore(replica.getCoreName())) {
      assertNotNull("core not found", core);
    }

    // get the core directory
    File coreIndexDir = new File(cc.getCoreRootDirectory() + "/" + replica.getCoreName());

    // stop the cluster's node
    cluster.stopJettySolrRunner(runner);
    cluster.waitForJettyToStop(runner);

    // remove the core locally
    PathUtils.deleteDirectory(coreIndexDir.toPath());

    // start up the node again
    runner = cluster.startJettySolrRunner(runner, true);
    cluster.waitForNode(runner, /* seconds */ 30);
    Map<String, CountDownLatch> asyncPullLatches = configureTestZeroProcessForNode(runner);
    if (blockBackgroundPulls) {
      stopAsyncCorePulling(runner);
    }

    // at core container load time we will discover that we have a replica for this node on ZK but
    // no corresponding core
    // thus, we will create one at load time
    cc = getCoreContainer(replica.getNodeName());
    try (SolrCore core = cc.getCore(replica.getCoreName())) {
      assertNotNull("core not found", core);
      assertEquals(
          "core is not empty", 1L, core.getDeletionPolicy().getLatestCommit().getGeneration());
    }

    // after restart, it takes some time for downed replica to be marked active
    final Replica r = replica;
    waitForState(
        "Timeout occurred while waiting for replica to become active ",
        collectionName,
        (liveNodes, collectionState) ->
            collectionState.getReplica(r.getName()).getState() == State.ACTIVE);

    return asyncPullLatches;
  }

  private void assertQueryReturnsAllDocs(
      CloudSolrClient cloudClient, String collectionName, List<String> expectedDocs)
      throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams().set("q", "*:*");
    QueryResponse resp = cloudClient.query(collectionName, params);
    assertQueryResponseHasAllDocs(resp, expectedDocs);
  }

  private void assertQueryResponseHasAllDocs(QueryResponse resp, List<String> expectedDocs) {
    assertEquals("wrong number of docs", expectedDocs.size(), resp.getResults().getNumFound());
    List<String> actualDocs =
        resp.getResults().stream()
            .map(r -> (String) r.getFieldValue("id"))
            .collect(Collectors.toList());
    Collections.sort(expectedDocs);
    Collections.sort(actualDocs);
    assertEquals("wrong docs", expectedDocs.toString(), actualDocs.toString());
  }
}
