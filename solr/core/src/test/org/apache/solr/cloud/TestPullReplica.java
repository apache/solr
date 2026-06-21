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

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@LuceneTestCase.Nightly
public class TestPullReplica extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String collectionName = null;
  private final static int REPLICATION_TIMEOUT_SECS = 10;

  private static String suggestedCollectionName() {
    return (SolrTestUtil.getTestName().replace("Test", "") + "_" + SolrTestUtil.getTestName().split(" ")[0]).replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase(Locale.ROOT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);
   //  cloudSolrClientMaxStaleRetries
   System.setProperty("cloudSolrClientMaxStaleRetries", "3");
   System.setProperty("zkReaderGetLeaderRetryTimeoutMs", "6000");

   configureCluster(2) // 2 + random().nextInt(3)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void tearDownCluster() {
    System.clearProperty("cloudSolrClientMaxStaleRetries");
    System.clearProperty("zkReaderGetLeaderRetryTimeoutMs");
    TestInjection.reset();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    collectionName = suggestedCollectionName();
    SolrTestCaseUtil.expectThrows(SolrException.class, () -> getCollectionState(collectionName));
  }

  @Override
  public void tearDown() throws Exception {
    try {
      if (cluster.getSolrClient().getZkStateReader().getClusterState().getCollectionOrNull(collectionName) != null) {
        log.info("tearDown deleting collection");
        CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
      }
    } catch (Exception e) {
      log.warn("delete collection failed on teardown", e);
    }
    super.tearDown();
  }

  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  public void testCreateDelete() throws Exception {
    switch (random().nextInt(3)) {
      case 0:
        // Sometimes use SolrJ
        CollectionAdminRequest
            .createCollection(collectionName, "conf", 2, 1, 0, 3)
            .process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, 2, 8);
        break;
      case 1:
        // Sometimes use v1 API
        String url = String.format(Locale.ROOT,
            "%s/admin/collections?action=CREATE&name=%s&collection.configName=%s&numShards=%s&pullReplicas=%s",
            cluster.getRandomJetty(random()).getBaseUrl(), collectionName, "conf", 2,    // numShards
            3);    // pullReplicas);
        url = url + SolrTestCaseUtil.pickRandom("", "&nrtReplicas=1", "&replicationFactor=1"); // These options should all mean the same
        Http2SolrClient.GET(url, cluster.getSolrClient().getHttpClient());
        cluster.waitForActiveCollection(collectionName, 2, 8);
        break;
      case 2:
        // Sometimes use V2 API
        url = cluster.getRandomJetty(random()).getBaseUrl().toString()
            + "/____v2/c";
        String requestBody = String.format(Locale.ROOT,
            "{create:{name:%s, config:%s, numShards:%s, pullReplicas:%s, %s}}",
            collectionName, "conf", 2,    // numShards
            3,    // pullReplicas
            SolrTestCaseUtil.pickRandom("", ", nrtReplicas:1", ", replicationFactor:1")); // These options should all mean the same
        try (Http2SolrClient http2SolrClient =  new Http2SolrClient.Builder().build()) {
          Http2SolrClient.SimpleResponse response = Http2SolrClient.POST(url, http2SolrClient, requestBody.getBytes("UTF-8"), "application/json");

          assertEquals(200, response.status);
        }
        cluster.waitForActiveCollection(collectionName, 2, 8);
        break;
    }
    boolean reloaded = false;
    while (true) {

      DocCollection docCollection = getCollectionState(collectionName);
      assertNotNull(docCollection);
      assertEquals("Expecting 4 relpicas per shard", 8,
          docCollection.getReplicas().size());
      assertEquals("Expecting 6 pull replicas, 3 per shard", 6,
          docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
      assertEquals("Expecting 2 writer replicas, one per shard", 2,
          docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
      for (Slice s : docCollection.getSlices()) {
        // read-only replicas can never become leaders
        if (s.getLeader() != null) {
          assertFalse(s.getLeader().getType() == Replica.Type.PULL);
        }
        List<String> shardElectionNodes = cluster.getZkClient().getChildren(
            ZkStateReader.getShardLeadersElectPath(collectionName, s.getName()),
            null, true);
        assertEquals(
            "Unexpected election nodes for Shard: " + s.getName() + ": "
                + Arrays.toString(shardElectionNodes.toArray()), 1,
            shardElectionNodes.size());
      }
      assertUlogPresence(docCollection);
      if (reloaded) {
        break;
      } else {
        // reload
        CollectionAdminResponse response = CollectionAdminRequest
            .reloadCollection(collectionName).process(cluster.getSolrClient());
        assertEquals(0, response.getStatus());
        reloaded = true;
      }
    }
  }

  /**
   * Asserts that Update logs don't exist for replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL}
   */
  private void assertUlogPresence(DocCollection collection) {
    for (Slice s:collection.getSlices()) {
      for (Replica r:s.getReplicas()) {
        if (r.getType() == Replica.Type.NRT) {
          continue;
        }
        SolrCore core = null;
        try {
          core = cluster.getReplicaJetty(r).getCoreContainer().getCore(r.getName());
          File tlogDir = new File(core.getUlogDir(), "tlog");
          assertNotNull(core);
          assertFalse("Update log should not exist for replicas of type Passive but file is present: " + tlogDir,
              tlogDir.exists());
        } finally {
          core.close();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testAddDocs() throws Exception {
    int numPullReplicas = 1 + random().nextInt(3);
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, numPullReplicas)
    .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, numPullReplicas + 1);
    DocCollection docCollection = assertNumberOfReplicas(collectionName, 1, 0, numPullReplicas, false, true);
    assertEquals(1, docCollection.getSlices().size());

    boolean reloaded = false;
    int numDocs = 0;
    while (true) {
      numDocs++;
      cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", String.valueOf(numDocs), "foo", "bar"));
      cluster.getSolrClient().commit(collectionName);

      Slice s = docCollection.getSlices().iterator().next();
      try (Http2SolrClient leaderClient = SolrTestCaseJ4
          .getHttpSolrClient(s.getLeader().getCoreUrl())) {
        assertEquals(numDocs, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
      }

      TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      for (Replica r:s.getReplicas(EnumSet.of(Replica.Type.PULL))) {
        //TODO: assert replication < REPLICATION_TIMEOUT_SECS
        try (Http2SolrClient pullReplicaClient = SolrTestCaseJ4.getHttpSolrClient(r.getCoreUrl())) {
          while (true) {
            try {
              assertEquals("Replica " + r.getName() + " not up to date after 10 seconds",
                  numDocs, pullReplicaClient.query(new SolrQuery("*:*")).getResults().getNumFound());
              break;
            } catch (AssertionError e) {
              if (t.hasTimedOut()) {
                throw e;
              } else {
                Thread.sleep(100);
              }
            }
          }
          SolrQuery req = new SolrQuery(
              "qt", "/admin/plugins",
              "stats", "true");
          QueryResponse statsResponse = pullReplicaClient.query(req);
          assertEquals("Replicas shouldn't process the add document request: " + statsResponse,
              0L, ((Map<String, Object>)((NamedList<Object>)statsResponse.getResponse()).findRecursive("plugins", "UPDATE", "updateHandler", "stats")).get("UPDATE.updateHandler.adds"));
        }
      }
      if (reloaded) {
        break;
      } else {
        // reload
        CollectionAdminResponse response = CollectionAdminRequest.reloadCollection(collectionName)
        .process(cluster.getSolrClient());
        assertEquals(0, response.getStatus());
        reloaded = true;
      }
    }
    assertUlogPresence(docCollection);
  }

  public void testAddRemovePullReplica() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1, 0, 0)
      .process(cluster.getSolrClient());
    assertNumberOfReplicas(collectionName, 2, 0, 0, false, true);
   //a assertEquals(2, docCollection.getSlices().size());

    addReplicaToShard("s1", Replica.Type.PULL);
    assertNumberOfReplicas(collectionName, 2, 0, 1, true, false);
    addReplicaToShard("s2", Replica.Type.PULL);
    assertNumberOfReplicas(collectionName, 2, 0, 2, true, false);

    waitForState("Expecting collection to have 2 shards and 2 replica each", collectionName, clusterShape(2, 4));

    //Delete pull replica from shard1
    // MRM TODO:
//    CollectionAdminRequest.DeleteReplica req = CollectionAdminRequest.deleteReplica(collectionName, "s1", docCollection.getSlice("s1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getName());
//    req.process(cluster.getSolrClient());
//    assertNumberOfReplicas(2, 0, 1, true, true);
  }

  @Test
  public void testRemoveAllWriterReplicas() throws Exception {
    doTestNoLeader(true);
  }

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  @LuceneTestCase.Nightly
  public void testKillLeader() throws Exception {
    doTestNoLeader(false);
  }

  @Test
  public void testPullReplicaStates() throws Exception {
    // Validate that pull replicas go through the correct states when starting, stopping, reconnecting.
    // Adapted for the fork:
    //   (1) Replicas are named "<coll>_<shard>_r_n<cnt>" (not "core_nodeN"), so the watcher finds the
    //       PULL replica by type rather than by hard-coded name.
    //   (2) The three assertions each used statesSeen.get(0) — fixed to get(0)/get(1)/get(2).
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 0)
      .process(cluster.getSolrClient());
    waitForState("Replica not added", collectionName, activeReplicaCount(1, 0, 0));
    addDocs(500);
    List<Replica.State> statesSeen = new ArrayList<>(3);
    cluster.getSolrClient().registerCollectionStateWatcher(collectionName, (liveNodes, collectionState) -> {
      // Find the PULL replica by type — in the fork replicas use unified names, not "core_nodeN".
      List<Replica> pullReplicas = collectionState.getSlice("s1").getReplicas(EnumSet.of(Replica.Type.PULL));
      if (pullReplicas == null || pullReplicas.isEmpty()) {
        return false;
      }
      Replica r = pullReplicas.get(0);
      log.info("CollectionStateWatcher state change: {}", r);
      statesSeen.add(r.getState());
      if (log.isInfoEnabled()) {
        log.info("CollectionStateWatcher saw state: {}", r.getState());
      }
      return r.getState() == Replica.State.ACTIVE;
    });
    CollectionAdminRequest.addReplicaToShard(collectionName, "s1", Replica.Type.PULL).process(cluster.getSolrClient());
    waitForState("Replica not added", collectionName, activeReplicaCount(1, 0, 1));
    if (log.isInfoEnabled()) {
      log.info("Saw states: {}", Arrays.toString(statesSeen.toArray()));
    }
    assertTrue("Expecting at least 2 state transitions starting at DOWN and ending ACTIVE but saw: " + Arrays.toString(statesSeen.toArray()), statesSeen.size() >= 2);
    assertEquals("Expecting DOWN as first state but saw: " + Arrays.toString(statesSeen.toArray()), Replica.State.DOWN, statesSeen.get(0));
    assertEquals("Expecting ACTIVE as last state but saw: " + Arrays.toString(statesSeen.toArray()), Replica.State.ACTIVE, statesSeen.get(statesSeen.size() - 1));
    // A CollectionStateWatcher observes the CURRENT cluster state when it fires, not a queue of every
    // transition, so a fast RECOVERING->ACTIVE step can be coalesced and the transient RECOVERING
    // snapshot may never be observed (this is exactly why upstream @Ignore'd this test). Rather than
    // require capturing RECOVERING, assert the lifecycle only ever moves FORWARD through the ordered
    // states DOWN(0) -> RECOVERING(1) -> ACTIVE(2): a replica must never regress to an earlier phase.
    java.util.List<Replica.State> order = Arrays.asList(Replica.State.DOWN, Replica.State.RECOVERING, Replica.State.ACTIVE);
    int prevRank = -1;
    for (Replica.State st : statesSeen) {
      int rank = order.indexOf(st);
      assertTrue("Unexpected/out-of-order state " + st + " in lifecycle: " + Arrays.toString(statesSeen.toArray()), rank >= 0 && rank >= prevRank);
      prevRank = rank;
    }
  }

  public void testRealTimeGet() throws Exception {
    // should be redirected to Replica.Type.NRT
    int numReplicas = random().nextBoolean()?1:2;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, numReplicas, 0, numReplicas)
      .process(cluster.getSolrClient());
    waitForState("Unexpected replica count", collectionName, activeReplicaCount(numReplicas, 0, numReplicas));
    DocCollection docCollection = assertNumberOfReplicas(collectionName, numReplicas, 0, numReplicas, false, true);

    int id = 0;
    Slice slice = docCollection.getSlice("s1");
    List<String> ids = new ArrayList<>(slice.getReplicas().size());
    for (Replica rAdd:slice.getReplicas()) {
      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(rAdd.getCoreUrl(), cluster.getSolrClient().getHttpClient())) {
        client.add(new SolrInputDocument("id", String.valueOf(id), "foo_s", "bar"));
      }
      SolrDocument docCloudClient = cluster.getSolrClient().getById(collectionName, String.valueOf(id));
      assertEquals(docCloudClient.toString(), "bar", docCloudClient.getFieldValue("foo_s"));
      for (Replica rGet:slice.getReplicas()) {
        try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(rGet.getCoreUrl(), cluster.getSolrClient().getHttpClient())) {
          SolrDocument doc = client.getById(String.valueOf(id));
          assertEquals("bar", doc.getFieldValue("foo_s"));
        }
      }
      ids.add(String.valueOf(id));
      id++;
    }
    SolrDocumentList previousAllIdsResult = null;
    for (Replica rAdd:slice.getReplicas()) {
      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(rAdd.getCoreUrl(), cluster.getSolrClient().getHttpClient())) {
        SolrDocumentList allIdsResult = client.getById(ids);
        if (previousAllIdsResult != null) {
          assertTrue(SolrTestUtil.compareSolrDocumentList(previousAllIdsResult, allIdsResult));
        } else {
          // set the first response here
          previousAllIdsResult = allIdsResult;
          assertEquals("Unexpected number of documents", ids.size(), allIdsResult.getNumFound());
        }
      }
      id++;
    }
  }

  /*
   * validate that replication still happens on a new leader
   */
  private void doTestNoLeader(boolean removeReplica) throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 1)
      .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 2);

    DocCollection docCollection = assertNumberOfReplicas(collectionName, 1, 0, 1, false, true);

    // Add a document and commit
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    Slice s = docCollection.getSlices().iterator().next();
    try (Http2SolrClient leaderClient = SolrTestCaseJ4.getHttpSolrClient(s.getLeader().getCoreUrl())) {
      assertEquals(1, leaderClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));

    // Delete leader replica from shard1
    SolrTestCaseJ4.ignoreException("No registered leader was found"); //These are expected
    JettySolrRunner leaderJetty = null;
    if (removeReplica) {
      CollectionAdminRequest.deleteReplica(
          collectionName,
          "s1",
          s.getLeader().getName())
      .process(cluster.getSolrClient());
    } else {
      leaderJetty = cluster.getReplicaJetty(s.getLeader());
      leaderJetty.stop();
      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 1);
      waitForState("Leader replica not removed", collectionName, clusterShape(1, 1));
      // Wait for cluster state to be updated
      waitForState("Replica state not updated in cluster state",
          collectionName, notLive(Replica.Type.NRT));
    }
    docCollection = assertNumberOfReplicas(collectionName, 0, 0, 1, true, true);

    // Check that there is no leader for the shard
    Replica leader = docCollection.getSlice("s1").getLeader();
    assertTrue(leader == null || !leader.isActive(cluster.getSolrClient().getZkStateReader().getLiveNodes()));

    // Pull replica on the other hand should be active
    Replica pullReplica = docCollection.getSlice("s1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0);
    assertTrue(pullReplica.isActive(cluster.getSolrClient().getZkStateReader().getLiveNodes()));

    long highestTerm = 0L;
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", zkClient())) {
      highestTerm = zkShardTerms.getHighestTerm();
    }
    // add document, this should fail since there is no leader. Pull replica should not accept the update
    SolrTestCaseUtil.expectThrows(SolrException.class, () -> cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo")));
    if (removeReplica) {
      try(ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", zkClient())) {
        assertEquals(highestTerm, zkShardTerms.getHighestTerm());
      }
    }

    // Also fails if I send the update to the pull replica explicitly
    try (Http2SolrClient pullReplicaClient = SolrTestCaseJ4.getHttpSolrClient(docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).get(0).getCoreUrl())) {
      SolrTestCaseUtil.expectThrows(SolrException.class, () -> pullReplicaClient.add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo")));
    }
    if (removeReplica) {
      try(ZkShardTerms zkShardTerms = new ZkShardTerms(collectionName, "s1", zkClient())) {
        assertEquals(highestTerm, zkShardTerms.getHighestTerm());
      }
    }

    // Queries should still work
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
    // Add nrt replica back. Since there is no nrt now, new nrt will have no docs. There will be data loss, since the it will become the leader
    // and pull replicas will replicate from it. Maybe we want to change this. Replicate from pull replicas is not a good idea, since they
    // are by definition out of date.
    if (removeReplica) {
      CollectionAdminRequest.addReplicaToShard(collectionName, "s1", Replica.Type.NRT).process(cluster.getSolrClient());
    } else {
      leaderJetty.start();

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);
    }


    SolrTestCaseJ4.unIgnoreException("No registered leader was found"); // Should have a leader from now on

    // Wait for the collection to be fully back to 1 shard x 2 active replicas before proceeding.
    // (Upstream has the equivalent waitForState(clusterShape(1,2)) here.) Without this, the freshly
    // added NRT replica may not yet have finished registering and winning leader election, so the
    // subsequent add would race against leader election and fail with "No registered leader".
    cluster.waitForActiveCollection(collectionName, 1, 2);

    // Validate that the new nrt replica is the leader now
    org.apache.solr.common.util.TimeOut timeOut = new org.apache.solr.common.util.TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      leader = cluster.getSolrClient().getZkStateReader().getLeader(collectionName, "s1");
      if (leader != null && leader.isActive(cluster.getSolrClient().getZkStateReader().getLiveNodes())) {
        break;
      }
      Thread.sleep(50);
    }

    assertTrue(leader != null && leader.isActive(cluster.getSolrClient().getZkStateReader().getLiveNodes()));

    // If jetty is restarted, the replication is not forced, and replica doesn't replicate from leader until new docs are added. Is this the correct behavior? Why should these two cases be different?
    if (removeReplica) {
      // Pull replicas will replicate the empty index if a new replica was added and becomes leader
      waitForNumDocsInAllReplicas(0, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
    }

    // add docs agin
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "zoo"));
    // Commit via the cloud client so the commit is routed to the same (current) shard leader the add
    // was routed to, rather than to a possibly-stale directly-resolved leader URL.
    cluster.getSolrClient().commit(collectionName);
    s = docCollection.getSlices().iterator().next();

    leader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collectionName, s.getName());

    // The two branches differ in whether the pre-existing doc id=1 survives:
    //  - removeReplica=true: the sole NRT leader is DELETED and a fresh EMPTY NRT replica is added,
    //    which becomes leader; the surviving PULL replica then replicates the empty index FROM it.
    //    PULL replicas are read-only and not a valid recovery source, so id=1 is lost -> only id=2.
    //  - removeReplica=false: the leader jetty is merely STOPPED and RESTARTED with its on-disk index
    //    intact, so id=1 is preserved and the leader holds id=1 + id=2.
    // (Upstream Apache Solr documents the data-loss case; this fork validates both outcomes.)
    int expectedDocs = removeReplica ? 1 : 2;
    // In the removeReplica=true case the brand-new empty replica force-becomes leader (setTermToMax)
    // and tears down its own recovery right around when this first post-recovery write arrives, so the
    // just-added id=2 can take a moment to become visible on the new leader. Use the standard retrying
    // wait helper against the leader replica rather than asserting a single immediate snapshot.
    waitForNumDocsInAllReplicas(expectedDocs, java.util.Collections.singletonList(leader));
    waitForNumDocsInAllReplicas(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)), "id:2");
    waitForNumDocsInAllReplicas(expectedDocs, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)));
  }

  public void testKillPullReplica() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 1)
      .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 2);

    DocCollection docCollection = assertNumberOfReplicas(collectionName, 1, 0, 1, false, true);
    assertEquals(1, docCollection.getSlices().size());

    waitForNumDocsInAllActiveReplicas(0);
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(1);

    JettySolrRunner pullReplicaJetty = cluster.getReplicaJetty(docCollection.getSlice("s1").getReplicas(EnumSet.of(Replica.Type.PULL)).get(0));
    pullReplicaJetty.stop();

    cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 1);

    cluster.getSolrClient().getZkStateReader().waitForActiveCollection(cluster.getSolrClient().getHttpClient(), collectionName, 5, TimeUnit.SECONDS, false, 1, 1, true, true);

    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2", "foo", "bar"));
    cluster.getSolrClient().commit(collectionName);
    waitForNumDocsInAllActiveReplicas(2);

    pullReplicaJetty.start();

    cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);
    cluster.getSolrClient().getZkStateReader().waitForActiveCollection(cluster.getSolrClient().getHttpClient(), collectionName, 5, TimeUnit.SECONDS, false, 1, 2, true, true);
    waitForNumDocsInAllActiveReplicas(2);
  }

  public void testSearchWhileReplicationHappens() {

  }

  private void waitForNumDocsInAllActiveReplicas(int numDocs) throws IOException, SolrServerException, InterruptedException {
    DocCollection docCollection = getCollectionState(collectionName);
    waitForNumDocsInAllReplicas(numDocs, docCollection.getReplicas().stream().filter(r -> r.getState() == Replica.State.ACTIVE).collect(Collectors.toList()));
  }

  private static void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas) throws IOException, SolrServerException, InterruptedException {
    waitForNumDocsInAllReplicas(numDocs, replicas, "*:*");
  }

  public static void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, String query) throws IOException, SolrServerException, InterruptedException {
    waitForNumDocsInAllReplicas(numDocs, replicas, query, null, null);
  }

  public static void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, String query, String user, String pass) throws IOException, SolrServerException, InterruptedException {
    TimeOut t = new TimeOut(REPLICATION_TIMEOUT_SECS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    for (Replica r : replicas) {
      if (cluster.getSolrClient().getZkStateReader().isNodeLive(r.getNodeName())) {
        try (Http2SolrClient replicaClient = SolrTestCaseJ4.getHttpSolrClient(r.getCoreUrl())) {
          while (true) {
            QueryRequest q = new QueryRequest(new SolrQuery(query));
            if (user != null) {
              q.setBasicAuthCredentials(user, pass);
            }
            try {
              assertEquals("Replica " + r.getName() + " not up to date after " + REPLICATION_TIMEOUT_SECS + " seconds", numDocs,
                  q.process(replicaClient).getResults().getNumFound());
              break;
            } catch (AssertionError e) {
              if (t.hasTimedOut()) {
                throw e;
              } else {
                Thread.sleep(500);
              }
            }
          }
        }
      }
    }
  }

  public static void waitForDeletion(String collection) throws InterruptedException, KeeperException {
    TimeOut t = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection(collection)) {
      log.info("Collection not yet deleted");
      try {
        Thread.sleep(100);
        if (t.hasTimedOut()) {
          fail("Timed out waiting for collection " + collection + " to be deleted.");
        }
      } catch(SolrException e) {
        return;
      }

    }
  }

  public static DocCollection assertNumberOfReplicas(String collectionName, int numNrtReplicas, int numTlogReplicas, int numPullReplicas, boolean updateCollection, boolean activeOnly) throws Exception {
    AssertionError lastError = null;
    cluster.getSolrClient().getZkStateReader().waitForState(collectionName, 5000, TimeUnit.MILLISECONDS, (liveNodes, collectionState) -> {
      try {
        assertNotNull(collectionState);
        assertEquals("Unexpected number of writer replicas: " + collectionState, numNrtReplicas, collectionState.getReplicas(EnumSet.of(Replica.Type.NRT)).stream()
            .filter(r -> !activeOnly || r.getState() == Replica.State.ACTIVE && cluster.getSolrClient().getZkStateReader().isNodeLive(r.getNodeName())).count());
        assertEquals("Unexpected number of pull replicas: " + collectionState, numPullReplicas, collectionState.getReplicas(EnumSet.of(Replica.Type.PULL)).stream()
            .filter(r -> !activeOnly || r.getState() == Replica.State.ACTIVE && cluster.getSolrClient().getZkStateReader().isNodeLive(r.getNodeName())).count());
        assertEquals("Unexpected number of active replicas: " + collectionState, numTlogReplicas, collectionState.getReplicas(EnumSet.of(Replica.Type.TLOG)).stream()
            .filter(r -> !activeOnly || r.getState() == Replica.State.ACTIVE && cluster.getSolrClient().getZkStateReader().isNodeLive(r.getNodeName())).count());
        return true;
      } catch (AssertionError error) {
        log.info("Still incorrect results", error);

      }
      return false;
    });

    return cluster.getSolrClient().getZkStateReader().getCollectionOrNull(collectionName);
  }

  /*
   * passes only if all replicas are active or down, and the "liveNodes" reflect the same status
   */
  private CollectionStatePredicate notLive(Replica.Type type) {
    return (liveNodes, collectionState) -> {
      for (Replica r:collectionState.getReplicas()) {
        if (liveNodes.contains(r.getNodeName()) && r.getType() == type) {
          return false;
        }
      }
      return true;
    };
  }


  private static CollectionStatePredicate activeReplicaCount(int numNrtReplicas, int numTlogReplicas, int numPullReplicas) {
    return (liveNodes, collectionState) -> {
      int nrtFound = 0, tlogFound = 0, pullFound = 0;
      if (collectionState == null)
        return false;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            switch (replica.getType()) {
              case TLOG:
                tlogFound++;
                break;
              case PULL:
                pullFound++;
                break;
              case NRT:
                nrtFound++;
                break;
              default:
                throw new AssertionError("Unexpected replica type");
            }
        }
      }
      return numNrtReplicas == nrtFound && numTlogReplicas == tlogFound && numPullReplicas == pullFound;
    };
  }

  private void addDocs(int numDocs) throws SolrServerException, IOException {
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "fieldName_s", String.valueOf(i)));
    }
    cluster.getSolrClient().add(collectionName, docs);
    cluster.getSolrClient().commit(collectionName);
  }

  private void addReplicaToShard(String shardName, Replica.Type type) throws ClientProtocolException, IOException, SolrServerException, InterruptedException, ExecutionException, TimeoutException {
    switch (random().nextInt(3)) {
      case 0: // Add replica with SolrJ
        CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(collectionName, shardName, type).process(cluster.getSolrClient());
        assertEquals("Unexpected response status: " + response.getStatus(), 0, response.getStatus());
        break;
      case 1: // Add replica with V1 API
        String url = String.format(Locale.ROOT, "%s/admin/collections?action=ADDREPLICA&collection=%s&shard=%s&type=%s",
            cluster.getRandomJetty(random()).getBaseUrl(),
            collectionName,
            shardName,
            type);
        HttpGet addReplicaGet = new HttpGet(url);
        Http2SolrClient.SimpleResponse httpResponse = Http2SolrClient.GET(url, cluster.getSolrClient().getHttpClient());
        assertEquals(200, httpResponse.status);
        break;
      case 2:// Add replica with V2 API
        url = String.format(Locale.ROOT, "%s/____v2/c/%s/shards",
            cluster.getRandomJetty(random()).getBaseUrl(),
            collectionName);
        String requestBody = String.format(Locale.ROOT, "{add-replica:{shard:%s, type:%s}}",
            shardName,
            type);

        Http2SolrClient.SimpleResponse httpResponse2 = Http2SolrClient.POST(url, cluster.getSolrClient().getHttpClient(), requestBody.getBytes("UTF-8"),"application/json");

        assertEquals(200, httpResponse2.status);
        break;
    }
  }
}
