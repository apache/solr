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

import static org.apache.solr.common.cloud.Replica.State.DOWN;
import static org.apache.solr.common.cloud.Replica.State.RECOVERING;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates HTTP partitions between a leader and replica but the replica does not lose its
 * ZooKeeper connection.
 */
@LuceneTestCase.Nightly // there are recovery commands that take a while to time out
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class HttpPartitionTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // To prevent the test assertions firing too fast before cluster state
  // recognizes (and propagates) partitions
  protected static final long sleepMsBeforeHealPartition = 300;

  // give plenty of time for replicas to recover when running in slow Jenkins test envs
  protected static final int maxWaitSecsToSeeAllActive = 90;

  @BeforeClass
  public static void setupSysProps() {
    System.setProperty("socketTimeout", "10000");
    System.setProperty("distribUpdateSoTimeout", "10000");
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("solr.retries.on.forward", "0");
    System.setProperty("solr.retries.to.followers", "0");
  }

  public HttpPartitionTest() {
    super();
    sliceCount = 2;
    fixShardCount(3);
  }

  /** Overrides the parent implementation to install a SocketProxy in-front of the Jetty server. */
  @Override
  public JettySolrRunner createJetty(
      Path solrHome,
      String dataDir,
      String shardList,
      String solrConfigOverride,
      String schemaOverride,
      Replica.Type replicaType)
      throws Exception {
    return createProxiedJetty(
        solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, replicaType);
  }

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    testDoRecoveryOnRestart();

    // test a 1x2 collection
    testRf2();

    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    // now do similar for a 1x3 collection while taking 2 replicas on-and-off
    if (TEST_NIGHTLY) {
      // each time
      testRf3();
    }

    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    // have the leader lose its Zk session temporarily
    testLeaderZkSessionLoss();

    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    log.info("HttpPartitionTest succeeded ... shutting down now!");
  }

  private void testDoRecoveryOnRestart() throws Exception {
    String testCollectionName = "collDoRecoveryOnRestart";
    try {
      // Inject pausing in recovery op, hence the replica won't be able to finish recovery

      TestInjection.prepRecoveryOpPauseForever = "true:100";

      createCollection(testCollectionName, "conf1", 1, 2);

      sendDoc(testCollectionName, 1);

      JettySolrRunner leaderJetty =
          getJettyOnPort(getReplicaPort(getShardLeader(testCollectionName, "shard1", 1000)));
      List<Replica> notLeaders =
          ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
      assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 1);

      SocketProxy proxy0 = getProxyForReplica(notLeaders.get(0));
      SocketProxy leaderProxy =
          getProxyForReplica(getShardLeader(testCollectionName, "shard1", 1000));

      proxy0.close();
      leaderProxy.close();

      // indexing during a partition
      int achievedRf = sendDoc(testCollectionName, 2, leaderJetty);
      assertEquals("Unexpected achieved replication factor", 1, achievedRf);
      try (ZkShardTerms zkShardTerms =
          new ZkShardTerms(
              testCollectionName, "shard1", ZkStateReader.from(cloudClient).getZkClient())) {
        assertFalse(zkShardTerms.canBecomeLeader(notLeaders.get(0).getName()));
      }
      waitForState(testCollectionName, notLeaders.get(0).getName(), DOWN, 10000);

      // heal partition
      proxy0.reopen();
      leaderProxy.reopen();

      waitForState(testCollectionName, notLeaders.get(0).getName(), RECOVERING, 10000);

      System.clearProperty("solrcloud.skip.autorecovery");
      JettySolrRunner notLeaderJetty = getJettyOnPort(getReplicaPort(notLeaders.get(0)));
      String notLeaderNodeName = notLeaderJetty.getNodeName();
      notLeaderJetty.stop();

      ZkStateReader.from(cloudClient)
          .waitForLiveNodes(
              15, TimeUnit.SECONDS, SolrCloudTestCase.missingLiveNode(notLeaderNodeName));

      notLeaderJetty.start();
      ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, 130);
      assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 2);
    } finally {
      TestInjection.prepRecoveryOpPauseForever = null;
      TestInjection.notifyPauseForeverDone();
    }

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);
  }

  protected void testRf2() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x2";
    createCollectionRetry(testCollectionName, "conf1", 1, 2);

    sendDoc(testCollectionName, 1);

    Replica notLeader =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive)
            .get(0);
    JettySolrRunner leaderJetty =
        getJettyOnPort(getReplicaPort(getShardLeader(testCollectionName, "shard1", 1000)));

    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy = getProxyForReplica(notLeader);
    SocketProxy leaderProxy =
        getProxyForReplica(getShardLeader(testCollectionName, "shard1", 1000));

    proxy.close();
    leaderProxy.close();

    // indexing during a partition
    sendDoc(testCollectionName, 2, leaderJetty);
    // replica should publish itself as DOWN if the network is not healed after some amount time
    waitForState(testCollectionName, notLeader.getName(), DOWN, 10000);

    proxy.reopen();
    leaderProxy.reopen();

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);

    int achievedRf = sendDoc(testCollectionName, 3);
    if (achievedRf == 1) {
      // this case can happen when leader reuse an connection get established before network
      // partition
      // TODO: Remove when SOLR-11776 get committed
      ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    }

    // sent 3 docs in so far, verify they are on the leader and replica
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 3);

    // Get the max version from the replica core to make sure it gets updated after recovery (see
    // SOLR-7625)
    JettySolrRunner replicaJetty = getJettyOnPort(getReplicaPort(notLeader));
    CoreContainer coreContainer = replicaJetty.getCoreContainer();
    String coreName = notLeader.getCoreName();
    try (SolrCore core = coreContainer.getCore(coreName)) {
      assertNotNull("Core '" + coreName + "' not found for replica: " + notLeader.getName(), core);
    }

    // now up the stakes and do more docs
    int numDocs = TEST_NIGHTLY ? 1000 : 105;
    boolean hasPartition = false;
    for (int d = 0; d < numDocs; d++) {
      // create / restore partition every 100 docs
      if (d % 10 == 0) {
        if (hasPartition) {
          proxy.reopen();
          leaderProxy.reopen();
          hasPartition = false;
        } else {
          if (d >= 10) {
            proxy.close();
            leaderProxy.close();
            hasPartition = true;
            Thread.sleep(sleepMsBeforeHealPartition);
          }
        }
      }
      // always send doc directly to leader without going through proxy
      sendDoc(testCollectionName, d + 4, leaderJetty); // 4 is offset as we've already indexed 1-3
    }

    // restore connectivity if lost
    if (hasPartition) {
      proxy.reopen();
      leaderProxy.reopen();
    }

    notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);

    // verify all docs received
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, numDocs + 3);

    log.info("testRf2 succeeded ... deleting the {} collection", testCollectionName);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);
  }

  protected void waitForState(String collection, String replicaName, Replica.State state, long ms)
      throws KeeperException, InterruptedException {
    ZkStateReader zkr = ZkStateReader.from(cloudClient);

    try {
      zkr.waitForState(
          collection,
          ms,
          TimeUnit.MILLISECONDS,
          c -> {
            Collection<Slice> slices = c.getActiveSlices();
            Slice slice = slices.iterator().next();
            Replica partitionedReplica = slice.getReplica(replicaName);
            Replica.State replicaState = partitionedReplica.getState();
            return replicaState == state;
          });
    } catch (TimeoutException e) {
      fail("Timeout waiting for state " + state + " of replica " + replicaName);
    }
  }

  protected void testRf3() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x3";
    createCollectionRetry(testCollectionName, "conf1", 1, 3);

    sendDoc(testCollectionName, 1);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);
    assertEquals(
        "Expected 2 replicas for collection "
            + testCollectionName
            + " but found "
            + notLeaders.size()
            + "; clusterState: "
            + printClusterStateInfo(testCollectionName),
        2,
        notLeaders.size());
    JettySolrRunner leaderJetty =
        getJettyOnPort(getReplicaPort(getShardLeader(testCollectionName, "shard1", 1000)));

    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy0 = getProxyForReplica(notLeaders.get(0));
    SocketProxy leaderProxy =
        getProxyForReplica(getShardLeader(testCollectionName, "shard1", 1000));

    proxy0.close();
    leaderProxy.close();

    // indexing during a partition
    sendDoc(testCollectionName, 2, leaderJetty);

    Thread.sleep(sleepMsBeforeHealPartition);
    proxy0.reopen();

    SocketProxy proxy1 = getProxyForReplica(notLeaders.get(1));
    proxy1.close();

    sendDoc(testCollectionName, 3, leaderJetty);

    Thread.sleep(sleepMsBeforeHealPartition);
    proxy1.reopen();

    leaderProxy.reopen();

    // sent 4 docs in so far, verify they are on the leader and replica
    notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);

    sendDoc(testCollectionName, 4);

    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 4);

    log.info("testRf3 succeeded ... deleting the {} collection", testCollectionName);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);
  }

  // test inspired by SOLR-6511
  @SuppressWarnings({"try"})
  protected void testLeaderZkSessionLoss() throws Exception {

    String testCollectionName = "c8n_1x2_leader_session_loss";
    createCollectionRetry(testCollectionName, "conf1", 1, 2);

    sendDoc(testCollectionName, 1);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    assertEquals(
        "Expected 1 replicas for collection "
            + testCollectionName
            + " but found "
            + notLeaders.size()
            + "; clusterState: "
            + printClusterStateInfo(testCollectionName),
        1,
        notLeaders.size());

    Replica leader = ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, "shard1");
    assertNotNull(
        "Could not find leader for shard1 of "
            + testCollectionName
            + "; clusterState: "
            + printClusterStateInfo(testCollectionName),
        leader);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(2));
    doc.addField("a_t", "hello" + 2);

    // cause leader migration by expiring the current leader's zk session
    chaosMonkey.expireSession(leaderJetty);

    String expectedNewLeaderCoreNodeName = notLeaders.get(0).getName();
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      String currentLeaderName = null;
      try {
        Replica currentLeader =
            ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, "shard1");
        currentLeaderName = currentLeader.getName();
      } catch (Exception exc) {
      }

      if (expectedNewLeaderCoreNodeName.equals(currentLeaderName))
        break; // new leader was elected after zk session expiration

      Thread.sleep(500);
    }

    Replica currentLeader =
        ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, "shard1");
    assertEquals(expectedNewLeaderCoreNodeName, currentLeader.getName());

    // TODO: This test logic seems to be timing dependent and fails on Jenkins
    // need to come up with a better approach
    if (log.isInfoEnabled()) {
      log.info("Sending doc 2 to old leader {}", leader.getName());
    }
    try (SolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName)) {

      leaderSolr.add(doc);
      leaderSolr.close();

      // if the add worked, then the doc must exist on the new leader
      try (SolrClient newLeaderSolr = getHttpSolrClient(currentLeader, testCollectionName)) {
        assertDocExists(newLeaderSolr, "2");
      }

    } catch (SolrException exc) {
      // this is ok provided the doc doesn't exist on the current leader
      try (SolrClient client = getHttpSolrClient(currentLeader, testCollectionName)) {
        client.add(doc); // this should work
      }
    }

    List<Replica> participatingReplicas =
        getActiveOrRecoveringReplicas(testCollectionName, "shard1");
    Set<String> replicasToCheck = new HashSet<>();
    for (Replica stillUp : participatingReplicas) replicasToCheck.add(stillUp.getName());
    waitToSeeReplicasActive(testCollectionName, "shard1", replicasToCheck, 30);
    assertDocsExistInAllReplicas(participatingReplicas, testCollectionName, 1, 2);

    log.info(
        "testLeaderZkSessionLoss succeeded ... deleting the {} collection", testCollectionName);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);
  }

  protected List<Replica> getActiveOrRecoveringReplicas(String testCollectionName, String shardId) {
    Map<String, Replica> activeReplicas = new HashMap<>();
    ZkStateReader zkr = ZkStateReader.from(cloudClient);
    ClusterState cs = zkr.getClusterState();
    assertNotNull(cs);
    for (Slice shard : cs.getCollection(testCollectionName).getActiveSlices()) {
      if (shard.getName().equals(shardId)) {
        for (Replica replica : shard.getReplicas()) {
          final Replica.State state = replica.getState();
          if (state == Replica.State.ACTIVE || state == Replica.State.RECOVERING) {
            activeReplicas.put(replica.getName(), replica);
          }
        }
      }
    }
    List<Replica> replicas = new ArrayList<>();
    replicas.addAll(activeReplicas.values());
    return replicas;
  }

  /**
   * Assert docs exists in {@code notLeaders} replicas, docs must also exist in the shard1 leader as
   * well. This method uses RTG for validation therefore it must work for asserting both TLOG and
   * NRT replicas.
   */
  protected void assertDocsExistInAllReplicas(
      List<Replica> notLeaders, String testCollectionName, int firstDocId, int lastDocId)
      throws Exception {
    Replica leader =
        ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, "shard1", 10000);
    SolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName);
    List<SolrClient> replicas = new ArrayList<SolrClient>(notLeaders.size());

    for (Replica r : notLeaders) {
      replicas.add(getHttpSolrClient(r, testCollectionName));
    }
    try {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists(leaderSolr, docId);
        for (SolrClient replicaSolr : replicas) {
          assertDocExists(replicaSolr, docId);
        }
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (SolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }

  protected SolrClient getHttpSolrClient(Replica replica, String collection) {
    return getHttpSolrClient(replica.getBaseUrl(), collection);
  }

  // Send doc directly to a server (without going through proxy)
  protected int sendDoc(String collectionName, int docId, JettySolrRunner leaderJetty)
      throws IOException, SolrServerException {
    try (SolrClient solrClient =
        new HttpSolrClient.Builder(leaderJetty.getBaseUrl().toString()).build()) {
      return sendDoc(docId, solrClient, collectionName);
    }
  }

  protected int sendDoc(String collectionName, int docId) throws Exception {
    return sendDoc(docId, cloudClient, collectionName);
  }

  protected int sendDoc(int docId, SolrClient solrClient, String collection)
      throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);

    UpdateRequest up = new UpdateRequest();
    up.add(doc);
    return cloudClient.getMinAchievedReplicationFactor(
        collection, solrClient.request(up, collection));
  }

  /**
   * Query the real-time get handler for a specific doc by ID to verify it exists in the provided
   * server, using distrib=false, so it doesn't route to another replica.
   */
  protected void assertDocExists(SolrClient solr, String docId) throws Exception {
    NamedList<?> rsp = realTimeGetDocId(solr, docId);
    String match = JSONTestUtil.matchObj("/id", rsp.get("doc"), docId);
    assertNull("Doc with id=" + docId + " not found due to: " + match + "; rsp=" + rsp, match);
  }

  private NamedList<Object> realTimeGetDocId(SolrClient solr, String docId)
      throws SolrServerException, IOException {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false"));
    return solr.request(qr);
  }

  @Override
  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();
    String tmp = replicaNode.substring(replicaNode.indexOf(':') + 1);
    if (tmp.indexOf('_') != -1) tmp = tmp.substring(0, tmp.indexOf('_'));
    return Integer.parseInt(tmp);
  }

  protected void waitToSeeReplicasActive(
      String testCollectionName, String shardId, Set<String> replicasToCheck, int maxWaitSecs)
      throws Exception {
    final RTimer timer = new RTimer();

    ZkStateReader zkr = ZkStateReader.from(cloudClient);
    zkr.forceUpdateCollection(testCollectionName);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      ClusterState cs = zkr.getClusterState();
      assertNotNull(cs);
      final DocCollection docCollection = cs.getCollectionOrNull(testCollectionName);
      assertNotNull(docCollection);
      Slice shard = docCollection.getSlice(shardId);
      assertNotNull("No Slice for " + shardId, shard);
      allReplicasUp = true; // assume true

      // wait to see all replicas are "active"
      for (Replica replica : shard.getReplicas()) {
        if (!replicasToCheck.contains(replica.getName())) continue;

        final Replica.State state = replica.getState();
        if (state != Replica.State.ACTIVE) {
          if (log.isInfoEnabled()) {
            log.info("Replica {} is currently {}", replica.getName(), state);
          }
          allReplicasUp = false;
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(200L);
        } catch (Exception ignoreMe) {
        }
        waitMs += 200L;
      }
    } // end while

    if (!allReplicasUp)
      fail(
          "Didn't see replicas "
              + replicasToCheck
              + " come up within "
              + maxWaitMs
              + " ms! ClusterState: "
              + printClusterStateInfo(testCollectionName));

    if (log.isInfoEnabled()) {
      log.info("Took {} ms to see replicas [{}] become active.", timer.getTime(), replicasToCheck);
    }
  }
}
