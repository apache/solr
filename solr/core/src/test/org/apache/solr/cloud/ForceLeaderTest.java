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

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Nightly // this test is currently too slow for non-nightly
public class ForceLeaderTest extends HttpPartitionTest {
  public static final String TEST_COLLECTION = "forceleader_lower_terms_collection";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClassSetup() {
    System.setProperty("socketTimeout", "15000");
    System.setProperty("distribUpdateSoTimeout", "15000");
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("solr.retries.on.forward", "0");
    System.setProperty("solr.retries.to.followers", "0");
  }

  @Test
  @Override
  @Ignore
  public void test() {}

  /**
   * Tests that FORCELEADER can get an active leader even only replicas with term lower than
   * leader's term are live
   */
  @Test
  public void testReplicasInLowerTerms() throws Exception {
    handle.put("maxScore", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    createCollection(TEST_COLLECTION, "conf1", 1, 3);

    try {
      List<Replica> notLeaders =
          ensureAllReplicasAreActive(TEST_COLLECTION, SHARD1, 1, 3, maxWaitSecsToSeeAllActive);
      assertEquals(
          "Expected 2 replicas for collection "
              + TEST_COLLECTION
              + " but found "
              + notLeaders.size()
              + "; clusterState: "
              + printClusterStateInfo(TEST_COLLECTION),
          2,
          notLeaders.size());

      Replica leader = ZkStateReader.from(cloudClient).getLeaderRetry(TEST_COLLECTION, SHARD1);
      JettySolrRunner notLeader0 = getJettyOnPort(getReplicaPort(notLeaders.get(0)));
      ZkController zkController = notLeader0.getCoreContainer().getZkController();

      if (log.isInfoEnabled()) {
        log.info("Before put non leaders into lower term: {}", printClusterStateInfo());
      }
      putNonLeadersIntoLowerTerm(TEST_COLLECTION, SHARD1, zkController, leader, notLeaders);

      for (Replica replica : notLeaders) {
        waitForState(TEST_COLLECTION, replica.getName(), State.DOWN, 60000);
      }
      waitForState(TEST_COLLECTION, leader.getName(), State.DOWN, 60000);
      ZkStateReader.from(cloudClient).forceUpdateCollection(TEST_COLLECTION);
      ClusterState clusterState = cloudClient.getClusterState();
      int numActiveReplicas = getNumberOfActiveReplicas(clusterState, TEST_COLLECTION, SHARD1);
      assertEquals(
          "Expected only 0 active replica but found "
              + numActiveReplicas
              + "; clusterState: "
              + printClusterStateInfo(),
          0,
          numActiveReplicas);

      int numReplicasOnLiveNodes = 0;
      for (Replica rep :
          clusterState.getCollection(TEST_COLLECTION).getSlice(SHARD1).getReplicas()) {
        if (clusterState.getLiveNodes().contains(rep.getNodeName())) {
          numReplicasOnLiveNodes++;
        }
      }
      assertEquals(2, numReplicasOnLiveNodes);
      if (log.isInfoEnabled()) {
        log.info("Before forcing leader: {}", printClusterStateInfo());
      }
      // Assert there is no leader yet
      assertNull(
          "Expected no leader right now. State: "
              + clusterState.getCollection(TEST_COLLECTION).getSlice(SHARD1),
          clusterState.getCollection(TEST_COLLECTION).getSlice(SHARD1).getLeader());

      assertSendDocFails(TEST_COLLECTION, 3);

      log.info("Do force leader...");
      doForceLeader(TEST_COLLECTION, SHARD1);

      // By now we have an active leader. Wait for recoveries to begin
      waitForRecoveriesToFinish(TEST_COLLECTION, ZkStateReader.from(cloudClient), true);

      ZkStateReader.from(cloudClient).forceUpdateCollection(TEST_COLLECTION);
      clusterState = cloudClient.getClusterState();
      if (log.isInfoEnabled()) {
        log.info(
            "After forcing leader: {}",
            clusterState.getCollection(TEST_COLLECTION).getSlice(SHARD1));
      }
      // we have a leader
      Replica newLeader =
          clusterState.getCollectionOrNull(TEST_COLLECTION).getSlice(SHARD1).getLeader();
      assertNotNull(newLeader);
      // leader is active
      assertEquals(State.ACTIVE, newLeader.getState());

      numActiveReplicas = getNumberOfActiveReplicas(clusterState, TEST_COLLECTION, SHARD1);
      assertEquals(2, numActiveReplicas);

      // Assert that indexing works again
      log.info("Sending doc 4...");
      sendDoc(TEST_COLLECTION, 4);
      log.info("Committing...");
      cloudClient.commit(TEST_COLLECTION);
      log.info("Doc 4 sent and commit issued");

      assertDocsExistInAllReplicas(notLeaders, TEST_COLLECTION, 1, 1);
      assertDocsExistInAllReplicas(notLeaders, TEST_COLLECTION, 4, 4);

      if (useTlogReplicas()) {}
      // Docs 1 and 4 should be here. 2 was lost during the partition, 3 had failed to be indexed.
      log.info("Checking doc counts...");
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "*:*");
      if (useTlogReplicas()) {
        TimeOut timeOut = new TimeOut(15, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        timeOut.waitFor(
            "Expected only 2 documents in the index",
            () -> {
              try {
                return 2 == cloudClient.query(TEST_COLLECTION, params).getResults().getNumFound();
              } catch (Exception e) {
                return false;
              }
            });
      } else {
        assertEquals(
            "Expected only 2 documents in the index",
            2,
            cloudClient.query(TEST_COLLECTION, params).getResults().getNumFound());
      }

      bringBackOldLeaderAndSendDoc(TEST_COLLECTION, leader, notLeaders, 5);
    } finally {
      log.info("Cleaning up after the test.");
      // try to clean up
      attemptCollectionDelete(cloudClient, TEST_COLLECTION);
    }
  }

  /**
   * For this test, we need a cloudClient that is not randomized since we need to NEVER send the
   * updates only to the leader. The way the RandomizingCloudSolrClientBuilder works, you can't
   * avoid its internal decision-making process to sometimes send updates only to leaders. We
   * override the definition of this class in AbstractFullDistribZkTestBase to make sure we always
   * use DEFAULT_COLLECTION defined in ForceLeaderTest.
   */
  @Override
  protected CloudSolrClient createCloudClient(String defaultCollection) {
    CloudLegacySolrClient.Builder builder =
        new CloudLegacySolrClient.Builder(
            Collections.singletonList(zkServer.getZkAddress()), Optional.empty());
    builder.withDefaultCollection(TEST_COLLECTION);
    return builder.withConnectionTimeout(30000).withSocketTimeout(120000).build();
  }

  private void putNonLeadersIntoLowerTerm(
      String collectionName,
      String shard,
      ZkController zkController,
      Replica leader,
      List<Replica> notLeaders)
      throws Exception {
    SocketProxy[] nonLeaderProxies = new SocketProxy[notLeaders.size()];
    for (int i = 0; i < notLeaders.size(); i++)
      nonLeaderProxies[i] = getProxyForReplica(notLeaders.get(i));

    sendDoc(collectionName, 1);

    // ok, now introduce a network partition between the leader and both replicas
    log.info("Closing proxies for the non-leader replicas...");
    for (SocketProxy proxy : nonLeaderProxies) proxy.close();
    getProxyForReplica(leader).close();

    // indexing during a partition
    log.info("Sending a doc during the network partition...");
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    sendDoc(collectionName, 2, leaderJetty);

    for (Replica replica : notLeaders) {
      waitForState(collectionName, replica.getName(), State.DOWN, 60000);
    }

    // Kill the leader
    if (log.isInfoEnabled()) {
      log.info("Killing leader for shard1 of {} on node {}", collectionName, leader.getNodeName());
    }
    leaderJetty.stop();

    // Wait for a steady state, till the shard is leaderless
    log.info("Sleep and periodically wake up to check for state...");
    for (int i = 0; i < 20; i++) {
      ClusterState clusterState = zkController.getZkStateReader().getClusterState();
      boolean allDown = true;
      for (Replica replica :
          clusterState.getCollection(collectionName).getSlice(shard).getReplicas()) {
        if (replica.getState() != State.DOWN) {
          allDown = false;
        }
      }
      if (allDown
          && clusterState.getCollection(collectionName).getSlice(shard).getLeader() == null) {
        break;
      }
      Thread.sleep(1000);
    }
    log.info("Waking up...");

    // remove the network partition
    log.info("Reopening the proxies for the non-leader replicas...");
    for (SocketProxy proxy : nonLeaderProxies) proxy.reopen();

    try (ZkShardTerms zkShardTerms =
        new ZkShardTerms(collectionName, shard, ZkStateReader.from(cloudClient).getZkClient())) {
      for (Replica notLeader : notLeaders) {
        assertTrue(
            zkShardTerms.getTerm(leader.getName()) > zkShardTerms.getTerm(notLeader.getName()));
      }
    }
  }

  private void assertSendDocFails(String collectionName, int docId) {
    // sending a doc in this state fails
    expectThrows(
        SolrException.class,
        "Should've failed indexing during a down state.",
        () -> sendDoc(collectionName, docId));
  }

  private void bringBackOldLeaderAndSendDoc(
      String collection, Replica leader, List<Replica> notLeaders, int docid) throws Exception {
    // Bring back the leader which was stopped
    log.info("Bringing back originally killed leader...");
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    getProxyForReplica(leader).reopen();
    leaderJetty.start();
    waitForRecoveriesToFinish(collection, ZkStateReader.from(cloudClient), true);
    ZkStateReader.from(cloudClient).forceUpdateCollection(collection);
    ClusterState clusterState = cloudClient.getClusterState();
    if (log.isInfoEnabled()) {
      log.info(
          "After bringing back leader: {}",
          clusterState.getCollection(collection).getSlice(SHARD1));
    }
    int numActiveReplicas = getNumberOfActiveReplicas(clusterState, collection, SHARD1);
    assertEquals(1 + notLeaders.size(), numActiveReplicas);
    log.info("Sending doc {}...", docid);
    sendDoc(collection, docid);
    log.info("Committing...");
    cloudClient.commit(collection);
    log.info("Doc {} sent and commit issued", docid);
    assertDocsExistInAllReplicas(notLeaders, collection, docid, docid);
    assertDocsExistInAllReplicas(Collections.singletonList(leader), collection, docid, docid);
  }

  @Override
  protected int sendDoc(String collectionName, int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);

    return sendDocsWithRetry(collectionName, Collections.singletonList(doc), 1, 5, 1);
  }

  private void doForceLeader(String collectionName, String shard)
      throws IOException, SolrServerException {
    CollectionAdminRequest.ForceLeader forceLeader =
        CollectionAdminRequest.forceLeaderElection(collectionName, shard);

    try (CloudSolrClient cloudClient =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(zkServer.getZkAddress()), Optional.empty())
            .withConnectionTimeout(3000)
            .withSocketTimeout(60000)
            .build()) {
      cloudClient.request(forceLeader);
    }
  }

  private int getNumberOfActiveReplicas(
      ClusterState clusterState, String collection, String sliceId) {
    int numActiveReplicas = 0;
    // Assert all replicas are active
    for (Replica rep : clusterState.getCollection(collection).getSlice(sliceId).getReplicas()) {
      if (rep.getState().equals(State.ACTIVE)) {
        numActiveReplicas++;
      }
    }
    return numActiveReplicas;
  }
}
