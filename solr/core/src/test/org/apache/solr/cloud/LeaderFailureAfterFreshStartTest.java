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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 * Test for SOLR-9446
 *
 * This test is modeled after SyncSliceTest
 */
@Slow
@LuceneTestCase.Nightly
public class LeaderFailureAfterFreshStartTest extends SolrCloudBridgeTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean success = false;
  int docId = 0;

  List<JettySolrRunner> nodesDown = new ArrayList<>();

  @BeforeClass
  public static void beforeLeaderFailureAfterFreshStartTest() {
    sliceCount = 1;
    numJettys = 3;
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
  }

  @Override
  public void tearDown() throws Exception {
    if (!success) {
      log.error("Test failed — dumping cluster state");
    }
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.ulog.numRecordsToKeep");
    System.clearProperty("tests.zk.violationReportAction");
    super.tearDown();
  }

  @Override
  public void setUp() throws Exception {
    // tlog gets deleted after node restarts if we use CachingDirectoryFactory.
    // make sure that tlog stays intact after we restart a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    super.setUp();
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    try {
      // Identify the initial leader and the other two jettys for shard1
      Replica initialLeaderReplica = getShardLeader(COLLECTION, SHARD1, 10000);
      JettySolrRunner initialLeaderJetty = getJettyOnPort(getReplicaPort(initialLeaderReplica));

      List<JettySolrRunner> otherJetties = getOtherAvailableJetties(initialLeaderJetty);

      log.info("Leader node: {}", initialLeaderJetty.getNodeName());
      for (JettySolrRunner j : otherJetties) {
        log.info("Non-leader node: {}", j.getNodeName());
      }

      JettySolrRunner secondNode = otherJetties.get(0);
      JettySolrRunner freshNode = otherJetties.get(1);

      // shutdown a node to simulate fresh start
      otherJetties.remove(freshNode);
      forceNodeFailures(singletonList(freshNode));

      del("*:*");

      checkShardConsistency(false, true);

      // index a few docs and commit
      for (int i = 0; i < 100; i++) {
        indexDoc(id, docId, i1, 50, tlong, 50, t1,
            "document number " + docId++);
      }
      commit();

      checkShardConsistency(false, true);

      // bring down the other node and index a few docs; so the leader and other node segments diverge
      forceNodeFailures(singletonList(secondNode));
      for (int i = 0; i < 10; i++) {
        indexDoc(id, docId, i1, 50, tlong, 50, t1,
            "document number " + docId++);
        if (i % 2 == 0) {
          commit();
        }
      }
      commit();
      restartNodes(singletonList(secondNode));

      // start the freshNode
      restartNodes(singletonList(freshNode));

      // In this fork cloud cores load asynchronously, so JettySolrRunner.start() returns before freshNode's
      // SolrCore is registered locally; waitForRecoveriesToFinish only guarantees cluster-state ACTIVE, not
      // local core registration, and waitForLoadingCoresToFinish returns immediately if the async load has
      // not yet been submitted. Poll until freshNode actually registers its core, otherwise
      // getCores().iterator().next() throws NoSuchElementException on an empty collection.
      TimeOut coreLoadTimeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while (freshNode.getCoreContainer().getCores().isEmpty() && !coreLoadTimeout.hasTimedOut()) {
        freshNode.getCoreContainer().waitForLoadingCoresToFinish(1000);
        Thread.sleep(250);
      }
      assertFalse("freshNode never registered its core after restart",
          freshNode.getCoreContainer().getCores().isEmpty());

      // Find freshNode's core name by looking at the running cores on that jetty
      String coreName = freshNode.getCoreContainer().getCores().iterator().next().getName();
      String replicationProperties = freshNode.getSolrHome() + "/cores/" + coreName + "/data/replication.properties";
      String md5 = DigestUtils.md5Hex(Files.readAllBytes(Paths.get(replicationProperties)));

      // shutdown the original leader
      log.info("Now shutting down initial leader");
      forceNodeFailures(singletonList(initialLeaderJetty));

      // Re-read the current leader replica object (it may have changed)
      Replica currentLeaderReplica = cloudClient.getZkStateReader().getLeaderRetry(COLLECTION, SHARD1);
      waitForNewLeader(cloudClient, COLLECTION, SHARD1, initialLeaderReplica,
          new TimeOut(15, TimeUnit.SECONDS, TimeSource.NANO_TIME));
      waitForRecoveriesToFinish(COLLECTION);

      log.info("Updating cluster state after leader failover");
      assertEquals("Node went into replication", md5, DigestUtils.md5Hex(Files.readAllBytes(Paths.get(replicationProperties))));

      success = true;
    } finally {
      System.clearProperty("solr.disableFingerprint");
    }
  }

  private void restartNodes(List<JettySolrRunner> nodesToRestart) throws Exception {
    for (JettySolrRunner node : nodesToRestart) {
      node.start();
      nodesDown.remove(node);
    }
    waitForRecoveriesToFinish(COLLECTION);
    checkShardConsistency(false, true);
  }

  private void forceNodeFailures(List<JettySolrRunner> replicasToShutDown) throws Exception {
    for (JettySolrRunner replica : replicasToShutDown) {
      replica.stop();
    }

    int totalDown = 0;

    Set<JettySolrRunner> jetties = new HashSet<>(cluster.getJettySolrRunners());

    if (replicasToShutDown != null) {
      jetties.removeAll(replicasToShutDown);
      totalDown += replicasToShutDown.size();
    }

    jetties.removeAll(nodesDown);
    totalDown += nodesDown.size();

    assertEquals(numJettys - totalDown, jetties.size());

    nodesDown.addAll(replicasToShutDown);
  }

  private List<JettySolrRunner> getOtherAvailableJetties(JettySolrRunner leader) {
    List<JettySolrRunner> candidates = new ArrayList<>(cluster.getJettySolrRunners());

    if (leader != null) {
      candidates.remove(leader);
    }

    candidates.removeAll(nodesDown);

    return candidates;
  }

  protected void indexDoc(Object... fields) throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();

    addFields(doc, fields);
    addFields(doc, "rnd_s", RandomStringUtils.random(random().nextInt(100) + 100));

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    ureq.setParams(params);
    ureq.process(cloudClient);
  }

  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }
}
