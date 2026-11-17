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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.LogListener;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecoveryUlogStateTest extends SolrCloudTestCase {

  private static final long WAIT_SECONDS = 60;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();

    Path retryConf = createTempDir("conf_one_retry");
    PathUtils.copyDirectory(configset("cloud-minimal"), retryConf);
    Path solrconfig = retryConf.resolve("solrconfig.xml");
    String xml = Files.readString(solrconfig, StandardCharsets.UTF_8);
    xml =
        xml.replace(
            "</config>",
            "<recoveryStrategy><int name=\"maxRetries\">1</int></recoveryStrategy></config>");
    Files.writeString(solrconfig, xml, StandardCharsets.UTF_8);
    cluster.uploadConfigSet(retryConf, "conf_one_retry");
  }

  @After
  public void resetHook() throws Exception {
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (!jetty.isRunning()) {
        jetty.start();
      }
    }
    cluster.waitForAllNodes(30);
    cluster.deleteAllCollections();
  }

  private void createCollection(String collection) throws Exception {
    createCollection(collection, "conf");
  }

  private void createCollection(String collection, String config) throws Exception {
    CollectionAdminRequest.createCollection(collection, config, 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 2);
    indexAndCommit(collection, 0, 10);
  }

  private void indexAndCommit(String collection, int start, int count) throws Exception {
    indexAndCommit(cluster.getSolrClient(), collection, start, count);
  }

  private static void indexAndCommit(SolrClient client, String collection, int start, int count)
      throws Exception {
    UpdateRequest req = new UpdateRequest();
    for (int i = start; i < start + count; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(i));
      req.add(doc);
    }
    req.commit(client, collection);
  }

  private Replica getNonLeader(String collection) {
    return getCollectionState(collection).getSlice("shard1").getReplicas().stream()
        .filter(r -> !r.isLeader())
        .findFirst()
        .orElseThrow();
  }

  private static LogListener recoveryFinishedListener() {
    return LogListener.info(RecoveryStrategy.class)
        .substring("Finished recovery process. recoveringAfterStartup=");
  }

  private static void waitForRecoveryToFinish(LogListener listener) throws Exception {
    assertNotNull(
        "recovery did not finish", listener.getQueue().poll(WAIT_SECONDS, TimeUnit.SECONDS));
  }

  @Test
  public void testCancelledRecoveryLeavesUlogActive() throws Exception {
    String collection = "stopped_recovery";
    createCollection(collection);

    Replica replica = getNonLeader(collection);
    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
    CoreContainer cc = jetty.getCoreContainer();

    try (SolrCore core = cc.getCore(replica.getCoreName());
        LogListener recoveryFinished = recoveryFinishedListener()) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      CountDownLatch hookReached = new CountDownLatch(1);
      RecoveryStrategy.testing_beforeReplayBufferingUpdates =
          () -> {
            core.getUpdateHandler().getSolrCoreState().cancelRecovery();
            hookReached.countDown();
            throw new RuntimeException("simulated recovery failure after buffering");
          };

      core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
      assertTrue(
          "recovery never reached replay", hookReached.await(WAIT_SECONDS, TimeUnit.SECONDS));
      RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;
      waitForRecoveryToFinish(recoveryFinished);

      assertEquals(
          "UpdateLog left in non-ACTIVE state after cancelled recovery",
          UpdateLog.State.ACTIVE,
          ulog.getState());
    }
  }

  @Test
  public void testPromotedReplicaUlogActiveAndTlogRolls() throws Exception {
    String collection = "orphaned_buffer";
    createCollection(collection);

    Replica replica = getNonLeader(collection);
    JettySolrRunner replicaJetty = cluster.getReplicaJetty(replica);
    Replica leader = getCollectionState(collection).getLeader("shard1");
    JettySolrRunner leaderJetty = cluster.getReplicaJetty(leader);
    CoreContainer cc = replicaJetty.getCoreContainer();

    try (SolrCore core = cc.getCore(replica.getCoreName());
        LogListener recoveryFinished = recoveryFinishedListener()) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      CountDownLatch hookReached = new CountDownLatch(1);
      CountDownLatch releaseHook = new CountDownLatch(1);
      RecoveryStrategy.testing_beforeReplayBufferingUpdates =
          () -> {
            hookReached.countDown();
            try {
              releaseHook.await(WAIT_SECONDS * 2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("simulated recovery failure after buffering");
          };

      core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
      assertTrue(
          "recovery never reached replay", hookReached.await(WAIT_SECONDS, TimeUnit.SECONDS));

      leaderJetty.stop();
      waitForState(
          "replica was not promoted to leader",
          collection,
          (liveNodes, state) -> {
            Replica l = state.getSlice("shard1").getLeader();
            return l != null
                && l.getName().equals(replica.getName())
                && l.getState() == Replica.State.ACTIVE
                && !liveNodes.contains(leader.getNodeName());
          });
      new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
          .waitFor(
              "core never saw itself as leader",
              () -> core.getCoreDescriptor().getCloudDescriptor().isLeader());

      RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;
      releaseHook.countDown();
      waitForRecoveryToFinish(recoveryFinished);

      long tlogIdBefore = ulog.getLastLogId();
      // the stopped node may still be in the cloud client's cached state
      try (SolrClient leaderClient = replicaJetty.newSolrClient(collection)) {
        for (int round = 0; round < 3; round++) {
          indexAndCommit(leaderClient, collection, 1000 + round * 100, 50);
        }
      }
      long tlogIdAfter = ulog.getLastLogId();

      assertEquals(
          "promoted leader's UpdateLog left in non-ACTIVE state",
          UpdateLog.State.ACTIVE,
          ulog.getState());
      assertTrue(
          "hard commits did not roll the leader's tlog (before="
              + tlogIdBefore
              + ", after="
              + tlogIdAfter
              + ")",
          tlogIdAfter > tlogIdBefore);
    }
  }

  @Test
  public void testRecoveryDoesNotDropBufferItDidNotStart() throws Exception {
    String collection = "others_buffer";
    createCollection(collection);

    Replica leader = getCollectionState(collection).getLeader("shard1");
    CoreContainer cc = cluster.getReplicaJetty(leader).getCoreContainer();

    try (SolrCore core = cc.getCore(leader.getCoreName());
        LogListener recoveryFinished = recoveryFinishedListener()) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      ulog.bufferUpdates();
      try {
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
        waitForRecoveryToFinish(recoveryFinished);
        assertEquals(
            "recovery dropped a buffer it did not start",
            UpdateLog.State.BUFFERING,
            ulog.getState());
      } finally {
        ulog.dropBufferedUpdates();
      }
    }
  }

  @Test
  public void testMaxRetriesLeavesUlogActive() throws Exception {
    String collection = "final_retry";
    createCollection(collection, "conf_one_retry");

    Replica replica = getNonLeader(collection);
    CoreContainer cc = cluster.getReplicaJetty(replica).getCoreContainer();

    try (SolrCore core = cc.getCore(replica.getCoreName());
        LogListener recoveryFinished = recoveryFinishedListener();
        LogListener gaveUp =
            LogListener.error(RecoveryStrategy.class).substring("max retries exceeded")) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      RecoveryStrategy.testing_beforeReplayBufferingUpdates =
          () -> {
            throw new RuntimeException("simulated recovery failure after buffering");
          };

      core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
      waitForRecoveryToFinish(recoveryFinished);
      RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;

      assertNotNull("recovery did not give up on max retries", gaveUp.getQueue().poll());
      assertEquals(
          "UpdateLog left in non-ACTIVE state after max retries",
          UpdateLog.State.ACTIVE,
          ulog.getState());
    }
  }

  @Test
  public void testShutdownKeepsUnfinishedBufferMarker() throws Exception {
    String collection = "shutdown_marker";
    createCollection(collection);

    Replica replica = getNonLeader(collection);
    JettySolrRunner replicaJetty = cluster.getReplicaJetty(replica);
    CoreContainer cc = replicaJetty.getCoreContainer();

    Path tlogDir;
    CountDownLatch hookReached = new CountDownLatch(1);
    try (SolrCore core = cc.getCore(replica.getCoreName())) {
      tlogDir = Path.of(core.getUpdateHandler().getUpdateLog().getTlogDir());
      RecoveryStrategy.testing_beforeReplayBufferingUpdates =
          () -> {
            hookReached.countDown();
            try {
              new TimeOut(WAIT_SECONDS * 2, TimeUnit.SECONDS, TimeSource.NANO_TIME)
                  .waitFor("node never shut down", cc::isShutDown);
            } catch (Exception e) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("simulated recovery failure during shutdown");
          };
      core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
    }
    assertTrue("recovery never reached replay", hookReached.await(WAIT_SECONDS, TimeUnit.SECONDS));

    indexAndCommit(collection, 2000, 20);
    assertTrue("no buffer tlog was written", hasBufferTlog(tlogDir));

    replicaJetty.stop();
    RecoveryStrategy.testing_beforeReplayBufferingUpdates = null;

    assertTrue(
        "buffer tlog (unfinished-recovery marker) was deleted on shutdown", hasBufferTlog(tlogDir));
  }

  private static boolean hasBufferTlog(Path tlogDir) throws Exception {
    try (Stream<Path> files = Files.list(tlogDir)) {
      return files.anyMatch(
          f -> f.getFileName().toString().startsWith(UpdateLog.BUFFER_TLOG_NAME + "."));
    }
  }
}
