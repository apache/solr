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

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SocketProxy;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that overseer election recovers correctly after a ZooKeeper connection blip (SUSPENDED →
 * RECONNECTED with same session). Reproduces the doom loop where the old ephemeral leader node
 * still exists on reconnect, causing NodeExistsException → unbounded recursion →
 * StackOverflowError.
 *
 * <p>Uses a SocketProxy between the Solr node and ZooKeeper so that pausing traffic triggers
 * SUSPENDED without expiring the session (ZK stays running, session stays alive, ephemerals
 * persist).
 */
public class OverseerElectionReconnectTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String SOLRXML = "<solr></solr>";

  @Test
  public void testOverseerSurvivesZkReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testOverseerReconnect-solr");

    ZkTestServer zkServer = new ZkTestServer(zkDir);
    zkServer.setTheTickTime(1000);
    try {
      zkServer.run();

      // Create a SocketProxy in front of ZK — this lets us pause traffic without killing sessions
      SocketProxy zkProxy = new SocketProxy();
      zkProxy.open(URI.create("http://127.0.0.1:" + zkServer.getPort()));
      String proxiedZkAddress = "127.0.0.1:" + zkProxy.getListenPort() + "/solr";
      log.info(
          "ZK on port {}, proxy on port {}", zkServer.getPort(), zkProxy.getListenPort());

      try {
        // Use a 30s ZK client timeout — large enough that a 3s disconnect won't expire
        // the session but the TCP break is immediately detected by the ZK client.
        int zkClientTimeout = 30000;

        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8984)
                .setLeaderConflictResolveWait(180000)
                .setLeaderVoteWait(180000)
                .build();

        CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
        try (ZkController zkController =
            new ZkController(cc, proxiedZkAddress, zkClientTimeout, cloudConfig)) {

          // Wait for overseer leader to be established
          String leader = waitForOverseerLeader(zkServer, 30);
          assertNotNull("Overseer leader should be elected", leader);
          log.info("Overseer leader established: {}", leader);

          // Close the proxy to sever TCP connections. The ZK client immediately detects
          // the broken socket and Curator fires SUSPENDED. The ZK server keeps the
          // session alive (30s timeout) and ephemerals persist.
          log.info("Closing ZK proxy to simulate network blip...");
          zkProxy.close();

          // Wait just long enough for SUSPENDED to fire (~500ms) but reopen BEFORE
          // OverseerExitThread's getData retries (ExponentialBackoffRetry base=1000ms).
          // This ensures onReconnect's makePath races against a still-existing leader
          // ephemeral, reproducing the production scenario where the client reconnects
          // to a different ZK ensemble member within ~100ms.
          Thread.sleep(600);

          // Reopen the proxy — the ZK client reconnects, Curator fires RECONNECTED
          // (same session). The old /overseer_elect/leader ephemeral still exists.
          log.info("Reopening ZK proxy...");
          zkProxy.reopen();

          // Wait for the ZkController to actually reconnect and for the overseer to
          // restart (via onReconnect → joinElection → runLeaderProcess → overseer.start).
          // If the doom loop bug is present, the election thread dies from
          // StackOverflowError and the overseer never restarts.
          TimeOut reconnectTimeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
          boolean overseerRunning = false;
          while (!reconnectTimeout.hasTimedOut()) {
            Overseer overseer = zkController.getOverseer();
            if (zkController.getZkClient().isConnected()
                && overseer != null
                && !overseer.isClosed()
                && overseer.getUpdaterThread() != null
                && !overseer.getUpdaterThread().isClosed()) {
              overseerRunning = true;
              break;
            }
            Thread.sleep(500);
          }
          assertTrue(
              "Overseer should be running after ZK reconnect (doom loop may have killed the"
                  + " election thread)",
              overseerRunning);
          log.info("Overseer confirmed running after reconnect");
        } finally {
          cc.shutdown();
        }
      } finally {
        zkProxy.close();
      }
    } finally {
      zkServer.shutdown();
    }
  }

  /**
   * Probabilistic reproduction of the single-node overseer livelock. Rapidly toggles connectivity to
   * the ZK ensemble so that, on some cycle, {@code onReconnect} fires while the {@code
   * OverseerExitThread} spawned by {@code onDisconnect} is mid-{@code runLeaderProcess} — landing a
   * {@code close()} in the window between {@code makePath(leaderPath)} and the guarded {@code
   * overseer.start()}. When that happens the leader znode is created with no overseer behind it (a
   * "zombie"), every later election fails with NodeExists, and the overseer never recovers even after
   * connectivity is stable.
   *
   * <p>Health/wedge is decided precisely: a healthy overseer has a live updater thread whose id equals
   * the id stored in {@code /overseer_elect/leader}. A zombie has a leader znode whose id matches no
   * running updater.
   */
  @Test
  public void testOverseerWedgesUnderRapidZkReconnects() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testOverseerRapidReconnect-solr");

    ZkTestServer zkServer = new ZkTestServer(zkDir);
    zkServer.setTheTickTime(1000);
    try {
      zkServer.run();

      SocketProxy zkProxy = new SocketProxy();
      zkProxy.open(URI.create("http://127.0.0.1:" + zkServer.getPort()));
      String proxiedZkAddress = "127.0.0.1:" + zkProxy.getListenPort() + "/solr";
      log.info("ZK on port {}, proxy on port {}", zkServer.getPort(), zkProxy.getListenPort());

      try {
        int zkClientTimeout = 30000; // >> outage durations, so the session never expires
        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8984)
                .setLeaderConflictResolveWait(180000)
                .setLeaderVoteWait(180000)
                .build();

        CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
        try (ZkController zkController =
                new ZkController(cc, proxiedZkAddress, zkClientTimeout, cloudConfig);
            SolrZkClient probe =
                new SolrZkClient.Builder()
                    .withUrl(zkServer.getZkAddress())
                    .withTimeout(30000, TimeUnit.MILLISECONDS)
                    .build()) {

          assertNotNull("Overseer leader should be elected", waitForOverseerLeader(zkServer, 30));
          assertTrue(
              "Overseer should be healthy before the storm",
              waitForHealthyOverseer(zkController, probe, 30));
          log.info("Baseline healthy overseer established; starting reconnect storm");

          // Sweep short outage durations so RECONNECTED lands at varying offsets relative to the
          // OET's makePath->start window. Short outages keep the client reconnecting to the same
          // session (no expiry) while giving onDisconnect time to spawn the OET.
          int[] outageMs = {80, 110, 140, 170, 200, 240, 300, 130, 95, 180, 260, 150};
          boolean recoveredThisCycle = true;
          for (int i = 0; i < 60 && recoveredThisCycle; i++) {
            int outage = outageMs[i % outageMs.length];
            log.info(
                "Reconnect storm cycle {}: cutting ZK connectivity for {}ms (leader={}, updater={})",
                i,
                outage,
                OverseerTaskProcessor.getLeaderId(probe),
                updaterId(zkController));
            zkProxy.close();
            Thread.sleep(outage);
            zkProxy.reopen();
            // Give this cycle a bounded chance to recover; if it can't, stop hammering (don't risk
            // eventually expiring the session, which would clear the zombie and mask the bug).
            recoveredThisCycle = waitForHealthyOverseer(zkController, probe, 6);
            if (!recoveredThisCycle) {
              log.info("Overseer did not recover within 6s after cycle {} (outage {}ms)", i, outage);
            }
          }

          // Authoritative check: with connectivity now stable, a healthy cluster returns to a
          // running overseer whose id matches /overseer_elect/leader. A zombie never does.
          boolean healthy = waitForHealthyOverseer(zkController, probe, 45);
          if (!healthy) {
            log.error(
                "WEDGED: /overseer_elect/leader id={} but no running updater matches it; updater={}",
                OverseerTaskProcessor.getLeaderId(probe),
                updaterId(zkController));
          }
          assertTrue(
              "Overseer wedged after rapid ZK reconnects (zombie leader / NodeExists spin) — see"
                  + " 'ZOMBIE LEADER' warning in the log",
              healthy);
        } finally {
          cc.shutdown();
        }
      } finally {
        zkProxy.close();
      }
    } finally {
      zkServer.shutdown();
    }
  }

  private boolean waitForHealthyOverseer(ZkController zkController, SolrZkClient probe, int seconds)
      throws Exception {
    TimeOut timeout = new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      if (isHealthyOverseer(zkController, probe)) {
        return true;
      }
      Thread.sleep(250);
    }
    return isHealthyOverseer(zkController, probe);
  }

  /** Healthy == connected, a live updater thread, and its id equals the leader znode's id. */
  private boolean isHealthyOverseer(ZkController zkController, SolrZkClient probe) {
    try {
      if (!zkController.getZkClient().isConnected()) return false;
      Overseer overseer = zkController.getOverseer();
      if (overseer == null || overseer.isClosed()) return false;
      Overseer.OverseerThread updater = overseer.getUpdaterThread();
      if (updater == null || updater.isClosed() || !updater.isAlive()) return false;
      String runningId = updaterId(zkController);
      String leaderId = OverseerTaskProcessor.getLeaderId(probe);
      return leaderId != null && leaderId.equals(runningId);
    } catch (Exception e) {
      return false;
    }
  }

  /** The id of the currently running updater, parsed from its thread name, or null. */
  private String updaterId(ZkController zkController) {
    Overseer overseer = zkController.getOverseer();
    if (overseer == null) return null;
    Overseer.OverseerThread updater = overseer.getUpdaterThread();
    if (updater == null) return null;
    String prefix = "OverseerStateUpdate-";
    String name = updater.getName();
    return name.startsWith(prefix) ? name.substring(prefix.length()) : name;
  }

  private String waitForOverseerLeader(ZkTestServer zkServer, int timeoutSeconds) throws Exception {
    TimeOut timeout = new TimeOut(timeoutSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    try (SolrZkClient zc =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(30000, TimeUnit.MILLISECONDS)
            .build()) {
      while (!timeout.hasTimedOut()) {
        try {
          String leaderNode = OverseerCollectionConfigSetProcessor.getLeaderNode(zc);
          if (leaderNode != null && !leaderNode.trim().isEmpty()) {
            return leaderNode;
          }
        } catch (Exception e) {
          // Leader not yet elected
        }
        Thread.sleep(500);
      }
    }
    return null;
  }
}
