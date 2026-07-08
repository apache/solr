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

  /**
   * Reproduction of the <em>residual</em> overseer zombie race that survives PR #4577 (which stops
   * {@code onReconnect}/{@code onDisconnect} from firing on same-session blips, so the first test's
   * same-session storm can no longer trigger it). Here we drive <em>real session expiries</em> and
   * try to make the expiry coincide with the reconnect.
   *
   * <p>Mechanics of the surviving race: on a genuine expiry the departing lineage's {@code
   * OverseerExitThread} (spawned from {@code ClusterStateUpdater.run()}'s {@code finally}) still runs
   * {@code checkIfIamStillLeader} → {@code getData(/overseer_elect/leader)}. If that read lands
   * <em>after</em> the new session's {@code onExpiredReconnection} lineage has already recreated the
   * leader znode, the OET does not take the {@code NoNode} early-out — it falls into its {@code
   * finally} and calls {@code rejoinOverseerElection} unconditionally. That second lineage races the
   * first on the shared {@code overseerElector}/{@code Overseer}, reopening the D1 window where a
   * {@code close()} lands between {@code makePath(leaderPath)} and the guarded {@code
   * overseer.start()} → a zombie leader znode with no updater behind it.
   *
   * <p>To hit it we need a short session timeout (so expiry is reachable in a test) and outages that
   * <em>straddle</em> the session-timeout boundary so the client reconnects right as/after the server
   * expires the session. ZooKeeper clamps the negotiated session timeout to [2*tickTime, 20*tickTime]
   * and only detects expiry on tickTime-wide buckets, so tickTime must be lowered to make a ~1s
   * session possible and to give fine timing granularity around the boundary.
   */
  @Test
  public void testOverseerWedgesOnExpiryRacingReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testOverseerExpiryRace-solr");

    ZkTestServer zkServer = new ZkTestServer(zkDir);
    // tick=250 => expiry detection granularity ~250ms. ZkTestServer otherwise clamps the negotiated
    // session timeout to [minSessionTimeout=3000, maxSessionTimeout=90000], which would silently
    // bump our requested 1000ms up to 3000ms and turn every outage below into a same-session blip.
    // Lower the floor so a real ~1s session is negotiated.
    zkServer.setTheTickTime(250);
    zkServer.setMinSessionTimeout(600);
    zkServer.setMaxSessionTimeout(5000);
    try {
      zkServer.run();

      SocketProxy zkProxy = new SocketProxy();
      zkProxy.open(URI.create("http://127.0.0.1:" + zkServer.getPort()));
      String proxiedZkAddress = "127.0.0.1:" + zkProxy.getListenPort() + "/solr";
      log.info("ZK on port {}, proxy on port {}", zkServer.getPort(), zkProxy.getListenPort());

      try {
        // ~1s session. NOTE: ZkController takes the persistent client's session timeout from
        // CloudConfig.getZkClientTimeout() (ZkController.java:311), NOT from the constructor arg
        // below (which only governs the initial bootstrap connect). So the 1s must be set on the
        // CloudConfigBuilder; with the server floor lowered to 600ms it is honored verbatim.
        int zkClientTimeout = 1000;
        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8984)
                .setZkClientTimeout(1000)
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
          int negotiated = zkController.getZkClient().getZkSessionTimeout();
          log.info(
              "Baseline healthy overseer established; negotiated session timeout={}ms; starting"
                  + " expiry-race storm",
              negotiated);
          assertTrue(
              "Expected a sub-2s negotiated session timeout so outages can expire the session, got "
                  + negotiated
                  + "ms",
              negotiated <= 1500);

          // The exact outage that lands the reconnect inside the OET-vs-reconnect race window is
          // hardware-sensitive (it depends on how fast re-election runs and when the departing OET's
          // getData is scheduled), so we do not bet on a single value. Instead we sweep a fine 10ms
          // comb across the band just past the ~1000ms expiry boundary (BAND_LO..BAND_HI) and repeat
          // it many times: breadth covers wherever the window sits on this machine, depth gives each
          // offset multiple independent shots at the inherently racy interleaving. Every outage is
          // >= the session timeout, so the session expires each cycle. We stop on the first wedge, so
          // the run is fast when it reproduces and only pays the full budget when it does not.
          final int BAND_LO = 1000;
          final int BAND_HI = 1250;
          final int STEP = 10;
          final int OFFSETS = (BAND_HI - BAND_LO) / STEP + 1; // 26
          final int MAX_CYCLES = OFFSETS * 6; // ~6 shots per offset
          boolean recoveredThisCycle = true;
          int wedgeOutage = -1; // the outage (ms) of the cycle that wedged, -1 if none
          int wedgeCycle = -1;
          for (int i = 0; i < MAX_CYCLES && recoveredThisCycle; i++) {
            int outage = BAND_LO + (i % OFFSETS) * STEP;
            long sidBefore = safeSessionId(zkController);
            log.info(
                "Expiry-race cycle {}: cutting ZK connectivity for {}ms (session={}, leader={}, updater={})",
                i,
                outage,
                sidBefore,
                OverseerTaskProcessor.getLeaderId(probe),
                updaterId(zkController));
            zkProxy.close();
            Thread.sleep(outage);
            zkProxy.reopen();
            // Expiry recovery (rejoin + re-elect + re-register) needs a little longer than a
            // same-session blip; give it a bounded window then stop if wedged.
            recoveredThisCycle = waitForHealthyOverseer(zkController, probe, 10);
            long sidAfter = safeSessionId(zkController);
            log.info(
                "Expiry-race cycle {} result: recovered={} sessionExpired={} (before={}, after={})",
                i,
                recoveredThisCycle,
                sidBefore != -1 && sidAfter != -1 && sidBefore != sidAfter,
                sidBefore,
                sidAfter);
            if (!recoveredThisCycle) {
              wedgeOutage = outage;
              wedgeCycle = i;
              log.info(
                  "Overseer did not recover within 10s after cycle {} (outage {}ms)", i, outage);
            }
          }

          boolean healthy = waitForHealthyOverseer(zkController, probe, 45);
          if (!healthy) {
            log.error(
                "WEDGED (expiry race): /overseer_elect/leader id={} but no running updater matches"
                    + " it; updater={}",
                OverseerTaskProcessor.getLeaderId(probe),
                updaterId(zkController));
            if (wedgeOutage != -1) {
              // Report the observed hitting point and how to narrow the sweep for faster repro on
              // this same hardware. The window is HW-specific, so we can only suggest it after a hit.
              int tightLo = Math.max(BAND_LO, wedgeOutage - 20);
              int tightHi = Math.min(BAND_HI + 50, wedgeOutage + 20);
              log.error(
                  "REPRO WINDOW: this run wedged at outage={}ms (cycle {}). This hitting point is"
                      + " hardware-specific. For faster repro on THIS machine, tighten the sweep to"
                      + " BAND_LO={} / BAND_HI={} (a narrow band around {}ms) so nearly every cycle"
                      + " targets the window; widen it again if a different machine stops"
                      + " reproducing.",
                  wedgeOutage,
                  wedgeCycle,
                  tightLo,
                  tightHi,
                  wedgeOutage);
            }
          }
          assertTrue(
              "Overseer wedged after an expiry coinciding with a reconnect (residual zombie-leader"
                  + " race not fixed by PR #4577) — see 'ZOMBIE LEADER' warning in the log",
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

  /** Current ZK session id, or -1 if not connected. */
  private long safeSessionId(ZkController zkController) {
    try {
      return zkController.getZkClient().getZkSessionId();
    } catch (Exception e) {
      return -1;
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
