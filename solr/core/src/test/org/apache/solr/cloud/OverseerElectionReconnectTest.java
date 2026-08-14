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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SocketProxy;
import org.apache.solr.util.TimeOut;
import org.jspecify.annotations.NonNull;
import org.junit.Test;

/**
 * Regression test that overseer election recovers correctly when a ZooKeeper session expiry
 * coincides with the reconnect that re-drives election. Specifically in the case where a departing
 * overseer lineage could race the reconnecting one and leave a "zombie" /overseer_elect/leader
 * znode with no running overseer behind it.
 */
// This test deliberately strands ZooKeeper connections (Curator abandons one per expiry), and a
// discarded ClientCnxn.SendThread sleeps briefly inside its socket cleanup on the way out. Give
// those threads a moment to finish rather than reporting them as leaks.
@ThreadLeakLingering(linger = 2000)
public class OverseerElectionReconnectTest extends SolrTestCaseJ4 {

  private static final String SOLRXML = "<solr></solr>";

  /**
   * ZooKeeper's fixed reconnect interval: ClientCnxn passes 1000 to hostProvider.next(), and with a
   * single-server connect string that sleep runs before every retry. It is not configurable, and
   * the constants below are chosen relative to it.
   */
  private static final int ZK_RECONNECT_INTERVAL_MS = 1000;

  /**
   * Short enough that a real expiry is reachable in a test, but deliberately larger than and offset
   * from the reconnect interval -- do not round this to a multiple of it.
   *
   * <p>Curator abandons a session on its own timer, at detectionLag + this, while the old
   * connection retries on a fixed ladder at detectionLag + N * interval. The detection lag appears
   * in both and cancels, so a timeout that is a multiple of the interval puts those two events on
   * the same millisecond. Whoever wins then decides whether the session expires at all -- and if
   * the old connection wins it resumes the session, nothing expires, and this test silently
   * exercises nothing. Offsetting by half an interval puts Curator's decision midway between two
   * rungs, so it wins reliably rather than on a coin flip.
   */
  private static final int SESSION_TIMEOUT_MS = ZK_RECONNECT_INTERVAL_MS + 500;

  /**
   * ZooKeeper's session-expiry bucket width, so the server's reap of an expired session's ephemeral
   * nodes can trail Curator's client-side expiry by up to this much. That lag is the whole reason
   * the departing OverseerExitThread still finds the old leader znode instead of taking its NoNode
   * early-out, so this must comfortably exceed the client's detection latency (~100ms). At 100 the
   * reap always wins and the race is never reached.
   */
  private static final int TICK_MS = 750;

  private static final int MAX_CYCLES = TEST_NIGHTLY ? 25 : 2;

  /**
   * Recovery cannot start until Curator gives up, which is one session timeout after the cut, so
   * this has to scale with the timeout -- a fixed value would silently time out on every cycle if
   * the timeout were raised, leaving the test doing a single cycle and still passing.
   */
  private static final int RECOVERY_WAIT_SECONDS = SESSION_TIMEOUT_MS / 1000 + 10;

  /**
   * Reproduction of the residual overseer zombie race that survives PR #4577 (which stops
   * onReconnect/onDisconnect from firing on same-session blips). Here we drive real session
   * expiries and try to make the expiry coincide with the reconnect.
   *
   * <p>The race needs two things to line up.
   *
   * <p>First, the departing OverseerExitThread has to miss its early-out. onExpiredReconnection
   * cancels the previous election context, which calls overseer.close(); the updater loop exits and
   * its finally block spawns the OET. The OET runs checkIfIamStillLeader, which returns immediately
   * if /overseer_elect/leader is already gone — so it is only dangerous while the old session's
   * ephemeral leader znode is still visible. That happens because Curator does not wait for the
   * server to declare the session dead; it injects the expiration on its own timer and starts a
   * fresh session, while the server only reaps ephemerals on a tickTime-wide bucket. The reap can
   * therefore trail the client's expiry by up to a tick, which is why the tick is set coarse here.
   *
   * <p>Second, the rejoin has to land in the registration window. Having found the stale node, the
   * OET deletes it and calls rejoinOverseerElection, which picks up the elector's current context —
   * by now the reconnect thread's brand-new one — and closes it while that thread is still joining.
   * If the close lands between creating the leader znode and starting the overseer, the znode is
   * left with no updater behind it and cancelElection() will not clean it up, so every later
   * election fails with NodeExists.
   */
  @Test
  public void testOverseerWedgesOnExpiryRacingReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testOverseerExpiryRace-solr");

    ZkTestServer zkServer = buildZkTestServer(zkDir);
    try {
      zkServer.run();

      SocketProxy zkProxy = new SocketProxy();
      zkProxy.open(URI.create("http://127.0.0.1:" + zkServer.getPort()));
      String proxiedZkAddress = "127.0.0.1:" + zkProxy.getListenPort() + "/solr";
      try {
        // The persistent client's session timeout comes from CloudConfig.getZkClientTimeout(), not
        // from the ZkController constructor arg (which only governs the bootstrap connect), so it
        // has to be set on both.
        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8984)
                .setZkClientTimeout(SESSION_TIMEOUT_MS)
                .setLeaderConflictResolveWait(180000)
                .setLeaderVoteWait(180000)
                .build();

        CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
        try (ZkController zkController =
                new ZkController(cc, proxiedZkAddress, SESSION_TIMEOUT_MS, cloudConfig);
            SolrZkClient probe =
                new SolrZkClient.Builder()
                    .withUrl(zkServer.getZkAddress())
                    .withTimeout(30000, TimeUnit.MILLISECONDS)
                    .build()) {

          assertNotNull("Overseer leader should be elected", waitForOverseerLeader(zkServer, 30));
          assertTrue(
              "Overseer should be healthy before the storm",
              waitForHealthyOverseer(zkController, probe, 30));
          assertEquals(
              "Session timeout must be negotiated verbatim; check tickTime and the min/max clamping",
              SESSION_TIMEOUT_MS,
              zkController.getZkClient().getZkSessionTimeout());
          long sessionBefore = zkController.getZkClient().getZkSessionId();

          for (int i = 0; i < MAX_CYCLES; i++) {
            zkProxy.close();
            // Deliberate fault injection, not a poll: hold ZK unreachable long enough to expire the
            // session. There is no condition to wait on, so waitFor/RetryUtil do not apply.
            //
            // Unpadded on purpose. The cut has to outlast the old connection's retry (detection lag
            // + the reconnect interval) or that retry resumes the session and nothing ever expires.
            // It also has to end before Curator's replacement connection reaches out (detection lag
            // + the session timeout), or that misses the port, waits another full interval, and the
            // server reaps the old leader znode before the race can happen. Reopening exactly at
            // the session timeout clears the second bound for any positive detection lag.
            Thread.sleep(SESSION_TIMEOUT_MS);
            zkProxy.reopen();
            if (!waitForHealthyOverseer(zkController, probe, RECOVERY_WAIT_SECONDS)) {
              break;
            }
          }

          boolean healthy = waitForHealthyOverseer(zkController, probe, RECOVERY_WAIT_SECONDS * 4);
          assertTrue(
              "Overseer wedged after an expiry coinciding with a reconnect (zombie leader /"
                  + " NodeExists spin): leader znode id="
                  + leaderId(probe)
                  + ", running updater id="
                  + updaterId(zkController),
              healthy);
          // Guards against this test silently becoming a no-op: if the outage stops severing the
          // session, every cycle is just a same-session blip and none of the above exercises the
          // race.
          assertNotEquals(
              "No session ever expired, so this test exercised nothing -- check SESSION_TIMEOUT_MS"
                  + " against the reconnect interval and the min/max clamping",
              sessionBefore,
              zkController.getZkClient().getZkSessionId());
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

  private static @NonNull ZkTestServer buildZkTestServer(Path zkDir) throws Exception {
    ZkTestServer zkServer = new ZkTestServer(zkDir);
    // The coarse tick is load-bearing: the server reaps an expired session's ephemerals on a
    // tickTime-wide bucket, so it can lag the client's (Curator-injected) expiry by up to tickTime,
    // and only that lag lets the departing OverseerExitThread still see the old leader znode. Pin
    // the min/max bounds so the session timeout is negotiated verbatim; ZkTestServer's own defaults
    // ([3000, 90000]) would otherwise clamp it up.
    zkServer.setTheTickTime(TICK_MS);
    zkServer.setMinSessionTimeout(SESSION_TIMEOUT_MS);
    zkServer.setMaxSessionTimeout(SESSION_TIMEOUT_MS);
    return zkServer;
  }

  private boolean waitForHealthyOverseer(ZkController zkController, SolrZkClient probe, int seconds)
      throws InterruptedException {
    try {
      new TimeOut(seconds, TimeUnit.SECONDS, TimeSource.NANO_TIME)
          .waitFor("overseer did not become healthy", () -> isHealthyOverseer(zkController, probe));
      return true;
    } catch (TimeoutException e) {
      return false;
    }
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

  /** The id stored in the leader znode, or null if it cannot be read. */
  private String leaderId(SolrZkClient probe) {
    try {
      return OverseerTaskProcessor.getLeaderId(probe);
    } catch (Exception e) {
      return null;
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
    AtomicReference<String> leader = new AtomicReference<>();
    try (SolrZkClient zc =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(30000, TimeUnit.MILLISECONDS)
            .build()) {
      try {
        new TimeOut(timeoutSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME)
            .waitFor(
                "overseer leader was not elected",
                () -> {
                  try {
                    String leaderNode = OverseerCollectionConfigSetProcessor.getLeaderNode(zc);
                    if (leaderNode != null && !leaderNode.trim().isEmpty()) {
                      leader.set(leaderNode);
                      return true;
                    }
                  } catch (Exception e) {
                    // Leader not yet elected
                  }
                  return false;
                });
      } catch (TimeoutException e) {
        // leave leader null
      }
    }
    return leader.get();
  }
}
