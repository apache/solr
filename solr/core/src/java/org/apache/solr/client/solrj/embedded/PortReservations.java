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
package org.apache.solr.client.solrj.embedded;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-infrastructure port broker. Closes the "a restart steals my port" race that plagues heavily
 * parallel cloud tests.
 *
 * <p>The hole: a node (a {@link JettySolrRunner} killed by ChaosMonkey, or a restarted
 * {@code ZkTestServer}) is stopped and then restarted on the SAME port so its URL/identity is
 * preserved. Between the stop (the listening socket is freed) and the restart (the rebind), any of
 * the dozens of other concurrent test JVMs — all constantly allocating OS-assigned (port 0) sockets
 * — can be handed that exact port by the OS and hold it for the rest of its run. The restart then
 * fails with {@code BindException("Address already in use")}, and no amount of retrying frees a port
 * a foreign JVM is sitting on.
 *
 * <p>Two complementary defenses; in practice you need BOTH to really close the hole:
 * <ol>
 *   <li>{@link #reserveFreePort()} — allocate the port UP FRONT by binding a held placeholder socket
 *       on port 0 and handing out its OS-assigned number. While this JVM holds the placeholder, the
 *       OS will not hand that port to anyone else, so a node that takes its port from here owns it
 *       from allocation all the way through its own bind (the placeholder is dropped just-in-time).
 *       This drastically shrinks the field of competitors racing for ports.
 *   <li>{@link #reserve(int)} — the instant a node stops for a restart, re-bind a held placeholder on
 *       its old port so nobody can steal it during the down-window; {@link #release(int)} drops the
 *       placeholder microseconds before the real rebind.
 * </ol>
 *
 * <p>Placeholders bind {@code 127.0.0.1} with NO {@code SO_REUSEADDR} (see {@link #open(int)} for
 * why both choices are load-bearing for accurate probing and a strong hold). The remaining race
 * window is only the few microseconds between {@link #release(int)} and the real server's bind —
 * orders of magnitude smaller than the whole stop→restart down-window, and confined to a port THIS
 * JVM was already holding, so the practical collision rate goes to ~zero.
 */
public final class PortReservations {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // port -> the placeholder socket holding it. Keyed by port so a re-reserve replaces cleanly.
  private static final ConcurrentHashMap<Integer, ServerSocket> HELD = new ConcurrentHashMap<>();

  // Ports this fork has handed out and not yet permanently returned (releaseAll). A port stays
  // CLAIMED even while its placeholder is dropped for the real bind (release()), so a concurrent
  // reserveFreePort() — jetties in a cluster start in PARALLEL — cannot re-hand the same port in
  // the brief release->bind gap. Without this, two jetties could be handed the same window port and
  // one would fight a BindException for the whole retry budget.
  private static final java.util.Set<Integer> CLAIMED = ConcurrentHashMap.newKeySet();

  // --- Per-fork port windows (the cross-PROCESS half of the fix) ----------------------------------
  //
  // The beast/test runner forks each test class into its OWN short-lived JVM (gradle Test:
  // forkEvery=1, maxParallelForks=N), so up to N forks run concurrently in separate PROCESSES. A
  // per-JVM placeholder map cannot stop a FOREIGN fork JVM from being handed a just-freed port: the
  // OS arbitrates ports across processes, and a foreign fork that calls bind(port 0) ("give me any
  // free port") can be handed the exact port THIS fork just freed for a same-port restart, then hold
  // it for its whole run -> BindException on rebind.
  //
  // The fix: partition the port space into disjoint WINDOWs, one per fork (keyed by gradle's unique
  // per-worker id), and have every restartable server (ZkTestServer, JettySolrRunner) take its
  // first-start port from its OWN window via reserveFreePort() instead of bind(port 0). A foreign
  // fork draws from a DIFFERENT window and never issues a bare port-0 request for a long-lived
  // server, so it can never be handed this fork's port. Combined with the placeholder hold across
  // the restart down-window (reserve/release below), cross-fork collisions go to ~zero.
  // Windows live in [10000, 30000), entirely BELOW the Linux ephemeral range (32768..60999) so the
  // OS never hands a window port to another process as an ephemeral (port-0 / outbound) port. ~100
  // ports per fork is far more than any test class uses (a handful of jetties + one ZK).
  private static final int WINDOW_SIZE = 100;
  private static final int WINDOW_BASE = 10000;
  private static final int WINDOW_SLOTS = 200; // 10000..30000

  // How long reserve() retries grabbing a port that a just-stopped server is still releasing.
  private static final int RESERVE_RETRY_SECONDS = 20;
  // 0 means "not running under a gradle test fork" (e.g. IDE) -> no window, fall back to port 0.
  private static final int forkWindowBase = computeForkWindowBase();

  private PortReservations() {}

  private static int computeForkWindowBase() {
    String w = System.getProperty("org.gradle.test.worker");
    if (w == null || w.trim().isEmpty()) {
      return 0;
    }
    try {
      long id = Long.parseLong(w.trim());
      // floorMod keeps the slot in range even if the worker id grows past WINDOW_SLOTS; concurrent
      // forks (only a handful alive at once, with near-contiguous ids) effectively never collide.
      int slot = (int) Math.floorMod(id, WINDOW_SLOTS);
      return WINDOW_BASE + slot * WINDOW_SIZE;
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Reserve a free port for a restartable server, holding it open with a placeholder until
   * {@link #release(int)}. When running under a gradle test fork the port is drawn from this fork's
   * disjoint {@linkplain #computeForkWindowBase() window} (so a foreign fork can never be handed it);
   * otherwise it falls back to an OS-assigned port. Returns the reserved port, or {@code 0} if no
   * placeholder could be bound (the caller should then just bind port 0 itself).
   */
  public static int reserveFreePort() {
    if (forkWindowBase > 0) {
      for (int p = forkWindowBase; p < forkWindowBase + WINDOW_SIZE; p++) {
        if (!CLAIMED.add(p)) {
          continue; // already handed out within this fork (claim survives the release->bind gap)
        }
        try {
          ServerSocket s = open(p);
          closeQuietly(HELD.put(p, s));
          return p;
        } catch (IOException inUse) {
          // bound by our own running server or some other process — drop the claim, try the next
          CLAIMED.remove(p);
        }
      }
      log.warn(
          "Fork port window {}..{} exhausted; falling back to an OS-assigned port",
          forkWindowBase, forkWindowBase + WINDOW_SIZE);
    }
    try {
      ServerSocket s = open(0);
      int port = s.getLocalPort();
      CLAIMED.add(port);
      closeQuietly(HELD.put(port, s)); // never leak if the OS somehow recycled a still-mapped port
      return port;
    } catch (IOException e) {
      log.warn("Could not pre-reserve a free port; caller will bind port 0 directly", e);
      return 0;
    }
  }

  /**
   * Best-effort: hold {@code port} with a placeholder so it survives a stop→restart down-window.
   * No-op for a non-positive port. On failure the restart simply falls back to its bind-retry loop.
   */
  public static boolean reserve(int port) {
    if (port <= 0) {
      return false;
    }
    // The server we are reserving for has just begun shutting down; its listening socket may not be
    // fully released yet (it closes asynchronously on another thread). Retry the placeholder bind
    // with a short backoff so we grab the port the instant it frees and hold it across the restart
    // down-window. Because callers draw ports from a per-fork window below the ephemeral range, no
    // foreign process competes for this port, so the retry reliably wins once the old socket closes.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESERVE_RETRY_SECONDS);
    while (true) {
      try {
        closeQuietly(HELD.put(port, open(port)));
        return true;
      } catch (IOException e) {
        if (System.nanoTime() >= deadline) {
          log.warn("Could not reserve port {} across restart after {}s; rebind will rely on retry",
              port, RESERVE_RETRY_SECONDS, e);
          return false;
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  /** Drop the placeholder on {@code port} — call IMMEDIATELY before the real bind. No-op if none. */
  public static void release(int port) {
    if (port <= 0) {
      return;
    }
    closeQuietly(HELD.remove(port));
  }

  /**
   * Drop every still-held placeholder. Call at full cluster/suite shutdown (no restart is in
   * flight then) so ports reserved by nodes that stopped-without-restarting do not accumulate
   * across test methods sharing one JVM.
   */
  public static void releaseAll() {
    for (Integer port : HELD.keySet()) {
      closeQuietly(HELD.remove(port));
    }
    CLAIMED.clear();
  }

  private static ServerSocket open(int port) throws IOException {
    ServerSocket s = new ServerSocket();
    try {
      // Bind 127.0.0.1 (the exact address JettySolrRunner binds) and do NOT set SO_REUSEADDR.
      //
      // This matters for the window SCAN in reserveFreePort, which probes a port by trying to bind
      // it: the probe MUST fail when a server already holds that port, or we would hand out an
      // in-use port and the server's later bind would fail for the whole retry budget. With
      // SO_REUSEADDR, Linux lets a 0.0.0.0 (or 127.0.0.1) bind "succeed" over an existing
      // overlapping bind, so the probe would false-positive a port a jetty already holds. A plain
      // loopback bind with no SO_REUSEADDR conflicts with any existing loopback OR wildcard bind on
      // the port, so the probe is accurate; the same plain bind, while held, also blocks a foreign
      // process's wildcard bind (which cannot reuse a non-SO_REUSEADDR socket's port).
      s.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port));
      return s;
    } catch (IOException e) {
      closeQuietly(s);
      throw e;
    }
  }

  private static void closeQuietly(ServerSocket s) {
    if (s != null) {
      try {
        s.close();
      } catch (IOException ignore) {
        // best effort
      }
    }
  }
}
