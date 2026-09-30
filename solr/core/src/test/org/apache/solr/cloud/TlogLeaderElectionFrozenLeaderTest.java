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

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.servlet.ServletOutputStreamWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduces a TLOG leader election that stalls because the outgoing leader froze rather than died.
 *
 * <p>{@link ShardLeaderElectionContext#runLeaderProcess} calls {@link
 * ZkController#stopReplicationFromLeader} inline on the election thread. That tears down the
 * follower's replication process, which blocks in {@code ExecutorUtil.shutdownAndAwaitTermination}
 * until the in-flight index fetch finishes. {@code IndexFetcher.abortFetch} only sets a flag that
 * is polled while streaming file packets, so a fetch parked in the network phase is not cut short:
 * the election parks for the 60s executor wait before {@code shutdownNow()} finally interrupts the
 * poll thread.
 */
public class TlogLeaderElectionFrozenLeaderTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "tlog_frozen_leader";
  private static final String SHARD = "shard1";

  /**
   * A replacement election sleeps a fixed 2.5s for in-flight updates to settle
   * (ShardLeaderElectionContext), then syncs, replays its tlog and publishes in well under a
   * second. The bug parks it for the 60s {@code ExecutorUtil.awaitTermination} wait instead.
   */
  private static final long MAX_ACCEPTABLE_ELECTION_MS = 10_000;

  /** How long to wait for the follower's next poll to reach the stalled leader. */
  private static final long STALL_ARRIVAL_TIMEOUT_MS = 30_000;

  /** Where a follower's fetch can park against a frozen leader. */
  public enum TestStallPoint {
    INDEX_VERSION(ReplicationHandler.CMD_INDEX_VERSION, false),
    FILE_LIST(ReplicationHandler.CMD_GET_FILE_LIST, false),
    FILE_CONTENT(ReplicationHandler.CMD_GET_FILE, false),
    /**
     * Headers and part of the first packet arrive, then the body stops: the common production case.
     */
    FILE_CONTENT_BODY(ReplicationHandler.CMD_GET_FILE, true);

    final String command;
    final boolean midBody;

    TestStallPoint(String command, boolean midBody) {
      this.command = command;
      this.midBody = midBody;
    }
  }

  /**
   * Nightly runs every stall point. Otherwise only the mid-body file download, the most likely
   * failure in production, so every CI run covers it.
   */
  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    List<TestStallPoint> stallPoints =
        TEST_NIGHTLY ? List.of(TestStallPoint.values()) : List.of(TestStallPoint.FILE_CONTENT_BODY);
    return stallPoints.stream().map(stallPoint -> new Object[] {stallPoint}).toList();
  }

  private final TestStallPoint stallPoint;

  public TlogLeaderElectionFrozenLeaderTest(@Name("stallPoint") TestStallPoint stallPoint) {
    this.stallPoint = stallPoint;
  }

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");

    // extraFilters are installed ahead of SolrServlet and its filters (JettySolrRunner:336-338), so
    // ours sees the request first. It is installed on every node but only acts on the armed core.
    configureCluster(2)
        .withJettyConfig(b -> b.withFilter(TestStallReplicationFilter.class, "/*"))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    TestStallChannel stallChannel = TestStallReplicationFilter.STALL_CHANNEL.getAndSet(null);
    if (stallChannel != null) {
      stallChannel.released().complete(null);
    }
    shutdownCluster();
    System.clearProperty("solr.directoryFactory");
  }

  @Test
  public void testElectionIsNotBlockedByFrozenOldLeader() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 0, 2, 0)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    // Index something so the follower has a real index to poll against.
    indexAndCommit(0, 10);

    Slice shard = getCollectionState(COLLECTION).getSlice(SHARD);
    Replica oldLeader = shard.getLeader();
    Replica follower =
        shard.getReplicas().stream()
            .filter(r -> !r.getName().equals(oldLeader.getName()))
            .findFirst()
            .orElseThrow();
    JettySolrRunner leaderJetty = cluster.getReplicaJetty(oldLeader);
    JettySolrRunner followerJetty = cluster.getReplicaJetty(follower);

    // Stall one of the leader's replication commands. The follower polls every second under
    // jetty.testMode, so its next request for that command lands in the filter and never returns.
    log.info("Stalling {} responses from leader core {}", stallPoint, oldLeader.getCoreName());
    TestStallChannel channel = new TestStallChannel(oldLeader.getCoreName(), stallPoint);
    assertTrue(
        "the replication filter was already armed; a previous test did not release it",
        TestStallReplicationFilter.STALL_CHANNEL.compareAndSet(null, channel));

    // Give the follower something new to fetch. Every poll sends indexversion, but filelist and
    // filecontent are only requested once the leader has a newer commit than the follower.
    indexAndCommit(10, 20);

    // The ordering here is important: once the leader leaves live_nodes, later polls bail out
    // early (LEADER_IS_NOT_ACTIVE) without making an HTTP call. Only a fetch that is *already* in
    // the network phase reproduces the bug.
    try {
      channel.arrived().get(STALL_ARRIVAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail("the follower never issued a " + stallPoint.command + " request to the leader");
    }
    log.info("Follower's index fetch is parked in the leader's replication handler");

    log.info("Expiring the ZooKeeper session of the frozen leader {}", leaderJetty.getNodeName());
    long start = System.nanoTime();
    cluster.expireZkSession(leaderJetty);

    waitForState(
        "the surviving TLOG replica never became leader",
        COLLECTION,
        90,
        TimeUnit.SECONDS,
        (liveNodes, collectionState) -> {
          Replica leader = collectionState.getLeader(SHARD);
          return leader != null
              && leader.isActive(liveNodes)
              && leader.getNodeName().equals(followerJetty.getNodeName());
        });
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    log.info("New leader elected after {}ms", elapsedMs);
    assertTrue(
        "Leader election took "
            + elapsedMs
            + "ms with a frozen old leader; expected under "
            + MAX_ACCEPTABLE_ELECTION_MS
            + "ms. The election thread is parked in stopReplicationFromLeader waiting on an index "
            + "fetch that abortFetch() cannot cancel.",
        elapsedMs < MAX_ACCEPTABLE_ELECTION_MS);
  }

  private static void indexAndCommit(int fromId, int toId) throws Exception {
    for (int i = fromId; i < toId; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      cluster.getSolrClient(COLLECTION).add(doc);
    }
    cluster.getSolrClient(COLLECTION).commit();
  }

  /**
   * Stalls {@code /replication} requests for one command addressed to one particular core,
   * simulating a leader whose process is alive but which has stopped answering. Depending on the
   * {@link TestStallPoint}, the stall happens either before any response is sent, or after the
   * headers and the first half of the body's first write have been flushed to the wire.
   */
  public static class TestStallReplicationFilter implements Filter {

    private static final AtomicReference<TestStallChannel> STALL_CHANNEL = new AtomicReference<>();

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      // Capture once: the test nulls STALL_CHANNEL out in tearDownCluster().
      TestStallChannel stallChannel = STALL_CHANNEL.get();
      if (stallChannel == null || !stallChannel.matches(request)) {
        chain.doFilter(request, response);
      } else if (stallChannel.stallPoint().midBody) {
        chain.doFilter(request, new TestMidBodyStall((HttpServletResponse) response, stallChannel));
      } else {
        stallChannel.stall();
        chain.doFilter(request, response);
      }
    }
  }

  /**
   * Lets the headers and the first half of the body's first write reach the client, then stalls.
   */
  private static class TestMidBodyStall extends HttpServletResponseWrapper {
    private final TestStallChannel stallChannel;
    private ServletOutputStream stream;
    private boolean stalled;

    TestMidBodyStall(HttpServletResponse response, TestStallChannel stallChannel) {
      super(response);
      this.stallChannel = stallChannel;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      if (stream == null) {
        // Each write overload delegates independently, so all three need the hook.
        stream =
            new ServletOutputStreamWrapper(super.getOutputStream()) {
              @Override
              public void write(int b) throws IOException {
                super.write(b);
                stallOnce(this);
              }

              @Override
              public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
              }

              @Override
              public void write(byte[] b, int off, int len) throws IOException {
                if (!stalled && len > 1) {
                  int head = len / 2;
                  super.write(b, off, head);
                  stallOnce(this);
                  super.write(b, off + head, len - head);
                } else {
                  super.write(b, off, len);
                  stallOnce(this);
                }
              }
            };
      }
      return stream;
    }

    private void stallOnce(ServletOutputStream out) throws IOException {
      if (!stalled) {
        stalled = true;
        // Need to flush Jetty buffer so client gets header bytes.
        out.flush();
        stallChannel.stall();
      }
    }
  }

  private record TestStallChannel(
      String coreToStall,
      TestStallPoint stallPoint,
      CompletableFuture<Void> arrived,
      CompletableFuture<Void> released) {
    TestStallChannel(String coreToStall, TestStallPoint stallPoint) {
      this(coreToStall, stallPoint, new CompletableFuture<>(), new CompletableFuture<>());
    }

    boolean matches(ServletRequest request) {
      if (!(request instanceof HttpServletRequest http)) {
        return false;
      }
      String uri = http.getRequestURI();
      return uri != null
          && uri.endsWith("/" + coreToStall + ReplicationHandler.PATH)
          && stallPoint.command.equals(http.getParameter("command"));
    }

    /**
     * Tells the test the fetch has arrived and cannot complete, then holds the calling Jetty thread
     * for longer than the acceptable leader election duration, or until the test releases it.
     */
    void stall() throws IOException {
      arrived.complete(null);
      try {
        released.get(MAX_ACCEPTABLE_ELECTION_MS * 2, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Hold cap reached without a release; carry on and let teardown proceed.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(e.toString());
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
  }
}
