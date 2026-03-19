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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.jetty.LBJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduces the {@link org.apache.solr.client.solrj.impl.LBAsyncSolrClient} semaphore leak that
 * causes distributed queries to hang permanently.
 *
 * <h3>Bug scenario</h3>
 *
 * <ol>
 *   <li>A shard HTTP request fails with a <em>connection-level</em> error (not an HTTP-level
 *       error). Jetty fires the {@code onFailure} response callback directly on the IO selector
 *       thread.
 *   <li>{@link org.apache.solr.client.solrj.jetty.HttpJettySolrClient#requestAsync} completes its
 *       {@code CompletableFuture} exceptionally from within that {@code onFailure} callback — still
 *       on the IO thread.
 *   <li>{@code LBAsyncSolrClient.doAsyncRequest} registered a {@code whenComplete} on that future.
 *       Because the future completes on the IO thread, {@code whenComplete} also fires
 *       <em>synchronously on the IO thread</em>.
 *   <li>The {@code whenComplete} action calls {@code doAsyncRequest} again (retry to the next
 *       endpoint), which eventually calls Jetty's {@code HttpClient.send()}. That triggers the
 *       {@code AsyncTracker.queuedListener} — which calls {@code semaphore.acquire()} — still on
 *       the IO thread, before the original request's {@code completeListener.onComplete()} has had
 *       a chance to call {@code semaphore.release()}.
 *   <li>If the semaphore is at zero, {@code acquire()} <em>blocks the IO thread</em>. The blocked
 *       IO thread cannot execute the {@code completeListener} that would release the original
 *       permit. The permit is permanently leaked, and the IO thread is permanently stuck. Repeat
 *       until all permits are exhausted: distributed queries hang forever.
 * </ol>
 *
 * <h3>Test setup</h3>
 *
 * <p>A raw TCP server accepts {@value #NUM_RETRY_REQUESTS} connections and holds them open until
 * all are established (so all semaphore permits are consumed). It then closes all connections
 * simultaneously via TCP RST, causing all Jetty {@code onFailure} events to fire on the IO threads
 * at the same time. Because the semaphore is already at 0, every retry's {@code acquire()} blocks
 * the IO thread immediately, and no {@code onComplete} release can fire.
 *
 * <p>The test asserts that after a short wait the semaphore is at 0 and none of the {@code
 * CompletableFuture}s returned by {@code requestAsync} have completed — proving the permanent
 * permit exhaustion.
 */
public class AsyncTrackerSemaphoreLeakTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "semaphore_leak_test";

  /** Reduced semaphore size so we can observe the drain without needing thousands of requests. */
  private static final int MAX_PERMITS = 40;

  /**
   * Number of concurrent requests. Set equal to MAX_PERMITS so that all permits are exhausted
   * before any retry can acquire, triggering the IO-thread deadlock.
   */
  private static final int NUM_RETRY_REQUESTS = MAX_PERMITS;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Reduce the semaphore size so we can observe drain with few requests.
    // This property is read when HttpJettySolrClient is constructed, so it must
    // be set BEFORE the cluster (and its HttpShardHandlerFactory) starts up.
    System.setProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP, String.valueOf(MAX_PERMITS));

    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .process(cluster.getSolrClient());

    waitForState(
        "Expected 1 active shard with 1 replica",
        COLLECTION,
        (n, c) -> SolrCloudTestCase.replicasForCollectionAreFullyActive(n, c, 2, 1));
  }

  @AfterClass
  public static void cleanup() {
    System.clearProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP);
  }

  /**
   * Demonstrates the permanent IO-thread deadlock caused by {@link
   * org.apache.solr.client.solrj.impl.LBAsyncSolrClient} retrying a request synchronously inside a
   * {@link CompletableFuture#whenComplete} callback that runs on the Jetty IO selector thread.
   *
   * <p>This assertion <b>FAILS</b> with the current code, demonstrating the bug. After a fix (e.g.
   * dispatching the retry to an executor thread instead of running it synchronously on the IO
   * thread), the retries proceed on executor threads, the IO threads remain free to fire {@code
   * onComplete → release()}, and all futures eventually complete via the real server.
   */
  @Test
  public void testSemaphoreLeakOnLBRetry() throws Exception {
    // Create a dedicated HttpJettySolrClient for this test so that if the IO threads are
    // permanently deadlocked they don't affect the cluster's shared client.
    // The system property is still set to MAX_PERMITS from setupCluster().
    HttpJettySolrClient testClient =
        new HttpJettySolrClient.Builder()
            .withConnectionTimeout(5, TimeUnit.SECONDS)
            .withIdleTimeout(30, TimeUnit.SECONDS)
            .useHttp1_1(true) // HTTP/1.1: every request gets its own TCP connection
            .build();

    // Fake TCP server: accepts exactly NUM_RETRY_REQUESTS connections and holds them open.
    // Once all are established (semaphore exhausted), closes all with RST simultaneously.
    ServerSocket fakeServer = new ServerSocket(0);
    CountDownLatch allConnected = new CountDownLatch(NUM_RETRY_REQUESTS);
    List<Socket> fakeConnections = Collections.synchronizedList(new ArrayList<>());

    Thread fakeServerThread =
        new Thread(
            () -> {
              try {
                while (fakeConnections.size() < NUM_RETRY_REQUESTS && !fakeServer.isClosed()) {
                  Socket s = fakeServer.accept();
                  fakeConnections.add(s);
                  allConnected.countDown();
                }
              } catch (IOException ignored) {
              }
            },
            "fake-tcp-server");
    fakeServerThread.setDaemon(true);
    fakeServerThread.start();

    String fakeBaseUrl = "http://127.0.0.1:" + fakeServer.getLocalPort() + "/solr";
    String realBaseUrl =
        cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTION;

    List<CompletableFuture<LBSolrClient.Rsp>> futures = new ArrayList<>();

    try (LBJettySolrClient lbClient = new LBJettySolrClient.Builder(testClient).build()) {

      assertEquals(
          "All permits should be available before the test (verifies sysprop was applied)",
          MAX_PERMITS,
          testClient.asyncTrackerAvailablePermits());

      // Submit NUM_RETRY_REQUESTS async requests.
      // Each request has two endpoints: fakeBaseUrl (first) and realBaseUrl (second/retry).
      // Each requestAsync() call acquires a semaphore permit synchronously during send().
      // After NUM_RETRY_REQUESTS calls, the semaphore is at 0.
      for (int i = 0; i < NUM_RETRY_REQUESTS; i++) {
        ModifiableSolrParams qparams = new ModifiableSolrParams();
        qparams.set("q", "*:*");
        QueryRequest qr = new QueryRequest(qparams);
        LBSolrClient.Req req =
            new LBSolrClient.Req(
                qr,
                List.of(
                    new LBSolrClient.Endpoint(fakeBaseUrl),
                    new LBSolrClient.Endpoint(realBaseUrl)));
        futures.add(lbClient.requestAsync(req));
      }

      log.info(
          "Queued {} requests (semaphore now at 0). Waiting for all TCP connections...",
          NUM_RETRY_REQUESTS);

      // Wait until the fake server has accepted all NUM_RETRY_REQUESTS connections.
      // At this point all semaphore permits are consumed and no onComplete has fired yet.
      assertTrue(
          "All "
              + NUM_RETRY_REQUESTS
              + " connections should be established within 15 s, but only "
              + (NUM_RETRY_REQUESTS - allConnected.getCount())
              + " were.",
          allConnected.await(15, TimeUnit.SECONDS));

      assertEquals(
          "Semaphore should be fully consumed after queuing all requests",
          0,
          testClient.asyncTrackerAvailablePermits());

      // Close all fake connections simultaneously with TCP RST.
      // This fires Jetty's onFailure callback on the IO selector thread for each request.
      // The onFailure path → future.completeExceptionally() → whenComplete fires synchronously
      // on the IO thread → LBAsyncSolrClient.doAsyncRequest (retry) → send() →
      // onRequestQueued → semaphore.acquire() → BLOCKS (semaphore = 0) → IO thread stuck.
      int connCount = fakeConnections.size();
      log.info("Closing {} fake connections via RST...", connCount);
      for (Socket s : fakeConnections) {
        try {
          s.setSoLinger(true, 0); // send RST instead of FIN
          s.close();
        } catch (IOException ignored) {
        }
      }

      // Give IO threads time to process the failure events and attempt the retry acquires.
      Thread.sleep(2000);

      int permitsAfterFailures = testClient.asyncTrackerAvailablePermits();
      long completedCount = futures.stream().filter(CompletableFuture::isDone).count();
      log.info(
          "Permits after 2s: {}/{}; futures completed: {}/{}",
          permitsAfterFailures,
          MAX_PERMITS,
          completedCount,
          NUM_RETRY_REQUESTS);

      // With the bug: the IO threads are deadlocked. Permits remain at 0 and no future completes.
      // This assertion FAILS with the current code, demonstrating the bug.
      assertEquals(
          "BUG (LBAsyncSolrClient retry leak): all "
              + NUM_RETRY_REQUESTS
              + " semaphore permits should be released once the retries complete on the"
              + " real server. Instead the IO threads are permanently blocked in"
              + " semaphore.acquire() because the retry fires synchronously on the IO thread"
              + " before the original request's completeListener can call release().",
          MAX_PERMITS,
          permitsAfterFailures);
    } finally {
      fakeServer.close();

      // Force-stop the underlying Jetty HttpClient to unblock any IO threads permanently
      // stuck in semaphore.acquire(), so that the test client can be closed without hanging.
      try {
        testClient.getHttpClient().stop();
      } catch (Exception ignored) {
      }
      try {
        testClient.close();
      } catch (Exception ignored) {
      }

      for (CompletableFuture<LBSolrClient.Rsp> f : futures) {
        f.cancel(true);
      }
    }
  }

  /**
   * Verifies that no semaphore permits are permanently leaked when connection-level failures
   * trigger LB retries on the Jetty IO selector thread, provided the semaphore is not exhausted.
   *
   * <p>This test uses the production default ({@code 1000}) permits and only {@code 20} requests.
   * With plenty of permits available, {@code acquire()} on the IO thread returns immediately (does
   * not block), so {@code onComplete} fires normally and every permit is returned.
   *
   * <p>This test <b>passes both with and without the Pattern A fix</b>. Run it with the fix
   * commented out to confirm that the deadlock only manifests when the semaphore is fully exhausted
   * (as demonstrated by {@link #testSemaphoreLeakOnLBRetry}).
   */
  @Test
  public void testNoPermitLeakOnLBRetryWithDefaultPermits() throws Exception {
    // The @BeforeClass set ASYNC_REQUESTS_MAX_SYSPROP=40 for the cluster. Clear it temporarily
    // so this test's dedicated client uses the real production default (1000 permits).
    String savedMax = System.getProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP);
    System.clearProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP);

    final int numRequests = 20;

    HttpJettySolrClient testClient =
        new HttpJettySolrClient.Builder()
            .withConnectionTimeout(5, TimeUnit.SECONDS)
            .withIdleTimeout(30, TimeUnit.SECONDS)
            .useHttp1_1(true)
            .build();

    ServerSocket fakeServer = new ServerSocket(0);
    CountDownLatch allConnected = new CountDownLatch(numRequests);
    List<Socket> fakeConnections = Collections.synchronizedList(new ArrayList<>());

    Thread fakeServerThread =
        new Thread(
            () -> {
              try {
                while (fakeConnections.size() < numRequests && !fakeServer.isClosed()) {
                  Socket s = fakeServer.accept();
                  fakeConnections.add(s);
                  allConnected.countDown();
                }
              } catch (IOException ignored) {
              }
            },
            "fake-tcp-server");
    fakeServerThread.setDaemon(true);
    fakeServerThread.start();

    String fakeBaseUrl = "http://127.0.0.1:" + fakeServer.getLocalPort() + "/solr";
    String realBaseUrl =
        cluster.getJettySolrRunners().get(0).getBaseUrl().toString() + "/" + COLLECTION;

    List<CompletableFuture<LBSolrClient.Rsp>> futures = new ArrayList<>();

    try (LBJettySolrClient lbClient = new LBJettySolrClient.Builder(testClient).build()) {

      int initialPermits = testClient.asyncTrackerMaxPermits();
      assertEquals("Should use the production default of 1000 permits", 1000, initialPermits);
      assertEquals(
          "All permits available before test",
          initialPermits,
          testClient.asyncTrackerAvailablePermits());

      for (int i = 0; i < numRequests; i++) {
        ModifiableSolrParams p = new ModifiableSolrParams();
        p.set("q", "*:*");
        futures.add(
            lbClient.requestAsync(
                new LBSolrClient.Req(
                    new QueryRequest(p),
                    List.of(
                        new LBSolrClient.Endpoint(fakeBaseUrl),
                        new LBSolrClient.Endpoint(realBaseUrl)))));
      }

      log.info(
          "Submitted {} requests ({} permits, semaphore far from exhaustion). "
              + "Waiting for connections...",
          numRequests,
          initialPermits);
      assertTrue(
          "All " + numRequests + " connections should be established within 15 s",
          allConnected.await(15, TimeUnit.SECONDS));

      int rstCount = fakeConnections.size();
      log.info("RST-ing {} fake connections...", rstCount);
      for (Socket s : fakeConnections) {
        try {
          s.setSoLinger(true, 0);
          s.close();
        } catch (IOException ignored) {
        }
      }

      // With permits >> 0, acquire() on the IO thread returns immediately (no blocking).
      // onComplete fires normally after each retry and restores every permit.
      // Expect all futures to resolve (via retry to the real server) within 30 s.
      try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
            .get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Retry failure (e.g. transient real-server error): permits are still released by
        // onComplete, so we proceed to the assertion.
        log.warn("Some futures completed exceptionally", e);
      }

      int permitsAfter = testClient.asyncTrackerAvailablePermits();
      long completedCount = futures.stream().filter(CompletableFuture::isDone).count();
      log.info(
          "Permits after retries: {}/{}; futures completed: {}/{}",
          permitsAfter,
          initialPermits,
          completedCount,
          numRequests);

      assertEquals(
          "No permits leaked: with "
              + numRequests
              + " requests against a "
              + initialPermits
              + "-permit semaphore, acquire() never blocks the IO thread so onComplete"
              + " always fires and restores every permit.",
          initialPermits,
          permitsAfter);

    } finally {
      // Restore the property so subsequent tests (and @AfterClass) see the expected value.
      if (savedMax != null) {
        System.setProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP, savedMax);
      }
      fakeServer.close();
      try {
        testClient.getHttpClient().stop();
      } catch (Exception ignored) {
      }
      try {
        testClient.close();
      } catch (Exception ignored) {
      }
      for (CompletableFuture<LBSolrClient.Rsp> f : futures) {
        f.cancel(true);
      }
    }
  }

  /**
   * Demonstrates the <em>gradual</em> permit leak caused by Jetty HTTP/2 internally re-queuing the
   * same request when it is dispatched to a connection that has already been marked {@code
   * closed=true} (post-GOAWAY race).
   *
   * <h3>Root cause</h3>
   *
   * <p>In Jetty 12, {@code HttpConnectionOverHTTP2.send(HttpExchange)} returns {@code
   * SendFailure(ClosedChannelException, retry=true)} when the target connection is already marked
   * closed (e.g. because the server sent a GOAWAY frame while the request was being dispatched from
   * the destination queue). {@code HttpDestination.process()} then calls {@code send(HttpExchange)}
   * again — re-enqueuing the <em>same</em> exchange object and re-firing {@code notifyQueued()} —
   * which invokes {@code AsyncTracker.queuedListener} a <b>second time</b>, calling {@code
   * semaphore.acquire()} again. Because {@code onComplete} still fires only once for the logical
   * request, there is no matching second {@code release()}. One permit is permanently consumed per
   * occurrence.
   *
   * <h3>Production impact</h3>
   *
   * <p>In a 2-node/2-shard cluster with default 1000 permits, each HTTP/2 connection close (server
   * restart, load-balancer connection draining, etc.) that races with an in-flight request leaks
   * one permit. Over hours or days, permits gradually drain from 1000 toward zero. Once the
   * semaphore is nearly exhausted, even a small burst of connection failures triggers the Pattern A
   * IO-thread deadlock and the cluster hangs permanently.
   *
   * <h3>Test approach</h3>
   *
   * <p>Rather than setting up a real HTTP/2 server with GOAWAY, this test directly simulates the
   * double {@code onRequestQueued} notification via reflection. It accesses {@code
   * AsyncTracker.queuedListener} and {@code AsyncTracker.completeListener}, invokes the queued
   * listener twice for the same {@code Request} object, and invokes the complete listener once.
   * With the bug present the semaphore count drops by one; with the fix (idempotency guard on the
   * request attribute) the count is unchanged.
   *
   * <p>This test <b>FAILS</b> without the {@code PERMIT_ACQUIRED_ATTR} idempotency guard,
   * demonstrating the leak.
   */
  @Test
  @SuppressForbidden(
      reason =
          "Reflection needed to access AsyncTracker's private fields for white-box testing without exposing them in the production API")
  public void testPermitLeakOnHttp2GoAwayDoubleQueuedListener() throws Exception {
    assumeWorkingMockito();
    // Clear the @BeforeClass 40-permit cap so this client gets the production default (1000).
    String savedMax = System.getProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP);
    System.clearProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP);

    HttpJettySolrClient testClient =
        new HttpJettySolrClient.Builder()
            .withConnectionTimeout(5, TimeUnit.SECONDS)
            .withIdleTimeout(30, TimeUnit.SECONDS)
            // HTTP/2 is the default transport where this GOAWAY race occurs.
            .build();

    // Capture asyncTracker and its class for reflection-based listener access and cleanup.
    Field asyncTrackerField = HttpJettySolrClient.class.getDeclaredField("asyncTracker");
    asyncTrackerField.setAccessible(true);
    Object asyncTracker = asyncTrackerField.get(testClient);
    Class<?> asyncTrackerClass = asyncTracker.getClass();

    try {
      int maxPermits = testClient.asyncTrackerMaxPermits();
      assertEquals("Should use production default of 1000 permits", 1000, maxPermits);
      assertEquals(
          "All permits available before test",
          maxPermits,
          testClient.asyncTrackerAvailablePermits());

      // Access the raw listeners via reflection to simulate Jetty's internal double-fire.
      Field queuedListenerField = asyncTrackerClass.getDeclaredField("queuedListener");
      queuedListenerField.setAccessible(true);
      Request.QueuedListener queuedListener =
          (Request.QueuedListener) queuedListenerField.get(asyncTracker);

      Field completeListenerField = asyncTrackerClass.getDeclaredField("completeListener");
      completeListenerField.setAccessible(true);
      Response.CompleteListener completeListener =
          (Response.CompleteListener) completeListenerField.get(asyncTracker);

      // Build a minimal fake Request that supports attribute get/set.
      // The fix reads request.getAttributes().get(key) and writes request.attribute(key, value).
      // Without the fix the request parameter is ignored, so null would also suffice here.
      Map<String, Object> reqAttributes = new HashMap<>();
      Request fakeRequest = Mockito.mock(Request.class);
      Mockito.when(fakeRequest.getAttributes()).thenReturn(reqAttributes);
      Mockito.when(fakeRequest.attribute(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
          .thenAnswer(
              inv -> {
                reqAttributes.put(inv.getArgument(0), inv.getArgument(1));
                return fakeRequest;
              });

      // Simulate the Jetty HTTP/2 GOAWAY race:
      //   1st call — normal queueing; acquire() consumes one permit.
      //   2nd call — Jetty internal retry after ClosedChannelException; BUG: acquire() again.
      queuedListener.onQueued(fakeRequest);
      queuedListener.onQueued(fakeRequest);

      // Only one onComplete fires for the logical request (regardless of internal retries).
      completeListener.onComplete(null);

      int permitsAfter = testClient.asyncTrackerAvailablePermits();
      log.info("Permits after double-queued + single complete: {}/{}", permitsAfter, maxPermits);

      // BUG: the second acquire() has no matching release(). One permit is permanently leaked.
      // This assertion FAILS with the current code, demonstrating the bug.
      // After applying the PERMIT_ACQUIRED_ATTR idempotency guard the second onQueued call is
      // a no-op, acquire() is called only once, and this assertion passes.
      assertEquals(
          "BUG (Jetty HTTP/2 GOAWAY retry permit leak): onRequestQueued fired twice for the"
              + " same Request object but onComplete fired only once. The second acquire()"
              + " was not matched by a release(), permanently leaking one permit per"
              + " occurrence. In production this causes gradual semaphore depletion over"
              + " hours/days until Pattern A IO-thread deadlock triggers.",
          maxPermits,
          permitsAfter);

    } finally {
      // Restore the system property for subsequent tests.
      if (savedMax != null) {
        System.setProperty(HttpJettySolrClient.ASYNC_REQUESTS_MAX_SYSPROP, savedMax);
      }

      // Force-terminate the Phaser as a safety net in case close() would otherwise hang.
      // With the PERMIT_ACQUIRED_ATTR fix the second onQueued() call is a no-op (the attribute
      // is already set), so the phaser is balanced. forceTermination() is therefore harmless here.
      try {
        Field phaserField = asyncTrackerClass.getDeclaredField("phaser");
        phaserField.setAccessible(true);
        Phaser phaser = (Phaser) phaserField.get(asyncTracker);
        phaser.forceTermination();
      } catch (Exception ignored) {
      }

      try {
        testClient.close();
      } catch (Exception ignored) {
      }
    }
  }
}
