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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.eclipse.jetty.client.Result;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for two semaphore-permit leak bugs in {@link HttpJettySolrClient}'s {@code AsyncTracker}
 * that cause distributed queries to hang permanently.
 *
 * <h3>Pattern A – HTTP/2 GOAWAY double-queue leak</h3>
 *
 * <p>Jetty HTTP/2 can re-queue the same exchange after a GOAWAY/connection race, firing {@code
 * onRequestQueued} twice for one logical request. Because {@code onComplete} fires only once, one
 * permit is permanently consumed per occurrence, gradually draining the semaphore over hours or
 * days until Pattern B triggers.
 *
 * <h3>Pattern B – IO-thread deadlock on LB retry when permits depleted</h3>
 *
 * <p>When a connection-level failure causes {@link
 * org.apache.solr.client.solrj.jetty.LBJettySolrClient} to retry synchronously inside a {@code
 * whenComplete} callback on the Jetty IO selector thread, the retry calls {@code acquire()} on that
 * same IO thread before the original request's {@code onComplete} can call {@code release()}. No
 * permits are permanently lost — the deadlock simply requires two permits to be available
 * simultaneously — but if the semaphore is at zero, {@code acquire()} blocks the IO thread
 * permanently and distributed queries hang forever.
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

    configureCluster(1).addConfig("conf", configset("cloud-dynamic")).configure();

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
   * Demonstrates the permanent IO-thread deadlock (Pattern B) caused by {@link
   * org.apache.solr.client.solrj.jetty.LBJettySolrClient} retrying a request synchronously inside a
   * {@link CompletableFuture#whenComplete} callback that runs on the Jetty IO selector thread.
   *
   * <p>This test <b>passes</b> with the {@code failureDispatchExecutor} fix in this branch. Without
   * the fix, the IO thread would block forever in {@code semaphore.acquire()} and this test would
   * time out.
   */
  @Test
  public void testSemaphoreLeakOnLBRetry() throws Exception {
    // Dedicated client so that permanently deadlocked IO threads don't affect the cluster's client.
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
      // onFailure fires on the IO thread → LBJettySolrClient retry → acquire() blocks
      // (semaphore=0).
      int connCount = fakeConnections.size();
      log.info("Closing {} fake connections via RST...", connCount);
      for (Socket s : fakeConnections) {
        try {
          s.setSoLinger(true, 0); // send RST instead of FIN
          s.close();
        } catch (IOException ignored) {
        }
      }

      try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
            .get(30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        // Individual request failure is fine; permits are released by onComplete regardless.
        log.warn("Some requests failed during retry", e);
      } catch (TimeoutException e) {
        // Force-stop the HttpClient to unblock any threads stuck in semaphore.acquire()
        // before asserting failure, so the finally block can close the client without hanging.
        try {
          testClient.getHttpClient().stop();
        } catch (Exception ignored) {
        }
        fail(
            "BUG (LBJettySolrClient retry deadlock): futures did not complete within 30s."
                + " IO threads are permanently blocked in semaphore.acquire() because the retry"
                + " fires synchronously on the IO thread before onComplete can release().");
      }

      int permitsAfterFailures = testClient.asyncTrackerAvailablePermits();
      log.info("Permits after retries: {}/{}", permitsAfterFailures, MAX_PERMITS);
      assertEquals(
          "All permits should be restored after retries complete",
          MAX_PERMITS,
          permitsAfterFailures);
    } finally {
      fakeServer.close();
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
   * <p>Uses only {@code 20} requests, well below the configured limit of {@code 40}. With permits
   * still available, {@code acquire()} on the IO thread returns immediately (does not block), so
   * {@code onComplete} fires normally and every permit is returned.
   *
   * <p>This test <b>passes both with and without the Pattern B fix</b>. The deadlock only manifests
   * when the semaphore is fully exhausted (demonstrated by {@link #testSemaphoreLeakOnLBRetry}).
   */
  @Test
  public void testNoPermitLeakOnLBRetryWhenSemaphoreNotExhausted() throws Exception {
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
      assertTrue("numRequests must be well below permit limit", numRequests < initialPermits);
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
      fakeServer.close();
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
   * Verifies that the {@code PERMIT_ACQUIRED_ATTR} idempotency guard prevents the Pattern A permit
   * leak where Jetty HTTP/2 re-queues the same exchange after a GOAWAY/connection race, firing
   * {@code onRequestQueued} twice for one logical request while {@code onComplete} fires only once.
   *
   * <p>Rather than setting up a real HTTP/2 server, this test uses reflection to invoke {@code
   * AsyncTracker.queuedListener} twice and {@code AsyncTracker.completeListener} once for the same
   * {@code Request} object. Without the guard the semaphore count drops by one; with the guard the
   * second queued call is a no-op and the count is unchanged.
   */
  @Test
  @SuppressForbidden(
      reason =
          "Reflection needed to access AsyncTracker's private fields for white-box testing without exposing them in the production API")
  public void testPermitLeakOnHttp2GoAwayDoubleQueuedListener() throws Exception {
    assumeWorkingMockito();

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

      // Fake Request that supports the attribute get/set used by the idempotency guard.
      Map<String, Object> reqAttributes = new HashMap<>();
      Request fakeRequest = Mockito.mock(Request.class);
      Mockito.when(fakeRequest.getAttributes()).thenReturn(reqAttributes);
      Mockito.when(fakeRequest.attribute(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
          .thenAnswer(
              inv -> {
                reqAttributes.put(inv.getArgument(0), inv.getArgument(1));
                return fakeRequest;
              });

      // Simulate the GOAWAY double-fire: 1st call acquires a permit; 2nd is the bug trigger.
      queuedListener.onQueued(fakeRequest);
      queuedListener.onQueued(fakeRequest);

      Result fakeResult = Mockito.mock(Result.class);
      Mockito.when(fakeResult.getRequest()).thenReturn(fakeRequest);
      // Only one onComplete fires for the logical request (regardless of internal retries).
      completeListener.onComplete(fakeResult);

      int permitsAfter = testClient.asyncTrackerAvailablePermits();
      log.info("Permits after double-queued + single complete: {}/{}", permitsAfter, maxPermits);

      assertEquals(
          "BUG (Jetty HTTP/2 GOAWAY retry permit leak): onRequestQueued fired twice for the"
              + " same Request object but onComplete fired only once. The second acquire()"
              + " was not matched by a release(), permanently leaking one permit per"
              + " occurrence. In production this causes gradual semaphore depletion over"
              + " hours/days until Pattern B IO-thread deadlock triggers.",
          maxPermits,
          permitsAfter);

    } finally {
      // Force-terminate the Phaser as a safety net; without the fix the phaser would be unbalanced.
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
