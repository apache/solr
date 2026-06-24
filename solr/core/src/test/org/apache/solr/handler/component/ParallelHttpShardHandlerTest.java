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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ParallelHttpShardHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  private static class DirectExecutorService extends AbstractExecutorService {
    private volatile boolean shutdown;

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

  @Test
  public void testSubmitFailureIsRecordedWhenSuperThrows() throws Exception {
    ParallelHttpShardHandlerFactory factory = new ParallelHttpShardHandlerFactory();
    factory.commExecutor = new DirectExecutorService();
    ParallelHttpShardHandler handler = new ParallelHttpShardHandler(factory);

    // Force super.makeShardRequest to throw before it enqueues the response future.
    handler.lbClient = null;

    ShardRequest shardRequest = buildShardRequest("shardA");

    ShardResponse shardResponse = new ShardResponse();
    shardResponse.setShardRequest(shardRequest);
    shardResponse.setShard("shardA");

    HttpShardHandler.SimpleSolrResponse simpleResponse = new HttpShardHandler.SimpleSolrResponse();
    shardResponse.setSolrResponse(simpleResponse);

    ModifiableSolrParams params = new ModifiableSolrParams();
    QueryRequest queryRequest = new QueryRequest(params);
    LBSolrClient.Endpoint endpoint = new LBSolrClient.Endpoint("http://ignored:8983/solr");
    LBSolrClient.Req lbReq =
        new LBSolrClient.Req(queryRequest, Collections.singletonList(endpoint));

    handler.makeShardRequest(
        shardRequest, "shardA", params, lbReq, simpleResponse, shardResponse, System.nanoTime());

    ShardResponse recorded = handler.responses.poll(1, TimeUnit.SECONDS);

    assertNotNull(
        "The asynchronous submit should record the shard failure when super.makeShardRequest fails",
        recorded);
    assertSame(
        "The recorded shard response should be the same instance passed into recordShardSubmitError",
        shardResponse,
        recorded);
    assertNotNull(
        "Expected an exception to be attached to the recorded shard response",
        recorded.getException());
    assertTrue(recorded.getException() instanceof SolrException);
  }

  /**
   * Verifies the contract that when the commExecutor rejects the runnable, the failure is recorded
   * via recordShardSubmitError (i.e., shows up in the responses queue) rather than being propagated
   * synchronously to the caller.
   *
   * <p>This exercises issue #1 from the ParallelHttpShardHandler review: with a single-thread
   * ThreadPoolExecutor backed by a SynchronousQueue, once the worker is busy, the next
   * CompletableFuture.runAsync(...) call throws RejectedExecutionException synchronously out of
   * makeShardRequest. The expected (post-fix) behavior is that the error is routed through
   * recordShardSubmitError instead.
   */
  @Test
  public void testRejectedExecutorRecordsErrorInsteadOfThrowing() throws Exception {
    CountDownLatch holdWorker = new CountDownLatch(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    ThreadPoolExecutor busyExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>()); // default AbortPolicy
    try {
      // Occupy the single worker thread so the next submission has nowhere to go.
      busyExecutor.execute(
          () -> {
            workerStarted.countDown();
            try {
              holdWorker.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      assertTrue("worker did not start within timeout", workerStarted.await(5, TimeUnit.SECONDS));

      ParallelHttpShardHandlerFactory factory = new ParallelHttpShardHandlerFactory();
      factory.commExecutor = busyExecutor;
      ParallelHttpShardHandler handler = new ParallelHttpShardHandler(factory);

      ShardRequest shardRequest = buildShardRequest("shardA");

      ShardResponse shardResponse = new ShardResponse();
      shardResponse.setShardRequest(shardRequest);
      shardResponse.setShard("shardA");

      HttpShardHandler.SimpleSolrResponse simpleResponse =
          new HttpShardHandler.SimpleSolrResponse();
      shardResponse.setSolrResponse(simpleResponse);

      ModifiableSolrParams params = new ModifiableSolrParams();
      QueryRequest queryRequest = new QueryRequest(params);
      LBSolrClient.Endpoint endpoint = new LBSolrClient.Endpoint("http://ignored:8983/solr");
      LBSolrClient.Req lbReq = new LBSolrClient.Req(queryRequest, List.of(endpoint));

      // The desired contract: rejection is captured and surfaced through the responses queue
      // (i.e., this call should not throw RejectedExecutionException).
      try {
        handler.makeShardRequest(
            shardRequest,
            "shardA",
            params,
            lbReq,
            simpleResponse,
            shardResponse,
            System.nanoTime());
      } catch (RejectedExecutionException ree) {
        fail(
            "makeShardRequest should not propagate RejectedExecutionException; the failure "
                + "should be recorded via recordShardSubmitError. Got: "
                + ree);
      }

      ShardResponse recorded = handler.responses.poll(2, TimeUnit.SECONDS);
      assertNotNull(
          "Expected the executor rejection to be recorded as a shard failure in the responses"
              + " queue, but no response arrived",
          recorded);
      assertSame(
          "The recorded shard response should be the same instance passed in",
          shardResponse,
          recorded);
      assertNotNull(
          "Expected an exception to be attached to the recorded shard response",
          recorded.getException());
    } finally {
      holdWorker.countDown();
      busyExecutor.shutdownNow();
      busyExecutor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private ShardRequest buildShardRequest(String shard) {
    ShardRequest sreq = new ShardRequest();
    sreq.params = new ModifiableSolrParams();
    sreq.actualShards = new String[] {shard};
    return sreq;
  }

  /**
   * Runs handler.takeCompletedIncludingErrors() on a worker thread with a timeout. If take() does
   * not return within timeoutMs, fails the test with a clear message naming the iteration and phase
   * — this is the signal for the lost-wakeup bug.
   */
  private ShardResponse runTakeWithTimeout(
      ParallelHttpShardHandler handler,
      ExecutorService takeExecutor,
      int iteration,
      String phaseLabel,
      long timeoutMs)
      throws Exception {
    Future<ShardResponse> future = takeExecutor.submit(handler::takeCompletedIncludingErrors);
    try {
      return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      future.cancel(true);
      throw new AssertionError(
          "take() hung in iteration "
              + iteration
              + " "
              + phaseLabel
              + ": did not return within "
              + timeoutMs
              + "ms. The worker thread is parked in LinkedBlockingQueue.take() waiting for"
              + " an element that will never arrive because the handler's state transitioned"
              + " to empty without anything being enqueued on the responses queue.");
    } catch (ExecutionException ee) {
      throw new AssertionError(
          "take() threw unexpectedly in iteration " + iteration + " " + phaseLabel, ee.getCause());
    }
  }

  /**
   * More aggressive variant of the lost-wakeup stress test that uses asynchronous inner-future
   * completion on a dedicated scheduler. In production the inner future (from {@code
   * lbClient.requestAsync}) completes on a Jetty IO thread, not synchronously at the registration
   * site. That timing gap between {@code super.makeShardRequest} returning (and the outer {@code
   * whenComplete} firing to remove {@code submitFutures}) and the inner {@code whenComplete} firing
   * (to add to {@code responses}) is exactly where the observed 930-handler hang lives. This test
   * matches that timing.
   */
  @Test
  public void testTakeDoesNotHangUnderAsyncInnerFutureCompletion() throws Exception {
    // The race window is wide, so a modest count reliably catches a regression on routine CI; run
    // many more under -Dtests.nightly to keep deep coverage without bounding every build by the
    // worst-case (iterations * perIterationTimeoutMs) hang time.
    final int iterations = TEST_NIGHTLY ? 1000 : 100;
    final long perIterationTimeoutMs = 3_000;

    ExecutorService commExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            5L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new SolrNamedThreadFactory("testCommExecutor"));

    // Simulates Jetty IO threads: a small pool that completes the inner future asynchronously
    // some tiny amount of time after requestAsync() returns, exposing the race window.
    ExecutorService mockIoThreads =
        ExecutorUtil.newMDCAwareFixedThreadPool(2, new SolrNamedThreadFactory("testMockIo"));

    ExecutorService takeExecutor =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("testTakeRunner"));

    try {
      for (int i = 0; i < iterations; i++) {
        runAsyncRaceCycle(commExecutor, mockIoThreads, takeExecutor, i, perIterationTimeoutMs);
      }
    } finally {
      takeExecutor.shutdownNow();
      takeExecutor.awaitTermination(5, TimeUnit.SECONDS);
      mockIoThreads.shutdownNow();
      mockIoThreads.awaitTermination(5, TimeUnit.SECONDS);
      commExecutor.shutdown();
      if (!commExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
        commExecutor.shutdownNow();
        commExecutor.awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  private void runAsyncRaceCycle(
      ExecutorService commExecutor,
      ExecutorService mockIoThreads,
      ExecutorService takeExecutor,
      int iteration,
      long timeoutMs)
      throws Exception {

    ParallelHttpShardHandlerFactory factory = new ParallelHttpShardHandlerFactory();
    factory.commExecutor = commExecutor;
    ParallelHttpShardHandler handler = new ParallelHttpShardHandler(factory);

    // LB client that returns a future which completes asynchronously on a separate thread —
    // mimicking the Jetty IO thread model. This creates a real race between:
    //   (a) the outer runAsync future completing + its whenComplete removing submitFutures,
    //   (b) the inner future completing + its whenComplete adding to responses.
    LBHttp2SolrClient mockLb = Mockito.mock(LBHttp2SolrClient.class);
    Mockito.when(mockLb.requestAsync(Mockito.any(LBSolrClient.Req.class)))
        .thenAnswer(
            inv -> {
              CompletableFuture<LBSolrClient.Rsp> f = new CompletableFuture<>();
              mockIoThreads.execute(() -> f.complete(new LBSolrClient.Rsp()));
              return f;
            });
    handler.lbClient = mockLb;

    // Single-shard submit → take. This is the simplest real workload. Under async inner-future
    // completion, the outer whenComplete (removing submitFutures) and inner whenComplete
    // (adding to responses) race. If there's a window where responsesPending() transitions to
    // false without the responses queue getting an entry, take() parks forever.
    ShardRequest sreq = buildShardRequest("shard-" + iteration);
    handler.submit(sreq, "shard-" + iteration, sreq.params);

    ShardResponse rsp =
        runTakeWithTimeout(handler, takeExecutor, iteration, "async-race", timeoutMs);

    assertNotNull(
        "async-race iteration " + iteration + " take() returned null — response was never enqueued",
        rsp);
  }

  /**
   * Invariant test for the cancellation synchronization contract in {@link
   * ParallelHttpShardHandler}: when {@code makeShardRequest} is invoked while {@code canceled} is
   * already {@code true}, the outer future must be cancelled and NOT tracked in {@code
   * submitFutures}. This keeps {@code submitFutures} consistent with the cancellation state —
   * mirroring {@link HttpShardHandler#makeShardRequest}'s check-and-put pattern on {@code
   * responseFutureMap}.
   *
   * <p>Without this invariant, a runnable could observe {@code canceled=true} (and early-return in
   * super) while {@code submitFutures} still tracks its outer future, leaving the outer
   * whenComplete's bookkeeping racing against {@code cancelAll}'s own submitFutures sweep.
   */
  @Test
  public void testCanceledMakeShardRequestDoesNotTrackSubmitFutures() throws Exception {
    ExecutorService commExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            0,
            Integer.MAX_VALUE,
            5L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new SolrNamedThreadFactory("invariantTestComm"));

    try {
      ParallelHttpShardHandlerFactory factory = new ParallelHttpShardHandlerFactory();
      factory.commExecutor = commExecutor;
      ParallelHttpShardHandler handler = new ParallelHttpShardHandler(factory);

      LBHttp2SolrClient mockLb = Mockito.mock(LBHttp2SolrClient.class);
      Mockito.when(mockLb.requestAsync(Mockito.any(LBSolrClient.Req.class)))
          .thenAnswer(inv -> new CompletableFuture<LBSolrClient.Rsp>());
      handler.lbClient = mockLb;

      // Force canceled=true and drain the CANCELLATION_NOTIFICATION so we can observe the
      // post-cancel state cleanly.
      handler.cancelAll();
      assertNotNull(
          "CANCELLATION_NOTIFICATION should be queued by cancelAll",
          handler.responses.poll(2, TimeUnit.SECONDS));

      ShardRequest sreq = buildShardRequest("shardA");
      ShardResponse srsp = new ShardResponse();
      srsp.setShardRequest(sreq);
      srsp.setShard("shardA");
      HttpShardHandler.SimpleSolrResponse ssr = new HttpShardHandler.SimpleSolrResponse();
      srsp.setSolrResponse(ssr);

      ModifiableSolrParams params = new ModifiableSolrParams();
      QueryRequest queryRequest = new QueryRequest(params);
      LBSolrClient.Endpoint endpoint = new LBSolrClient.Endpoint("http://ignored:8983/solr");
      LBSolrClient.Req lbReq = new LBSolrClient.Req(queryRequest, List.of(endpoint));

      // Invoke makeShardRequest while canceled=true. Expected: outer is cancelled, nothing is
      // tracked in submitFutures, responsesPending() stays false.
      handler.makeShardRequest(sreq, "shardA", params, lbReq, ssr, srsp, System.nanoTime());

      assertFalse(
          "submitFutures must not track requests submitted while canceled=true",
          handler.responsesPending());
    } finally {
      commExecutor.shutdownNow();
      commExecutor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
