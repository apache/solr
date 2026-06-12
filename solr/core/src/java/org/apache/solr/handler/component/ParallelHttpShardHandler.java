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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.NotThreadSafe;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * A version of {@link HttpShardHandler} optimized for massively-sharded collections.
 *
 * <p>Uses a {@link HttpShardHandlerFactory#commExecutor} thread for all work related to outgoing
 * requests, allowing {@link #submit(ShardRequest, String, ModifiableSolrParams)} to return more
 * quickly. (See {@link HttpShardHandler} for comparison.)
 *
 * <p>The additional focus on parallelization makes this an ideal implementation for collections
 * with many shards.
 */
@NotThreadSafe
public class ParallelHttpShardHandler extends HttpShardHandler {

  private final ExecutorService commExecutor;

  /*
   * Track in-flight async submits so responsesPending() doesn't return false
   * while a runnable is still queued or executing.
   *
   * inFlightSubmits is the authoritative loop-guard counter. AtomicInteger.get()
   * is exact; ConcurrentHashMap.size() is documented as an estimate — its
   * sumCount() can settle at a non-zero value while the table is empty under
   * concurrent put/remove — so we don't use submitFutures.isEmpty() as the guard.
   *
   * submitFutures is retained only as the iteration target for cancelAll.
   */
  private final AtomicInteger inFlightSubmits = new AtomicInteger();
  private final ConcurrentMap<ShardResponse, CompletableFuture<Void>> submitFutures;

  public ParallelHttpShardHandler(ParallelHttpShardHandlerFactory httpShardHandlerFactory) {
    super(httpShardHandlerFactory);
    this.commExecutor = httpShardHandlerFactory.commExecutor;
    this.submitFutures = new ConcurrentHashMap<>();
  }

  @Override
  protected boolean responsesPending() {
    // Take the lock so the read of inFlightSubmits is serialized with super.responsesPending()'s
    // reads of responseFutureMap and responses. Without it, take()'s loop can transiently observe
    // both responseFutureMap empty (super.makeShardRequest hasn't put yet) and inFlightSubmits == 0
    // (outer.whenComplete already decremented for a different submit) even though those events are
    // causally ordered, and the in-flight inner whenComplete's responses.add would be silently
    // lost.
    synchronized (cancellationLock()) {
      return super.responsesPending() || inFlightSubmits.get() > 0;
    }
  }

  /**
   * Override the base class's blocking wait with a timed poll. inFlightSubmits is an async tracker
   * outside the {@link #responses} queue's lifecycle: when an outer {@code whenComplete} decrements
   * the last in-flight submit but the inner {@code whenComplete} hasn't fired yet to enqueue the
   * response, a thread parked in {@code responses.take()} would never be woken — the cancellation
   * lock cannot signal the queue's internal {@code Condition}. Polling lets the outer {@link
   * #take(boolean)} loop re-evaluate {@link #responsesPending()} until the inner callback enqueues
   * the real response (or the trackers drain to empty).
   */
  @Override
  protected ShardResponse awaitNextResponse() throws InterruptedException {
    return responses.poll(50, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void makeShardRequest(
      ShardRequest sreq,
      String shard,
      ModifiableSolrParams params,
      LBSolrClient.Req lbReq,
      SimpleSolrResponse ssr,
      ShardResponse srsp,
      long startTimeNS) {
    // Holder so the lambda can read its own outer future. We can't capture the variable directly
    // (it would have to be effectively final, but we assign it from runAsync). AtomicReference
    // gives the lambda volatile-style visibility on the assignment that happens after runAsync
    // returns.
    AtomicReference<CompletableFuture<Void>> selfRef = new AtomicReference<>();
    CompletableFuture<Void> completableFuture;
    // Increment BEFORE runAsync so responsesPending() never observes inFlightSubmits == 0 while
    // there is a submit in flight. The matching decrement is in the unconditional finally of
    // whenComplete below, or in the catch block if runAsync itself rejects.
    inFlightSubmits.incrementAndGet();
    try {
      completableFuture =
          CompletableFuture.runAsync(
              () -> {
                // Skip the work if THIS specific outer future was cancelled (e.g. cancelAll
                // cancelled
                // it before this runnable got CPU time). Avoids a wasted lbClient.requestAsync that
                // super.makeShardRequest would just immediately cancel anyway. selfRef may briefly
                // be null if the runnable runs before the assignment below — in that case we fall
                // through to super, which has its own canceled-check guard.
                CompletableFuture<Void> self = selfRef.get();
                if (self != null && self.isCancelled()) {
                  return;
                }
                super.makeShardRequest(sreq, shard, params, lbReq, ssr, srsp, startTimeNS);
              },
              commExecutor);
    } catch (RejectedExecutionException ree) {
      // Saturation or shutdown of commExecutor would otherwise propagate synchronously,
      // crash SearchHandler's distributed loop before cancelAll() runs, abandon any
      // already-submitted shard requests, and return HTTP 500 even when shards.tolerant=true.
      // Treat it as a shard failure so the responses queue stays consistent and shards.tolerant
      // semantics are honored. SERVICE_UNAVAILABLE (503) marks it as transient.
      // No future was produced, so whenComplete will never fire — undo the increment here.
      inFlightSubmits.decrementAndGet();
      recordShardSubmitError(
          srsp,
          new SolrException(
              SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Comm executor thread pool is full, unable to send request to shard: " + shard,
              ree));
      return;
    }
    // Publish the self-reference BEFORE the cancellation check so that if the cancellation block
    // below cancels this future, the runnable (whenever it runs) will see the cancellation via
    // selfRef.get().isCancelled(). AtomicReference provides happens-before across threads.
    selfRef.set(completableFuture);

    // Synchronize registering submitFutures with the same monitor super uses for responseFutureMap.
    // If cancelAll has already set canceled=true, don't track this request — cancel the outer
    // future and return. The runnable, when it runs, will see self.isCancelled() and short-circuit.
    // Mirrors super.makeShardRequest's check-and-put-or-early-return pattern on responseFutureMap.
    synchronized (cancellationLock()) {
      if (isCanceled()) {
        completableFuture.cancel(true);
        // whenComplete is never registered on this early-return path, so undo the increment.
        inFlightSubmits.decrementAndGet();
        return;
      }
      submitFutures.put(srsp, completableFuture);
    }
    completableFuture.whenComplete(
        (r, t) -> {
          try {
            if (t != null) {
              Throwable failure =
                  (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
              if (!(failure instanceof CancellationException)) {
                recordShardSubmitError(
                    srsp,
                    new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "Exception occurred while trying to send a request to shard: " + shard,
                        failure));
              }
            }
          } finally {
            // Order matters: remove from submitFutures (under the cross-tracker lock so cancelAll
            // sees a consistent set), then decrement the loop-guard counter. Decrementing last
            // means responsesPending() — which reads under the same lock — never observes
            // inFlightSubmits == 0 while submitFutures still contains this entry.
            synchronized (cancellationLock()) {
              submitFutures.remove(srsp);
            }
            inFlightSubmits.decrementAndGet();
          }
        });
  }

  @Override
  public void cancelAll() {
    // Synchronize the whole cancellation — super.cancelAll plus our submitFutures ops — on the
    // same monitor so the invariant matches HttpShardHandler.cancelAll. Without this, a runnable
    // entering super.makeShardRequest's synchronized block could observe canceled=true while
    // submitFutures is still being walked, leaving the maps mutually inconsistent.
    synchronized (cancellationLock()) {
      super.cancelAll();
      submitFutures
          .values()
          .forEach(
              future -> {
                if (!future.isDone()) {
                  future.cancel(true);
                }
              });
      submitFutures.clear();
    }
  }
}
