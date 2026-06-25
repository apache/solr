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
 *
 * <h2>Concurrency model</h2>
 *
 * <p>Each shard submit runs as two chained async stages. This class uses the words <b>outer</b> and
 * <b>inner</b> throughout to keep them straight:
 *
 * <ul>
 *   <li><b>outer future</b> — the {@link CompletableFuture#runAsync} we schedule on {@link
 *       #commExecutor}. All it does is call {@code super.makeShardRequest}, which kicks off the
 *       actual HTTP request. Doing this on commExecutor is the whole point of the class: {@code
 *       submit} returns immediately instead of blocking the caller once per shard.
 *   <li><b>inner future</b> — the HTTP future that {@code super.makeShardRequest} obtains from
 *       {@code lbClient.requestAsync}. It completes later, on a Jetty IO thread, and its callback
 *       enqueues the finished {@link ShardResponse} onto {@link #responses}.
 * </ul>
 *
 * <p>Meanwhile the consumer thread sits in {@link #take(boolean)}, looping while {@link
 * #responsesPending()} is true and draining finished responses off {@link #responses}.
 *
 * <p><b>The core invariant:</b> {@code responsesPending()} must stay true as long as any response
 * might still arrive. If it ever reports false too early, the consumer stops waiting and a response
 * that lands a moment later is lost — and the consumer can park forever. To answer "could anything
 * still arrive?" it has to consult three pieces of state at once:
 *
 * <ul>
 *   <li>{@link #inFlightSubmits} — outer futures not yet finished (this class).
 *   <li>{@code responseFutureMap} — inner futures not yet finished (base class).
 *   <li>{@link #responses} — finished responses not yet consumed (base class).
 * </ul>
 *
 * <p>The risky instant is the hand-off between the two stages: an outer future finishes (so {@link
 * #inFlightSubmits} drops) a hair before its inner future's callback puts the response on the
 * queue. A consumer that samples the three trackers in that gap, unsynchronized, can see all three
 * empty even though a response is moments away. Every {@code synchronized} block in this class
 * exists to close that gap: reads and mutations of all three trackers share the one monitor
 * returned by {@link #cancellationLock()}, so the consumer can never observe a torn, in-between
 * view.
 */
@NotThreadSafe
public class ParallelHttpShardHandler extends HttpShardHandler {

  private final ExecutorService commExecutor;

  /*
   * Number of outer futures (see class javadoc) not yet finished. This is the authoritative
   * "is a submit still in flight?" signal that responsesPending() consults.
   *
   * Why a counter and not submitFutures.isEmpty(): ConcurrentHashMap.size()/isEmpty() are
   * documented as estimates. Under concurrent put/remove the internal counter cells can settle at
   * a non-zero sum while the table is physically empty, so isEmpty() can return false for a
   * logically empty map — which would leave responsesPending() stuck at true and park the consumer
   * forever. AtomicInteger.get() is exact under any concurrency.
   */
  private final AtomicInteger inFlightSubmits = new AtomicInteger();

  /* The outer futures themselves, kept only so cancelAll() has something to iterate and cancel. */
  private final ConcurrentMap<ShardResponse, CompletableFuture<Void>> submitFutures;

  public ParallelHttpShardHandler(ParallelHttpShardHandlerFactory httpShardHandlerFactory) {
    super(httpShardHandlerFactory);
    this.commExecutor = httpShardHandlerFactory.commExecutor;
    this.submitFutures = new ConcurrentHashMap<>();
  }

  @Override
  protected boolean responsesPending() {
    // Read all three trackers under the shared monitor so the consumer sees a consistent view (see
    // "core invariant" in the class javadoc). Without the lock it could catch the brief gap where
    // an outer future has finished (inFlightSubmits == 0) but its inner future's callback has not
    // yet added the response to the queue, conclude nothing is pending, and lose that response.
    synchronized (cancellationLock()) {
      return super.responsesPending() || inFlightSubmits.get() > 0;
    }
  }

  /**
   * Wait for the next response by polling instead of blocking.
   *
   * <p>The lock fixes torn reads, but it cannot wake a thread blocked in {@code responses.take()}:
   * when the last outer future finishes without the inner callback enqueuing anything (e.g. the
   * trackers simply drain to empty), nothing is ever added to the queue to signal its internal
   * condition, so {@code take()} would block forever. Polling instead lets {@link #take(boolean)}
   * periodically re-check {@link #responsesPending()} and exit cleanly — or pick up the inner
   * callback's response once it lands.
   */
  @Override
  protected ShardResponse awaitNextResponse() throws InterruptedException {
    return responses.poll(50, TimeUnit.MILLISECONDS);
  }

  /**
   * Schedules {@code super.makeShardRequest} onto {@link #commExecutor} as an outer future (see
   * class javadoc) instead of running it on the caller's thread.
   *
   * <p>The {@link #inFlightSubmits} counter is the thread-safety backbone here. It is bumped up
   * before the outer future is created and back down exactly once when that future settles, so it
   * always reflects the true number of submits still in flight — every exit path below maintains
   * that pairing.
   */
  @Override
  protected void makeShardRequest(
      ShardRequest sreq,
      String shard,
      ModifiableSolrParams params,
      LBSolrClient.Req lbReq,
      SimpleSolrResponse ssr,
      ShardResponse srsp,
      long startTimeNS) {
    // The runnable below needs a reference to its own outer future to check isCancelled(), but that
    // future doesn't exist until runAsync returns. We can't close over a not-yet-assigned local, so
    // we hand the runnable an AtomicReference and fill it in once runAsync returns the future. That
    // AtomicReference also gives the runnable's thread a safe, visible read of the late assignment.
    AtomicReference<CompletableFuture<Void>> selfRef = new AtomicReference<>();
    CompletableFuture<Void> completableFuture;
    // Count this submit as in flight before it can possibly start, so responsesPending() never sees
    // a zero count while a submit exists. Each branch below pairs this with exactly one decrement.
    inFlightSubmits.incrementAndGet();
    try {
      completableFuture =
          CompletableFuture.runAsync(
              () -> {
                // If cancelAll already cancelled this specific outer future before it got to run,
                // skip the HTTP call that super would otherwise start and immediately cancel.
                // selfRef can still be null if this runnable beats the selfRef.set() below; that's
                // harmless, since super.makeShardRequest re-checks the canceled flag itself.
                CompletableFuture<Void> self = selfRef.get();
                if (self != null && self.isCancelled()) {
                  return;
                }
                super.makeShardRequest(sreq, shard, params, lbReq, ssr, srsp, startTimeNS);
              },
              commExecutor);
    } catch (RejectedExecutionException ree) {
      // commExecutor is saturated or shutting down. If we let this propagate it would blow up
      // SearchHandler's distributed loop before cancelAll() runs, stranding shard requests already
      // submitted and turning a transient overload into an HTTP 500 even under shards.tolerant.
      // Instead record it as an ordinary shard failure (503, i.e. transient) so the responses queue
      // stays consistent. No outer future was created, so there is no whenComplete to balance the
      // increment — undo it here. (If the rejection happens because we are already canceling,
      // recordShardSubmitError intentionally drops the response: the consumer is tearing down and
      // is woken by cancelAll's CANCELLATION_NOTIFICATION, not by this shard.)
      inFlightSubmits.decrementAndGet();
      recordShardSubmitError(
          srsp,
          new SolrException(
              SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Comm executor thread pool is full, unable to send request to shard: " + shard,
              ree));
      return;
    }
    // Publish the future before the cancellation check below: if that block cancels it, the
    // runnable is then guaranteed to observe the cancellation through selfRef.get().isCancelled().
    selfRef.set(completableFuture);

    // Register the outer future for cancelAll to find — but only if we haven't already been
    // canceled. This mirrors super.makeShardRequest's check-and-put on responseFutureMap and runs
    // under the same monitor, so registration and cancellation can't interleave: if cancelAll has
    // already run, we cancel this future and skip tracking it (the runnable will short-circuit).
    synchronized (cancellationLock()) {
      if (isCanceled()) {
        completableFuture.cancel(true);
        // Early return: no whenComplete will be attached, so balance the increment here.
        inFlightSubmits.decrementAndGet();
        return;
      }
      submitFutures.put(srsp, completableFuture);
    }
    completableFuture.whenComplete(
        (r, t) -> {
          try {
            if (t != null) {
              Throwable failure = t;
              if (failure instanceof CompletionException completionException
                  && completionException.getCause() != null) {
                failure = completionException.getCause();
              }
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
            // Remove from submitFutures first (under the shared monitor, so cancelAll always sees a
            // consistent set), then drop the count. Decrementing last guarantees responsesPending()
            // — which reads both under that monitor — never sees inFlightSubmits == 0 while this
            // entry is still in submitFutures.
            synchronized (cancellationLock()) {
              submitFutures.remove(srsp);
            }
            inFlightSubmits.decrementAndGet();
          }
        });
  }

  @Override
  public void cancelAll() {
    // Cancel base-class state and our outer futures together under the one monitor, so a concurrent
    // makeShardRequest can't slip a new outer future into submitFutures while we are sweeping it.
    // Same monitor as super.cancelAll(), so the canceled flag, responseFutureMap, and submitFutures
    // all flip to the canceled state atomically.
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
