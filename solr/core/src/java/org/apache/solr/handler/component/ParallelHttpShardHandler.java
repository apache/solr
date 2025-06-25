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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.NotThreadSafe;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ExecutorService commExecutor;
  private final AtomicBoolean canceled = new AtomicBoolean(false);

  /*
   * Unlike the basic HttpShardHandler, this class allows us to exit submit before
   * the responseFutureMap is updated. If the runnables that
   * do that are slow to execute the calling code could attempt to takeCompleted(),
   * while pending is still zero. In this condition, the code would assume that all
   * requests are processed (despite the runnables created by this class still
   * waiting). Thus, we need to track that there are attempts still in flight.
   *
   * This condition is added to the check that controls the loop in take via the
   * override for #responsesPending(). We rely on calling code call submit for all
   * requests desired before the call to takeCompleted()
   */
  AtomicInteger attemptStart = new AtomicInteger(0);
  AtomicInteger attemptCount = new AtomicInteger(0);

  public ParallelHttpShardHandler(ParallelHttpShardHandlerFactory httpShardHandlerFactory) {
    super(httpShardHandlerFactory);
    this.commExecutor = httpShardHandlerFactory.commExecutor;
  }

  @Override
  protected boolean responsesPending() {
    // ensure we can't exit while loop in HttpShardHandler.take(boolean) until we've completed
    // as many Runnable actions as we created.
    return super.responsesPending() || attemptStart.get() > attemptCount.get();
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
    final Runnable executeRequestRunnable =
        () -> {
          try {
            if (!canceled.get()) {
              log.info("Sending request for shard {}", shard);
              super.makeShardRequest(sreq, shard, params, lbReq, ssr, srsp, startTimeNS);
            }
          } catch (Throwable t) {
            SolrException exception =
                new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Exception occurred while trying to send a request to shard: " + shard,
                    t);
            srsp.setException(exception);
            srsp.setResponseCode(exception.code());

            // Add a response, so that take() will have something to listen for
            responses.add(srsp);
          } finally {
            // it must not be possible to exit the runnable in any way without calling this, even if
            // the request has been canceled
            attemptCount.incrementAndGet();
          }
        };

    // not clear how errors emanating from requestAsync or the whenComplete() callback
    // are to propagated out of the runnable?
    attemptStart.incrementAndGet();
    // Since we are submitting new shard requests, the request is not canceled
    canceled.set(false);
    try {
      CompletableFuture.runAsync(executeRequestRunnable, commExecutor);
    } catch (Throwable t) {
      // We incremented the attemptStart already, therefore we should increment attemptCount on a
      // failure to submit, since the async code to increment it will not be run.
      attemptCount.incrementAndGet();
      throw t;
    }
  }

  @Override
  public void cancelAll() {
    // Canceled must be set to true before calling the cancellation code, to ensure that new tasks
    // are not enqueued after the outstanding requests have been canceled.
    // This code isn't perfectly threadsafe, and there can be a race-condition, but for our purposes
    // it should be fine. Failing to cancel a request, a very small percentage of the time, will
    // have very little noticeable effect.
    canceled.set(true);
    super.cancelAll();
  }
}
