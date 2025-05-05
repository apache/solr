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
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.NotThreadSafe;
import org.apache.solr.client.solrj.impl.LBSolrClient;
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

  /*
   * Unlike the basic HttpShardHandler, this class allows us to exit submit before
   * pending is incremented and the responseFutureMap is updated. If the runnables that
   * do that are slow to execute the calling code could attempt to takeCompleted(),
   * while pending is still zero. In this condition, the code would assume that all
   * requests are processed (despite the runnables created by this class still
   * waiting). Thus, we need to track that there are attempts still in flight.
   *
   * This tracking is complicated by the fact that there could be a failure in the
   * runnable that causes the request to never be created and pending to never be
   * incremented. Thus, we need to know that we have attempted something AND that that
   * attempt has also been processed by the executor.
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
            CompletableFuture<LBSolrClient.Rsp> future = this.lbClient.requestAsync(lbReq);
            future.whenComplete(
                new ShardRequestCallback(ssr, srsp, startTimeNS, sreq, shard, params));
            synchronized (FUTURE_MAP_LOCK) {
              // we want to ensure that there is a future in flight before incrementing
              // pending, because there is a risk that the  request will hang forever waiting
              // on a responses.take() in HttpShardHandler.take(boolean) if anything failed
              // during future creation. It is not a problem if the response shows up before
              // we increment pending. The attemptingSubmit flag guards us against inadvertently
              // skipping the while loop in HttpShardHandler.take(boolean) until at least
              // one runnable has been executed.
              pending.incrementAndGet();
              responseFutureMap.put(srsp, future);
            }
          } finally {
            // it must not be possible to exit the runnable in any way without calling this.
            attemptCount.incrementAndGet();
          }
        };

    // not clear how errors emanating from requestAsync or the whenComplete() callback
    // are to propagated out of the runnable?
    attemptStart.incrementAndGet();
    CompletableFuture.runAsync(executeRequestRunnable, commExecutor);
  }
}
