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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
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

  /*
   * Unlike the basic HttpShardHandler, this class allows us to exit submit before
   * the responseFutureMap is updated. If the runnables that
   * do that are slow to execute the calling code could attempt to takeCompleted(),
   * while pending is still zero. In this condition, the code would assume that all
   * requests are processed (despite the runnables created by this class still
   * waiting). Thus, we need to track that there are attempts still in flight.
   */
  private final ConcurrentMap<ShardResponse, FutureTask<Void>> submitFutures;

  public ParallelHttpShardHandler(ParallelHttpShardHandlerFactory httpShardHandlerFactory) {
    super(httpShardHandlerFactory);
    this.commExecutor = httpShardHandlerFactory.commExecutor;
    this.submitFutures = new ConcurrentHashMap<>();
  }

  @Override
  protected boolean responsesPending() {
    // ensure we can't exit while loop in HttpShardHandler.take(boolean) until we've completed
    // submitting all of the shard requests
    return super.responsesPending() || !submitFutures.isEmpty();
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
    FutureTask<Void> futureTask =
        new FutureTask<>(
            () -> super.makeShardRequest(sreq, shard, params, lbReq, ssr, srsp, startTimeNS), null);
    CompletableFuture<Void> completableFuture =
        CompletableFuture.runAsync(futureTask, commExecutor);
    submitFutures.put(srsp, futureTask);
    completableFuture.whenComplete(
        (r, t) -> {
          try {
            if (t != null) {
              recordShardSubmitError(
                  srsp,
                  new SolrException(
                      SolrException.ErrorCode.SERVER_ERROR,
                      "Exception occurred while trying to send a request to shard: " + shard,
                      t));
            }
          } finally {
            // Remove so that we keep track of in-flight submits only
            submitFutures.remove(srsp);
          }
        });
  }

  @Override
  public void cancelAll() {
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
