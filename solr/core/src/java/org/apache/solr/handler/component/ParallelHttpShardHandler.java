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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ExecutorService commExecutor;

  public ParallelHttpShardHandler(ParallelHttpShardHandlerFactory httpShardHandlerFactory) {
    super(httpShardHandlerFactory);
    this.commExecutor = httpShardHandlerFactory.commExecutor;
  }

  @Override
  public void submit(ShardRequest sreq, String shard, ModifiableSolrParams params) {
    // do this outside of the callable for thread safety reasons
    final List<String> urls = getURLs(shard);
    final var lbReq = prepareLBRequest(sreq, shard, params, urls);
    final var srsp = prepareShardResponse(sreq, shard);
    final var ssr = new SimpleSolrResponse();
    srsp.setSolrResponse(ssr);
    pending.incrementAndGet();

    if (urls.isEmpty()) {
      recordNoUrlShardResponse(srsp, shard);
      return;
    }

    long startTime = System.nanoTime();
    final Runnable executeRequestRunnable =
        () -> {
          CompletableFuture<LBSolrClient.Rsp> future = this.lbClient.requestAsync(lbReq);
          future.whenComplete(
              (rsp, throwable) -> {
                if (rsp != null) {
                  ssr.nl = rsp.getResponse();
                  srsp.setShardAddress(rsp.getServer());
                  ssr.elapsedTime =
                      TimeUnit.MILLISECONDS.convert(
                          System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                  responses.add(srsp);
                } else if (throwable != null) {
                  ssr.elapsedTime =
                      TimeUnit.MILLISECONDS.convert(
                          System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                  srsp.setException(throwable);
                  if (throwable instanceof SolrException) {
                    srsp.setResponseCode(((SolrException) throwable).code());
                  }
                  responses.add(srsp);
                }
              });
          responseFutureMap.put(srsp, future);
        };

    CompletableFuture.runAsync(executeRequestRunnable, commExecutor);
  }
}
