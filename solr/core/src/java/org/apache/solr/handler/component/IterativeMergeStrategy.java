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

import static org.apache.solr.common.params.CommonParams.DISTRIB;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.search.SolrIndexSearcher;

public abstract class IterativeMergeStrategy implements MergeStrategy {

  protected volatile ExecutorService executorService;

  protected volatile Http2SolrClient httpSolrClient;

  @Override
  public void merge(ResponseBuilder rb, ShardRequest sreq) {
    rb._responseDocs = new SolrDocumentList(); // Null pointers will occur otherwise.
    rb.onePassDistributedQuery = true; // Turn off the second pass distributed.
    httpSolrClient = rb.req.getCoreContainer().getDefaultHttpSolrClient();
    // TODO use httpSolrClient.requestAsync instead; it has an executor
    executorService =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            new SolrNamedThreadFactory("IterativeMergeStrategy"));
    try {
      process(rb, sreq);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      executorService.shutdownNow();
    }
  }

  @Override
  public boolean mergesIds() {
    return true;
  }

  @Override
  public int getCost() {
    return 0;
  }

  @Override
  public boolean handlesMergeFields() {
    return false;
  }

  @Override
  public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {}

  public class CallBack implements Callable<CallBack> {
    private final String shardBaseUrl;
    private final String shardCoreName;

    private QueryRequest req;
    private QueryResponse response;
    private ShardResponse originalShardResponse;

    public CallBack(ShardResponse originalShardResponse, QueryRequest req) {
      this.shardBaseUrl = URLUtil.extractBaseUrl(originalShardResponse.getShardAddress());
      this.shardCoreName = URLUtil.extractCoreFromCoreUrl(originalShardResponse.getShardAddress());
      this.req = req;
      this.originalShardResponse = originalShardResponse;
      req.setMethod(SolrRequest.METHOD.POST);
      ModifiableSolrParams params = (ModifiableSolrParams) req.getParams();
      params.add(DISTRIB, "false");
    }

    public QueryResponse getResponse() {
      return this.response;
    }

    public ShardResponse getOriginalShardResponse() {
      return this.originalShardResponse;
    }

    @Override
    public CallBack call() throws Exception {
      response = httpSolrClient.requestWithBaseUrl(shardBaseUrl, shardCoreName, req);
      return this;
    }
  }

  public List<Future<CallBack>> callBack(List<ShardResponse> responses, QueryRequest req) {
    List<Future<CallBack>> futures = new ArrayList<>();
    for (ShardResponse response : responses) {
      futures.add(this.executorService.submit(new CallBack(response, req)));
    }
    return futures;
  }

  public Future<CallBack> callBack(ShardResponse response, QueryRequest req) {
    return this.executorService.submit(new CallBack(response, req));
  }

  protected abstract void process(ResponseBuilder rb, ShardRequest sreq) throws Exception;
}
