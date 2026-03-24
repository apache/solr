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
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

public class ParallelHttpShardHandlerTest extends SolrTestCaseJ4 {

  private static class DirectExecutorService extends AbstractExecutorService {
    private volatile boolean shutdown;

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return List.of();
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

    ShardRequest shardRequest = new ShardRequest();
    shardRequest.params = new ModifiableSolrParams();
    shardRequest.actualShards = new String[] {"shardA"};

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
}
