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
package org.apache.solr.bench.search;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(value = 1)
@Warmup(time = 5, iterations = 9)
@Measurement(time = 5, iterations = 9)
@Threads(value = 16)
public class SimpleSearch {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"false", "true"})
    boolean useHttp1;

    @Param({"false"})
    boolean strict;

    AtomicLong total = new AtomicLong();
    AtomicLong err = new AtomicLong();

    QueryRequest q = new QueryRequest(new SolrQuery("q", "id:0")); // no match is OK

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(useHttp1);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);
      String base = miniClusterState.nodes.get(0);
      q.setBasePath(base);
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      // Reload the collection/core to drop existing caches
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      reload.setBasePath(miniClusterState.nodes.get(0));
      miniClusterState.client.request(reload);

      total = new AtomicLong();
      err = new AtomicLong();
    }

    @TearDown(Level.Iteration)
    public void teardownIt() {
      if (err.get() > 0) {
        BaseBenchState.log(
            "Completed Iteration with " + total.get() + " queries and " + err.get() + " errors");
      }
    }
  }

  /**
   * Under high load http2 client could throw the below exception
   *
   * <pre>{@code
   * org.apache.solr.client.solrj.SolrServerException: IOException occurred when talking to server at: http://127.0.0.1:49436/solr/c1/select?q=id%3A0&wt=javabin&version=2
   * at org.apache.solr.client.solrj.impl.Http2SolrClient.request(Http2SolrClient.java:533)
   * at org.apache.solr.bench.search.SimpleSearch.query(SimpleSearch.java:89)
   * at org.apache.solr.bench.search.jmh_generated.SimpleSearch_query_jmhTest.query_thrpt_jmhStub(SimpleSearch_query_jmhTest.java:275)
   * at org.apache.solr.bench.search.jmh_generated.SimpleSearch_query_jmhTest.query_Throughput(SimpleSearch_query_jmhTest.java:127)
   * at jdk.internal.reflect.GeneratedMethodAccessor8.invoke(Unknown Source)
   * at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   * at java.base/java.lang.reflect.Method.invoke(Method.java:566)
   * at org.openjdk.jmh.runner.BenchmarkHandler$BenchmarkTask.call(BenchmarkHandler.java:475)
   * at org.openjdk.jmh.runner.BenchmarkHandler$BenchmarkTask.call(BenchmarkHandler.java:458)
   * at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
   * at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
   * at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
   * at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
   * at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
   * at java.base/java.lang.Thread.run(Thread.java:829)
   * Caused by: java.io.IOException: cancel_stream_error
   * at org.eclipse.jetty.http2.client.http.HttpReceiverOverHTTP2.onReset(HttpReceiverOverHTTP2.java:202)
   * at org.eclipse.jetty.http2.client.http.HttpChannelOverHTTP2$Listener.onReset(HttpChannelOverHTTP2.java:206)
   * at org.eclipse.jetty.http2.api.Stream$Listener.onReset(Stream.java:271)
   * at org.eclipse.jetty.http2.HTTP2Stream.notifyReset(HTTP2Stream.java:853)
   * at org.eclipse.jetty.http2.HTTP2Stream.onReset(HTTP2Stream.java:563)
   * at org.eclipse.jetty.http2.HTTP2Stream.process(HTTP2Stream.java:392)
   * at org.eclipse.jetty.http2.HTTP2Session.onReset(HTTP2Session.java:320)
   * at org.eclipse.jetty.http2.parser.Parser$Listener$Wrapper.onReset(Parser.java:367)
   * at org.eclipse.jetty.http2.parser.BodyParser.notifyReset(BodyParser.java:139)
   * at org.eclipse.jetty.http2.parser.ResetBodyParser.onReset(ResetBodyParser.java:92)
   * at org.eclipse.jetty.http2.parser.ResetBodyParser.parse(ResetBodyParser.java:61)
   * at org.eclipse.jetty.http2.parser.Parser.parseBody(Parser.java:193)
   * at org.eclipse.jetty.http2.parser.Parser.parse(Parser.java:122)
   * at org.eclipse.jetty.http2.HTTP2Connection$HTTP2Producer.produce(HTTP2Connection.java:278)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.produceTask(AdaptiveExecutionStrategy.java:450)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.tryProduce(AdaptiveExecutionStrategy.java:243)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.produce(AdaptiveExecutionStrategy.java:194)
   * at org.eclipse.jetty.http2.HTTP2Connection.produce(HTTP2Connection.java:208)
   * at org.eclipse.jetty.http2.HTTP2Connection.onFillable(HTTP2Connection.java:155)
   * at org.eclipse.jetty.http2.HTTP2Connection$FillableCallback.succeeded(HTTP2Connection.java:378)
   * at org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:100)
   * at org.eclipse.jetty.io.SelectableChannelEndPoint$1.run(SelectableChannelEndPoint.java:53)
   * at org.eclipse.jetty.util.thread.Invocable.invokeNonBlocking(Invocable.java:151)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.invokeAsNonBlocking(AdaptiveExecutionStrategy.java:433)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.consumeTask(AdaptiveExecutionStrategy.java:375)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.tryProduce(AdaptiveExecutionStrategy.java:272)
   * at org.eclipse.jetty.util.thread.strategy.AdaptiveExecutionStrategy.produce(AdaptiveExecutionStrategy.java:194)
   * at org.apache.solr.common.util.ExecutorUtil$MDCAwareThreadPoolExecutor.lambda$execute$0(ExecutorUtil.java:289)
   * ... 3 more
   * }</pre>
   */
  @Benchmark
  public Object query(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState, Blackhole bh)
      throws SolrServerException, IOException {
    if (benchState.strict) {
      return miniClusterState.client.request(benchState.q, COLLECTION);
    }

    // non strict run ignores exceptions
    try {
      return miniClusterState.client.request(benchState.q, COLLECTION);
    } catch (SolrServerException e) {
      bh.consume(e);
      benchState.err.getAndIncrement();
      return null;
    } finally {
      benchState.total.getAndIncrement();
    }
  }
}
