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

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.MiniClusterState.MiniClusterBenchState;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 5, iterations = 1)
@Measurement(time = 30, iterations = 4)
@Threads(value = 1)
public class StreamingSearch {

  private static final String collection = "benchStreamingSearch";

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"false", "true"})
    boolean useHttp1;

    private int docs = 1000;
    private String zkHost;
    private ModifiableSolrParams params;
    private StreamContext streamContext;
    private Http2SolrClient http2SolrClient;

    @Setup(Level.Trial)
    public void setup(MiniClusterBenchState miniClusterState) throws Exception {

      miniClusterState.startMiniCluster(3);
      miniClusterState.createCollection(collection, 3, 1);
      Docs docGen =
          docs()
              .field("id", integers().incrementing())
              .field("text2_ts", strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
              .field("text3_ts", strings().basicLatinAlphabet().multi(312).ofLengthBetween(30, 64))
              .field("int1_i_dv", integers().all());
      miniClusterState.index(collection, docGen, docs);
      miniClusterState.waitForMerges(collection);

      zkHost = miniClusterState.zkHost;

      params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.set(CommonParams.FL, "id,text2_ts,text3_ts,int1_i_dv");
      params.set(CommonParams.SORT, "id asc,int1_i_dv asc");
      params.set(CommonParams.ROWS, docs);
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      SolrClientCache solrClientCache;
      if (useHttp1) {
        var httpClient = HttpClientUtil.createClient(null); // TODO tune params?
        solrClientCache = new SolrClientCache(httpClient);
      } else {
        http2SolrClient = newHttp2SolrClient();
        solrClientCache = new SolrClientCache(http2SolrClient);
      }

      streamContext = new StreamContext();
      streamContext.setSolrClientCache(solrClientCache);
    }

    @TearDown(Level.Iteration)
    public void teardownIt() {
      streamContext.getSolrClientCache().close();
      if (http2SolrClient != null) {
        http2SolrClient.close();
      }
    }
  }

  @Benchmark
  public Object stream(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    CloudSolrStream stream = new CloudSolrStream(benchState.zkHost, collection, benchState.params);
    stream.setStreamContext(benchState.streamContext);
    return getTuples(stream);
  }

  private static List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();
    try {
      tupleStream.open();
      while (true) {
        Tuple t = tupleStream.read();
        if (t.EOF) {
          break;
        } else {
          tuples.add(t);
        }
      }
      return tuples;
    } finally {
      tupleStream.close();
    }
  }

  public static Http2SolrClient newHttp2SolrClient() {
    // TODO tune params?
    var builder = new Http2SolrClient.Builder();
    return builder.build();
  }
}
