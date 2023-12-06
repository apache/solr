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
import static org.apache.solr.bench.generators.SourceDSL.booleans;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.io.IOException;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.MiniClusterState.MiniClusterBenchState;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Warmup(time = 5, iterations = 2)
@Measurement(time = 30, iterations = 4)
@Threads(value = 1)
public class QueryResponseWriters {

  // See also TestWriterPerf

  private static final String collection = "benchQueryResponseWriters";

  @State(Scope.Benchmark)
  public static class BenchState {

    /** See {@link SolrCore#DEFAULT_RESPONSE_WRITERS} */
    @Param({
      CommonParams.JAVABIN,
      CommonParams.JSON,
      "cbor",
      "smile",
      "xml",
      "python",
      "phps",
      "ruby",
      "raw"
    })
    String wt;

    private int docs = 100;
    private QueryRequest q;

    @Setup(Level.Trial)
    public void setup(MiniClusterBenchState miniClusterState) throws Exception {

      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(collection, 1, 1);

      // only stored fields are needed to cover the response writers perf
      Docs docGen =
          docs()
              .field("id", integers().incrementing())
              .field("text2_ts", strings().basicLatinAlphabet().multi(25).ofLengthBetween(30, 64))
              .field("bools_b", booleans().all())
              .field("int1_is", integers().all());
      miniClusterState.index(collection, docGen, docs);
      miniClusterState.forceMerge(collection, 5);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, "*:*");
      params.set(CommonParams.WT, wt);
      params.set(CommonParams.ROWS, docs);
      q = new QueryRequest(params);
      q.setResponseParser(new NoOpResponseParser(wt));
      String base = miniClusterState.nodes.get(0);
      q.setBasePath(base);
    }
  }

  @Benchmark
  public Object query(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    return miniClusterState.client.request(benchState.q, collection);
  }
}
