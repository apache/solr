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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
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
public class SearchOptimizations {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {
    @Param({"false", "true"})
    boolean singleShard;

    @Param({"false", "true"})
    boolean matchAllDocs;

    @Param({"false", "true"})
    boolean noRowsQuery;

    @Param({"false", "true"})
    boolean flScore;

    @Param({"false", "true"})
    boolean sortScore;

    int numDocs = 10000;

    AtomicLong total = new AtomicLong();
    AtomicLong err = new AtomicLong();

    QueryRequest q;

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(true);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, singleShard ? 1 : 2, 1);
      Docs docGen =
          docs()
              .field("id", integers().incrementing())
              .field("text2_ts", strings().alpha().multi(10).ofLengthBetween(5, 10));
      miniClusterState.index(COLLECTION, docGen, numDocs);
      miniClusterState.forceMerge(COLLECTION, 1);
      String base = miniClusterState.nodes.get(0);

      q = new QueryRequest(getSolrQuery());
      q.setBasePath(base);
    }

    private SolrQuery getSolrQuery() {
      final String qstr;
      if (matchAllDocs) {
        qstr = "*:*";
      } else {
        qstr =
            "id:10 OR id:20 OR id:30 OR id:40 OR id:50 OR id:60 OR id:70 OR id:80 OR id:90 OR id:100";
      }
      SolrQuery solrQuery = new SolrQuery("q", qstr);
      if (noRowsQuery) {
        solrQuery.set(CommonParams.ROWS, 0);
      } else {
        solrQuery.set(CommonParams.ROWS, CommonParams.ROWS_DEFAULT);
      }
      if (flScore) {
        solrQuery.set(CommonParams.FL, "id,score");
      } else {
        solrQuery.set(CommonParams.FL, "id");
      }
      if (sortScore) {
        solrQuery.set(CommonParams.SORT, "score desc,id asc");
      } else {
        solrQuery.set(CommonParams.SORT, "id asc");
      }
      return solrQuery;
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

  @Benchmark
  public Object query(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState, Blackhole bh)
      throws IOException {
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
