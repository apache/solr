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

import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

@Fork(value = 1)
@Warmup(time = 5, iterations = 9)
@Measurement(time = 5, iterations = 9)
@Threads(value = 16)
public class MultiFqSimpleSearch {

  static final String COLLECTION = "benchMultiFqSearch";

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"false", "true"})
    boolean useHttp1;

    @Param({"false"})
    boolean strict;

    private int docs = 10000;

    AtomicLong total = new AtomicLong();
    AtomicLong err = new AtomicLong();

    QueryRequest q = new QueryRequest(new SolrQuery(
            "q", "*:*",
            "fq", "*:* AND NOT int1_i_dv:1",
            "fq", "*:* AND NOT int1_i_dv:2",
            "fq", "*:* AND NOT int1_i_dv:3",
            "fq", "*:* AND NOT int1_i_dv:4",
            "fq", "*:* AND NOT int1_i_dv:5",
            "fq", "*:* AND NOT int1_i_dv:6",
            "fq", "*:* AND NOT int1_i_dv:7",
            "fq", "*:* AND NOT int1_i_dv:8",
            "fq", "*:* AND NOT int1_i_dv:9",
            "fq", "*:* AND NOT int1_i_dv:10",
            "fq", "*:* AND NOT int1_i_dv:11",
            "fq", "*:* AND NOT int1_i_dv:12",
            "fq", "*:* AND NOT int1_i_dv:13",
            "fq", "*:* AND NOT int1_i_dv:14",
            "fq", "*:* AND NOT int1_i_dv:15",
            "fq", "*:* AND NOT int1_i_dv:16",
            "fq", "*:* AND NOT int1_i_dv:17",
            "fq", "*:* AND NOT int1_i_dv:18",
            "fq", "*:* AND NOT int1_i_dv:19",
            "fq", "*:* AND NOT int1_i_dv:20"
    )); // no match is OK

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(useHttp1);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);
      Docs docGen =
              docs()
                      .field("id", integers().incrementing())
                      .field("int1_i_dv", integers().between(0, docs));
      miniClusterState.index(COLLECTION, docGen, docs);
      miniClusterState.waitForMerges(COLLECTION);
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
