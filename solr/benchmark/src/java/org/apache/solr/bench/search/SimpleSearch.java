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
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1)
@Warmup(time = 5, iterations = 9)
@Measurement(time = 5, iterations = 9)
@Threads(value = 16)
public class SimpleSearch {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {

    QueryRequest q = new QueryRequest(new SolrQuery("q", "id:0")); // no match is OK

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(true);
      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);

      //      Docs docs = Docs.docs().field("id", integers().incrementing());

      //      miniClusterState.index(COLLECTION, docs, 30 * 1000);
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
    }
  }

  @Benchmark
  public Object query(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    return miniClusterState.client.request(benchState.q, COLLECTION);
  }
}
