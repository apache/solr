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

import static org.apache.solr.bench.generators.SourceDSL.integers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.SolrRandomnessSource;
import org.apache.solr.bench.generators.SolrGen;
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

@Fork(value = 1)
@Warmup(time = 1, iterations = 9)
@Measurement(time = 1, iterations = 9)
@Threads(value = 4)
public class FilterCache {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {
    /**
     * The document hit frequency for the benchmarked queries. Larger values will hit more
     * documents, and slow down query rates. Changing the frequency to a lower value like 50 (half
     * the document match) or 2 (very few documents match) will increase the time spent resolving
     * individual query misses and decrease the impact that caching will have on performance.
     */
    @Param({"2", "98"})
    String frequency;

    // We don't need to test the full cross-product of these paramaters, so pick the relevant ones
    @Param({"true:true:1", "true:false:1", "true:true:0", "true:false:0", "false:false:0"})
    String cacheEnabledAsyncSize;

    QueryRequest q1 = new QueryRequest(new SolrQuery("q", "*:*", "fq", "Ea_b:true"));
    QueryRequest q2 = new QueryRequest(new SolrQuery("q", "*:*", "fq", "FB_b:true"));

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      String cacheEnabled = cacheEnabledAsyncSize.split(":")[0];
      String asyncCache = cacheEnabledAsyncSize.split(":")[1];
      String cacheSize = cacheEnabledAsyncSize.split(":")[2];
      System.setProperty("filterCache.enabled", cacheEnabled);
      System.setProperty("filterCache.size", cacheSize);
      System.setProperty("filterCache.initialSize", cacheSize);
      System.setProperty("filterCache.async", asyncCache);

      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);

      Docs docs = Docs.docs().field("id", integers().incrementing());

      SolrGen<Boolean> booleans =
          new SolrGen<>() {
            int threshold = Integer.parseInt(frequency);

            @Override
            public Boolean generate(SolrRandomnessSource in) {
              return in.next(0, 100) < threshold;
            }
          };
      // Field names purposely chosen to create collision:
      // "Ea_b".hashCode() == "FB_b".hashCode() == 2151839
      docs.field("Ea_b", booleans);
      docs.field("FB_b", booleans);

      miniClusterState.index(COLLECTION, docs, 30 * 1000);
      String base = miniClusterState.nodes.get(0);
      q1.setBasePath(base);
      q2.setBasePath(base);
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      // Reload the collection/core to drop existing caches
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      reload.setBasePath(miniClusterState.nodes.get(0));
      miniClusterState.client.request(reload);
    }

    @TearDown(Level.Iteration)
    public void dumpMetrics(MiniClusterState.MiniClusterBenchState miniClusterState) {
      // TODO add a verbose flag

      String url =
          miniClusterState.nodes.get(0)
              + "/admin/metrics?prefix=CACHE.searcher.filterCache&omitHeader=true";
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
        conn.connect();
        BaseBenchState.log(
            new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
      } catch (IOException e) {
        // ignored
      } finally {
        if (conn != null) conn.disconnect();
      }
    }
  }

  @Benchmark
  public Object filterCacheMultipleQueries(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    return miniClusterState.client.request(
        miniClusterState.getRandom().nextBoolean() ? benchState.q1 : benchState.q2, COLLECTION);
  }

  @Benchmark
  public Object filterCacheSingleQuery(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    return miniClusterState.client.request(benchState.q1, COLLECTION);
  }
}
