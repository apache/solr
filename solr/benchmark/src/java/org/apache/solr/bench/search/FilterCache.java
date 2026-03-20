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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Collectors;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.SolrBenchState;
import org.apache.solr.bench.SolrRandomnessSource;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
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
    String baseUrl;

    @Setup(Level.Trial)
    public void setupTrial(SolrBenchState solrBenchState) throws Exception {
      String cacheEnabled = cacheEnabledAsyncSize.split(":")[0];
      String asyncCache = cacheEnabledAsyncSize.split(":")[1];
      String cacheSize = cacheEnabledAsyncSize.split(":")[2];
      System.setProperty("filterCache.enabled", cacheEnabled);
      System.setProperty("filterCache.size", cacheSize);
      System.setProperty("filterCache.initialSize", cacheSize);
      System.setProperty("filterCache.async", asyncCache);

      solrBenchState.startSolr(1);
      solrBenchState.createCollection(COLLECTION, 1, 1);

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

      solrBenchState.index(COLLECTION, docs, 30 * 1000);
      baseUrl = solrBenchState.nodes.get(0);
    }

    @Setup(Level.Iteration)
    public void setupIteration(SolrBenchState solrBenchState)
        throws SolrServerException, IOException {
      // Reload the collection/core to drop existing caches
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      solrBenchState.client.requestWithBaseUrl(solrBenchState.nodes.get(0), reload, null);
    }

    @TearDown(Level.Iteration)
    public void dumpMetrics(SolrBenchState solrBenchState) {
      // TODO add a verbose flag

      String url = solrBenchState.nodes.getFirst() + "/admin/metrics?category=CACHE?wt=prometheus";
      try (HttpClient client = HttpClient.newHttpClient()) {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Filter to only lines containing filterCache metrics
        String filteredMetrics =
            response
                .body()
                .lines()
                .filter(line -> line.contains("filterCache"))
                .collect(Collectors.joining("\n"));
        BaseBenchState.log(filteredMetrics);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Benchmark
  public Object filterCacheMultipleQueries(BenchState benchState, SolrBenchState solrBenchState)
      throws SolrServerException, IOException {
    return solrBenchState.client.requestWithBaseUrl(
        benchState.baseUrl,
        solrBenchState.getRandom().nextBoolean() ? benchState.q1 : benchState.q2,
        COLLECTION);
  }

  @Benchmark
  public Object filterCacheSingleQuery(BenchState benchState, SolrBenchState solrBenchState)
      throws SolrServerException, IOException {
    return solrBenchState.client.requestWithBaseUrl(benchState.baseUrl, benchState.q1, COLLECTION);
  }
}
