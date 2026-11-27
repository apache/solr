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

import static org.apache.solr.bench.BaseBenchState.log;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.SolrBenchState;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Measures query latency with different shared search-pool settings and client loads. */
@Fork(1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 20)
@Threads(1)
public class KnnConcurrentLoad {
  private static final String COLLECTION = "knn";
  private static final String VECTOR_FIELD = "vector";
  private static final String CONFIG_DIR = "src/resources/configs/knn-cloud-minimal/conf";
  private static final int BATCH_SIZE = 2_500;
  private static final int QUERY_COUNT = 32;
  private static final int TOP_K = 10;
  private static final long CORPUS_SEED = 1_234_567_890L;

  @State(Scope.Benchmark)
  public static class SearchState {
    @Param({"500000"})
    int numDocs;

    @Param({"0", "4"})
    int searchThreads;

    @Param({"false", "true"})
    boolean multiThreaded;

    @Param({"knn", "term", "range", "matchAll"})
    String queryShape;

    private List<String> queries;
    private String resultSha256;
    private int segmentCount;
    private long tasksBeforeIteration;

    @Setup(Level.Trial)
    public void setupTrial(SolrBenchState solrBenchState) throws Exception {
      if (numDocs < TOP_K || numDocs % BATCH_SIZE != 0) {
        throw new IllegalArgumentException(
            "numDocs must be at least topK and divisible by " + BATCH_SIZE);
      }

      System.setProperty("mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      System.setProperty("maxBufferedDocs", Integer.toString(BATCH_SIZE));
      System.setProperty("segmentsPerTier", "200");
      System.setProperty("autoCommit.maxTime", "-1");
      System.setProperty("documentCache.enabled", "false");
      System.setProperty("queryResultCache.enabled", "false");
      System.setProperty("filterCache.enabled", "false");
      System.setProperty("myPerSegmentCache.enabled", "false");

      solrBenchState.setUseHttp1(true);
      solrBenchState.startSolr(1, searchThreads, CONFIG_DIR);
      solrBenchState.createCollection(COLLECTION, 1, 1);
      index(solrBenchState);
      queries = createQueries(numDocs, queryShape);
      validateQueries(solrBenchState);
      segmentCount = readSegmentCount(solrBenchState);
      int expectedSegments = numDocs / BATCH_SIZE;
      if (segmentCount != expectedSegments) {
        throw new IllegalStateException(
            "Expected " + expectedSegments + " segments, got " + segmentCount);
      }

      log(
          "Search benchmark setup: docs="
              + numDocs
              + ", batchSize="
              + BATCH_SIZE
              + ", queryShape="
              + queryShape
              + ", searchThreads="
              + searchThreads
              + ", multiThreaded="
              + multiThreaded
              + ", segmentCount="
              + segmentCount
              + ", resultSha256="
              + resultSha256);
      solrBenchState.dumpCoreInfo();
    }

    @Setup(Level.Iteration)
    public void setupIteration(SolrBenchState solrBenchState) {
      tasksBeforeIteration = solrBenchState.completedSearchTasks();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(SolrBenchState solrBenchState) {
      long completedTasks = solrBenchState.completedSearchTasks() - tasksBeforeIteration;
      log("Completed shared search-pool tasks in iteration: " + completedTasks);
    }

    private void index(SolrBenchState solrBenchState) throws Exception {
      List<SolrInputDocument> batch = new ArrayList<>(BATCH_SIZE);
      for (int docId = 0; docId < numDocs; docId++) {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", Integer.toString(docId));
        document.addField(VECTOR_FIELD, vectorFor(docId));
        document.addField("term_s", "bucket" + (docId % 32));
        document.addField("rank_i", docId % 1_000);
        batch.add(document);
        if (batch.size() == BATCH_SIZE) {
          addBatch(solrBenchState, batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        addBatch(solrBenchState, batch);
      }

      UpdateRequest commit = new UpdateRequest();
      commit.setAction(UpdateRequest.ACTION.COMMIT, false, true);
      solrBenchState.client.requestWithBaseUrl(solrBenchState.nodes.get(0), commit, COLLECTION);
    }

    private static void addBatch(SolrBenchState solrBenchState, List<SolrInputDocument> documents)
        throws Exception {
      UpdateRequest update = new UpdateRequest();
      update.add(documents);
      solrBenchState.client.requestWithBaseUrl(solrBenchState.nodes.get(0), update, COLLECTION);
    }

    private List<String> createQueries(int numDocs, String queryShape) {
      List<String> queries = new ArrayList<>(QUERY_COUNT);
      for (int queryId = 0; queryId < QUERY_COUNT; queryId++) {
        queries.add(queryFor(queryShape, numDocs + queryId, queryId));
      }
      return List.copyOf(queries);
    }

    private void validateQueries(SolrBenchState solrBenchState) throws Exception {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String query : queries) {
        digest.update(query.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
        QueryResponse response = runQuery(solrBenchState, query, multiThreaded);
        if (response.getResults().size() != TOP_K) {
          throw new IllegalStateException(
              "Expected " + TOP_K + " KNN results, got " + response.getResults().size());
        }
        for (SolrDocument document : response.getResults()) {
          digest.update(document.getFieldValue("id").toString().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) '|');
          digest.update(
              document.getFieldValue("score").toString().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) '\n');
        }
      }
      resultSha256 = HexFormat.of().formatHex(digest.digest());
    }

    private static int readSegmentCount(SolrBenchState solrBenchState) throws Exception {
      ModifiableSolrParams params =
          SolrBenchState.params("action", "STATUS", "indexInfo", "true", "wt", "json");
      GenericSolrRequest request =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/cores", params);
      NamedList<Object> response =
          solrBenchState.client.requestWithBaseUrl(solrBenchState.nodes.get(0), request, null);
      Object statusValue = response.get("status");
      if (!(statusValue instanceof Map<?, ?> status) || status.size() != 1) {
        throw new IllegalStateException("Expected one core in the benchmark cluster status");
      }
      Object coreStatusValue = status.values().iterator().next();
      if (!(coreStatusValue instanceof Map<?, ?> coreStatus)) {
        throw new IllegalStateException("Core status did not have the expected response shape");
      }
      Object indexValue = coreStatus.get("index");
      if (!(indexValue instanceof Map<?, ?> index)) {
        throw new IllegalStateException("Core status did not report index information");
      }
      if (index == null || !(index.get("segmentCount") instanceof Number count)) {
        throw new IllegalStateException("Core status did not report the segment count");
      }
      return count.intValue();
    }
  }

  @State(Scope.Thread)
  public static class ClientState {
    private int queryIndex;

    private String nextQuery(SearchState searchState) {
      String query = searchState.queries.get(queryIndex);
      queryIndex = (queryIndex + 1) % searchState.queries.size();
      return query;
    }
  }

  @Benchmark
  public void searchQuery(
      SolrBenchState solrBenchState,
      SearchState searchState,
      ClientState clientState,
      Blackhole blackhole)
      throws Exception {
    QueryResponse response =
        runQuery(solrBenchState, clientState.nextQuery(searchState), searchState.multiThreaded);
    blackhole.consume(response);
  }

  private static String queryFor(String queryShape, int vectorId, int queryId) {
    return switch (queryShape) {
      case "knn" -> knnQuery(vectorId);
      case "term" -> "{!term f=term_s}bucket" + (queryId % 32);
      case "range" -> {
        int lower = (queryId % 4) * 250;
        yield "rank_i:[" + lower + " TO " + (lower + 249) + "]";
      }
      case "matchAll" -> "*:*";
      default -> throw new IllegalArgumentException("Unknown query shape: " + queryShape);
    };
  }

  private static QueryResponse runQuery(
      SolrBenchState solrBenchState, String query, boolean multiThreaded) throws Exception {
    ModifiableSolrParams params =
        SolrBenchState.params(
            "q",
            query,
            "rows",
            Integer.toString(TOP_K),
            "fl",
            "id,score",
            "multiThreaded",
            Boolean.toString(multiThreaded));
    return new QueryRequest(params).process(solrBenchState.client, COLLECTION);
  }

  private static String knnQuery(int queryId) {
    StringBuilder query =
        new StringBuilder("{!knn f=")
            .append(VECTOR_FIELD)
            .append(" topK=")
            .append(TOP_K)
            .append("}[");
    for (int dimension = 0; dimension < 8; dimension++) {
      if (dimension > 0) {
        query.append(',');
      }
      query.append(Float.toString(vectorComponent(queryId, dimension)));
    }
    return query.append(']').toString();
  }

  private static List<Float> vectorFor(int docId) {
    List<Float> vector = new ArrayList<>(8);
    for (int dimension = 0; dimension < 8; dimension++) {
      vector.add(vectorComponent(docId, dimension));
    }
    return vector;
  }

  private static float vectorComponent(int id, int dimension) {
    return (float) Math.sin((id + 1.0 + CORPUS_SEED) * (dimension + 3.0) * 0.013);
  }
}
