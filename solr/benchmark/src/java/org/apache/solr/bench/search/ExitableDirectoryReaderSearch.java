package org.apache.solr.bench.search;

import static org.apache.solr.bench.BaseBenchState.log;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.CallerSpecificQueryLimit;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.TestInjection;
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

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(time = 20, iterations = 2)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(time = 30, iterations = 4)
@Threads(value = 1)
public class ExitableDirectoryReaderSearch {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {

    Docs queryFields;

    int NUM_DOCS = 500_000;
    int WORDS = NUM_DOCS / 100;

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      miniClusterState.setUseHttp1(true);
      System.setProperty("documentCache.enabled", "false");
      System.setProperty("queryResultCache.enabled", "false");
      System.setProperty("filterCache.enabled", "false");
      System.setProperty("miniClusterBaseDir", "build/work/mini-cluster");
      // create a lot of small segments
      System.setProperty("segmentsPerTier", "200");
      System.setProperty("maxBufferedDocs", "100");

      miniClusterState.startMiniCluster(1);
      log("######### Creating index ...");
      miniClusterState.createCollection(COLLECTION, 1, 1);
      // create a lot of large-ish fields to scan positions
      Docs docs =
          Docs.docs(1234567890L)
              .field("id", integers().incrementing())
              .field("f1_ts", strings().alpha().maxCardinality(WORDS).ofLengthBetween(3, 10))
              .field(
                  "f2_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f3_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f4_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f5_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f6_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f7_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f8_ts", strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10))
              .field(
                  "f9_ts",
                  strings().alpha().maxCardinality(WORDS).multi(50).ofLengthBetween(3, 10));
      miniClusterState.index(COLLECTION, docs, NUM_DOCS, true);
      miniClusterState.forceMerge(COLLECTION, 200);
      miniClusterState.dumpCoreInfo();
    }

    @Param({"false", "true"})
    boolean useEDR;

    // this adds significant processing time to the checking of query limits
    // both to verify that it's actually used and to illustrate the impact of limit checking
    @Param({"false", "true"})
    boolean verifyEDRInUse = true;

    private static final String matchExpression = "ExitableTermsEnum:-1";

    @Setup(Level.Iteration)
    public void setupQueries(MiniClusterState.MiniClusterBenchState state) throws Exception {
      System.setProperty(SolrIndexSearcher.EXITABLE_READER_PROPERTY, String.valueOf(useEDR));
      if (verifyEDRInUse) {
        TestInjection.queryTimeout = new CallerSpecificQueryLimit(Set.of(matchExpression));
      }
      // reload collection to force searcher / reader refresh
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      state.client.request(reload);

      queryFields =
          Docs.docs(1234567890L)
              .field("id", integers().incrementing())
              .field("f1_ts", strings().alpha().maxCardinality(WORDS).ofLengthBetween(3, 10))
              .field(
                  "f2_ts", strings().alpha().maxCardinality(WORDS).multi(5).ofLengthBetween(3, 10));
    }

    @TearDown(Level.Iteration)
    public void tearDownTrial() throws Exception {
      if (useEDR && verifyEDRInUse) {
        CallerSpecificQueryLimit queryLimit = (CallerSpecificQueryLimit) TestInjection.queryTimeout;
        if (queryLimit == null) {
          throw new RuntimeException("Missing setup!");
        }
        Map<String, Integer> callCounts = queryLimit.getCallerMatcher().getCallCounts();
        log("######### Caller specific stats:");
        log("Call counts: " + callCounts);
        if (callCounts.get(matchExpression) == null) {
          throw new RuntimeException("Missing call counts!");
        }
        if (callCounts.get(matchExpression).intValue() == 0) {
          throw new RuntimeException("No call counts!");
        }
      }
    }
  }

  private static ModifiableSolrParams createInitialParams() {
    ModifiableSolrParams params =
        MiniClusterState.params("rows", "100", "timeAllowed", "1000", "fl", "*");
    return params;
  }

  @Benchmark
  public void testShortQuery(
      MiniClusterState.MiniClusterBenchState miniClusterState, Blackhole bh, BenchState state)
      throws Exception {
    SolrInputDocument queryDoc = state.queryFields.inputDocument();
    ModifiableSolrParams params = createInitialParams();
    params.set("q", "f1_ts:" + queryDoc.getFieldValue("f1_ts").toString());
    QueryRequest queryRequest = new QueryRequest(params);
    QueryResponse rsp = queryRequest.process(miniClusterState.client, COLLECTION);
    bh.consume(rsp);
  }

  @Benchmark
  public void testLongQuery(
      MiniClusterState.MiniClusterBenchState miniClusterState, Blackhole bh, BenchState state)
      throws Exception {
    SolrInputDocument queryDoc = state.queryFields.inputDocument();
    ModifiableSolrParams params = createInitialParams();
    StringBuilder query = new StringBuilder();
    for (int i = 2; i < 10; i++) {
      if (query.length() > 0) {
        query.append(" ");
      }
      String fld = "f" + i + "_ts";
      query.append(fld + ":\"" + queryDoc.getFieldValue(fld) + "\"~20");
    }
    params.set("q", query.toString());
    QueryRequest queryRequest = new QueryRequest(params);
    QueryResponse rsp = queryRequest.process(miniClusterState.client, COLLECTION);
    bh.consume(rsp);
  }
}
