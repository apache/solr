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
import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.lists;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.bench.CircularIterator;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.SolrBenchState;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks the docValues filter optimization for range field types.
 *
 * <p>Docs hold wide ranges {@code [m, m+SPAN]} in indexed-only (BKD) and indexed+docValues fields,
 * single-valued ({@code r_ir}/{@code r_ir_dv}) and multiValued ({@code r_mv_ir}/{@code
 * r_mv_ir_dv}). A central window matches ~half the docs, so the range clause is costly to
 * materialize but cheap to verify per-doc. It is AND-ed with a selective term filter whose
 * doc-frequency is controlled, so we know -- and the {@code [range-bench]} setup log confirms --
 * which {@link org.apache.lucene.search.IndexOrDocValuesQuery} branch runs.
 *
 * <p>Params: {@code criteria} (the {@code {!numericRange criteria=...}} relation), {@code field}
 * (the DV / non-DV pairs), and {@code scenario} -- the lead's selectivity vs the {@code
 * rangeCost/8} cutover: {@code selective_dv} (~{@value #SELECTIVE_LEAD_DF} docs -&gt; docValues
 * branch), {@code broad_points} (~{@value #BROAD_LEAD_DF} docs -&gt; points branch), {@code
 * none_points} (standalone -&gt; points).
 */
@Fork(value = 1)
@Warmup(time = 5, iterations = 5)
@Measurement(time = 5, iterations = 5)
@Threads(value = 1)
public class RangeSearch {

  static final String COLLECTION = "c1";

  static final int NUM_DOCS = 1_000_000;

  /** Width of each indexed document range: docs store {@code [m, m+SPAN]}. */
  static final int SPAN = 500_000;

  /**
   * Narrow window for {@code contains}/{@code intersects}/{@code crosses}: contained by ~half the
   * docs.
   */
  static final String OTHER_QUERY = "[500000 TO 500100]";

  /**
   * Wide window for {@code within} (wider than {@code SPAN}), so ~half the docs' ranges fit inside.
   */
  static final String WITHIN_QUERY = "[0 TO 1000000]";

  /** "Selective" lead df: far below {@code rangeCost/8} (~62.5k) -&gt; docValues branch. */
  static final int SELECTIVE_LEAD_DF = 2_000;

  /** "Broad" lead df: above {@code rangeCost/8} but below {@code rangeCost} -&gt; points branch. */
  static final int BROAD_LEAD_DF = 250_000;

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"contains", "intersects", "within", "crosses"})
    public String criteria;

    @Param({"r_ir", "r_ir_dv", "r_mv_ir", "r_mv_ir_dv"})
    public String field;

    @Param({"selective_dv", "broad_points", "none_points"})
    public String scenario;

    // Rotated so repeated queries are distinct top-level queries (each ~SELECTIVE_LEAD_DF docs).
    Iterator<String> selectiveTerms;
    // Rotated broad-lead terms (each ~BROAD_LEAD_DF docs).
    Iterator<String> broadTerms;

    @Setup(Level.Trial)
    public void setupTrial(SolrBenchState solrBenchState) throws Exception {
      solrBenchState.setUseHttp1(true);
      solrBenchState.startSolr(1);
      solrBenchState.createCollection(COLLECTION, 1, 1);

      // Controlled lead selectivity: ~numDocs/DF distinct values -> each term matches ~DF docs.
      SolrGen<String> selectiveLeadTerms =
          integers().between(0, NUM_DOCS / SELECTIVE_LEAD_DF - 1).map(i -> "s" + i, String.class);
      SolrGen<String> broadLeadTerms =
          integers().between(0, NUM_DOCS / BROAD_LEAD_DF - 1).map(i -> "b" + i, String.class);
      // m in [0, NUM_DOCS] so that m + SPAN never overflows int; range string is "[m TO m+SPAN]".
      SolrGen<String> rangeValues =
          integers()
              .between(0, NUM_DOCS)
              .map(m -> "[" + m + " TO " + (m + SPAN) + "]", String.class);
      // multiValued docs hold 2-3 ranges each, drawn from the same distribution.
      var mvRangeValues = lists().of(rangeValues).ofSizeBetween(2, 3);

      Docs docs =
          docs()
              .field("id", integers().incrementing())
              .field("term_high_s", selectiveLeadTerms)
              .field("term_low_s", broadLeadTerms)
              .field("r_ir", rangeValues) // single-valued, indexed-only (BKD)
              .field("r_ir_dv", rangeValues) // single-valued, indexed + docValues
              .field("r_mv_ir", mvRangeValues) // multiValued, indexed-only (BKD)
              .field("r_mv_ir_dv", mvRangeValues); // multiValued, indexed + docValues

      solrBenchState.index(COLLECTION, docs, NUM_DOCS, false);

      // Discover the real term values (and their counts) so queries hit existing terms.
      String basePath = solrBenchState.nodes.getFirst();
      SolrQuery q = new SolrQuery("*:*");
      q.setParam("facet", "true");
      q.setParam("rows", "0");
      q.setParam("facet.field", "term_high_s", "term_low_s");
      q.setParam("facet.limit", "2000");
      q.setParam("facet.mincount", "1");
      QueryResponse response =
          new QueryRequest(q).processWithBaseUrl(solrBenchState.client, basePath, COLLECTION);
      selectiveTerms = new CircularIterator<>(facetValues(response, "term_high_s"));
      broadTerms = new CircularIterator<>(facetValues(response, "term_low_s"));

      // Measure rangeCost (the range clause's own match count) and derive the branch the same way
      // IndexOrDocValuesQuery does: rangeCost >>> 3 <= leadCost -> points, else docValues. leadCost
      // is
      // the actual doc-frequency of the term this scenario leads with.
      String window = criteria.equals("within") ? WITHIN_QUERY : OTHER_QUERY;
      SolrQuery rangeOnly =
          new SolrQuery(
              "q", "{!numericRange criteria=" + criteria + " field=" + field + "}" + window);
      rangeOnly.setParam("rows", "0");
      long rangeCost =
          new QueryRequest(rangeOnly)
              .processWithBaseUrl(solrBenchState.client, basePath, COLLECTION)
              .getResults()
              .getNumFound();
      long threshold = rangeCost >>> 3;
      boolean hasDocValues = field.endsWith("_dv");
      long leadCost;
      long dvBranch; // 1 = docValues verify branch, 0 = points/BKD
      if (scenario.startsWith("none")) {
        leadCost = 0; // standalone: no lead -> points
        dvBranch = 0;
      } else {
        String facetField = scenario.startsWith("selective") ? "term_high_s" : "term_low_s";
        leadCost = firstFacetCount(response, facetField);
        // IndexOrDocValuesQuery picks docValues only when the range is >8x the lead (threshold >
        // leadCost); otherwise points. A docValues=false field never gets that choice.
        dvBranch = (hasDocValues && threshold > leadCost) ? 1 : 0;
      }
      log(
          "[range-bench] criteria="
              + criteria
              + " field="
              + field
              + " scenario="
              + scenario
              + " rangeCost="
              + rangeCost
              + " threshold="
              + threshold
              + " leadCost="
              + leadCost
              + " -> "
              + (dvBranch == 1 ? "docValues" : "points"));
    }

    private Set<String> facetValues(QueryResponse response, String fieldName) {
      return response.getFacetField(fieldName).getValues().stream()
          .map(FacetField.Count::getName)
          .collect(Collectors.toSet());
    }

    private long firstFacetCount(QueryResponse response, String fieldName) {
      return response.getFacetField(fieldName).getValues().stream()
          .mapToLong(FacetField.Count::getCount)
          .findFirst()
          .orElse(0L);
    }

    @Setup(Level.Iteration)
    public void setupIteration(SolrBenchState solrBenchState)
        throws SolrServerException, IOException {
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      solrBenchState.client.requestWithBaseUrl(solrBenchState.nodes.getFirst(), reload, null);
    }

    /** Builds the query for the current (criteria, field, scenario) parameter combination. */
    QueryRequest query() {
      // within needs a window WIDER than the doc ranges (so they can fit inside it); the other
      // relations need a narrow window that the doc ranges contain / overlap.
      String window = criteria.equals("within") ? WITHIN_QUERY : OTHER_QUERY;
      String rangeExpr = "{!numericRange criteria=" + criteria + " field=" + field + "}" + window;
      if (scenario.startsWith("none")) {
        return new QueryRequest(new SolrQuery("q", rangeExpr)); // standalone: no lead -> points
      }

      boolean selective = scenario.startsWith("selective");
      Iterator<String> terms = selective ? selectiveTerms : broadTerms;
      String termField = selective ? "term_high_s" : "term_low_s";
      // Nest the {!numericRange} subquery via _query_ so it joins the term clause's BooleanQuery.
      String query = "+" + termField + ":\"" + terms.next() + "\" +_query_:\"" + rangeExpr + "\"";
      return new QueryRequest(new SolrQuery("q", query));
    }
  }

  @Benchmark
  public void rangeQuery(Blackhole blackhole, BenchState benchState, SolrBenchState solrBenchState)
      throws SolrServerException, IOException {
    // Sink the result exactly once, via Blackhole (do NOT also return it): mixing a return-value
    // sink and a Blackhole sink can resolve to different JMH blackhole modes, which makes the
    // r_ir vs r_ir_dv comparison apples-to-oranges.
    blackhole.consume(benchState.query().process(solrBenchState.client, COLLECTION));
  }
}
