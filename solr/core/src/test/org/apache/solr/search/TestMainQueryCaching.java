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
package org.apache.solr.search;

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Verify caching interactions between main query and filterCache
 */
public class TestMainQueryCaching extends SolrTestCaseJ4 {

  static int NUM_DOCS = 100;
  private static final String TEST_UFFSQ_PROPNAME = "solr.test.useFilterForSortedQuery";
  static String RESTORE_UFFSQ_PROP;
  static boolean USE_FILTER_FOR_SORTED_QUERY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // TODO: figure out why the line below (accepting this property as overridden on test invocation) isn't working
    //  as expected.
    final String uffsq = System.getProperty(TEST_UFFSQ_PROPNAME, Boolean.toString(random().nextBoolean()));
    USE_FILTER_FOR_SORTED_QUERY = Boolean.parseBoolean(uffsq);
    RESTORE_UFFSQ_PROP = System.setProperty(TEST_UFFSQ_PROPNAME, uffsq);
    initCore("solrconfig-deeppaging.xml", "schema-sorts.xml");
    createIndex();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (RESTORE_UFFSQ_PROP == null) {
      System.clearProperty(TEST_UFFSQ_PROPNAME);
    } else {
      System.setProperty(TEST_UFFSQ_PROPNAME, RESTORE_UFFSQ_PROP);
    }
  }

  public static void createIndex() {
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "str", "d" + i));
      if (random().nextInt(NUM_DOCS) == 0) {
        assertU(commit());  // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  @Before
  public void beforeTest() throws Exception {
    // testing caching, it's far simpler to just reload the core every time to prevent
    // subsequent requests from affecting each other
    h.reload();
  }

  private static long coreToInserts(SolrCore core) {
    return (long)((MetricsMap)((SolrMetricManager.GaugeWrapper<?>)core
            .getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.filterCache")).getGauge())
            .getValue().get("inserts");
  }

  private static long coreToSortCount(SolrCore core, String skipOrFull) {
    return (long)((SolrMetricManager.GaugeWrapper<?>)core
            .getCoreMetricManager().getRegistry().getMetrics().get("SEARCHER.searcher." + skipOrFull + "SortCount")).getGauge()
            .getValue();
  }

  private static long coreToMatchAllDocsCacheConsultationCount(SolrCore core) {
    return (long)((SolrMetricManager.GaugeWrapper<?>)core
            .getCoreMetricManager().getRegistry().getMetrics().get("SEARCHER.searcher.matchAllDocsCacheConsultationCount")).getGauge()
            .getValue();
  }

  private static final String SCORING_QUERY = "str:d*";
  private static final String CONSTANT_SCORE_QUERY = "(" + SCORING_QUERY + ")^=1.0"; // wrapped as a ConstantScoreQuery
  private static final String MATCH_ALL_DOCS_QUERY = "*:*";

  private static final String[] ALL_QUERIES = new String[] { SCORING_QUERY, CONSTANT_SCORE_QUERY, MATCH_ALL_DOCS_QUERY };

  @Test
  public void testScoringQuery() throws Exception {
    // plain request should have no caching or sorting optimization
    String response = JQ(req("q", SCORING_QUERY, "indent", "true"));
    assertMetricCounts(response, false, 0, 1, 0);
  }

  @Test
  public void testConstantScoreFlScore() throws Exception {
    // explicitly requesting scores should unconditionally disable caching and sorting optimizations
    String response = JQ(req("q", CONSTANT_SCORE_QUERY, "indent", "true", "rows", "0", "fl", "id,score", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
    assertMetricCounts(response, false, 0, 1, 0);
  }

  @Test
  public void testScoringQueryNonScoreSort() throws Exception {
    // plain request with no score in sort should consult filterCache, but need full sorting
    String response = JQ(req("q", SCORING_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, false, USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0);
  }

  @Test
  public void testScoringQueryZeroRows() throws Exception {
    // always hit cache, optimize sort because rows=0
    String response = JQ(req("q", SCORING_QUERY, "indent", "true", "rows", "0", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
    final int insertAndSkipCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
    assertMetricCounts(response, false, insertAndSkipCount, USE_FILTER_FOR_SORTED_QUERY ? 0 : 1, insertAndSkipCount);
  }

  @Test
  public void testConstantScoreSortByScore() throws Exception {
    // hit cache and skip sort because constant score query
    String response = JQ(req("q", CONSTANT_SCORE_QUERY, "indent", "true"));
    final int insertAndSkipCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
    assertMetricCounts(response, false, insertAndSkipCount, USE_FILTER_FOR_SORTED_QUERY ? 0 : 1, insertAndSkipCount);
  }

  @Test
  public void testConstantScoreNonScoreSort() throws Exception {
    // consult filterCache because constant score query, but no skip sort (because sort-by-id)
    String response = JQ(req("q", CONSTANT_SCORE_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, false, USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0);
  }

  @Test
  public void testMatchAllDocsPlain() throws Exception {
    // plain request with "score" sort should skip sort even if `rows` requested
    String response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true"));
    assertMetricCounts(response, true, 0, 0, 1);
  }

  @Test
  public void testMatchAllDocsFlScore() throws Exception {
    // explicitly requesting scores should unconditionally disable all cache consultation and sort optimization
    String response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "rows", "0", "fl", "id,score", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
    // NOTE: pretend we're not MatchAllDocs ...
    assertMetricCounts(response, false, 0, 1, 0);
  }

  @Test
  public void testMatchAllDocsZeroRows() throws Exception {
    // plain request should _always_ skip sort when `rows=0`
    String response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "rows", "0", "sort", "id asc"));
    assertMetricCounts(response, true, 0, 0, 1);
  }

  @Test
  public void testMatchAllDocsNonScoreSort() throws Exception {
    // plain request _with_ rows and non-score sort should consult cache, but not skip sort
    String response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "sort", "id asc"));
    assertMetricCounts(response, true, 0, 1, 0);
  }

  @Test
  public void testCursorMark() throws Exception {
    String q = pickRandom(ALL_QUERIES);
    boolean includeScoreInSort = random().nextBoolean();
    String response = JQ(req("q", q, "indent", "true", "cursorMark", "*", "sort", includeScoreInSort ? "score desc,id asc" : "id asc"));
    assertMetricCounts(response, MATCH_ALL_DOCS_QUERY.equals(q) && !includeScoreInSort,
            !MATCH_ALL_DOCS_QUERY.equals(q) && !includeScoreInSort && USE_FILTER_FOR_SORTED_QUERY ? 1 : 0, 1, 0);
  }

  @Test
  public void testCursorMarkZeroRows() throws Exception {
    String q = pickRandom(ALL_QUERIES);
    String response = JQ(req("q", q, "indent", "true", "cursorMark", "*", "rows", "0", "sort", random().nextBoolean() ? "id asc" : "score desc,id asc"));
    assertMetricCounts(response, MATCH_ALL_DOCS_QUERY.equals(q), !MATCH_ALL_DOCS_QUERY.equals(q) && USE_FILTER_FOR_SORTED_QUERY ? 1 : 0,
            MATCH_ALL_DOCS_QUERY.equals(q) || USE_FILTER_FOR_SORTED_QUERY ? 0 : 1, MATCH_ALL_DOCS_QUERY.equals(q) || USE_FILTER_FOR_SORTED_QUERY ? 1 : 0);
  }

  private static void assertMetricCounts(String response, boolean matchAllDocs, int expectFilterCacheInsertCount, int expectFullSortCount, int expectSkipSortCount) {
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    SolrCore core = h.getCore();
    assertEquals("Bad matchAllDocsCacheConsultation count", (matchAllDocs ? 1 : 0), coreToMatchAllDocsCacheConsultationCount(core));
    assertEquals("Bad filterCache insert count", expectFilterCacheInsertCount, coreToInserts(core));
    assertEquals("Bad full sort count", expectFullSortCount, coreToSortCount(core, "full"));
    assertEquals("Bad skip sort count", expectSkipSortCount, coreToSortCount(core, "skip"));
    assertEquals("Should have exactly " + NUM_DOCS, NUM_DOCS, (long) (body.get("numFound"))); // sanity check
  }
}


