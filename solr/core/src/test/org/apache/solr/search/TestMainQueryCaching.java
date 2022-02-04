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

  @Test
  public void testQueryCaching() throws Exception {
    String q = "str:d*";
    String constQ = "("+q+")^=1.0"; // wrapped as a ConstantScoreQuery

    for (int i = 0; i < 6; i++) {
      // testing caching, it's far simpler to just reload the core every time to prevent
      // subsequent requests from affecting each other
      h.reload();
      final String response;
      final int expectInserts;
      final int expectSkipSortCount;
      final int expectFullSortCount;
      switch (i) {
        case 0:
          // plain request should not be cached
          response = JQ(req("q", q, "indent", "true"));
          expectInserts = 0;
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        case 1:
          // explicitly requesting scores should unconditionally disable fq insert
          response = JQ(req("q", constQ, "indent", "true", "rows", "0", "fl", "id,score", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
          expectInserts = 0;
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        case 2:
          // plain request with no score in sort should consult filterCache, but need full sorting
          response = JQ(req("q", q, "indent", "true", "sort", "id asc"));
          expectInserts = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        case 3:
          // hit cache because rows=0
          response = JQ(req("q", q, "indent", "true", "rows", "0", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
          expectInserts = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          expectFullSortCount = USE_FILTER_FOR_SORTED_QUERY ? 0 : 1;
          expectSkipSortCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          break;
        case 4:
          // hit cache because constant score query
          response = JQ(req("q", constQ, "indent", "true"));
          expectInserts = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          expectFullSortCount = USE_FILTER_FOR_SORTED_QUERY ? 0 : 1;
          expectSkipSortCount = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          break;
        case 5:
          // consult filterCache because constant score query, but no skip sort (because sort-by-id)
          response = JQ(req("q", constQ, "indent", "true", "sort", "id asc"));
          expectInserts = USE_FILTER_FOR_SORTED_QUERY ? 1 : 0;
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        default:
          throw new IllegalStateException();
      }
      Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
      Map<?, ?> body = (Map<?, ?>) (res.get("response"));
      assertEquals("Bad filterCache insert count", expectInserts, coreToInserts(h.getCore()));
      assertEquals("Bad full sort count", expectFullSortCount, coreToSortCount(h.getCore(), "full"));
      assertEquals("Bad skip sort count", expectSkipSortCount, coreToSortCount(h.getCore(), "skip"));
      assertEquals("Should have exactly " + NUM_DOCS, NUM_DOCS, (long) (body.get("numFound"))); // sanity check
    }
  }

  @Test
  public void testMatchAllDocsQueryCaching() throws Exception {
    String q = "*:*";

    for (int i = 0; i < 4; i++) {
      h.reload();
      final String response;
      final int expectSkipSortCount;
      final int expectFullSortCount;
      switch (i) {
        case 0:
          // plain request should consult cache, irrespective of `rows` requested
          response = JQ(req("q", q, "indent", "true"));
          expectFullSortCount = 0;
          expectSkipSortCount = 1;
          break;
        case 1:
          // explicitly requesting scores should unconditionally disable fq insert
          response = JQ(req("q", q, "indent", "true", "rows", "0", "fl", "id,score", "sort", (random().nextBoolean() ? "id asc" : "score desc")));
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        case 2:
          // plain request should _always_ consult cache and skip sort when `rows=0`
          response = JQ(req("q", q, "indent", "true", "rows", "0", "sort", "id asc"));
          expectFullSortCount = 0;
          expectSkipSortCount = 1;
          break;
        case 3:
          // plain request _with_ rows should consult cache, but not skip sort
          response = JQ(req("q", q, "indent", "true", "sort", "id asc"));
          expectFullSortCount = 1;
          expectSkipSortCount = 0;
          break;
        default:
          throw new IllegalStateException();
      }
      Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
      Map<?, ?> body = (Map<?, ?>) (res.get("response"));
      assertEquals("MatchAllDocsQuery should bypass the actual filterCache", 0, coreToInserts(h.getCore()));
      assertEquals("Bad full sort count", expectFullSortCount, coreToSortCount(h.getCore(), "full"));
      assertEquals("Bad skip sort count", expectSkipSortCount, coreToSortCount(h.getCore(), "skip"));
      assertEquals("Should have exactly " + NUM_DOCS, NUM_DOCS, (long) (body.get("numFound"))); // sanity check
    }
  }

}


