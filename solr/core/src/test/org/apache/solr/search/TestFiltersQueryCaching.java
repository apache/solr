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
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.fromJSONString;

/**
 * Verify caching impacts of FiltersQParser and FilterQuery
 */
public class TestFiltersQueryCaching extends SolrTestCaseJ4 {

  private static final int NUM_DOCS = 20;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema_latest.xml");
    createIndex();
  }

  public static void createIndex() {
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "field_s", "d" + i));
      if (random().nextInt(NUM_DOCS) == 0) {
        assertU(commit());  // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  private static final String MATCH_ALL_DOCS_QUERY = "*:*";

  private static Map<String, Object> coreToFilterCacheMetrics(SolrCore core) {
    return ((MetricsMap)((SolrMetricManager.GaugeWrapper<?>)core.getCoreMetricManager().getRegistry()
            .getMetrics().get("CACHE.searcher.filterCache")).getGauge()).getValue();
  }

  private static long coreToInserts(SolrCore core) {
    return (long)((MetricsMap)((SolrMetricManager.GaugeWrapper<?>)core
            .getCoreMetricManager().getRegistry().getMetrics().get("CACHE.searcher.filterCache")).getGauge())
            .getValue().get("inserts");
  }

  private static long getNumFound(String response) {
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    return (long) body.get("numFound");
  }

  @Test
  public void testRecursiveFilter() throws Exception {
    final String termQuery = "{!term f=field_s v='d0'}";
    final String filterTermQuery = "filter(" + termQuery + ")";
    final int expectNumFound = 1;
    String response;

    h.reload();
    response = JQ(req("q", termQuery, "indent", "true"));
    assertEquals(0, coreToInserts(h.getCore()));
    assertEquals(expectNumFound, getNumFound(response));

    h.reload();
    response = JQ(req("q", filterTermQuery, "indent", "true"));
    assertEquals(1, coreToInserts(h.getCore()));
    assertEquals(expectNumFound, getNumFound(response));

    h.reload();
    response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", termQuery));
    assertEquals(1, coreToInserts(h.getCore()));
    assertEquals(expectNumFound, getNumFound(response));

    h.reload();
    response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", filterTermQuery));
    assertEquals(1, coreToInserts(h.getCore()));
    assertEquals(expectNumFound, getNumFound(response));

    h.reload();
    SolrCore core = h.getCore();
    Map<String, Object> filterCacheMetrics;
    final String termQuery2 = "{!term f=field_s v='d1'}";
    final String filterTermQuery2 = "filter(" + termQuery2 + ")";
    response = JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", "{!bool cache=false should=$ftq should=$ftq2}",
            "ftq", filterTermQuery, "ftq2", filterTermQuery2));
    assertEquals(2, coreToInserts(core));
    assertEquals(2, getNumFound(response));
    JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", random().nextBoolean() ? termQuery : filterTermQuery));
    filterCacheMetrics = coreToFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(1, (long) filterCacheMetrics.get("hits"));
    JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", random().nextBoolean() ? termQuery2 : filterTermQuery2));
    filterCacheMetrics = coreToFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(2, (long) filterCacheMetrics.get("hits"));
    JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", "{!bool cache=false should=$ftq should=$ftq2}",
            "ftq", filterTermQuery, "ftq2", filterTermQuery2, "cursorMark", "*", "sort", "id asc"));
    filterCacheMetrics = coreToFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(4, (long) filterCacheMetrics.get("hits"));
    JQ(req("q", MATCH_ALL_DOCS_QUERY, "indent", "true", "fq", "{!bool should=$ftq should=$ftq2}",
            "ftq", filterTermQuery, "ftq2", filterTermQuery2, "cursorMark", "*", "sort", "id asc"));
    filterCacheMetrics = coreToFilterCacheMetrics(core);
    assertEquals(3, (long) filterCacheMetrics.get("inserts")); // added top-level
    assertEquals(6, (long) filterCacheMetrics.get("hits"));
  }
}
