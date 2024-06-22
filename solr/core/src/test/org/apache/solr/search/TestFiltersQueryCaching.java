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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verify caching impacts of FiltersQParser and FilterQuery */
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
        assertU(commit()); // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  private static Map<String, Object> lookupFilterCacheMetrics(SolrCore core) {
    return ((MetricsMap)
            ((SolrMetricManager.GaugeWrapper<?>)
                    core.getCoreMetricManager()
                        .getRegistry()
                        .getMetrics()
                        .get("CACHE.searcher.filterCache"))
                .getGauge())
        .getValue();
  }

  private static long lookupFilterCacheInserts(SolrCore core) {
    return (long)
        ((MetricsMap)
                ((SolrMetricManager.GaugeWrapper<?>)
                        core.getCoreMetricManager()
                            .getRegistry()
                            .getMetrics()
                            .get("CACHE.searcher.filterCache"))
                    .getGauge())
            .getValue()
            .get("inserts");
  }

  @Test
  public void testRecursiveFilter() throws Exception {
    final String termQuery = "{!term f=field_s v='d0'}";
    final String filterTermQuery = "filter(" + termQuery + ")";
    final int expectNumFound = 1;
    final String expectNumFoundXPath = "/response/numFound==" + expectNumFound;

    h.reload();
    assertJQ(req("q", termQuery, "indent", "true"), expectNumFoundXPath);
    assertEquals(0, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(req("q", filterTermQuery, "indent", "true"), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=false}field_s:d0"), expectNumFoundXPath);
    assertEquals(0, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=true}field_s:d0"), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=false}filter(field_s:d0)"),
        expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=true}filter(field_s:d0)"),
        expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(req("q", "*:*", "indent", "true", "fq", termQuery), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    assertJQ(req("q", "*:*", "indent", "true", "fq", filterTermQuery), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    h.reload();
    SolrCore core = h.getCore();
    Map<String, Object> filterCacheMetrics;
    final String termQuery2 = "{!term f=field_s v='d1'}";
    final String filterTermQuery2 = "filter(" + termQuery2 + ")";
    assertJQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            "{!bool cache=false should=$ftq should=$ftq2}",
            "ftq",
            filterTermQuery,
            "ftq2",
            filterTermQuery2),
        "/response/numFound==2");
    assertEquals(2, lookupFilterCacheInserts(core));
    JQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            random().nextBoolean() ? termQuery : filterTermQuery));
    filterCacheMetrics = lookupFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(1, (long) filterCacheMetrics.get("hits"));
    JQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            random().nextBoolean() ? termQuery2 : filterTermQuery2));
    filterCacheMetrics = lookupFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(2, (long) filterCacheMetrics.get("hits"));
    JQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            "{!bool cache=false should=$ftq should=$ftq2}",
            "ftq",
            filterTermQuery,
            "ftq2",
            filterTermQuery2,
            "cursorMark",
            "*",
            "sort",
            "id asc"));
    filterCacheMetrics = lookupFilterCacheMetrics(core);
    assertEquals(2, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(4, (long) filterCacheMetrics.get("hits"));
    JQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            "{!bool should=$ftq should=$ftq2}",
            "ftq",
            filterTermQuery,
            "ftq2",
            filterTermQuery2,
            "cursorMark",
            "*",
            "sort",
            "id asc"));
    filterCacheMetrics = lookupFilterCacheMetrics(core);
    assertEquals(3, (long) filterCacheMetrics.get("inserts")); // added top-level
    assertEquals(6, (long) filterCacheMetrics.get("hits"));
  }

  @Test
  public void testAbsentParams() throws Exception {
    // no `fqs` at all
    doTestAbsentParams(Collections.emptyList(), NUM_DOCS);
    // simple term query `fqs`
    doTestAbsentParams(List.of("fqs", "{!term f=field_s v='d0'}"), 1);
    // empty `fqs`
    doTestAbsentParams(List.of("fqs", ""), NUM_DOCS);
  }

  private static void doTestAbsentParams(Collection<String> fqsArgs, int expectNumFound)
      throws Exception {
    List<String> request = new ArrayList<>();
    request.addAll(List.of("q", "*:*", "indent", "true", "fq", "{!filters param=$fqs}"));
    request.addAll(fqsArgs);
    assertJQ(req(request.toArray(new String[0])), "/response/numFound==" + expectNumFound);
  }
}
