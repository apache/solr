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

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.SolrMetricTestUtils;
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

  private static CounterSnapshot.CounterDataPointSnapshot getFilterCacheInserts(SolrCore core) {
    return SolrMetricTestUtils.getCacheSearcherOpsInserts(core, SolrMetricTestUtils.FILTER_CACHE);
  }

  private static CounterSnapshot.CounterDataPointSnapshot getFilterCacheHits(SolrCore core) {
    return SolrMetricTestUtils.getCacheSearcherOpsHits(core, SolrMetricTestUtils.FILTER_CACHE);
  }

  /** Reload the core to reset non-cumulative cache metrics. */
  private void reloadAndWait() throws Exception {
    h.reload();

    // Make sure the new searcher (after reload) is fully initialized. This is to avoid races
    // between queries submitted by the test and cache metrics warmup of the new searcher.
    // Another option could be to not reload the core and use cumulative metrics, but the test
    // would be harder to understand.
    Future<?>[] waitSearcher = (Future<?>[]) Array.newInstance(Future.class, 1);
    h.getCore().getSearcher(true, false, waitSearcher, true);
    waitSearcher[0].get();
  }

  private static long lookupFilterCacheInserts(SolrCore core) {
    return (long) getFilterCacheInserts(core).getValue();
  }

  private static long lookupFilterCacheHits(SolrCore core) {
    return (long) getFilterCacheHits(core).getValue();
  }

  @Test
  public void testRecursiveFilter() throws Exception {
    final String termQuery = "{!term f=field_s v='d0'}";
    final String filterTermQuery = "filter(" + termQuery + ")";
    final int expectNumFound = 1;
    final String expectNumFoundXPath = "/response/numFound==" + expectNumFound;

    reloadAndWait();
    assertJQ(req("q", termQuery, "indent", "true"), expectNumFoundXPath);
    assertEquals(0, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(req("q", filterTermQuery, "indent", "true"), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=false}field_s:d0"), expectNumFoundXPath);
    assertEquals(0, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=true}field_s:d0"), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=false}filter(field_s:d0)"),
        expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(
        req("q", "*:*", "indent", "true", "fq", "{!cache=true}filter(field_s:d0)"),
        expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(req("q", "*:*", "indent", "true", "fq", termQuery), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    assertJQ(req("q", "*:*", "indent", "true", "fq", filterTermQuery), expectNumFoundXPath);
    assertEquals(1, lookupFilterCacheInserts(h.getCore()));

    reloadAndWait();
    SolrCore core = h.getCore();
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
    assertEquals(2, lookupFilterCacheInserts(core)); // unchanged
    assertEquals(1, lookupFilterCacheHits(core));
    JQ(
        req(
            "q",
            "*:*",
            "indent",
            "true",
            "fq",
            random().nextBoolean() ? termQuery2 : filterTermQuery2));
    assertEquals(2, lookupFilterCacheInserts(core)); // unchanged
    assertEquals(2, lookupFilterCacheHits(core));
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
    assertEquals(2, lookupFilterCacheInserts(core)); // unchanged
    assertEquals(4, lookupFilterCacheHits(core));
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
    assertEquals(3, lookupFilterCacheInserts(core)); // added top-level
    assertEquals(6, lookupFilterCacheHits(core));
  }

  @Test
  public void testAbsentParams() throws Exception {
    // no `fqs` at all
    doTestAbsentParams(List.of(), NUM_DOCS);
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
