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

import static org.apache.solr.common.util.Utils.fromJSONString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.index.SimpleMergePolicyFactory;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verify caching impacts of FiltersQParser and FilterQuery */
public class TestFiltersQueryCaching extends SolrTestCaseJ4 {

  private static final int NUM_DOCS = 100;
  private static final int AVG_SEG_SIZE = NUM_DOCS / 10;
  private static final int MAX_SEG_SIZE = AVG_SEG_SIZE * 2;
  private static final int MAX_MERGE_AT_ONCE = 2;
  private static final String FILTER_CACHE_IMPL_CLASS_PROPNAME = "solr.filterCache.class";
  private static final String FILTER_CACHE_REGEN_CLASS_PROPNAME = "solr.filterCache.regenerator";
  private static final String FILTER_CACHE_AUTOWARM_COUNT_PROPNAME =
      "solr.filterCache.autowarmCount";
  private static String RESTORE_FILTER_CACHE_IMPL_PROPERTY = null;
  private static String RESTORE_FILTER_CACHE_REGEN_PROPERTY = null;
  private static String RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY = null;
  private static boolean SEG_AWARE_FILTER_CACHE;

  /**
   * There may be a better way to do this; because partial cache can be invalidated by
   * too-aggressive merging, but because we still want to actually test caching against merging
   * (i.e., NoMergePolicy would skip testing relevant functionality), we need a way to throttle
   * merges coming out of the TieredMergePolicy in order to control the portion of the overall index
   * that might be invalidated for any one commit.
   */
  public static class ThrottledMergePolicy extends SimpleMergePolicyFactory {
    public ThrottledMergePolicy(
        SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
      super(resourceLoader, args, schema);
    }

    @Override
    protected MergePolicy getMergePolicyInstance() {
      TieredMergePolicy ret =
          new TieredMergePolicy() {
            @Override
            public MergePolicy.MergeSpecification findMerges(
                MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext)
                throws IOException {
              MergeSpecification ret = super.findMerges(mergeTrigger, infos, mergeContext);
              if (ret == null) {
                return null;
              }
              for (OneMerge oneMerge : ret.merges) {
                assert oneMerge.segments.size() == 2;
                final SegmentCommitInfo first = oneMerge.segments.get(0);
                final SegmentCommitInfo second = oneMerge.segments.get(1);
                if (first.info.maxDoc()
                        - first.getDelCount()
                        + second.info.maxDoc()
                        - second.getDelCount()
                    <= MAX_SEG_SIZE) {
                  // prevent segments from growing too big
                  ret = new MergeSpecification();
                  ret.add(oneMerge);
                  return ret;
                }
              }
              return null;
            }
          };
      ret.setMaxMergeAtOnce(MAX_MERGE_AT_ONCE);
      return ret;
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    systemSetPropertySolrTestsMergePolicyFactory(ThrottledMergePolicy.class.getName());
    SEG_AWARE_FILTER_CACHE = random().nextBoolean();
    RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY =
        System.setProperty(FILTER_CACHE_AUTOWARM_COUNT_PROPNAME, "0");
    if (SEG_AWARE_FILTER_CACHE) {
      System.setProperty(FILTER_CACHE_AUTOWARM_COUNT_PROPNAME, "100%");
      RESTORE_FILTER_CACHE_IMPL_PROPERTY =
          System.setProperty(FILTER_CACHE_IMPL_CLASS_PROPNAME, "solr.CaffeineCache");
      RESTORE_FILTER_CACHE_REGEN_PROPERTY =
          System.setProperty(FILTER_CACHE_REGEN_CLASS_PROPNAME, "solr.KeepAliveRegenerator");
    }
    initCore("solrconfig.xml", "schema_latest.xml");
    createIndex();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (SEG_AWARE_FILTER_CACHE) {
      if (RESTORE_FILTER_CACHE_IMPL_PROPERTY == null) {
        System.clearProperty(FILTER_CACHE_IMPL_CLASS_PROPNAME);
      } else {
        System.setProperty(FILTER_CACHE_IMPL_CLASS_PROPNAME, RESTORE_FILTER_CACHE_IMPL_PROPERTY);
      }
      if (RESTORE_FILTER_CACHE_REGEN_PROPERTY == null) {
        System.clearProperty(FILTER_CACHE_REGEN_CLASS_PROPNAME);
      } else {
        System.setProperty(FILTER_CACHE_REGEN_CLASS_PROPNAME, RESTORE_FILTER_CACHE_REGEN_PROPERTY);
      }
    }
    if (RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY == null) {
      System.clearProperty(FILTER_CACHE_AUTOWARM_COUNT_PROPNAME);
    } else {
      System.setProperty(
          FILTER_CACHE_AUTOWARM_COUNT_PROPNAME, RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY);
    }
    systemClearPropertySolrTestsMergePolicyFactory();
  }

  public static void createIndex() {
    int lastSegBoundary = -1;
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(adoc("id", Integer.toString(i), "field_s", "d" + i));
      if (random().nextInt(AVG_SEG_SIZE) == 0 || i - lastSegBoundary > MAX_SEG_SIZE) {
        lastSegBoundary = i;
        assertU(commit());
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

  private static void forceNewSearcher() {
    final int i = random().nextInt(NUM_DOCS);
    assertU(adoc("id", Integer.toString(i), "field_s", "d" + i));
    assertU(commit());
  }

  private static final String MATCH_ALL_DOCS_QUERY = "*:*";

  private static Map<String, Object> coreToFilterCacheMetrics(SolrCore core) {
    return ((MetricsMap)
            ((SolrMetricManager.GaugeWrapper<?>)
                    core.getCoreMetricManager()
                        .getRegistry()
                        .getMetrics()
                        .get("CACHE.searcher.filterCache"))
                .getGauge())
        .getValue();
  }

  private static long getNumFound(String response) {
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    return (long) body.get("numFound");
  }

  private static final List<String> VALS;

  static {
    final String[] vals = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      vals[i] = Integer.toString(i);
    }
    VALS = List.of(vals);
  }

  private static String getQuery(int count) {
    List<String> vals = new ArrayList<>(VALS);
    Collections.shuffle(vals, random());
    return vals.subList(0, count).stream()
        .collect(
            Collectors.joining(
                "}' should='{!term f=field_s v=d", "{!bool should='{!term f=field_s v=d", "}'}"));
  }

  @Test
  public void testCoreReloads() throws Exception {
    final int expectNumFound = random().nextInt(NUM_DOCS / 2) + 1;
    final String q = getQuery(expectNumFound);
    String response;

    h.reload();
    response =
        JQ(
            req(
                "q",
                MATCH_ALL_DOCS_QUERY,
                "indent",
                "true",
                "cursorMark",
                "*",
                "sort",
                "id asc",
                "fq",
                q));
    Map<String, Object> filterCacheMetrics = coreToFilterCacheMetrics(h.getCore());
    assertEquals(1, (long) filterCacheMetrics.get("cumulative_inserts"));
    assertEquals(0, (long) filterCacheMetrics.get("cumulative_hits"));
    assertEquals(1, (long) filterCacheMetrics.get("inserts")); // unchanged
    assertEquals(0, (long) filterCacheMetrics.get("hits"));
    assertEquals(expectNumFound, getNumFound(response));

    for (int reloads = 1; reloads <= 10; reloads++) {
      forceNewSearcher();
      response =
          JQ(
              req(
                  "q",
                  MATCH_ALL_DOCS_QUERY,
                  "indent",
                  "true",
                  "cursorMark",
                  "*",
                  "sort",
                  "id asc",
                  "fq",
                  q));
      filterCacheMetrics = coreToFilterCacheMetrics(h.getCore());
      if (SEG_AWARE_FILTER_CACHE) {
        // System.err.println("XXX \n"+filterCacheMetrics.entrySet().stream()
        //    .map((e) -> "\t" + e).collect(Collectors.joining("\n")));
        assertEquals(1, (long) filterCacheMetrics.get("cumulative_inserts"));
        assertEquals(reloads, (long) filterCacheMetrics.get("cumulative_hits"));
        assertEquals(0, (long) filterCacheMetrics.get("inserts"));
        assertEquals(1, (long) filterCacheMetrics.get("hits"));
      } else {
        assertEquals(1, (int) filterCacheMetrics.get("size"));
        assertEquals(reloads + 1, (long) filterCacheMetrics.get("cumulative_inserts"));
        assertEquals(0, (long) filterCacheMetrics.get("cumulative_hits"));
        assertEquals(1, (long) filterCacheMetrics.get("inserts"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));
      }
      assertEquals(expectNumFound, getNumFound(response));
    }
  }
}
