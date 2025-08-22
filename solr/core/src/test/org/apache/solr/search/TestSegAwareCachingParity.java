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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verify caching impacts of FiltersQParser and FilterQuery */
public class TestSegAwareCachingParity extends SolrTestCaseJ4 {

  // fails: `-Ptests.seed=152284B971099372`

  private static final int NUM_DOCS = 100;
  private static final String FILTER_CACHE_IMPL_CLASS_PROPNAME = "solr.filterCache.class";
  private static final String FILTER_CACHE_REGEN_CLASS_PROPNAME = "solr.filterCache.regenerator";
  private static final String FILTER_CACHE_AUTOWARM_COUNT_PROPNAME =
      "solr.filterCache.autowarmCount";
  private static String RESTORE_FILTER_CACHE_IMPL_PROPERTY = null;
  private static String RESTORE_FILTER_CACHE_REGEN_PROPERTY = null;
  private static String RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY =
        System.setProperty(FILTER_CACHE_AUTOWARM_COUNT_PROPNAME, "100%");
    RESTORE_FILTER_CACHE_IMPL_PROPERTY =
        System.setProperty(FILTER_CACHE_IMPL_CLASS_PROPNAME, "solr.CaffeineCache");
    RESTORE_FILTER_CACHE_REGEN_PROPERTY =
        System.setProperty(FILTER_CACHE_REGEN_CLASS_PROPNAME, "solr.KeepAliveRegenerator");
    initCore("solrconfig.xml", "schema_latest.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
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
    if (RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY == null) {
      System.clearProperty(FILTER_CACHE_AUTOWARM_COUNT_PROPNAME);
    } else {
      System.setProperty(
          FILTER_CACHE_AUTOWARM_COUNT_PROPNAME, RESTORE_FILTER_AUTOWARM_COUNT_PROPERTY);
    }
  }

  private static Callable<Integer> sustainedIndexing(Random r, AtomicBoolean exit) {
    return () -> {
      int ret = 0;
      try {
        while (!exit.get()) {
          ret++;
          int doc = r.nextInt(NUM_DOCS);
          assertU(adoc("id", Integer.toString(doc), "field_s", "d" + doc));
          if (r.nextInt(10) == 0) {
            assertU(commit());
          }
        }
      } catch (Throwable t) {
        exit.set(true);
        throw t;
      }
      return ret;
    };
  }

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

  private static final List<Integer> VALS;

  static {
    final Integer[] vals = new Integer[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      vals[i] = i;
    }
    VALS = List.of(vals);
  }

  private static Query getQuery(int count, Random r) {
    List<Integer> vals = new ArrayList<>(VALS);
    Collections.shuffle(vals, r);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    vals.subList(0, count)
        .forEach(
            (i) -> {
              builder.add(new TermQuery(new Term("field_s", "d" + i)), BooleanClause.Occur.MUST);
            });
    return builder.build();
  }

  private static Callable<Integer> sustainedQuerying(
      Random r,
      AtomicBoolean exit,
      Query[] queries,
      Query[] noCacheQueries,
      Set<Object> uniqueSearchers) {
    int nQueries = queries.length;
    SolrCore core = h.getCore();
    return () -> {
      int ret = 0;
      try {
        while (!exit.get()) {
          ret++;
          int i = r.nextInt(nQueries);
          core.withSearcher(
              (s) -> {
                uniqueSearchers.add(s.getIndexReader().getReaderCacheHelper().getKey());
                DocSet docSet;
                DocSet uncachedDocSet;
                if (r.nextBoolean()) {
                  docSet = s.getDocSet(queries[i]);
                  uncachedDocSet = s.getDocSet(noCacheQueries[i]);
                } else {
                  uncachedDocSet = s.getDocSet(noCacheQueries[i]);
                  docSet = s.getDocSet(queries[i]);
                }
                if (i == 1 || s.numDocs() == 0) {
                  // special case for MatchAllDocsQuery and empty index
                  assertEquals(docSet, uncachedDocSet);
                } else {
                  assertNotEquals(
                      "cached and uncached should be different! compare: " + docSet,
                      docSet,
                      uncachedDocSet);
                }
                int size = docSet.size();
                assertEquals(size, uncachedDocSet.size());
                assertEquals(size, docSet.intersectionSize(uncachedDocSet));
                return null;
              });
        }
      } catch (Throwable t) {
        exit.set(true);
        throw t;
      }
      return ret;
    };
  }

  private static Query noCache(Query q) {
    WrappedQuery wrapped = new WrappedQuery(q);
    wrapped.setCache(false);
    return wrapped;
  }

  /**
   * This test forces a ton of new searchers to open (indexing the same pool of doc ids, so
   * overwriting, lots of deletes on segments, lots of merging, etc.). At the same time it runs a
   * fixed pool of queries from multiple querying threads, validating cached vs. uncached {@link
   * DocSet}s.
   *
   * <p>Runs heavily for N seconds (20 atm), checks for parity repeatedly, and if no errors, the
   * test passes.
   */
  @Test
  @SuppressWarnings("try")
  public void testParity() throws Exception {
    Random r = random();
    h.reload();

    // larger number of queries gets lower non-partial hit count, thus higher
    // relative partial hit count. Setting nQueries same as `NUM_DOCS` strikes
    // a reasonable balance.
    final int nQueries = NUM_DOCS;

    Query[] queries = new Query[nQueries];

    // special-case queries
    queries[0] = new MatchNoDocsQuery();
    queries[1] = new MatchAllDocsQuery();
    queries[2] = getQuery(NUM_DOCS, r);

    // random other queries
    for (int i = 3; i < nQueries; i++) {
      queries[i] = getQuery(r.nextInt(NUM_DOCS), r);
    }

    // uncached versions of all queries
    Query[] noCacheQueries =
        Arrays.stream(queries).map(TestSegAwareCachingParity::noCache).toArray(Query[]::new);

    int nQueryThreads = 2;
    ExecutorService exec =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nQueryThreads + 1, new SolrNamedThreadFactory("sustainedIndexAndQuery"));
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec)) {
      AtomicBoolean exit = new AtomicBoolean();
      @SuppressWarnings({"unchecked", "rawtypes"})
      Future<Integer>[] futures = new Future[nQueryThreads + 1];
      futures[0] = exec.submit(sustainedIndexing(new Random(r.nextLong()), exit));
      Set<Object> uniqueSearchers = ConcurrentHashMap.newKeySet();
      for (int i = 1; i <= nQueryThreads; i++) {
        futures[i] =
            exec.submit(
                sustainedQuerying(
                    new Random(r.nextLong()), exit, queries, noCacheQueries, uniqueSearchers));
      }
      int nSeconds = 20;
      for (int i = 1; i <= nSeconds && !exit.get(); i++) {
        Thread.sleep(1000);
        System.err.println("running for " + i + " seconds of " + nSeconds);
      }
      exit.set(true);
      System.err.println("indexed " + futures[0].get() + " docs");
      for (int i = 1; i <= nQueryThreads; i++) {
        System.err.println("query thread" + i + " ran " + futures[i].get() + " query tests");
      }
      System.err.println("unique new searchers: " + uniqueSearchers.size());
      Map<String, Object> filterCacheMetrics = coreToFilterCacheMetrics(h.getCore());
      System.err.println("filterCacheMetrics: " + filterCacheMetrics);
      Double partialRatioPerHit = (Double) filterCacheMetrics.get("cumulative_partialRatioPerHit");
      assertNotNull(partialRatioPerHit);
      assertTrue(partialRatioPerHit > 0.5);
      assertTrue("found " + partialRatioPerHit, partialRatioPerHit < 1.0);
      System.err.println("cumulative_partialRatioPerHit: " + partialRatioPerHit);
    }
  }
}
