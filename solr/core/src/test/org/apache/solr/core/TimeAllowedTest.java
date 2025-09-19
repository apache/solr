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
package org.apache.solr.core;

import static org.apache.solr.common.util.Utils.fromJSONString;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that checks that long-running queries are exited by Solr using the {@link
 * org.apache.solr.search.QueryLimits} implementation.
 */
public class TimeAllowedTest extends SolrTestCaseJ4 {

  static final int NUM_DOCS = 100;
  static final String assertionString = "/response/numFound==" + NUM_DOCS;
  static final String failureAssertionString = "/responseHeader/partialResults==true]";
  static final String longTimeout = "10000";
  static final String sleep = "2";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-delaying-component.xml", "schema_latest.xml");
    createIndex();
  }

  public static void createIndex() {
    for (int i = 0; i < NUM_DOCS; i++) {
      assertU(
          adoc(
              "id",
              Integer.toString(i),
              "name",
              "a" + i + " b" + i + " c" + i + " d" + i + " e" + i));
      if (random().nextInt(NUM_DOCS) == 0) {
        assertU(commit()); // sometimes make multiple segments
      }
    }
    assertU(commit());
  }

  @Test
  public void testPrefixQuery() throws Exception {
    String q = "name:a*";
    assertJQ(req("q", q, "timeAllowed", "1", "sleep", sleep), failureAssertionString);

    // do the same query and test for both success, and that the number of documents matched (i.e.
    // make sure no partial results were cached)
    assertJQ(req("q", q, "timeAllowed", longTimeout), assertionString);

    // this time we should get a query cache hit and hopefully no exception?  this may change in the
    // future if time checks are put into other places.

    // 2024-4-15: it did change..., and now this fails with 1 or 2 ms and passes with 3ms... I see
    // no way this won't be terribly brittle. Maybe TestInjection of some sort to bring this back?

    // assertJQ(req("q", q, "timeAllowed", "2", "sleep", sleep), assertionString);

    // The idea that the request won't time out due to caching is a flawed test methodology,
    // It relies on the test running quickly and not stalling. The above test should possibly
    // be doing something along the lines of this (but we lack api for it)
    //
    //    SolrCores solrCores = ExitableDirectoryReaderTest.h.getCoreContainer().solrCores;
    //    List<SolrCore> cores = solrCores.getCores();
    //    for (SolrCore core : cores) {
    //      if (<<< find the right core >>> ) {
    //        ((SolrCache)core.getSearcher().get().<<<check cache for a key like name:a* >>>
    //      }
    //    }

    // now do the same for the filter cache
    // 2024-4-15: this still passes probably because *:* is so fast, but it still worries me
    assertJQ(req("q", "*:*", "fq", q, "timeAllowed", "1", "sleep", sleep), failureAssertionString);

    // make sure that the result succeeds this time, and that a bad filter wasn't cached
    assertJQ(req("q", "*:*", "fq", q, "timeAllowed", longTimeout), assertionString);

    // test that Long.MAX_VALUE works
    assertJQ(req("q", "name:b*", "timeAllowed", Long.toString(Long.MAX_VALUE)), assertionString);

    // negative timeAllowed should disable timeouts.
    assertJQ(req("q", "name:c*", "timeAllowed", "-7"), assertionString);
  }

  // There are lots of assumptions about how/when cache entries should be changed in this method.
  // The simple case above shows the root problem without the confusion. testFilterSimpleCase should
  // be removed once it is running and this test should be un-ignored and the assumptions verified.
  // With all the weirdness, I'm not going to vouch for this test. Feel free to change it.
  @Test
  public void testCacheAssumptions() throws Exception {
    String fq = "name:d*";
    SolrCore core = h.getCore();

    CounterSnapshot.CounterDataPointSnapshot fqInsertsPre =
        SolrMetricTestUtils.getCacheSearcherOps(core, "filterCache", "inserts");
    long fqInserts = (long) fqInsertsPre.getValue();

    CounterSnapshot.CounterDataPointSnapshot qrInsertsPre =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "inserts");
    long qrInserts = (long) qrInsertsPre.getValue();

    // This gets 0 docs back. Use 10000 instead of 1 for timeAllowed, and it gets 100 back and the
    // for loop below succeeds.
    String response =
        JQ(req("q", "*:*", "fq", fq, "indent", "true", "timeAllowed", "1", "sleep", sleep));
    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    assertTrue("Should have fewer docs than " + NUM_DOCS, (long) (body.get("numFound")) < NUM_DOCS);

    Map<?, ?> header = (Map<?, ?>) (res.get("responseHeader"));
    assertTrue(
        "Should have partial results",
        (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY)));

    CounterSnapshot.CounterDataPointSnapshot qrInsertsPost =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "inserts");
    assertEquals(
        "Should NOT have inserted partial results in the cache!",
        (long) qrInsertsPost.getValue(),
        qrInserts);

    CounterSnapshot.CounterDataPointSnapshot fqInsertsPost =
        SolrMetricTestUtils.getCacheSearcherOps(core, "filterCache", "inserts");
    assertEquals("Should NOT have another insert", fqInserts, (long) fqInsertsPost.getValue());

    // At the end of all this, we should have no hits in the queryResultCache.
    response = JQ(req("q", "*:*", "fq", fq, "indent", "true", "timeAllowed", longTimeout));

    // Check that we did insert this one.
    CounterSnapshot.CounterDataPointSnapshot fqHitsPost =
        SolrMetricTestUtils.getCacheSearcherOps(core, "filterCache", "hits");
    assertEquals("Hits should still be 0", (long) fqHitsPost.getValue(), 0L);

    CounterSnapshot.CounterDataPointSnapshot fqInsertsPostSecond =
        SolrMetricTestUtils.getCacheSearcherOps(core, "filterCache", "inserts");
    assertEquals("Inserts should be bumped", (long) fqInsertsPostSecond.getValue(), fqInserts + 1);

    res = (Map<?, ?>) fromJSONString(response);
    body = (Map<?, ?>) (res.get("response"));

    assertEquals("Should have exactly " + NUM_DOCS, (long) (body.get("numFound")), NUM_DOCS);
    header = (Map<?, ?>) (res.get("responseHeader"));
    assertNull(
        "Should NOT have partial results",
        header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }

  // When looking at a problem raised on the user's list I ran across this anomaly with timeAllowed
  // This tests for the second query NOT returning partial results, along with some other
  @Test
  public void testQueryResults() throws Exception {
    String q = "name:e*";
    SolrCore core = h.getCore();

    CounterSnapshot.CounterDataPointSnapshot insertsPre =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "inserts");
    long inserts = (long) insertsPre.getValue();

    CounterSnapshot.CounterDataPointSnapshot hitsPre =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "hits");
    long hits = (long) hitsPre.getValue();

    String response = JQ(req("q", q, "indent", "true", "timeAllowed", "1", "sleep", sleep));

    // The queryResultCache should NOT get an entry here.
    CounterSnapshot.CounterDataPointSnapshot insertsPost =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "inserts");
    assertEquals(
        "Should NOT have inserted partial results!", inserts, (long) insertsPost.getValue());

    Map<?, ?> res = (Map<?, ?>) fromJSONString(response);
    Map<?, ?> body = (Map<?, ?>) (res.get("response"));
    Map<?, ?> header = (Map<?, ?>) (res.get("responseHeader"));

    assertTrue("Should have fewer docs than " + NUM_DOCS, (long) (body.get("numFound")) < NUM_DOCS);
    assertTrue(
        "Should have partial results",
        (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY)));

    response = JQ(req("q", q, "indent", "true", "timeAllowed", longTimeout));

    // Check that we did insert this one.
    CounterSnapshot.CounterDataPointSnapshot hitsPost =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "hits");
    CounterSnapshot.CounterDataPointSnapshot insertsPost2 =
        SolrMetricTestUtils.getCacheSearcherOps(core, "queryResultCache", "inserts");
    assertEquals("Hits should still be 0", hits, (long) hitsPost.getValue());
    assertTrue("Inserts should be bumped", inserts < (long) insertsPost2.getValue());

    res = (Map<?, ?>) fromJSONString(response);
    body = (Map<?, ?>) (res.get("response"));
    header = (Map<?, ?>) (res.get("responseHeader"));

    assertEquals("Should have exactly " + NUM_DOCS, NUM_DOCS, (long) (body.get("numFound")));
    Boolean test = (Boolean) (header.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    if (test != null) {
      assertFalse("Should NOT have partial results", test);
    }
  }
}
