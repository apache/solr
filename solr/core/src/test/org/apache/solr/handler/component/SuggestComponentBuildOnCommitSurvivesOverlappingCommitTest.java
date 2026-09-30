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
package org.apache.solr.handler.component;

import java.util.concurrent.TimeUnit;
import javax.xml.xpath.XPathConstants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.BaseTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Proves that an async {@code buildOnCommit} rebuild survives a second commit landing while it's
 * still running, instead of operating on a searcher that SolrCore has since torn down.
 *
 * <p>{@code buildOnCommitInProgress} coalescing only stops {@code SuggesterListener} from starting
 * a *second* async build while one is in flight - it does nothing to stop SolrCore's own
 * searcher-lifecycle bookkeeping from registering the second commit's newer searcher and decref'ing
 * (and, once that hits zero, closing - including caches and the directory reference) the first
 * commit's searcher, which the in-flight async build is still actively reading from. An earlier
 * version of this fix only pinned the searcher's raw {@code IndexReader} open ({@code
 * IndexReader.incRef()}), which does not prevent that: {@code SolrIndexSearcher.close()} decrefs a
 * *different* reader instance ({@code rawReader}, not the wrapped one {@code getIndexReader()}
 * returns) and tears down its own caches/directory reference regardless of the wrapped reader's
 * refcount. The fix instead takes a real reference through {@code SolrCore.getNewestSearcher()},
 * the same {@code RefCounted} mechanism every other consumer of a searcher uses to keep it alive
 * beyond a single synchronous callback.
 *
 * <p>This reproduces that overlap directly: commit once (starting a ~2s async build against
 * searcher #1), then immediately commit again (registering searcher #2, which decrefs searcher #1)
 * while the first build is still reading its slow dictionary. With the bug, {@code
 * suggester.build()} would throw partway through (surfacing as an {@code
 * AlreadyClosedException}/{@code IOException} from the closed searcher's directory or caches),
 * which {@code buildSuggesterIndex} swallows and logs at {@code ERROR} - so {@code
 * builtFromIndexVersion} would never advance past its initial {@code -1} for this suggester, since
 * this is its only build attempt. With the fix, the build completes and {@code
 * builtFromIndexVersion} catches up to whatever searcher #1's index version was - not searcher
 * #2's, since the coalescing correctly skipped rebuilding for the second commit.
 */
public class SuggestComponentBuildOnCommitSurvivesOverlappingCommitTest extends SolrTestCaseJ4 {

  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;
  private static final long SLOW_BUILD_DURATION_MS = SLOW_DICT_NUM_TERMS * SLOW_DICT_SLEEP_MS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowDictNumTerms", String.valueOf(SLOW_DICT_NUM_TERMS));
    System.setProperty("solr.tests.slowDictSleepMs", String.valueOf(SLOW_DICT_SLEEP_MS));
    System.setProperty("solr.tests.suggestBuildOnCommit", "true");
    System.setProperty("solr.tests.suggestBuildOnCommitAsync", "true");
    initCore("solrconfig-suggest-buildoncommit-slow.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.tests.slowDictNumTerms");
    System.clearProperty("solr.tests.slowDictSleepMs");
    System.clearProperty("solr.tests.suggestBuildOnCommit");
    System.clearProperty("solr.tests.suggestBuildOnCommitAsync");
  }

  @Test
  public void testAsyncBuildSurvivesASecondCommitLandingWhileItsStillRunning() throws Exception {
    assertU(adoc("id", "1", "text", "hello world"));
    assertU(commit());

    // searcher #1 is current now, and its async buildOnCommit rebuild just started in the
    // background - capture the index version it should eventually finish building from.
    long expectedBuiltFromVersion =
        extractLongField(
            h.query(
                req(
                    "qt",
                    "/suggest_slow",
                    "suggest.q",
                    "slowterm",
                    "suggest.dictionary",
                    "slowSuggester")),
            "currentIndexVersion");

    // The build against searcher #1 takes ~2s; this second commit lands well within that window,
    // registering searcher #2 - which is exactly the moment SolrCore decrefs (and, if nothing
    // else holds a reference, closes) searcher #1 out from under the still-running build if it
    // isn't properly pinned.
    assertU(adoc("id", "2", "text", "hello again"));
    assertU(commit());

    assertTrue(
        "the buildOnCommit rebuild that started after the first commit never finished reading "
            + "its dictionary within the expected window - it should have kept running in the "
            + "background regardless of the second commit.",
        SlowIOSimulatingDictionaryFactory.awaitDictionaryFullyRead(
            SLOW_BUILD_DURATION_MS * 10, TimeUnit.MILLISECONDS));

    // SolrSuggester.build() only sets builtFromIndexVersion once lookup.build() actually
    // succeeds, so poll for it to reach the version we captured above.
    long builtFrom = -1L;
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (builtFrom != expectedBuiltFromVersion && System.nanoTime() < deadlineNanos) {
      Thread.sleep(20);
      builtFrom =
          extractLongField(
              h.query(
                  req(
                      "qt",
                      "/suggest_slow",
                      "suggest.q",
                      "slowterm",
                      "suggest.dictionary",
                      "slowSuggester")),
              "builtFromIndexVersion");
    }
    assertEquals(
        "expected builtFromIndexVersion to catch up to the first commit's index version - if "
            + "it's stuck at -1, the async build silently failed, most likely because searcher #1 "
            + "was closed out from under it when the second commit registered searcher #2.",
        expectedBuiltFromVersion,
        builtFrom);
  }

  private static long extractLongField(String xml, String fieldName) throws Exception {
    String value =
        (String)
            BaseTestHarness.evaluateXPath(
                xml,
                "//lst[@name='suggesterIndexVersions']/lst[@name='slowSuggester']/long[@name='"
                    + fieldName
                    + "']/text()",
                XPathConstants.STRING);
    assertFalse("field " + fieldName + " not found in response: " + xml, value.isEmpty());
    return Long.parseLong(value);
  }
}
