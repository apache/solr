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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Proves the opt-in fix for a customer-reported production incident: a suggester configured with
 * {@code buildOnCommit=true} whose dictionary is slow to read (e.g. an I/O-throttled disk under
 * heavy concurrent indexing, as happens once Azure disk burst credits are exhausted) used to make
 * the commit call itself, and therefore the client waiting on it, block for the full duration of
 * the suggester rebuild with no timeout.
 *
 * <p>With {@code buildOnCommitAsync=true}, {@code SuggestComponent.SuggesterListener} runs {@code
 * buildOnCommit} rebuilds on a dedicated background executor instead of inline inside the {@code
 * newSearcherListener} callback, so {@code SolrCore}'s single-threaded {@code searcherExecutor} -
 * and therefore {@code DirectUpdateHandler2.commit()}'s {@code waitSearcher[0].get()} - is no
 * longer blocked by a slow rebuild. This test asserts both halves of that fix: {@code commit()}
 * returns quickly, and the suggester rebuild still actually happens (just a little after
 * commit(), instead of blocking it).
 *
 * @see SuggestComponentBuildOnCommitSyncBlocksCommitTest the counterpart showing that without
 *     opting in (buildOnCommitAsync=false, the default), the old blocking behavior is unchanged -
 *     this fix is opt-in specifically so existing buildOnCommit users aren't silently switched
 *     from "suggestions guaranteed fresh immediately after commit" to eventually-consistent.
 * @see SuggestComponentBuildOnCommitDisabledCommitStaysFastTest the control/baseline counterpart,
 *     which uses the exact same slow dictionary but with buildOnCommit=false, and shows
 *     commit() stays fast there too - because no rebuild is triggered at all in that case.
 */
public class SuggestComponentBuildOnCommitDoesNotBlockCommitTest extends SolrTestCaseJ4 {

  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;
  private static final long SLOW_BUILD_DURATION_MS = SLOW_DICT_NUM_TERMS * SLOW_DICT_SLEEP_MS;

  // Generous ceiling for commit() itself: comfortably less than the ~2s the suggester build
  // alone takes, but with plenty of slack for a slow test machine so this doesn't flake.
  private static final long MAX_EXPECTED_COMMIT_MS = SLOW_BUILD_DURATION_MS / 2;

  // Generous ceiling for the async build to finish *after* commit() returns.
  private static final long ASYNC_BUILD_TIMEOUT_MS = SLOW_BUILD_DURATION_MS * 10;

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
  public void testCommitReturnsFastAndSuggesterBuildsAsynchronously() throws Exception {
    assertU(adoc("id", "1", "text", "hello world"));

    long startNanos = System.nanoTime();
    assertU(commit());
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(
        "commit() took "
            + elapsedMs
            + "ms; it should return well before the "
            + SLOW_BUILD_DURATION_MS
            + "ms the suggester's slow dictionary takes to build ("
            + SLOW_DICT_NUM_TERMS
            + " terms x "
            + SLOW_DICT_SLEEP_MS
            + "ms/term), since buildOnCommit's rebuild is expected to run asynchronously - if "
            + "it's blocking again, this fix regressed.",
        elapsedMs < MAX_EXPECTED_COMMIT_MS);

    // Right after commit() returns, the async rebuild is still running in the background: a
    // caller can tell the suggestions it gets right now are stale, because the response's
    // builtFromIndexVersion (still -1: this suggester has never finished a build) is older than
    // currentIndexVersion (the index the commit we just did produced).
    String staleResponse =
        h.query(
            req(
                "qt", "/suggest_slow", "suggest.q", "slowterm", "suggest.dictionary",
                "slowSuggester"));
    long staleBuiltFrom = extractLongField(staleResponse, "builtFromIndexVersion");
    long indexVersionAtCommit = extractLongField(staleResponse, "currentIndexVersion");
    assertTrue(
        "expected builtFromIndexVersion ("
            + staleBuiltFrom
            + ") to be older than currentIndexVersion ("
            + indexVersionAtCommit
            + ") right after commit() returns, since the async rebuild triggered by that commit "
            + "should still be running",
        staleBuiltFrom < indexVersionAtCommit);

    assertTrue(
        "the async buildOnCommit suggester rebuild never finished reading its dictionary within "
            + ASYNC_BUILD_TIMEOUT_MS
            + "ms of commit() returning - commit() no longer blocks, but the suggester should "
            + "still eventually get rebuilt.",
        SlowIOSimulatingDictionaryFactory.awaitDictionaryFullyRead(
            ASYNC_BUILD_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    // SolrSuggester.build() does a little more work after the dictionary iterator is exhausted
    // (the latch above only tells us the last term was read), so poll briefly for
    // builtFromIndexVersion to catch up rather than asserting immediately.
    long freshBuiltFrom = staleBuiltFrom;
    long freshCurrent = indexVersionAtCommit;
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (freshBuiltFrom != freshCurrent && System.nanoTime() < deadlineNanos) {
      Thread.sleep(20);
      String freshResponse =
          h.query(
              req(
                  "qt", "/suggest_slow", "suggest.q", "slowterm", "suggest.dictionary",
                  "slowSuggester"));
      freshBuiltFrom = extractLongField(freshResponse, "builtFromIndexVersion");
      freshCurrent = extractLongField(freshResponse, "currentIndexVersion");
    }
    assertEquals(
        "expected builtFromIndexVersion to catch up to currentIndexVersion once the async "
            + "rebuild finished, so a caller can tell the suggestions are now fresh",
        freshCurrent, freshBuiltFrom);
  }

  private static long extractLongField(String xml, String fieldName) {
    Matcher m = Pattern.compile("<long name=\"" + fieldName + "\">(-?\\d+)</long>").matcher(xml);
    assertTrue("field " + fieldName + " not found in response: " + xml, m.find());
    return Long.parseLong(m.group(1));
  }
}
