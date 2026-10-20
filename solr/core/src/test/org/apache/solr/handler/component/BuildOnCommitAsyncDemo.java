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
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;

/**
 * Demonstrates the worst case this whole effort (SOLR-18348) is about, then the improvement:
 *
 * <ol>
 *   <li><strong>BEFORE</strong>: a suggester, a spellchecker, and filterCache autowarming are all
 *       configured to be slow (simulating a large dictionary, an expensive distance measure, and
 *       expensive cached filter queries, respectively) and all run synchronously, one after
 *       another, inline as part of opening the new searcher a commit triggers - so a single commit
 *       pays for all three back-to-back.
 *   <li><strong>AFTER</strong>: the same setup, but with {@code buildOnCommitAsync=true} for the
 *       suggester and spellchecker. Their rebuilds move to the background (via the shared {@link
 *       org.apache.solr.core.AsyncPostCommitRebuilder}) and commit() no longer waits for them -
 *       while still eventually completing, which this demo verifies before declaring success.
 * </ol>
 *
 * <p><strong>What's NOT fixed yet:</strong> filterCache autowarming has no async option today - it
 * runs inside {@code SolrCore.openNewSearcher()}/{@code SolrIndexSearcher.warm()}, before any
 * listener (suggester/spellchecker included) even fires, and currently gates searcher registration
 * itself. So the AFTER commit is still slower than it could be - its time is dominated entirely by
 * the still-synchronous cache autowarm. Making that async too is future work, not attempted here;
 * see the class comment on {@code AsyncPostCommitRebuilder} for why it's architecturally a bigger
 * change than reusing that class for caches would suggest.
 *
 * <p>Run standalone via {@code dev-docs/demos/build-on-commit-async-demo.sh}, or directly with
 * {@code ./gradlew :solr:core:test --tests
 * "org.apache.solr.handler.component.BuildOnCommitAsyncDemo" --tests-show-standard-streams}.
 */
public class BuildOnCommitAsyncDemo extends SolrTestCaseJ4 {

  // 20 * 100ms = 2000ms
  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;

  private static final long SLOW_SPELLCHECK_SLEEP_MS = 2000;

  // 4 * 500ms = 2000ms
  private static final int SLOW_CACHE_AUTOWARM_COUNT = 4;
  private static final long SLOW_CACHE_REGEN_SLEEP_MS = 500;

  private static final long AWAIT_TIMEOUT_MS = 20_000;

  @Test
  public void demo() throws Exception {
    banner("SOLR-18348: buildOnCommitAsync demo");
    println("Worst case: a suggester (~2s build), a spellchecker (~2s build), and filterCache");
    println(
        "autowarming (~2s: "
            + SLOW_CACHE_AUTOWARM_COUNT
            + " entries x "
            + SLOW_CACHE_REGEN_SLEEP_MS
            + "ms) are all slow and all synchronous by default.");
    println("");

    long beforeMs = runScenario("BEFORE (all synchronous, today's default)", false, false);
    long afterMs =
        runScenario(
            "AFTER  (suggester + spellchecker opt into buildOnCommitAsync=true)", true, true);

    banner("Summary");
    println(String.format("  BEFORE commit() took: %,6d ms", beforeMs));
    println(String.format("  AFTER  commit() took: %,6d ms", afterMs));
    println(
        String.format(
            "  Improvement: %,d ms (%.0f%%) - the remainder is filterCache autowarming, which"
                + " isn't async yet.",
            beforeMs - afterMs, 100.0 * (beforeMs - afterMs) / beforeMs));
    println("");

    assertTrue(
        "expected the AFTER commit (suggester+spellchecker async) to be meaningfully faster than"
            + " BEFORE (both synchronous) - if this fails, buildOnCommitAsync regressed",
        afterMs < beforeMs / 2);
  }

  private long runScenario(String label, boolean suggestAsync, boolean spellcheckAsync)
      throws Exception {
    banner(label);
    System.setProperty("solr.tests.slowDictNumTerms", String.valueOf(SLOW_DICT_NUM_TERMS));
    System.setProperty("solr.tests.slowDictSleepMs", String.valueOf(SLOW_DICT_SLEEP_MS));
    System.setProperty(
        "solr.tests.slowSpellCheckerSleepMs", String.valueOf(SLOW_SPELLCHECK_SLEEP_MS));
    System.setProperty(
        "solr.tests.slowCacheAutowarmCount", String.valueOf(SLOW_CACHE_AUTOWARM_COUNT));
    System.setProperty(
        SlowCacheRegenerator.SLEEP_MS_PROPERTY, String.valueOf(SLOW_CACHE_REGEN_SLEEP_MS));
    System.setProperty("solr.tests.suggestBuildOnCommitAsync", String.valueOf(suggestAsync));
    System.setProperty("solr.tests.spellcheckBuildOnCommitAsync", String.valueOf(spellcheckAsync));
    try {
      initCore("solrconfig-buildoncommit-async-demo.xml", "schema.xml");
      try {
        // initCore() itself already opens an implicit first searcher over the (still empty)
        // index - that consumes the suggester/spellchecker's "first searcher" event (fast: no
        // buildOnStartup, and spellcheck's first-searcher handling is just a reload(), not a
        // build()). So the very next commit is already the one that triggers buildOnCommit for
        // both - there's no "free" extra commit available first, which is exactly why we seed
        // filterCache *before* committing at all: these queries run against that implicit first
        // searcher, over the not-yet-committed (so far invisible, 0-hit) docs below. That's fine
        // for this purpose - what matters is that SLOW_CACHE_AUTOWARM_COUNT distinct filter
        // queries end up cached on the searcher that's about to be superseded, so the *next*
        // commit's autowarm has that many entries to carry forward.
        for (int i = 1; i <= SLOW_CACHE_AUTOWARM_COUNT; i++) {
          assertU(adoc("id", String.valueOf(i), "text", "hello world " + i));
          try (SolrQueryRequest req = req("q", "*:*", "fq", "id:" + i)) {
            h.query(req);
          }
        }
        println("  Seeded filterCache with " + SLOW_CACHE_AUTOWARM_COUNT + " entries.");

        println(
            "  Committing - this is the one that makes the docs visible, and triggers the"
                + " suggester rebuild, the spellchecker rebuild, and filterCache autowarm all at"
                + " once...");
        long startNanos = System.nanoTime();
        assertU(commit());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        println(String.format("  commit() returned after %,d ms.", elapsedMs));

        if (suggestAsync) {
          println("  Waiting for the async suggester rebuild to finish in the background...");
          assertTrue(
              "async suggester rebuild never finished",
              SlowIOSimulatingDictionaryFactory.awaitDictionaryFullyRead(
                  AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
          println("  ...suggester rebuild finished.");
        }
        if (spellcheckAsync) {
          println("  Waiting for the async spellchecker rebuild to finish in the background...");
          assertTrue(
              "async spellchecker rebuild never finished",
              SlowIOSimulatingSpellChecker.awaitBuildFinished(
                  AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));
          println("  ...spellchecker rebuild finished.");
        }
        println("");
        return elapsedMs;
      } finally {
        deleteCore();
      }
    } finally {
      System.clearProperty("solr.tests.slowDictNumTerms");
      System.clearProperty("solr.tests.slowDictSleepMs");
      System.clearProperty("solr.tests.slowSpellCheckerSleepMs");
      System.clearProperty("solr.tests.slowCacheAutowarmCount");
      System.clearProperty(SlowCacheRegenerator.SLEEP_MS_PROPERTY);
      System.clearProperty("solr.tests.suggestBuildOnCommitAsync");
      System.clearProperty("solr.tests.spellcheckBuildOnCommitAsync");
    }
  }

  private static void banner(String text) {
    println("");
    println("==== " + text + " " + "=".repeat(Math.max(0, 60 - text.length())));
  }

  private static void println(String text) {
    System.out.println("[DEMO] " + text);
  }
}
