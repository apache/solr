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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Reproduces a customer-reported production incident: a suggester configured with {@code
 * buildOnCommit=true} whose dictionary is slow to read (e.g. an I/O-throttled disk under heavy
 * concurrent indexing, as happens once Azure disk burst credits are exhausted) does not just make
 * indexing slower - it makes the commit call itself, and therefore the client waiting on it,
 * block for the full duration of the suggester rebuild with no timeout.
 *
 * <p>Root cause: {@code SuggestComponent.SuggesterListener.newSearcher()} runs {@code
 * suggester.build()} synchronously as a {@code newSearcherListener}, which {@code
 * SolrCore.getSearcher()} invokes on the core's single-threaded {@code searcherExecutor}.
 * Because {@code DirectUpdateHandler2.commit()} blocks the committing thread on {@code
 * waitSearcher[0].get()} (a no-arg, no-timeout {@code Future.get()}) whenever {@code
 * waitSearcher=true} (the default for a {@code commit=true} request), an arbitrarily slow
 * suggester build directly delays the commit response - it is not decoupled into the background.
 *
 * @see SuggestComponentBuildOnCommitDisabledCommitStaysFastTest the control/baseline
 *     counterpart, which uses the exact same slow dictionary but with buildOnCommit=false, and
 *     shows commit() staying fast - proving the slowdown here is specifically caused by
 *     buildOnCommit=true, not by the slow dictionary or test setup in general.
 */
public class SuggestComponentBuildOnCommitBlocksCommitTest extends SolrTestCaseJ4 {

  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;
  private static final long EXPECTED_MIN_BUILD_MS = SLOW_DICT_NUM_TERMS * SLOW_DICT_SLEEP_MS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowDictNumTerms", String.valueOf(SLOW_DICT_NUM_TERMS));
    System.setProperty("solr.tests.slowDictSleepMs", String.valueOf(SLOW_DICT_SLEEP_MS));
    initCore("solrconfig-suggest-buildoncommit-slow.xml", "schema.xml");
  }

  @Test
  public void testSlowBuildOnCommitBlocksTheCommittingThread() {
    // A prior commit already happened as part of core/searcher init (buildOnStartup=false, so it
    // didn't trigger a build); this add+commit is what opens the *next* new searcher, which is
    // what buildOnCommit reacts to.
    assertU(adoc("id", "1", "text", "hello world"));

    long startNanos = System.nanoTime();
    assertU(commit());
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(
        "commit() returned after only "
            + elapsedMs
            + "ms, but the configured suggester dictionary needs at least "
            + EXPECTED_MIN_BUILD_MS
            + "ms to build ("
            + SLOW_DICT_NUM_TERMS
            + " terms x "
            + SLOW_DICT_SLEEP_MS
            + "ms/term). Either the suggester didn't build on this commit, or "
            + "buildOnCommit's rebuild is no longer blocking the commit call synchronously - "
            + "if the latter is an intentional fix, please update this test.",
        elapsedMs >= EXPECTED_MIN_BUILD_MS);
  }
}
