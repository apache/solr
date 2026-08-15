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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Control/baseline counterpart to {@link SuggestComponentBuildOnCommitBlocksCommitTest}: it uses
 * the exact same slow, I/O-bound dictionary ({@link SlowIOSimulatingDictionaryFactory}) and the
 * same number of terms/sleep-per-term, but with {@code buildOnCommit=false}.
 *
 * <p>With {@code buildOnCommit=false}, {@code SuggestComponent} never registers a {@code
 * newSearcherListener} for this suggester at all, so {@code commit()} is not slowed down by it.
 * This isolates the cause of the blocking seen in the other test to {@code buildOnCommit=true}
 * itself, rather than to the slow dictionary or shared test setup.
 */
public class SuggestComponentBuildOnCommitDisabledCommitStaysFastTest extends SolrTestCaseJ4 {

  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;
  private static final long SLOW_BUILD_DURATION_MS = SLOW_DICT_NUM_TERMS * SLOW_DICT_SLEEP_MS;

  // Generous ceiling for a commit that does *not* trigger the slow suggester build: comfortably
  // less than the ~2s the suggester build alone would take, but with plenty of slack for a slow
  // test machine so this doesn't flake.
  private static final long MAX_EXPECTED_COMMIT_MS = SLOW_BUILD_DURATION_MS / 2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowDictNumTerms", String.valueOf(SLOW_DICT_NUM_TERMS));
    System.setProperty("solr.tests.slowDictSleepMs", String.valueOf(SLOW_DICT_SLEEP_MS));
    System.setProperty("solr.tests.suggestBuildOnCommit", "false");
    initCore("solrconfig-suggest-buildoncommit-slow.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.tests.slowDictNumTerms");
    System.clearProperty("solr.tests.slowDictSleepMs");
    System.clearProperty("solr.tests.suggestBuildOnCommit");
  }

  @Test
  public void testCommitStaysFastWhenBuildOnCommitIsDisabled() {
    assertU(adoc("id", "1", "text", "hello world"));

    long startNanos = System.nanoTime();
    assertU(commit());
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(
        "commit() took "
            + elapsedMs
            + "ms, which is not comfortably faster than the "
            + SLOW_BUILD_DURATION_MS
            + "ms the suggester's slow dictionary alone would need to build ("
            + SLOW_DICT_NUM_TERMS
            + " terms x "
            + SLOW_DICT_SLEEP_MS
            + "ms/term). With buildOnCommit=false the suggester should never be rebuilt on "
            + "commit, so commit() should stay well under "
            + MAX_EXPECTED_COMMIT_MS
            + "ms regardless of how slow the dictionary is.",
        elapsedMs < MAX_EXPECTED_COMMIT_MS);
  }
}
