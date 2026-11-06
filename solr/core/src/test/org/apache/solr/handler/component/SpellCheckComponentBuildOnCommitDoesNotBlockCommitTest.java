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
 * SpellCheckComponent counterpart to {@link SuggestComponentBuildOnCommitDoesNotBlockCommitTest}:
 * proves that {@code buildOnCommitAsync=true} keeps a slow spellchecker build from blocking
 * commit(), using the same {@link org.apache.solr.core.AsyncPostCommitRebuilder} that
 * SuggestComponent uses.
 *
 * @see SpellCheckComponentBuildOnCommitSyncBlocksCommitTest the counterpart showing that without
 *     opting in (buildOnCommitAsync=false, the default), the old blocking behavior is unchanged.
 */
public class SpellCheckComponentBuildOnCommitDoesNotBlockCommitTest extends SolrTestCaseJ4 {

  private static final long SLOW_BUILD_SLEEP_MS = 2000;

  // Generous ceiling for commit() itself: comfortably less than the build's sleep, but with
  // plenty of slack for a slow test machine so this doesn't flake.
  private static final long MAX_EXPECTED_COMMIT_MS = SLOW_BUILD_SLEEP_MS / 2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowSpellCheckerSleepMs", String.valueOf(SLOW_BUILD_SLEEP_MS));
    System.setProperty("solr.tests.spellcheckBuildOnCommit", "true");
    System.setProperty("solr.tests.spellcheckBuildOnCommitAsync", "true");
    initCore("solrconfig-spellcheck-buildoncommit-slow.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.tests.slowSpellCheckerSleepMs");
    System.clearProperty("solr.tests.spellcheckBuildOnCommit");
    System.clearProperty("solr.tests.spellcheckBuildOnCommitAsync");
  }

  @Test
  public void testCommitReturnsFastAndSpellCheckerBuildsAsynchronously() throws Exception {
    assertU(adoc("id", "1", "text", "hello world"));

    long startNanos = System.nanoTime();
    assertU(commit());
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(
        "commit() took "
            + elapsedMs
            + "ms; it should return well before the "
            + SLOW_BUILD_SLEEP_MS
            + "ms the spellchecker's slow build sleeps for, since buildOnCommit's rebuild is"
            + " expected to run asynchronously - if it's blocking again, this fix regressed.",
        elapsedMs < MAX_EXPECTED_COMMIT_MS);

    assertTrue(
        "the async buildOnCommit spellchecker build never finished within the expected window -"
            + " commit() no longer blocks, but the spellchecker should still eventually get"
            + " rebuilt.",
        SlowIOSimulatingSpellChecker.awaitBuildFinished(
            SLOW_BUILD_SLEEP_MS * 10, TimeUnit.MILLISECONDS));
  }
}
