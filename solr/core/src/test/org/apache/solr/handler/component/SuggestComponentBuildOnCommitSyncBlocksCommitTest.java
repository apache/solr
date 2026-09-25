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
 * Backward-compatibility counterpart to {@link SuggestComponentBuildOnCommitDoesNotBlockCommitTest}:
 * {@code buildOnCommitAsync} defaults to {@code false}, so a suggester configured with just {@code
 * buildOnCommit=true} (no opt-in) keeps the original, pre-fix behavior - commit() blocks for the
 * full duration of the rebuild.
 *
 * <p>This is intentional, not a bug: {@code buildOnCommitAsync} is opt-in so existing {@code
 * buildOnCommit} users - including tests elsewhere in this suite (e.g. {@code
 * SuggestComponentTest}) that query a suggester immediately after commit() and expect it to
 * already be built - keep that guarantee unless they explicitly ask for the new, non-blocking,
 * eventually-consistent behavior.
 */
public class SuggestComponentBuildOnCommitSyncBlocksCommitTest extends SolrTestCaseJ4 {

  private static final int SLOW_DICT_NUM_TERMS = 20;
  private static final long SLOW_DICT_SLEEP_MS = 100;
  private static final long EXPECTED_MIN_BUILD_MS = SLOW_DICT_NUM_TERMS * SLOW_DICT_SLEEP_MS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowDictNumTerms", String.valueOf(SLOW_DICT_NUM_TERMS));
    System.setProperty("solr.tests.slowDictSleepMs", String.valueOf(SLOW_DICT_SLEEP_MS));
    System.setProperty("solr.tests.suggestBuildOnCommit", "true");
    System.setProperty("solr.tests.suggestBuildOnCommitAsync", "false");
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
  public void testSyncBuildOnCommitStillBlocksTheCommittingThreadByDefault() {
    assertU(adoc("id", "1", "text", "hello world"));

    long startNanos = System.nanoTime();
    assertU(commit());
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(
        "commit() returned after only "
            + elapsedMs
            + "ms, but with buildOnCommitAsync=false (the default) it should still take at "
            + "least the "
            + EXPECTED_MIN_BUILD_MS
            + "ms the suggester's slow dictionary needs to build ("
            + SLOW_DICT_NUM_TERMS
            + " terms x "
            + SLOW_DICT_SLEEP_MS
            + "ms/term). If this is failing, the default (non-opt-in) behavior of buildOnCommit "
            + "changed - existing users relying on 'suggestions are fresh immediately after "
            + "commit' would be silently affected.",
        elapsedMs >= EXPECTED_MIN_BUILD_MS);
  }
}
