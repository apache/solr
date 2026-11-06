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
 * Backward-compatibility counterpart to {@link
 * SpellCheckComponentBuildOnCommitDoesNotBlockCommitTest}: {@code buildOnCommitAsync} defaults to
 * {@code false}, so a spellchecker configured with just {@code buildOnCommit=true} (no opt-in)
 * keeps the original, pre-fix behavior - commit() blocks for the full duration of the build.
 */
public class SpellCheckComponentBuildOnCommitSyncBlocksCommitTest extends SolrTestCaseJ4 {

  private static final long SLOW_BUILD_SLEEP_MS = 2000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.slowSpellCheckerSleepMs", String.valueOf(SLOW_BUILD_SLEEP_MS));
    System.setProperty("solr.tests.spellcheckBuildOnCommit", "true");
    System.setProperty("solr.tests.spellcheckBuildOnCommitAsync", "false");
    initCore("solrconfig-spellcheck-buildoncommit-slow.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.tests.slowSpellCheckerSleepMs");
    System.clearProperty("solr.tests.spellcheckBuildOnCommit");
    System.clearProperty("solr.tests.spellcheckBuildOnCommitAsync");
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
            + "ms, but with buildOnCommitAsync=false (the default) it should still take at least"
            + " the "
            + SLOW_BUILD_SLEEP_MS
            + "ms the spellchecker's slow build sleeps for. If this is failing, the default"
            + " (non-opt-in) behavior of buildOnCommit changed - existing users relying on"
            + " 'spellcheck index is fresh immediately after commit' would be silently affected.",
        elapsedMs >= SLOW_BUILD_SLEEP_MS);
  }
}
