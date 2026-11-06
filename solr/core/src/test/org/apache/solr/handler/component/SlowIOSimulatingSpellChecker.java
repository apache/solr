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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.IndexBasedSpellChecker;

/**
 * Test-only spellchecker that stands in for a build that's slow for reasons other than raw
 * dictionary I/O (e.g. an expensive distance-measure computation over a very large dictionary): it
 * sleeps a fixed amount of time before delegating to the real {@link IndexBasedSpellChecker} build.
 * The delay is deterministic and bounded, unlike a real slow build, so tests built on it stay fast
 * and reproducible.
 */
public class SlowIOSimulatingSpellChecker extends IndexBasedSpellChecker {
  public static final String SLEEP_MS_PARAM = "slowSpellCheckerSleepMs";

  private long sleepMs;

  // Test hook: counts down once the most recently started build() call completes. Reset at the
  // start of every build() call, so it reflects the most recently started build.
  //
  // Deliberately static rather than instance state, for the same reason as
  // SlowIOSimulatingDictionaryFactory.dictionaryFullyReadLatch (see its javadoc) - safe only as
  // long as at most one test using this class runs at a time per JVM.
  private static volatile CountDownLatch buildFinishedLatch = new CountDownLatch(1);

  /**
   * Blocks until the most recently started build() call has finished, or the timeout elapses. Used
   * by tests to detect that an asynchronous spellchecker build actually completed.
   */
  public static boolean awaitBuildFinished(long timeout, TimeUnit unit)
      throws InterruptedException {
    return buildFinishedLatch.await(timeout, unit);
  }

  @Override
  public String init(NamedList<?> config, SolrCore core) {
    sleepMs = Long.parseLong((String) config.get(SLEEP_MS_PARAM));
    return super.init(config, core);
  }

  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    CountDownLatch latch = new CountDownLatch(1);
    buildFinishedLatch = latch;
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    try {
      super.build(core, searcher);
    } finally {
      latch.countDown();
    }
  }
}
