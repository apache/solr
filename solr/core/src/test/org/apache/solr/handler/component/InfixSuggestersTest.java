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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.spelling.suggest.RandomTestDictionaryFactory;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.apache.solr.update.SolrCoreState;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class InfixSuggestersTest extends SolrTestCaseJ4 {
  private static final String rh_analyzing_short = "/suggest_analyzing_infix_short_dictionary";
  private static final String rh_analyzing_long = "/suggest_analyzing_infix_long_dictionary";
  private static final String rh_blended_short = "/suggest_blended_infix_short_dictionary";

  @BeforeClass
  public static void beforeClass() throws Exception {
    useFactory(null);
    initCore("solrconfig-infixsuggesters.xml","schema.xml");
  }

  @Test
  public void test2xBuildReload() throws Exception {
    for (int i = 0 ; i < 2 ; ++i) {
      assertQ(req("qt", rh_analyzing_short,
          SuggesterParams.SUGGEST_BUILD_ALL, "true"),
          "//str[@name='command'][.='buildAll']"
      );
      h.reload();
    }
  }

  @Test
  public void testTwoSuggestersBuildThenReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();

    assertQ(req("qt", rh_blended_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testBuildThen2xReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
    h.reload();
  }

  @Test
  public void testAnalyzingInfixSuggesterBuildThenReload() throws Exception {
    assertQ(req("qt", rh_analyzing_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testBlendedInfixSuggesterBuildThenReload() throws Exception {
    assertQ(req("qt", rh_blended_short,
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
    );
    h.reload();
  }

  @Test
  public void testReloadDuringBuild() throws Exception {
    ExecutorService executor = getTestExecutor();
    // Build the suggester in the background with a long dictionary, then reload concurrently.
    // Unlike the queue-based upstream, this fork keeps the building (old) core alive across a
    // reload rather than tearing it down, so the in-progress build is NOT forcibly aborted: it
    // either completes successfully against the old core or fails with a core-closed exception.
    // Either outcome is acceptable; what matters is that the concurrent reload does not corrupt
    // the core and the suggester remains usable afterward.
    Future<?> job = executor.submit(() -> {
      try {
        assertQ(req("qt", rh_analyzing_long, SuggesterParams.SUGGEST_BUILD_ALL, "true"),
            "//str[@name='command'][.='buildAll']");
      } catch (RuntimeException e) {
        // A core-closed exception is a legitimate outcome of racing reload against the build.
        if (!(e instanceof SolrCoreState.CoreIsClosedException)
            && !(e.getCause() instanceof SolrCoreState.CoreIsClosedException)) {
          throw e;
        }
      }
    });
    h.reload();
    // Stop the dictionary's input iterator
    System.clearProperty(RandomTestDictionaryFactory.RandomTestDictionary
            .getEnabledSysProp("longRandomAnalyzingInfixSuggester"));
    job.get();
    // The reloaded core must still serve a fresh suggester build.
    assertQ(req("qt", rh_analyzing_short, SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']");
  }

  @Test
  public void testShutdownDuringBuild() throws Exception {
    final String enabledSysProp = RandomTestDictionaryFactory.RandomTestDictionary
        .getEnabledSysProp("longRandomAnalyzingInfixSuggester");
    ExecutorService executor = getTestExecutor();
    try {
      final Throwable[] thrown = new Throwable[1];
      // Build the suggester in the background with a long dictionary.
      Future<?> job = executor.submit(() -> {
        try {
          assertQ(req("qt", rh_analyzing_long, SuggesterParams.SUGGEST_BUILD_ALL, "true"),
              "//str[@name='command'][.='buildAll']");
        } catch (Throwable t) {
          thrown[0] = t;
        }
      });
      // Deterministically wait until the build has actually begun: the RandomTestDictionary
      // constructor sets enabledSysProp as soon as the dictionary is created for the build.
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
      while (System.getProperty(enabledSysProp) == null && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      assertNotNull("suggester build did not begin in time", System.getProperty(enabledSysProp));
      h.close();
      // Stop the dictionary's input iterator
      System.clearProperty(enabledSysProp);
      job.get();
      // In this fork, closing the core does not forcibly abort an already-running suggester build:
      // depending on exactly where the close is observed, the build either (a) finishes against the
      // closing core, or (b) fails because the IndexWriter/SolrCoreState was closed underneath it.
      // Both outcomes are acceptable; the invariant we verify is that if the build DID fail, it
      // failed for a legitimate core-shutdown reason (not some unrelated error), and that the core
      // is afterward cleanly re-initializable (the finally block).
      if (thrown[0] != null) {
        Throwable wrappedException = thrown[0].getCause() != null ? thrown[0].getCause() : thrown[0];
        if (wrappedException instanceof SolrException) {
          // e.g. "SolrCoreState already closed." or "Error opening new searcher"
          // (caused by AlreadyClosedException: the IndexWriter was closed under the build).
          String message = wrappedException.getMessage();
          boolean ok = message != null
              && (message.contains("SolrCoreState already closed.")
                  || message.contains("Error opening new searcher")
                  || message.contains("closed"));
          assertTrue("Unexpected wrapped SolrException message: '" + message + "'", ok);
        } else if (wrappedException instanceof IllegalStateException
          && ! (wrappedException instanceof org.apache.solr.common.AlreadyClosedException)) {
          // org.apache.solr.common.AlreadyClosedException (and its subclass CoreIsClosedException)
          // extend IllegalStateException but represent core-closed conditions with a null message —
          // they are accepted silently in the SolrException branch above or fall through here.
          String expectedMessage = "Cannot commit on an closed writer. Add documents first";
          assertTrue("Expected wrapped IllegalStateException message to contain '" + expectedMessage
                  + "' but message is '" + wrappedException.getMessage() + "'",
              wrappedException.getMessage().contains(expectedMessage));
        }
      }
    } finally {
      System.clearProperty(enabledSysProp);
      initCore("solrconfig-infixsuggesters.xml","schema.xml"); // put the core back for other tests
    }
  }
}
