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
package org.apache.solr.update;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * SOLR-7022, commit()-level wiring proof: a real commit with {@code waitSearcher} set must block in
 * the searcher wait at the end of {@link DirectUpdateHandler2#commit} (the {@code awaitSearcher}
 * call), and an interrupt of that wait must return from the commit with the interrupt status
 * restored instead of being swallowed. The unit-level behaviour of the helper itself is covered by
 * DirectUpdateHandler2AwaitSearcherTest.
 *
 * <p>The wait is made observable without touching production code: the searcher registration future
 * that a commit waits on is the last task submitted to the core's single-threaded searcher
 * executor, behind the new-searcher listener notifications. A listener that blocks on a latch
 * therefore keeps the future incomplete for exactly as long as the test needs.
 */
public class DirectUpdateHandler2CommitWaitTest extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule
        .newCollection()
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withSchemaFile("schema15.xml")
        .create();
  }

  /** Blocks the first new-searcher notification after it is armed, until released. */
  private static final class BlockingSearcherListener implements SolrEventListener {
    private final AtomicBoolean armed = new AtomicBoolean(false);
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    public void postCommit() {}

    @Override
    public void postSoftCommit() {}

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      if (!armed.compareAndSet(true, false)) {
        return;
      }
      entered.countDown();
      try {
        release.await(2, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  public void testCommitWaitsForSearcherAndRestoresInterruptStatus() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();

    SolrCore core = solrTestRule.getCoreContainer().getCore("collection1");
    try {
      BlockingSearcherListener listener = new BlockingSearcherListener();
      core.registerNewSearcherListener(listener);

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "7022");
      doc.addField("name", "commit wait wiring");
      client.add(doc);

      UpdateHandler handler = core.getUpdateHandler();
      assertTrue(handler instanceof DirectUpdateHandler2);

      SolrQueryRequest req = new SolrQueryRequestBase(core, new ModifiableSolrParams());
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
      cmd.openSearcher = true;
      cmd.waitSearcher = true;
      cmd.softCommit = false;

      AtomicReference<Throwable> commitError = new AtomicReference<>();
      AtomicBoolean interruptedAfterCommit = new AtomicBoolean(false);
      Thread committer =
          new Thread(
              () -> {
                try {
                  handler.commit(cmd);
                } catch (Throwable t) {
                  commitError.set(t);
                }
                interruptedAfterCommit.set(Thread.currentThread().isInterrupted());
              },
              "solr-7022-commit-wait");
      committer.setDaemon(true);

      listener.armed.set(true);
      committer.start();
      try {
        assertTrue(
            "new searcher listener was never entered", listener.entered.await(2, TimeUnit.MINUTES));
        // Registration is queued behind the blocked listener, so the commit thread must
        // still be inside commit(), parked in the searcher wait.
        committer.join(500);
        assertTrue("commit returned before the searcher was registered", committer.isAlive());

        committer.interrupt();
        committer.join(TimeUnit.MINUTES.toMillis(2));
        assertFalse("commit did not return after being interrupted", committer.isAlive());
      } finally {
        listener.release.countDown();
        req.close();
      }
      assertNull("commit threw: " + commitError.get(), commitError.get());
      assertTrue(
          "commit() must restore the interrupt status consumed while waiting for the searcher",
          interruptedAfterCommit.get());

      // With the listener released the searcher registers and the committed doc is visible.
      client.commit();
      assertEquals(1, client.query(new SolrQuery("id:7022")).getResults().getNumFound());
    } finally {
      core.close();
    }
  }
}
