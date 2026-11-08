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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Auto commits are issued by the {@link CommitTracker} without any client request. They must still
 * pass through the core's default update processing chain, like client requested commits do
 * (SOLR-5941), so that update processors can see, record, or veto them. The commit itself stays
 * local to the core: it is marked as an end point and is never distributed.
 *
 * <p>The default chain of the test core (see solrconfig-autocommit-chain.xml) starts with {@link
 * RecordingCommitProcessorFactory}, which records every commit it is asked to process.
 */
public class AutoCommitUpdateChainTest extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule
        .newCollection()
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withConfigFile("solrconfig-autocommit-chain.xml")
        .create();
  }

  @Before
  public void resetState() {
    RecordingCommitProcessorFactory.reset();
    // make sure no auto commit can fire unless a test enables it
    hardCommitTracker().setTimeUpperBound(-1);
  }

  @Test
  public void testClientCommitPassesThroughDefaultChain() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(new SolrInputDocument("id", "594101", "text", "hello"));
    client.commit();

    assertEquals(1, RecordingCommitProcessorFactory.commitsSeen.get());
    // a client commit is neither marked as an auto commit nor as an end point
    assertEquals(0, RecordingCommitProcessorFactory.autoCommitsSeen.get());
    assertEquals(0, RecordingCommitProcessorFactory.endPointCommitsSeen.get());
  }

  @Test
  public void testAutoCommitPassesThroughDefaultChain() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    CommitTracker tracker = hardCommitTracker();
    tracker.setTimeUpperBound(250);
    int commitsBefore = tracker.getCommitCount();

    client.add(new SolrInputDocument("id", "594102", "text", "hello"));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (tracker.getCommitCount() == commitsBefore && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertTrue("auto commit did not happen", tracker.getCommitCount() > commitsBefore);

    // the auto commit reached the processor in the default chain, marked as an auto commit
    // and as an end point (local only, never distributed)
    assertEquals(1, RecordingCommitProcessorFactory.commitsSeen.get());
    assertEquals(1, RecordingCommitProcessorFactory.autoCommitsSeen.get());
    assertEquals(1, RecordingCommitProcessorFactory.endPointCommitsSeen.get());

    // and the commit really happens: the document becomes searchable once the commit,
    // which runs on the commit scheduler thread, has opened a new searcher
    long numFound = -1;
    long queryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < queryDeadline) {
      numFound = client.query(new SolrQuery("id:594102")).getResults().getNumFound();
      if (numFound == 1) {
        break;
      }
      Thread.sleep(100);
    }
    assertEquals(1, numFound);
  }

  private CommitTracker hardCommitTracker() {
    SolrCore core =
        solrTestRule.getCoreContainer().getCore(SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME);
    try {
      return ((DirectUpdateHandler2) core.getUpdateHandler()).commitTracker;
    } finally {
      core.close();
    }
  }

  /** An update processor factory that records every commit passing through the chain. */
  public static class RecordingCommitProcessorFactory extends UpdateRequestProcessorFactory {
    static final AtomicInteger commitsSeen = new AtomicInteger();
    static final AtomicInteger autoCommitsSeen = new AtomicInteger();
    static final AtomicInteger endPointCommitsSeen = new AtomicInteger();

    static void reset() {
      commitsSeen.set(0);
      autoCommitsSeen.set(0);
      endPointCommitsSeen.set(0);
    }

    @Override
    public UpdateRequestProcessor getInstance(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new UpdateRequestProcessor(next) {
        @Override
        public void processCommit(CommitUpdateCommand cmd) throws IOException {
          commitsSeen.incrementAndGet();
          // literal key, matching CommitTracker.AUTOCOMMIT_CONTEXT_KEY, so this test also
          // compiles and runs against code that does not route auto commits through the chain
          if (Boolean.TRUE.equals(cmd.getReq().getContext().get("autocommit"))) {
            autoCommitsSeen.incrementAndGet();
          }
          if (cmd.getReq()
              .getParams()
              .getBool(DistributedUpdateProcessor.COMMIT_END_POINT, false)) {
            endPointCommitsSeen.incrementAndGet();
          }
          super.processCommit(cmd);
        }
      };
    }
  }
}
