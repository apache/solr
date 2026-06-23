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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regression test for the load-dependent SolrCloud divergence where a follower that goes through
 * <b>replication</b> recovery exposes the leader's still-<b>uncommitted</b> documents to ordinary
 * search ({@code q=*:*}) while the leader itself still hides them.
 *
 * <p>Mechanism guarded: a recovering follower must NOT force a fresh hard commit on the leader before
 * fetching the index. {@code RecoveryStrategy.replicate()} used to call {@code commitOnLeader()} (a HARD
 * commit) before the index fetch. A hard commit fsyncs the leader's in-RAM, not-yet-committed docs into a
 * NEW durable commit point, which the follower then replicates and exposes via {@code q=*:*}. The fix
 * (RecoveryStrategy no longer issues a commit-on-leader) makes the follower replicate only the leader's
 * <em>existing</em> latest commit point; updates forwarded during the BUFFERING window are caught up by
 * the buffered-update replay, staying RTG-visible/tlog-durable but not {@code *:*}-visible until a real
 * commit, identically on leader and follower.
 *
 * <p>This is the deterministic, single-shard / two-NRT-replica analogue of the flake originally observed
 * under load in {@link TestCloudPseudoReturnFields} (its uncommitted {@code id=99}).
 *
 * <p><b>Why this deterministically exercises the replication path (not PeerSync):</b> recovery tries
 * PeerSync first, but for a follower that has never recovered, {@code recoveringAfterStartup} is true
 * (DefaultSolrCoreState) so {@code RecoveryStrategy} uses {@code ulog.getStartingVersions()} as its
 * version frame of reference, which is captured once at ulog init and is <em>empty</em> for a follower
 * that never received any updates. With an empty starting-version list and a non-empty leader,
 * {@code PeerSyncWithLeader.sync()} returns {@code failure(leaderHasData=true)} ("no frame of reference"),
 * and the only failure-to-success bypass requires the leader to <em>also</em> be empty. So PeerSync
 * structurally fails and recovery falls through to full-index replication every time. To keep the
 * follower genuinely empty we index the committed docs (and the uncommitted doc) straight to the
 * <b>leader core</b> with {@code distrib=false}, bypassing leader-to-follower forwarding.
 */
@LuceneTestCase.Nightly
public class TestRecoveryUncommittedDocNotExposed extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "uncommittedDocNotExposed";
  private static final int COMMITTED_DOCS = 5;
  private static final String UNCOMMITTED_ID = "9999";

  @Before
  public void setupCluster() throws Exception {
    useFactory(null);
    // autoCommit / autoSoftCommit disabled so the uncommitted doc stays uncommitted unless something
    // (the bug) promotes it.
    System.setProperty("solr.autoCommit.maxTime", "-1");
    System.setProperty("solr.autoSoftCommit.maxTime", "-1");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
    System.clearProperty("solr.autoCommit.maxTime");
    System.clearProperty("solr.autoSoftCommit.maxTime");
    System.clearProperty("solr.ulog.numRecordsToKeep");
  }

  @Test
  public void testRecoveringFollowerDoesNotExposeLeaderUncommittedDoc() throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    Slice slice = getCollectionState(COLLECTION).getSlice("s1");
    Replica leader = slice.getLeader();
    assertNotNull("no leader", leader);
    Replica follower = null;
    for (Replica r : slice.getReplicas()) {
      if (!r.getName().equals(leader.getName())) {
        follower = r;
        break;
      }
    }
    assertNotNull("no follower", follower);

    // Sanity: both replicas start empty.
    assertEquals(0, distribFalseCount(leader));
    assertEquals(0, distribFalseCount(follower));

    // 1) Index COMMITTED_DOCS straight to the LEADER core with distrib=false (do NOT forward to the
    //    follower) and commit them on the leader only. The follower stays genuinely empty, which is what
    //    forces the deterministic full-index replication recovery path below (PeerSync has no frame of
    //    reference for an empty follower against a non-empty leader -> structural failure).
    try (Http2SolrClient leaderClient = SolrTestCaseJ4.getHttpSolrClient(leader.getCoreUrl())) {
      List<SolrInputDocument> docs = new ArrayList<>(COMMITTED_DOCS);
      for (int i = 0; i < COMMITTED_DOCS; i++) {
        docs.add(new SolrInputDocument("id", String.valueOf(i), "val_s", "v" + i));
      }
      UpdateRequest add = new UpdateRequest();
      add.add(docs);
      add.setParam("distrib", "false");
      add.process(leaderClient);

      UpdateRequest commit = new UpdateRequest();
      commit.setParam("distrib", "false");
      commit.setAction(UpdateRequest.ACTION.COMMIT, true, true);
      commit.process(leaderClient);

      // 2) Add one UNCOMMITTED doc to the leader core (distrib=false, no commit). It lives in the leader's
      //    IndexWriter RAM only -> RTG-visible, not *:*-visible, and not in any durable commit point.
      UpdateRequest uncommitted = new UpdateRequest();
      uncommitted.add(new SolrInputDocument("id", UNCOMMITTED_ID, "val_s", "uncommitted"));
      uncommitted.setParam("distrib", "false");
      uncommitted.process(leaderClient);
      // intentionally NO commit
    }

    // leader committed-visible at COMMITTED_DOCS (its uncommitted doc is hidden from *:*); follower empty.
    assertEquals("leader must not expose its own uncommitted doc to *:*",
        COMMITTED_DOCS, distribFalseCount(leader));
    assertEquals("follower should still be empty before recovery", 0, distribFalseCount(follower));

    // 3) Force the follower through recovery. It will fall through to full-index replication. With the
    //    bug, recovery's commit-on-leader hard-commits the leader's uncommitted doc into a fresh commit
    //    point, the follower replicates it, and the follower exposes it via *:* (numFound==6). With the
    //    fix, the follower replicates only the leader's existing 5-doc commit point (numFound==5).
    JettySolrRunner followerJetty = jettyForReplica(follower);
    try (Http2SolrClient followerClient =
             SolrTestCaseJ4.getHttpSolrClient(followerJetty.getBaseUrl().toString())) {
      CoreAdminRequest.RequestRecovery req = new CoreAdminRequest.RequestRecovery();
      req.setCoreName(follower.getName());
      followerClient.request(req);
    }

    // wait until the follower has finished its recovery cycle and is back ACTIVE. We deliberately wait
    // on ACTIVE only (not on a doc count) so the doc-count assertions below run on the post-recovery
    // state and report a clear "expected 5 but was 6" if the uncommitted doc leaked.
    final String followerName = follower.getName();
    waitFor("follower did not return to ACTIVE after recovery", () -> {
      Replica r = getCollectionState(COLLECTION).getSlice("s1").getReplica(followerName);
      return r != null && r.getState() == Replica.State.ACTIVE;
    });
    // small settle so the replicated commit/searcher is visible to *:* before we assert
    waitFor("follower index did not advance past empty after recovery", () -> {
      Replica r = getCollectionState(COLLECTION).getSlice("s1").getReplica(followerName);
      return r != null && r.getState() == Replica.State.ACTIVE && distribFalseCount(r) >= COMMITTED_DOCS;
    });

    // re-resolve (leadership should not have moved here)
    Slice after = getCollectionState(COLLECTION).getSlice("s1");
    Replica leaderAfter = after.getLeader();
    Replica followerAfter = after.getReplica(followerName);
    assertNotNull(followerAfter);

    int leaderCount = distribFalseCount(leaderAfter);
    int followerCount = distribFalseCount(followerAfter);

    // 4) Core assertions: the uncommitted doc must NOT have become *:*-visible on the follower, and the
    //    follower must agree with the leader. (Path-agnostic invariant: a recovering follower must never
    //    expose the leader's uncommitted docs to ordinary search.)
    assertEquals("recovering follower exposed the leader's uncommitted doc to *:*",
        COMMITTED_DOCS, followerCount);
    assertEquals("leader exposed its own uncommitted doc to *:* after the follower recovered",
        COMMITTED_DOCS, leaderCount);
    assertEquals("follower and leader disagree on committed-doc count after recovery",
        leaderCount, followerCount);
  }

  /** {@code q=*:*} count against a single replica core ({@code distrib=false}). */
  private int distribFalseCount(Replica replica) throws Exception {
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
      QueryResponse rsp = new QueryRequest(params("q", "*:*", "distrib", "false", "rows", "0"))
          .process(client);
      return (int) rsp.getResults().getNumFound();
    }
  }

  private JettySolrRunner jettyForReplica(Replica replica) {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getNodeName() != null && jetty.getNodeName().equals(replica.getNodeName())) {
        return jetty;
      }
    }
    throw new AssertionError("no jetty for replica " + replica);
  }

  private interface Condition {
    boolean check() throws Exception;
  }

  private void waitFor(String message, Condition condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try {
        if (condition.check()) return;
        last = null;
      } catch (Exception e) {
        last = e;
      }
      Thread.sleep(250);
    }
    if (last != null) {
      throw new AssertionError(message, last);
    }
    fail(message);
  }
}
