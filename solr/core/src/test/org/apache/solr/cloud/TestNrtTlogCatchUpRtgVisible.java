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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Positive regression test for <b>Option B: real NRT tlog replication</b>. The companion test
 * {@link TestRecoveryUncommittedDocNotExposed} guards the <em>no-leak</em> invariant (the leader's
 * still-uncommitted doc must not become {@code q=*:*}-visible on a recovering follower). This test
 * guards the <em>catch-up</em> invariant: that same down-window doc <b>must</b> be replicated to the
 * recovering NRT follower and become <b>RTG-visible</b> ({@code /get}) there -- it just must not be
 * promoted into a durable commit point or exposed to ordinary search, exactly mirroring the leader.
 *
 * <p>Mechanism exercised: a doc added to the leader after its last commit lives only in the leader's
 * open tlog + IndexWriter RAM, not in any commit point the follower replicates. Option B has the
 * recovering follower fetch the leader's tlog files and replay them (ahead of its own buffer) so the
 * down-window updates land in the follower's IndexWriter + live tlog and become RTG-visible, while a
 * soft commit with {@code openSearcher=false} keeps them out of {@code q=*:*}.
 *
 * <p>Setup mirrors {@link TestRecoveryUncommittedDocNotExposed}: index the committed docs and the one
 * uncommitted doc straight to the <b>leader core</b> with {@code distrib=false} so the follower stays
 * genuinely empty, which deterministically forces full-index replication recovery (PeerSync has no
 * frame of reference for an empty follower against a non-empty leader).
 */
@LuceneTestCase.Nightly
public class TestNrtTlogCatchUpRtgVisible extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "nrtTlogCatchUp";
  private static final int COMMITTED_DOCS = 5;
  private static final String UNCOMMITTED_ID = "9999";

  @Before
  public void setupCluster() throws Exception {
    useFactory(null);
    // autoCommit / autoSoftCommit disabled so the uncommitted doc stays uncommitted unless recovery
    // (correctly, via RTG) catches it up.
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
  public void testRecoveringFollowerCatchesUpLeaderUncommittedDocViaRtg() throws Exception {
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

    assertEquals(0, distribFalseCount(leader));
    assertEquals(0, distribFalseCount(follower));

    // 1) Index COMMITTED_DOCS straight to the LEADER core (distrib=false) and commit on the leader
    //    only; then add one UNCOMMITTED doc to the leader (distrib=false, no commit). The follower
    //    stays empty -> deterministic full-index replication recovery below.
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

      UpdateRequest uncommitted = new UpdateRequest();
      uncommitted.add(new SolrInputDocument("id", UNCOMMITTED_ID, "val_s", "uncommitted"));
      uncommitted.setParam("distrib", "false");
      uncommitted.process(leaderClient);
      // intentionally NO commit
    }

    // Baseline on the leader: the uncommitted doc is RTG-visible but not *:*-visible.
    assertEquals("leader must not expose its own uncommitted doc to *:*",
        COMMITTED_DOCS, distribFalseCount(leader));
    assertTrue("leader must serve its own uncommitted doc via RTG", rtgHasDoc(leader, UNCOMMITTED_ID));
    assertEquals("follower should still be empty before recovery", 0, distribFalseCount(follower));

    // 2) Force the follower through recovery (-> full-index replication + NRT tlog catch-up).
    JettySolrRunner followerJetty = jettyForReplica(follower);
    try (Http2SolrClient followerClient =
             SolrTestCaseJ4.getHttpSolrClient(followerJetty.getBaseUrl().toString())) {
      CoreAdminRequest.RequestRecovery req = new CoreAdminRequest.RequestRecovery();
      req.setCoreName(follower.getName());
      followerClient.request(req);
    }

    final String followerName = follower.getName();
    waitFor("follower did not return to ACTIVE after recovery", () -> {
      Replica r = getCollectionState(COLLECTION).getSlice("s1").getReplica(followerName);
      return r != null && r.getState() == Replica.State.ACTIVE;
    });
    // wait until the replicated commit point is visible to *:* on the follower
    waitFor("follower index did not advance past empty after recovery", () -> {
      Replica r = getCollectionState(COLLECTION).getSlice("s1").getReplica(followerName);
      return r != null && r.getState() == Replica.State.ACTIVE && distribFalseCount(r) >= COMMITTED_DOCS;
    });
    // wait until the NRT tlog catch-up has made the down-window doc RTG-visible on the follower
    waitFor("follower did not catch up the leader's uncommitted doc via RTG", () -> {
      Replica r = getCollectionState(COLLECTION).getSlice("s1").getReplica(followerName);
      return r != null && rtgHasDoc(r, UNCOMMITTED_ID);
    });

    Slice after = getCollectionState(COLLECTION).getSlice("s1");
    Replica followerAfter = after.getReplica(followerName);
    assertNotNull(followerAfter);

    // 3) Catch-up invariant: the down-window doc is RTG-visible on the follower (Option B replicated
    //    the leader's tlog), but the follower's *:* count still matches the committed count -- the
    //    doc is durable in the tlog + IndexWriter and RTG-visible, NOT promoted to a commit point and
    //    NOT exposed to ordinary search, identical to the leader.
    assertTrue("recovering follower failed to catch up the leader's down-window doc via RTG",
        rtgHasDoc(followerAfter, UNCOMMITTED_ID));
    assertEquals("recovering follower exposed the caught-up down-window doc to *:*",
        COMMITTED_DOCS, distribFalseCount(followerAfter));
  }

  /** {@code q=*:*} count against a single replica core ({@code distrib=false}). */
  private int distribFalseCount(Replica replica) throws Exception {
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
      QueryResponse rsp = new QueryRequest(params("q", "*:*", "distrib", "false", "rows", "0"))
          .process(client);
      return (int) rsp.getResults().getNumFound();
    }
  }

  /** Realtime-get ({@code /get}) for a single id against a single replica core ({@code distrib=false}). */
  private boolean rtgHasDoc(Replica replica, String id) throws Exception {
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
      ModifiableSolrParams p = new ModifiableSolrParams();
      p.set("id", id);
      p.set("distrib", "false");
      QueryRequest req = new QueryRequest(p);
      req.setPath("/get");
      NamedList<Object> resp = client.request(req);
      return resp != null && resp.get("doc") != null;
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
