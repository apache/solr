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

import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.Test;

/**
 * Regression for review finding S1: the overseer leader is registered as an ephemeral CHILD node
 * under {@code /overseer/overseer_elect/leader/&lt;id&gt;}, whose name is a BARE id
 * ({@code replica.getInternalId()}), not a {@code -n_&lt;seq&gt;} election-queue node.
 * {@code getLeaderId} had been changed to call {@code LeaderElector.sortSeqs(children)}, whose
 * {@code getSeq} throws {@code IllegalStateException} on a non-sequence name — so every
 * {@code getLeaderId}/{@code getLeaderNode}/OVERSEERSTATUS call while a leader exists threw and
 * surfaced as a 500. {@code getLeaderId} must return the id without sorting by {@code getSeq}.
 */
public class OverseerTaskProcessorGetLeaderIdTest extends SolrTestCaseJ4 {

  private static final String LEADER_PATH = Overseer.OVERSEER_ELECT + "/leader";

  @Test
  public void testGetLeaderIdReturnsBareIdChild() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run(true);
    try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT).start()) {
      assertNull("no leader node yet", OverseerTaskProcessor.getLeaderId(zkClient));

      // The leader registration node name is a bare id, not a -n_ sequence node.
      zkClient.makePath(LEADER_PATH, false, true);
      zkClient.mkdir(LEADER_PATH + "/54065");

      // Before the fix this threw IllegalStateException via sortSeqs->getSeq("54065").
      assertEquals("54065", OverseerTaskProcessor.getLeaderId(zkClient));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testGetLeaderIdPicksNewestDuringHandoff() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run(true);
    try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT).start()) {
      zkClient.makePath(LEADER_PATH, false, true);
      // Two transient children during a handoff; the departing leader registered first (lower czxid),
      // the incoming leader second (higher czxid). The newest is the current leader.
      zkClient.mkdir(LEADER_PATH + "/111");
      zkClient.mkdir(LEADER_PATH + "/222");

      assertEquals("222", OverseerTaskProcessor.getLeaderId(zkClient));
    } finally {
      server.shutdown();
    }
  }
}
