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
package org.apache.solr.handler;

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test for SOLR-18225: {@code ReplicationHandler.getPermissionName()} now returns
 * {@code UPDATE_PERM} for state-changing commands including {@code command=fetchindex}, which
 * SolrCloud follower recovery invokes via {@code RecoveryStrategy → IndexFetcher.fetchLatestIndex}.
 *
 * <p><b>Risk this test guards.</b> An operator who locks down {@code security.json} could break
 * recovery if the gate switch now requires {@code update} on a path internode traffic previously
 * passed through with {@code read}. The standard operator setup grants {@code all} to the admin
 * user, and PKI internode traffic carries the calling principal forward. This test exercises that
 * happy path under auth: the standard admin/{@code all} configuration must still allow follower
 * recovery to complete.
 *
 * <p><b>How recovery is forced.</b> The follower is stopped, a meaningful number of doc updates are
 * committed on the leader (large enough to push past peer-sync's default threshold so {@code
 * command=fetchindex} is the recovery path actually exercised), and the follower is restarted. The
 * test asserts that the follower returns to {@code ACTIVE} and that a {@code distrib=false} query
 * to the recovered replica returns the full document set — proof that {@code command=fetchindex}
 * was authorized at the leader and the index transfer completed.
 */
public class ReplicationHandlerRecoveryAuthIntegrationTest extends SolrCloudAuthTestCase {

  private static final String COLLECTION = "recovery_auth_test";
  private static final String SHARD = "shard1";
  private static final String ADMIN_USER = "solr";
  private static final String ADMIN_PASS = "SolrRocks";

  // Same hash format and password ("SolrRocks") used by BasicAuthIntegrationTest.STD_CONF.
  private static final String SECURITY_JSON =
      "{"
          + "\"authentication\":{"
          + "  \"blockUnknown\":false,"
          + "  \"class\":\"solr.BasicAuthPlugin\","
          + "  \"credentials\":{\"solr\":\"orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= "
          + "Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw=\"}"
          + "},"
          + "\"authorization\":{"
          + "  \"class\":\"solr.RuleBasedAuthorizationPlugin\","
          + "  \"user-role\":{\"solr\":\"admin\"},"
          + "  \"permissions\":["
          + "    {\"name\":\"all\",\"role\":\"admin\"}"
          + "  ]"
          + "}"
          + "}";

  // 200 > peer-sync default ceiling; large enough to push recovery onto the fetchindex path
  // rather than peer-sync, which is the path SOLR-18225 changed the permission gate on.
  private static final int DOCS_BEFORE = 50;
  private static final int DOCS_DURING_DOWNTIME = 200;
  private static final int TOTAL_EXPECTED = DOCS_BEFORE + DOCS_DURING_DOWNTIME;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 2)
        .setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testFollowerRecoveryUnderAuth() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();

    // Index baseline before any node goes down, then commit so both replicas have a stable state.
    indexDocs(client, "init-", 0, DOCS_BEFORE);
    commit(client);

    // Identify leader and follower for the single shard.
    DocCollection coll = client.getClusterState().getCollection(COLLECTION);
    Slice slice = coll.getSlice(SHARD);
    Replica leader = slice.getLeader();
    Replica follower =
        slice.getReplicas().stream()
            .filter(r -> !r.getName().equals(leader.getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected a non-leader replica on " + SHARD));
    JettySolrRunner followerJetty = jettyFor(follower.getNodeName());

    // Stop the follower; index a substantial volume on the leader so recovery must do a real
    // fetch (not just peer-sync). command=fetchindex is the path SOLR-18225 changed.
    followerJetty.stop();
    cluster.waitForJettyToStop(followerJetty);
    indexDocs(client, "delta-", 0, DOCS_DURING_DOWNTIME);
    commit(client);

    // Restart — recovery is triggered. Under SOLR-18225, command=fetchindex now requires
    // UPDATE_PERM at the leader's authz layer; if PKI internode auth doesn't carry a principal
    // with update permission, recovery would 403 and the follower would never return to ACTIVE.
    followerJetty.start();
    cluster.waitForAllNodes(60);
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    // Wait for the recovered replica to report ACTIVE (not RECOVERING/DOWN).
    cluster
        .getZkStateReader()
        .waitForState(
            COLLECTION,
            60,
            TimeUnit.SECONDS,
            c -> {
              if (c == null) return false;
              Slice s = c.getSlice(SHARD);
              if (s == null) return false;
              for (Replica r : s.getReplicas()) {
                if (r.getState() != Replica.State.ACTIVE) return false;
              }
              return true;
            });

    // Cluster-level query confirms the collection is fully readable. CloudSolrClient does not
    // forward credentials by default, so wrap in a QueryRequest with explicit auth.
    SolrQuery q = new SolrQuery("*:*");
    QueryRequest cluReq = new QueryRequest(q);
    cluReq.setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS);
    QueryResponse cluResp = cluReq.process(client, COLLECTION);
    assertEquals(
        "cluster-level query must see all docs after recovery",
        TOTAL_EXPECTED,
        cluResp.getResults().getNumFound());

    // Direct distrib=false query against the *recovered follower* proves command=fetchindex
    // was authorized at the leader and the index transfer completed locally on this replica.
    String followerCoreUrl = followerJetty.getBaseUrl().toString() + "/" + follower.getCoreName();
    try (Http2SolrClient followerClient =
        new Http2SolrClient.Builder(followerCoreUrl)
            .withBasicAuthCredentials(ADMIN_USER, ADMIN_PASS)
            .build()) {
      SolrQuery local = new SolrQuery("*:*");
      local.set("distrib", "false");
      QueryResponse localResp = followerClient.query(local);
      assertEquals(
          "follower must have replicated every document via fetchindex; "
              + "if numFound < "
              + TOTAL_EXPECTED
              + ", command=fetchindex was likely denied at "
              + "the leader's authz layer (SOLR-18225 gate change requires UPDATE_PERM)",
          TOTAL_EXPECTED,
          localResp.getResults().getNumFound());
    }
  }

  private static void indexDocs(CloudSolrClient client, String prefix, int from, int count)
      throws Exception {
    UpdateRequest ur = new UpdateRequest();
    ur.setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS);
    for (int i = from; i < from + count; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", prefix + i);
      ur.add(doc);
    }
    ur.process(client, COLLECTION);
  }

  private static void commit(CloudSolrClient client) throws Exception {
    UpdateRequest commit = new UpdateRequest();
    commit.setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS);
    commit.commit(client, COLLECTION);
  }

  private static JettySolrRunner jettyFor(String nodeName) {
    for (JettySolrRunner j : cluster.getJettySolrRunners()) {
      if (nodeName.equals(j.getNodeName())) return j;
    }
    throw new AssertionError("no JettySolrRunner found for node " + nodeName);
  }
}
