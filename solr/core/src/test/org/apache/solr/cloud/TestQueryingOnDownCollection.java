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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestQueryingOnDownCollection extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_NAME = "infected";

  private static final String USERNAME = "solr";
  private static final String PASSWORD = "solr";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .withSecurityJson(STD_CONF)
        .configure();
  }

  @Test
  /**
   * Assert that requests to "down collection", i.e. a collection which has all replicas in down state
   * (but are hosted on nodes that are live), fail fast and throw meaningful exceptions
   */

  // TODO: this is rarely flakey, fails at  Without the SOLR-13793 fix,
  public void testQueryToDownCollectionShouldFailFast() throws Exception {

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
        .setBasicAuthCredentials(USERNAME, PASSWORD)
        .process(cluster.getSolrClient());

    // Add some dummy documents
    UpdateRequest update = (UpdateRequest) new UpdateRequest().setBasicAuthCredentials(USERNAME, PASSWORD);
    for (int i = 0; i < 100; i++) {
      update.add("id", Integer.toString(i));
    }
    update.commit(cluster.getSolrClient(), COLLECTION_NAME);

    // Bring down replicas but keep nodes up. This could've been done by some combinations of collections API operations;
    // however, to make it faster, altering cluster state directly! ;-)
    downAllReplicas();
    waitForAllJettysToSeeReplicasDown();

    // assert all replicas are in down state
    List<Replica> replicas = getCollectionState(COLLECTION_NAME).getReplicas();
    for (Replica replica: replicas){
      assertEquals(replica.getState(), Replica.State.DOWN);
    }

    // assert all nodes as active
    assertEquals(3, cluster.getSolrClient().getClusterStateProvider().getLiveNodes().size());

    try (SolrClient client = cluster.getJettySolrRunner(0).newClient()) {

      SolrRequest req = new QueryRequest(new SolrQuery("*:*").setRows(0)).setBasicAuthCredentials(USERNAME, PASSWORD);

      // Without the SOLR-13793 fix, this causes requests to "down collection" to pile up (until the nodes run out
      // of serviceable threads and they crash, even for other collections hosted on the nodes).
      SolrException error = SolrTestCaseUtil.expectThrows(SolrException.class, "Request should fail after trying all replica nodes once", () -> client.request(req, COLLECTION_NAME));

      assertEquals(503, error.code());
    }

    // run same set of tests on v2 client which uses V2HttpCall
    try (Http2SolrClient v2Client = new Http2SolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl())
        .build()) {
      SolrRequest req = new QueryRequest(new SolrQuery("*:*").setRows(0)).setBasicAuthCredentials(USERNAME, PASSWORD);
      SolrException error = SolrTestCaseUtil.expectThrows(SolrException.class, "Request should fail after trying all replica nodes once", () -> v2Client.request(req, COLLECTION_NAME));

      assertEquals(503, error.code());
    }
  }

  private void waitForAllJettysToSeeReplicasDown() throws Exception {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      jetty
          .getCoreContainer()
          .getZkController()
          .getZkStateReader()
          .waitForState(COLLECTION_NAME, 10, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
            if (collectionState == null) {
              return false;
            }
            for (Slice slice : collectionState) {
              for (Replica replica : slice.getReplicas()) {
                if (replica.getState() != Replica.State.DOWN) {
                  return false;
                }
              }
            }
            return true;
          });
    }
  }

  /** Always-elected fence so the test can force-publish DOWN deltas without being fenced out. */
  private static class AlwaysElectedFence implements StatePlaneWriter.ElectionFence {
    private final String id;
    AlwaysElectedFence(String id) { this.id = id; }
    public boolean stillElected() { return true; }
    public String writerId() { return id; }
    public boolean isFencedBy(String ringWriterId) { return false; }
  }

  private void downAllReplicas() throws Exception {
    DocCollection liveCollection = cluster.getSolrClient().getZkStateReader().getCollection(COLLECTION_NAME);

    byte[] collectionState = cluster.getZkClient().getData("/collections/" + COLLECTION_NAME + "/state.json",
        null, null);

    Map<String,Map<String,?>> infectedState = (Map<String,Map<String,?>>) Utils.fromJSON(collectionState);
    Map<String, Object> shards = (Map<String, Object>) infectedState.get(COLLECTION_NAME).get("shards");
    for(Map.Entry<String, Object> shard: shards.entrySet()) {
      Map<String, Object> replicas = (Map<String, Object>) ((Map<String, Object>) shard.getValue() ).get("replicas");
      for (Map.Entry<String, Object> replica : replicas.entrySet()) {
        ((Map<String, Object>) replica.getValue()).put("state", Replica.State.DOWN.toString());
      }
    }

    log.info("force down states");
    cluster.getZkClient().setData("/collections/" + COLLECTION_NAME + "/state.json", Utils.toJSON(infectedState)
        , true);
    // Drive DOWN through the per-shard delta plane (the only live-state representation): append a DOWN
    // delta for every replica of every shard, so readers observe all replicas DOWN.
    StatePlaneWriter forceDown =
        new StatePlaneWriter(cluster.getZkClient(), new AlwaysElectedFence("test-forcedown"));
    for (Slice slice : liveCollection.getSlices()) {
      List<StateDelta.Entry> downs = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        downs.add(new StateDelta.Entry(replica.getInternalId(), 5)); // 5 = DOWN
      }
      forceDown.publish(COLLECTION_NAME, slice.getName(), downs, Collections.emptyList());
    }
    cluster.getZkClient().setData("/collections/" + COLLECTION_NAME + "/" + ZkStateReader.STRUCTURE_CHANGE_NOTIFIER, (byte[]) null
        , true);

    cluster.getSolrClient().getZkStateReader().waitForState(COLLECTION_NAME, 10, TimeUnit.SECONDS, (l, c) -> {
      if (c == null) return false;
      for (Slice slice : c.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (replica.getState() != Replica.State.DOWN) {
            return false;
          }
        }
      }
      return true;
    });
  }

  protected static final String STD_CONF = "{\n" +
      "  \"authentication\":{\n" +
      "   \"blockUnknown\": true,\n" +
      "   \"class\":\"solr.BasicAuthPlugin\",\n" +
      "   \"credentials\":{\"solr\":\"EEKn7ywYk5jY8vG9TyqlG2jvYuvh1Q7kCCor6Hqm320= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw=\"}\n" +
      "  },\n" +
      "  \"authorization\":{\n" +
      "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n" +
      "   \"permissions\":[\n" +
      " {\"name\":\"security-edit\", \"role\":\"admin\"},\n" +
      " {\"name\":\"collection-admin-edit\", \"role\":\"admin\"},\n" +
      " {\"name\":\"core-admin-edit\", \"role\":\"admin\"}\n" +
      "   ],\n" +
      "   \"user-role\":{\"solr\":\"admin\"}\n" +
      "  }\n" +
      "}";


}
