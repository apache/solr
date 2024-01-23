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

import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestCloudDeleteByQuery extends SolrCloudTestCase {

  private static final int NUM_SHARDS = 2;
  private static final int REPLICATION_FACTOR = 2;
  private static final int NUM_SERVERS = 5;

  private static final String COLLECTION_NAME = "test_col";

  /** A collection specific client for operations at the cloud level */
  private static CloudSolrClient COLLECTION_CLIENT;

  /** A client for talking directly to the leader of shard1 */
  private static SolrClient S_ONE_LEADER_CLIENT;

  /** A client for talking directly to the leader of shard2 */
  private static SolrClient S_TWO_LEADER_CLIENT;

  /** A client for talking directly to a passive replica of shard1 */
  private static SolrClient S_ONE_NON_LEADER_CLIENT;

  /** A client for talking directly to a passive replica of shard2 */
  private static SolrClient S_TWO_NON_LEADER_CLIENT;

  /** A client for talking directly to a node that has no piece of the collection */
  private static SolrClient NO_COLLECTION_CLIENT;

  /** id field doc routing prefix for shard1 */
  private static final String S_ONE_PRE = "abc!";

  /** id field doc routing prefix for shard2 */
  private static final String S_TWO_PRE = "XYZ!";

  @AfterClass
  public static void afterClass() throws Exception {
    if (null != COLLECTION_CLIENT) {
      COLLECTION_CLIENT.close();
      COLLECTION_CLIENT = null;
    }
    if (null != S_ONE_LEADER_CLIENT) {
      S_ONE_LEADER_CLIENT.close();
      S_ONE_LEADER_CLIENT = null;
    }
    if (null != S_TWO_LEADER_CLIENT) {
      S_TWO_LEADER_CLIENT.close();
      S_TWO_LEADER_CLIENT = null;
    }
    if (null != S_ONE_NON_LEADER_CLIENT) {
      S_ONE_NON_LEADER_CLIENT.close();
      S_ONE_NON_LEADER_CLIENT = null;
    }
    if (null != S_TWO_NON_LEADER_CLIENT) {
      S_TWO_NON_LEADER_CLIENT.close();
      S_TWO_NON_LEADER_CLIENT = null;
    }
    if (null != NO_COLLECTION_CLIENT) {
      NO_COLLECTION_CLIENT.close();
      NO_COLLECTION_CLIENT = null;
    }
  }

  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {

    final String configName = "solrCloudCollectionConfig";
    final Path configDir = TEST_COLL1_CONF();

    configureCluster(NUM_SERVERS).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml");
    collectionProperties.put("schema", "schema15.xml"); // string id for doc routing prefix

    CollectionAdminRequest.createCollection(
            COLLECTION_NAME, configName, NUM_SHARDS, REPLICATION_FACTOR)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, NUM_SHARDS, REPLICATION_FACTOR * NUM_SHARDS);

    COLLECTION_CLIENT = cluster.getSolrClient(COLLECTION_NAME);

    ZkStateReader zkStateReader = cluster.getZkStateReader();

    // really hackish way to get a URL for specific nodes based on shard/replica hosting
    // inspired by TestMiniSolrCloudCluster
    HashMap<String, String> urlMap = new HashMap<>();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      URL jettyURL = jetty.getBaseUrl();
      String nodeKey =
          jettyURL.getHost() + ":" + jettyURL.getPort() + jettyURL.getPath().replace("/", "_");
      urlMap.put(nodeKey, jettyURL.toString());
    }
    ClusterState clusterState = zkStateReader.getClusterState();
    for (Slice slice : clusterState.getCollection(COLLECTION_NAME).getSlices()) {
      String shardName = slice.getName();
      Replica leader = slice.getLeader();
      assertNotNull("slice has null leader: " + slice, leader);
      assertNotNull("slice leader has null node name: " + slice, leader.getNodeName());
      String leaderUrl = urlMap.remove(leader.getNodeName());
      assertNotNull(
          "could not find URL for " + shardName + " leader: " + leader.getNodeName(), leaderUrl);
      assertEquals(
          "expected two total replicas for: " + slice.getName(), 2, slice.getReplicas().size());

      String passiveUrl = null;

      for (Replica replica : slice.getReplicas()) {
        if (!replica.equals(leader)) {
          passiveUrl = urlMap.remove(replica.getNodeName());
          assertNotNull(
              "could not find URL for " + shardName + " replica: " + replica.getNodeName(),
              passiveUrl);
        }
      }
      assertNotNull("could not find URL for " + shardName + " replica", passiveUrl);

      if (shardName.equals("shard1")) {
        S_ONE_LEADER_CLIENT = getHttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_ONE_NON_LEADER_CLIENT = getHttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else if (shardName.equals("shard2")) {
        S_TWO_LEADER_CLIENT = getHttpSolrClient(leaderUrl + "/" + COLLECTION_NAME + "/");
        S_TWO_NON_LEADER_CLIENT = getHttpSolrClient(passiveUrl + "/" + COLLECTION_NAME + "/");
      } else {
        fail("unexpected shard: " + shardName);
      }
    }
    assertEquals("Should be exactly one server left (not hosting either shard)", 1, urlMap.size());
    NO_COLLECTION_CLIENT =
        getHttpSolrClient(urlMap.values().iterator().next() + "/" + COLLECTION_NAME + "/");

    assertNotNull(S_ONE_LEADER_CLIENT);
    assertNotNull(S_TWO_LEADER_CLIENT);
    assertNotNull(S_ONE_NON_LEADER_CLIENT);
    assertNotNull(S_TWO_NON_LEADER_CLIENT);
    assertNotNull(NO_COLLECTION_CLIENT);

    // sanity check that our S_ONE_PRE & S_TWO_PRE really do map to shard1 & shard2 with default
    // routing
    assertEquals(
        0,
        COLLECTION_CLIENT
            .add(doc(f("id", S_ONE_PRE + random().nextInt()), f("expected_shard_s", "shard1")))
            .getStatus());
    assertEquals(
        0,
        COLLECTION_CLIENT
            .add(doc(f("id", S_TWO_PRE + random().nextInt()), f("expected_shard_s", "shard2")))
            .getStatus());
    assertEquals(0, COLLECTION_CLIENT.commit().getStatus());
    SolrDocumentList docs =
        COLLECTION_CLIENT
            .query(
                params(
                    "q", "*:*",
                    "fl", "id,expected_shard_s,[shard]"))
            .getResults();
    assertEquals(2, docs.getNumFound());
    assertEquals(2, docs.size());
    for (SolrDocument doc : docs) {
      String expected = doc.getFirstValue("expected_shard_s").toString();
      String docShard = doc.getFirstValue("[shard]").toString();
      assertTrue(
          "shard routing prefixes don't seem to be aligned anymore, "
              + "did someone change the default routing rules? "
              + "and/or the the default shard name rules? "
              + "and/or the numShards used by this test? ... "
              + expected
              + " is ot the same as [shard] == '"
              + docShard
              + "' ... for docId == "
              + doc.getFirstValue("id"),
          docShard.equals(expected));
    }
  }

  @Before
  public void clearCloudCollection() throws Exception {
    assertEquals(0, COLLECTION_CLIENT.deleteByQuery("*:*").getStatus());
    assertEquals(0, COLLECTION_CLIENT.commit().getStatus());
  }

  public void testMalformedDBQ(SolrClient client) {
    assertNotNull("client not initialized", client);
    SolrException e =
        expectThrows(
            SolrException.class,
            "Expected DBQ failure",
            () -> update(params()).deleteByQuery("foo_i:not_a_num").process(client));
    assertEquals("not the expected DBQ failure: " + e.getMessage(), 400, e.code());
  }

  //
  public void testMalformedDBQViaCloudClient() {
    testMalformedDBQ(COLLECTION_CLIENT);
  }

  public void testMalformedDBQViaShard1LeaderClient() {
    testMalformedDBQ(S_ONE_LEADER_CLIENT);
  }

  public void testMalformedDBQViaShard2LeaderClient() {
    testMalformedDBQ(S_TWO_LEADER_CLIENT);
  }

  public void testMalformedDBQViaShard1NonLeaderClient() {
    testMalformedDBQ(S_ONE_NON_LEADER_CLIENT);
  }

  public void testMalformedDBQViaShard2NonLeaderClient() {
    testMalformedDBQ(S_TWO_NON_LEADER_CLIENT);
  }

  public void testMalformedDBQViaNoCollectionClient() {
    testMalformedDBQ(NO_COLLECTION_CLIENT);
  }

  public static UpdateRequest update(SolrParams params, SolrInputDocument... docs) {
    UpdateRequest r = new UpdateRequest();
    r.setParams(new ModifiableSolrParams(params));
    r.add(Arrays.asList(docs));
    return r;
  }

  public static SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    for (SolrInputField f : fields) {
      doc.put(f.getName(), f);
    }
    return doc;
  }

  public static SolrInputField f(String fieldName, Object... values) {
    SolrInputField f = new SolrInputField(fieldName);
    f.setValue(values);
    return f;
  }
}
