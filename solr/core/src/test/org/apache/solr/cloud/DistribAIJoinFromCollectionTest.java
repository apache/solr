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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.solr.search.join.AIJoinQParserPlugin} version of {@link
 * DistribJoinFromCollectionTest}: same collocated-replica {@code fromIndex} setup (a single-shard
 * "from" collection deployed onto every node holding a "to" replica, so {@link
 * org.apache.solr.search.join.ScoreJoinQParserPlugin#getCoreName}'s collocation lookup always finds
 * a local "from" replica), but adapted for two ways {@code {!aijoin}} differs from {@code {!join}}:
 *
 * <ul>
 *   <li>no scoring -- {@code {!aijoin}} is always a constant-score match, so there's no {@code
 *       score=} local param and no scoring assertions.
 *   <li>M:1 only -- {@link org.apache.solr.search.join.aijoin.AIJoinUtil#computeDocMapping} keeps
 *       exactly one to-doc per from-doc, so the join here always goes from the "from" collection's
 *       docs (each with a single-valued FK) to the "to" collection's docs by their unique key,
 *       never the reverse (one "to" doc resolving to many "from" docs would silently drop matches).
 * </ul>
 */
public class DistribAIJoinFromCollectionTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String toColl = "aijoin_to_2x2";
  private static final String fromColl = "aijoin_from_1x4";

  private static String toDocId;

  @BeforeClass
  public static void setupCluster() throws Exception {
    String configName = "aijoinCloudCollectionConfig";
    int nodeCount = 5;
    configureCluster(nodeCount).addConfig(configName, configset("aijoin")).configure();

    Map<String, String> collectionProperties = new HashMap<>();

    // create a collection holding data for the "to" (unique-key) side of the JOIN
    int shards = 2;
    int replicas = 2;
    CollectionAdminRequest.createCollection(toColl, configName, shards, replicas)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    // get the set of nodes where replicas for the "to" collection exist
    Set<String> nodeSet = new HashSet<>();
    ZkStateReader zkStateReader = cluster.getZkStateReader();
    ClusterState cs = zkStateReader.getClusterState();
    for (Slice slice : cs.getCollection(toColl).getActiveSlices())
      for (Replica replica : slice.getReplicas()) nodeSet.add(replica.getNodeName());
    assertTrue(nodeSet.size() > 0);

    // deploy the single-shard "from" collection to every node where the "to" collection exists,
    // so AIJoinQParserPlugin's collocation lookup always finds a local "from" replica -- and stays
    // on the single-shard fast path, which skips the router-field cross-checks that a sharded
    // "from" collection would otherwise require
    CollectionAdminRequest.createCollection(fromColl, configName, 1, 4)
        .setCreateNodeSet(String.join(",", nodeSet))
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    toDocId = indexToDoc(1001, "b");
    indexFromDoc(2001, toDocId, "c");
    indexFromDoc(2002, toDocId, "d"); // a second "from" doc mapping to the same "to" doc: still M:1

    Thread.sleep(1000); // so the commits fire
  }

  @AfterClass
  public static void shutdown() {
    log.info(
        "DistribAIJoinFromCollectionTest logic complete ... deleting the {} and {} collections",
        toColl,
        fromColl);

    // try to clean up
    for (String c : new String[] {toColl, fromColl}) {
      try {
        CollectionAdminRequest.Delete req = CollectionAdminRequest.deleteCollection(c);
        req.process(cluster.getSolrClient());
      } catch (Exception e) {
        // don't fail the test
        log.warn("Could not delete collection {} after test completed due to:", c, e);
      }
    }

    log.info("DistribAIJoinFromCollectionTest succeeded ... shutting down now!");
  }

  @Test
  public void testJoin() throws Exception {
    // verify the join with fromIndex works: two different "from" docs (match_s:c and match_s:d)
    // both resolve to the same single "to" doc, still M:1 since it's one to-doc per from-doc
    CloudSolrClient client = cluster.getSolrClient();
    {
      final String joinQ = "{!aijoin from=join_s fromIndex=" + fromColl + " to=id}match_s:c";
      QueryRequest qr =
          new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
      QueryResponse rsp = qr.process(client);
      SolrDocumentList hits = rsp.getResults();
      assertEquals("Expected 1 doc, got " + hits, 1, hits.getNumFound());
      SolrDocument doc = hits.get(0);
      assertEquals(toDocId, doc.getFirstValue("id"));
      assertEquals("b", doc.getFirstValue("get_s"));
      // {!aijoin} never scores -- always a constant-score match, like {!join score=none}
      assertEquals("1.0", doc.getFirstValue("score").toString());
    }

    // negative test before creating an alias
    checkAbsentFromIndex();

    // create an alias for the fromIndex and then query through the alias
    String alias = fromColl + "Alias";
    CollectionAdminRequest.createAlias(alias, fromColl).process(client);

    {
      final String joinQ = "{!aijoin from=join_s fromIndex=" + alias + " to=id}match_s:d";
      final QueryRequest qr =
          new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
      final QueryResponse rsp = qr.process(client);
      final SolrDocumentList hits = rsp.getResults();
      assertEquals("Expected 1 doc", 1, hits.getNumFound());
      SolrDocument doc = hits.get(0);
      assertEquals(toDocId, doc.getFirstValue("id"));
      assertEquals("b", doc.getFirstValue("get_s"));
    }

    // negative test after creating an alias
    checkAbsentFromIndex();

    {
      // verify join doesn't work if no match in the "from" index
      final String joinQ = "{!aijoin from=join_s fromIndex=" + fromColl + " to=id}match_s:nomatch";
      final QueryRequest qr =
          new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
      final QueryResponse rsp = qr.process(client);
      final SolrDocumentList hits = rsp.getResults();
      assertEquals("Expected no hits", 0, hits.getNumFound());
    }
  }

  private void checkAbsentFromIndex() {
    final String wrongName = fromColl + "WrongName";
    final String joinQ = "{!aijoin from=join_s fromIndex=" + wrongName + " to=id}match_s:c";
    final QueryRequest qr =
        new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "id,get_s,score"));
    RemoteSolrException ex =
        assertThrows(RemoteSolrException.class, () -> cluster.getSolrClient().request(qr));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains(wrongName));
  }

  /** Indexes a "to"-side doc: {@code id} is the join target, unique per doc (required for M:1). */
  private static String indexToDoc(int id, String getField) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(50);
    up.setParam("collection", toColl);
    SolrInputDocument doc = new SolrInputDocument();
    String docId = "" + id;
    doc.addField("id", docId);
    doc.addField("get_s", getField);
    up.add(doc);
    cluster.getSolrClient().request(up);
    return docId;
  }

  /** Indexes a "from"-side doc with a single-valued {@code join_s} FK pointing to a "to" doc. */
  private static void indexFromDoc(int id, String joinFieldValue, String matchField)
      throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setCommitWithin(50);
    up.setParam("collection", fromColl);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "" + id);
    doc.addField("join_s", joinFieldValue);
    doc.addField("match_s", matchField);
    up.add(doc);
    cluster.getSolrClient().request(up);
  }
}
