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

package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.SolrJMetricTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class CrossCollectionJoinQueryTest extends SolrCloudTestCase {

  private static final int NUM_NODES = 3;
  private static final int NUM_SHARDS = 3;
  private static final int NUM_REPLICAS = 1;

  private static final int NUM_PRODUCTS = 200;
  private static final String[] SIZES = new String[] {"S", "M", "L", "XL"};

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig("ccjoin", configset("ccjoin"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    CollectionAdminRequest.createCollection("products", "ccjoin", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.createCollection("parts", "ccjoin", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());
  }

  public static void setupIndexes(boolean routeByKey) throws IOException, SolrServerException {
    clearCollection("products");
    clearCollection("parts");

    buildIndexes(routeByKey);
  }

  private static void clearCollection(String collection) throws IOException, SolrServerException {
    UpdateRequest update = new UpdateRequest();
    update.deleteByQuery("*:*");
    update.process(cluster.getSolrClient(), collection);
  }

  private static void buildIndexes(boolean routeByKey) throws IOException, SolrServerException {
    List<SolrInputDocument> productDocs = new ArrayList<>();
    List<SolrInputDocument> partDocs = new ArrayList<>();

    for (int productId = 0; productId < NUM_PRODUCTS; ++productId) {
      int sizeNum = productId % SIZES.length;
      String size = SIZES[sizeNum];

      productDocs.add(
          new SolrInputDocument(
              "id", buildId(productId, String.valueOf(productId), routeByKey),
              "product_id_i", String.valueOf(productId),
              "product_id_l", String.valueOf(productId),
              "product_id_s", String.valueOf(productId),
              "size_s", size));

      // Index 1 parts document for each small product, 2 for each medium, 3 for each large, etc.
      for (int partNum = 0; partNum <= sizeNum; partNum++) {
        String partId = String.format(Locale.ROOT, "%d_%d", productId, partNum);
        partDocs.add(
            new SolrInputDocument(
                "id", buildId(productId, partId, routeByKey),
                "product_id_i", String.valueOf(productId),
                "product_id_l", String.valueOf(productId),
                "product_id_s", String.valueOf(productId)));
      }
    }

    // some extra docs in each collection (not counted in NUM_PRODUCTS) that should drop out of the
    // joins because they don't have the join key
    productDocs.add(
        new SolrInputDocument(
            "id",
            buildId(NUM_PRODUCTS + 10, String.valueOf(NUM_PRODUCTS + 10), routeByKey),
            "size_s",
            "M"));
    partDocs.add(
        new SolrInputDocument(
            "id", buildId(NUM_PRODUCTS + 10, String.valueOf(NUM_PRODUCTS + 10), routeByKey)));

    Collections.shuffle(productDocs, random());
    Collections.shuffle(partDocs, random());

    indexDocs("products", productDocs);
    cluster.getSolrClient().commit("products");
    assertResultCount("products", "*:*", 1 + NUM_PRODUCTS, true);

    indexDocs("parts", partDocs);
    cluster.getSolrClient().commit("parts");
    assertResultCount("parts", "*:*", 1 + (NUM_PRODUCTS * 10 / 4), true);
  }

  private static String buildId(int productId, String id, boolean routeByKey) {
    return routeByKey ? productId + "!" + id : id;
  }

  private static void indexDocs(String collection, Collection<SolrInputDocument> docs)
      throws IOException, SolrServerException {
    UpdateRequest update = new UpdateRequest();
    update.add(docs);
    update.process(cluster.getSolrClient(), collection);
  }

  private String getSolrUrl() {
    List<JettySolrRunner> runners = cluster.getJettySolrRunners();
    JettySolrRunner runner = runners.get(random().nextInt(runners.size()));
    return runner.getBaseUrl().toString();
  }

  @Test
  public void testCcJoinRoutedCollection() throws Exception {
    setupIndexes(true);
    testCcJoinQuery(
        "{!join method=crossCollection fromIndex=products from=product_id_i to=product_id_i}size_s:M",
        true);
    int i = 0;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      i++;
      String url = runner.getBaseUrl().toString();
      System.setProperty("test.ccjoin.solr.url." + i, url);
    }
    try {
      // now we need to re-upload our config , now that we know a valid solr url for the cluster.
      CloudSolrClient client = cluster.getSolrClient();
      cluster.uploadConfigSet(configset("ccjoin"), "ccjoin");
      // reload the cores with the updated allowSolrUrls config.
      CollectionAdminRequest.Reload.reloadCollection("products").process(client);
      CollectionAdminRequest.Reload.reloadCollection("parts").process(client);
      Thread.sleep(10000);

      testCcJoinQuery(
          "{!join method=crossCollection fromIndex=products from=product_id_i to=product_id_i}size_s:M",
          true);

      testCcJoinQuery(
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_i to=product_id_i}size_s:M",
              getSolrUrl()),
          true);

      testCcJoinQuery(
          "{!join method=crossCollection fromIndex=products from=product_id_l to=product_id_l}size_s:M",
          true);
      testCcJoinQuery(
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_l to=product_id_l}size_s:M",
              getSolrUrl()),
          true);

      testCcJoinQuery(
          "{!join method=crossCollection fromIndex=products from=product_id_s to=product_id_s}size_s:M",
          true);
      testCcJoinQuery(
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_s to=product_id_s}size_s:M",
              getSolrUrl()),
          true);
      testCcJoinQuery(
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection zkHost=\"%s\" fromIndex=products from=product_id_s to=product_id_s}size_s:M",
              client.getClusterStateProvider().getQuorumHosts()),
          true);

      // Test the ability to set other parameters on crossCollection join and have them passed
      // through
      assertResultCount(
          "parts",
          "{!join method=crossCollection fromIndex=products from=product_id_s to=product_id_s fq=product_id_s:1}size_s:M",
          2,
          true);
      assertResultCount(
          "parts",
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_s to=product_id_s fq=product_id_s:1}size_s:M",
              getSolrUrl()),
          2,
          true);
    } finally {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        i++;
        System.getProperties().remove("test.ccjoin.solr.url." + i);
      }
    }
  }

  @Test
  public void testCcJoinNonroutedCollection() throws Exception {
    setupIndexes(false);

    // This query will expect the collection to have been routed on product_id, so it should return
    // incomplete results.
    testCcJoinQuery(
        "{!join method=crossCollection fromIndex=products from=product_id_s to=product_id_s}size_s:M",
        false);
    // Now if we set routed=false we should get a complete set of results.
    testCcJoinQuery(
        "{!join method=crossCollection fromIndex=products from=product_id_s to=product_id_s routed=false}size_s:M",
        true);
    // The join_nonrouted query parser doesn't assume that the collection was routed on product_id,
    // so we should get the full set of results.
    testCcJoinQuery(
        "{!join_nonrouted method=crossCollection fromIndex=products from=product_id_s to=product_id_s}size_s:M",
        true);
    // But if we set routed=true, we are now assuming again that the collection was routed on
    // product_id, so we should get incomplete results.
    testCcJoinQuery(
        "{!join_nonrouted method=crossCollection fromIndex=products from=product_id_s to=product_id_s routed=true}size_s:M",
        false);
  }

  @Test
  public void testAllowSolrUrlsList() throws Exception {
    setupIndexes(false);

    // programmatically add the current jetty solr url to the allowSolrUrls property in the
    // solrconfig.xml
    int i = 0;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      i++;
      System.setProperty("test.ccjoin.solr.url." + i, runner.getBaseUrl().toString());
    }
    try {
      // now we need to re-upload our config , now that we know a valid solr url for the cluster.
      CloudSolrClient client = cluster.getSolrClient();
      cluster.uploadConfigSet(configset("ccjoin"), "ccjoin");
      // reload the cores with the updated allowSolrUrls config.
      CollectionAdminRequest.Reload.reloadCollection("products").process(client);
      CollectionAdminRequest.Reload.reloadCollection("parts").process(client);

      final ModifiableSolrParams params = new ModifiableSolrParams();
      //  a bogus solrUrl
      params.add("q", "");
      params.add("rows", "0");

      // we expect an exception because bogus url isn't valid.
      try {
        // This should throw an exception.
        // verify the join plugin definition has the current valid urls and works.
        testCcJoinQuery(
            String.format(
                Locale.ROOT,
                "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_i to=product_id_i}size_s:M",
                "http://bogus.example.com:8983/solr"),
            true);
        fail("The query involving bogus.example.com should not succeed");
      } catch (Exception e) {
        // should get here.
        String message = e.getMessage();
        assertTrue(
            "message was " + message,
            message.contains("SyntaxError: Solr URL was not in allowSolrUrls list"));
      }

      // verify the join plugin definition has the current valid urls and works.
      testCcJoinQuery(
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection solrUrl=\"%s\" fromIndex=products from=product_id_i to=product_id_i}size_s:M",
              getSolrUrl()),
          true);

    } finally {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        i++;
        System.getProperties().remove("test.ccjoin.solr.url." + i);
      }
    }
  }

  @Test
  public void testShardsPreferenceRequestParamPropagation() throws Exception {
    // shards.preference set as a request-level param (not in localParams) should be
    // propagated to the query's otherParams
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.set(ShardParams.SHARDS_PREFERENCE, "replica.leader:false");

    ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.set(QueryParsing.V, "*:*");
    localParams.set(CrossCollectionJoinQParser.FROM_INDEX, "products");
    localParams.set(CrossCollectionJoinQParser.FROM, "product_id_s");
    localParams.set(CrossCollectionJoinQParser.TO, "product_id_s");
    localParams.set(CrossCollectionJoinQParser.ROUTED_BY_JOIN_KEY, "false");

    try (SolrQueryRequest req = new SolrQueryRequestBase(null, requestParams) {}) {
      CrossCollectionJoinQParser parser =
          new CrossCollectionJoinQParser(
              null, localParams, requestParams, req, "product_id_s", null);
      Query query = parser.parse();

      CrossCollectionJoinQuery ccjQuery = (CrossCollectionJoinQuery) query;
      assertEquals("replica.leader:false", ccjQuery.otherParams.get(ShardParams.SHARDS_PREFERENCE));
    }
  }

  @Test
  public void testShardsPreferenceLocalParamTakesPrecedence() throws Exception {
    // When shards.preference is set in both localParams and request params,
    // the localParams value should take precedence
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.set(ShardParams.SHARDS_PREFERENCE, "replica.leader:false");

    ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.set(QueryParsing.V, "*:*");
    localParams.set(CrossCollectionJoinQParser.FROM_INDEX, "products");
    localParams.set(CrossCollectionJoinQParser.FROM, "product_id_s");
    localParams.set(CrossCollectionJoinQParser.TO, "product_id_s");
    localParams.set(CrossCollectionJoinQParser.ROUTED_BY_JOIN_KEY, "false");
    localParams.set(ShardParams.SHARDS_PREFERENCE, "replica.leader:true");

    try (SolrQueryRequest req = new SolrQueryRequestBase(null, requestParams) {}) {
      CrossCollectionJoinQParser parser =
          new CrossCollectionJoinQParser(
              null, localParams, requestParams, req, "product_id_s", null);
      Query query = parser.parse();

      CrossCollectionJoinQuery ccjQuery = (CrossCollectionJoinQuery) query;
      // localParams value should take precedence over request-level param
      assertEquals("replica.leader:true", ccjQuery.otherParams.get(ShardParams.SHARDS_PREFERENCE));
    }
  }

  @Test
  public void testShardsPreferenceWithCrossCollectionJoin() throws Exception {
    // Use 1 shard with 2 replicas for the "from" collection so there is exactly
    // 1 leader and 1 non-leader, each on a different node. This lets us verify
    // via per-node /export metrics which replica actually served the stream request.
    final String fromCollection = "products_pref_test";
    final String toCollection = "parts_pref_test";
    try {
      CollectionAdminRequest.createCollection(fromCollection, "ccjoin", 1, 2)
          .process(cluster.getSolrClient());
      CollectionAdminRequest.createCollection(toCollection, "ccjoin", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(fromCollection, 1, 2);
      cluster.waitForActiveCollection(toCollection, 1, 1);

      // Index test data
      List<SolrInputDocument> productDocs = new ArrayList<>();
      List<SolrInputDocument> partDocs = new ArrayList<>();
      for (int productId = 0; productId < NUM_PRODUCTS; ++productId) {
        int sizeNum = productId % SIZES.length;
        String size = SIZES[sizeNum];
        productDocs.add(
            new SolrInputDocument(
                "id", String.valueOf(productId),
                "product_id_s", String.valueOf(productId),
                "size_s", size));
        for (int partNum = 0; partNum <= sizeNum; partNum++) {
          String partId = String.format(Locale.ROOT, "%d_%d", productId, partNum);
          partDocs.add(
              new SolrInputDocument("id", partId, "product_id_s", String.valueOf(productId)));
        }
      }
      indexDocs(fromCollection, productDocs);
      cluster.getSolrClient().commit(fromCollection);
      indexDocs(toCollection, partDocs);
      cluster.getSolrClient().commit(toCollection);

      // Identify leader and non-leader replicas for the "from" collection's single shard
      DocCollection fromDocCollection =
          cluster.getSolrClient().getClusterState().getCollection(fromCollection);
      Slice shard = fromDocCollection.getSlices().iterator().next();
      Replica leader = shard.getLeader();
      assertNotNull("Leader should exist for shard", leader);
      Replica nonLeader =
          shard.getReplicas().stream()
              .filter(r -> !r.getName().equals(leader.getName()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Expected a non-leader replica"));

      String leaderBaseUrl = leader.getBaseUrl();
      String nonLeaderBaseUrl = nonLeader.getBaseUrl();
      assertNotEquals(
          "Leader and non-leader should be on different nodes for this test to be meaningful",
          leaderBaseUrl,
          nonLeaderBaseUrl);

      // --- Test 1: replica.leader:false should route /export to the non-leader ---
      double leaderCountBefore = getNumExportRequests(leaderBaseUrl, fromCollection);
      double nonLeaderCountBefore = getNumExportRequests(nonLeaderBaseUrl, fromCollection);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(
          "q",
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection fromIndex=%s from=product_id_s to=product_id_s routed=false ttl=0}size_s:M",
              fromCollection));
      params.set("rows", "0");
      params.set(ShardParams.SHARDS_PREFERENCE, "replica.leader:false");

      QueryResponse resp = cluster.getSolrClient().query(toCollection, params);
      assertEquals(NUM_PRODUCTS / 2, resp.getResults().getNumFound());

      double leaderCountAfter = getNumExportRequests(leaderBaseUrl, fromCollection);
      double nonLeaderCountAfter = getNumExportRequests(nonLeaderBaseUrl, fromCollection);

      assertTrue(
          "Non-leader replica should have received the /export request"
              + " (before="
              + nonLeaderCountBefore
              + ", after="
              + nonLeaderCountAfter
              + ")",
          nonLeaderCountAfter > nonLeaderCountBefore);
      assertEquals(
          "Leader replica should NOT have received the /export request",
          leaderCountBefore,
          leaderCountAfter,
          0.0);

      // --- Test 2: replica.leader:true should route /export to the leader ---
      leaderCountBefore = leaderCountAfter;
      nonLeaderCountBefore = nonLeaderCountAfter;

      params.set(ShardParams.SHARDS_PREFERENCE, "replica.leader:true");
      resp = cluster.getSolrClient().query(toCollection, params);
      assertEquals(NUM_PRODUCTS / 2, resp.getResults().getNumFound());

      leaderCountAfter = getNumExportRequests(leaderBaseUrl, fromCollection);
      nonLeaderCountAfter = getNumExportRequests(nonLeaderBaseUrl, fromCollection);

      assertTrue(
          "Leader replica should have received the /export request"
              + " (before="
              + leaderCountBefore
              + ", after="
              + leaderCountAfter
              + ")",
          leaderCountAfter > leaderCountBefore);
      assertEquals(
          "Non-leader replica should NOT have received the /export request",
          nonLeaderCountBefore,
          nonLeaderCountAfter,
          0.0);
    } finally {
      CollectionAdminRequest.deleteCollection(toCollection).process(cluster.getSolrClient());
      CollectionAdminRequest.deleteCollection(fromCollection).process(cluster.getSolrClient());
    }
  }

  @Test
  public void testShardsPreferenceLocationLocal() throws Exception {
    // Test that replica.location:local routes the join's /export stream to a replica
    // on the same node where the join query is processed. This validates that the
    // RequestReplicaListTransformerGenerator is initialized with full node context
    // (nodeName, baseUrl, hostName).
    final String fromCollection = "products_local_test";
    final String toCollection = "parts_local_test";
    try {
      // "from" collection: 1 shard, NUM_NODES replicas → one replica on every node
      CollectionAdminRequest.createCollection(fromCollection, "ccjoin", 1, NUM_NODES)
          .process(cluster.getSolrClient());
      // "to" collection: 1 shard, 1 replica → on exactly one node
      CollectionAdminRequest.createCollection(toCollection, "ccjoin", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(fromCollection, 1, NUM_NODES);
      cluster.waitForActiveCollection(toCollection, 1, 1);

      // Index test data
      List<SolrInputDocument> productDocs = new ArrayList<>();
      List<SolrInputDocument> partDocs = new ArrayList<>();
      for (int productId = 0; productId < NUM_PRODUCTS; ++productId) {
        int sizeNum = productId % SIZES.length;
        String size = SIZES[sizeNum];
        productDocs.add(
            new SolrInputDocument(
                "id", String.valueOf(productId),
                "product_id_s", String.valueOf(productId),
                "size_s", size));
        for (int partNum = 0; partNum <= sizeNum; partNum++) {
          String partId = String.format(Locale.ROOT, "%d_%d", productId, partNum);
          partDocs.add(
              new SolrInputDocument("id", partId, "product_id_s", String.valueOf(productId)));
        }
      }
      indexDocs(fromCollection, productDocs);
      cluster.getSolrClient().commit(fromCollection);
      indexDocs(toCollection, partDocs);
      cluster.getSolrClient().commit(toCollection);

      // Find the node hosting the "to" collection's shard. The join stream will execute
      // on this node, so replica.location:local should prefer the "from" replica here.
      DocCollection toDocCollection =
          cluster.getSolrClient().getClusterState().getCollection(toCollection);
      Slice toShard = toDocCollection.getSlices().iterator().next();
      String toNodeBaseUrl = toShard.getReplicas().iterator().next().getBaseUrl();

      // Collect all "from" replica base URLs
      DocCollection fromDocCollection =
          cluster.getSolrClient().getClusterState().getCollection(fromCollection);
      Slice fromShard = fromDocCollection.getSlices().iterator().next();
      List<String> fromBaseUrls = new ArrayList<>();
      for (Replica r : fromShard.getReplicas()) {
        fromBaseUrls.add(r.getBaseUrl());
      }
      assertTrue(
          "The 'from' collection should have a replica on the same node as the 'to' collection",
          fromBaseUrls.contains(toNodeBaseUrl));

      // Get baseline /export request counts for the "from" collection on all nodes
      double localCountBefore = getNumExportRequests(toNodeBaseUrl, fromCollection);
      List<double[]> remoteCountsBefore = new ArrayList<>();
      for (String baseUrl : fromBaseUrls) {
        if (!baseUrl.equals(toNodeBaseUrl)) {
          remoteCountsBefore.add(new double[] {getNumExportRequests(baseUrl, fromCollection)});
        }
      }

      // Execute join query with replica.location:local
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(
          "q",
          String.format(
              Locale.ROOT,
              "{!join method=crossCollection fromIndex=%s from=product_id_s to=product_id_s routed=false ttl=0}size_s:M",
              fromCollection));
      params.set("rows", "0");
      params.set(ShardParams.SHARDS_PREFERENCE, "replica.location:local");

      QueryResponse resp = cluster.getSolrClient().query(toCollection, params);
      assertEquals(NUM_PRODUCTS / 2, resp.getResults().getNumFound());

      // Verify the local node's "from" replica received the /export request
      double localCountAfter = getNumExportRequests(toNodeBaseUrl, fromCollection);
      assertTrue(
          "Local 'from' replica should have received the /export request"
              + " (before="
              + localCountBefore
              + ", after="
              + localCountAfter
              + ")",
          localCountAfter > localCountBefore);

      // Verify remote nodes did NOT receive /export requests
      int remoteIdx = 0;
      for (String baseUrl : fromBaseUrls) {
        if (!baseUrl.equals(toNodeBaseUrl)) {
          double remoteCountAfter = getNumExportRequests(baseUrl, fromCollection);
          assertEquals(
              "Remote 'from' replica on " + baseUrl + " should NOT have received /export request",
              remoteCountsBefore.get(remoteIdx)[0],
              remoteCountAfter,
              0.0);
          remoteIdx++;
        }
      }
    } finally {
      CollectionAdminRequest.deleteCollection(toCollection).process(cluster.getSolrClient());
      CollectionAdminRequest.deleteCollection(fromCollection).process(cluster.getSolrClient());
    }
  }

  private static double getNumExportRequests(String baseUrl, String collectionName)
      throws SolrServerException, IOException {
    return SolrJMetricTestUtils.getNumCoreRequests(baseUrl, collectionName, "QUERY", "/export");
  }

  public void testCcJoinQuery(String query, boolean expectFullResults) throws Exception {
    assertResultCount("parts", query, NUM_PRODUCTS / 2, expectFullResults);
  }

  private static void assertResultCount(
      String collection, String query, long expectedCount, boolean expectFullResults)
      throws IOException, SolrServerException {

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", query);
    params.add("rows", "0");

    QueryResponse resp = cluster.getSolrClient().query(collection, params);

    if (expectFullResults) {
      assertEquals(expectedCount, resp.getResults().getNumFound());
    } else {
      assertTrue(resp.getResults().getNumFound() < expectedCount);
    }
  }
}
