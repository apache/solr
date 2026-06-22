/* * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.solr.ltr;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.ltr.feature.OriginalScoreFeature;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Test;

@LuceneTestCase.Nightly
public class TestLTROnSolrCloud extends TestRerankBase {

  private MiniSolrCloudCluster solrCluster;
  String solrconfig = "solrconfig-ltr.xml";
  String schema = "schema.xml";

  SortedMap<ServletHolder,String> extraServlets = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    extraServlets = setupTestInit(solrconfig, schema, true);
    System.setProperty("enable.update.log", "true");

    int numberOfShards = TEST_NIGHTLY ? random().nextInt(TEST_NIGHTLY ? 4 : 2)+1 : 2;
    int numberOfReplicas = TEST_NIGHTLY ? random().nextInt(TEST_NIGHTLY ? 2 : 1)+1 : 2;
    int maxShardsPerNode = TEST_NIGHTLY ? random().nextInt(4)+1 : 4;

    int numberOfNodes = (numberOfShards*numberOfReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

    setupSolrCluster(numberOfShards, numberOfReplicas, numberOfNodes, maxShardsPerNode);
  }


  @Override
  public void tearDown() throws Exception {
    restTestHarness.close();
    restTestHarness = null;
    solrCluster.shutdown();
    super.tearDown();
  }

  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testSimpleQuery() throws Exception {
    // will randomly pick a configuration with [1..5] shards and [1..3] replicas

    // Test regular query, it will sort the documents by inverse
    // popularity (the less popular, docid == 1, will be in the first
    // position
    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");

    query.setRequestHandler("/query");
    query.setFields("*,score");
    query.setParam("rows", "8");

    QueryResponse queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("1", queryResponse.getResults().get(0).get("id").toString());
    assertEquals("2", queryResponse.getResults().get(1).get("id").toString());
    assertEquals("3", queryResponse.getResults().get(2).get("id").toString());
    assertEquals("4", queryResponse.getResults().get(3).get("id").toString());
    assertEquals("5", queryResponse.getResults().get(4).get("id").toString());
    assertEquals("6", queryResponse.getResults().get(5).get("id").toString());
    assertEquals("7", queryResponse.getResults().get(6).get("id").toString());
    assertEquals("8", queryResponse.getResults().get(7).get("id").toString());

    final Float original_result0_score = (Float)queryResponse.getResults().get(0).get("score");
    final Float original_result1_score = (Float)queryResponse.getResults().get(1).get("score");
    final Float original_result2_score = (Float)queryResponse.getResults().get(2).get("score");
    final Float original_result3_score = (Float)queryResponse.getResults().get(3).get("score");
    final Float original_result4_score = (Float)queryResponse.getResults().get(4).get("score");
    final Float original_result5_score = (Float)queryResponse.getResults().get(5).get("score");
    final Float original_result6_score = (Float)queryResponse.getResults().get(6).get("score");
    final Float original_result7_score = (Float)queryResponse.getResults().get(7).get("score");

    final String result0_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS","64.0", "c3","2.0", "original","0.0");
    final String result1_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS","49.0", "c3","2.0", "original","1.0");
    final String result2_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS","36.0", "c3","2.0", "original","2.0");
    final String result3_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS","25.0", "c3","2.0", "original","3.0");
    final String result4_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS","16.0", "c3","2.0", "original","4.0");
    final String result5_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "9.0", "c3","2.0", "original","5.0");
    final String result6_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "4.0", "c3","2.0", "original","6.0");
    final String result7_features= FeatureLoggerTestUtils.toFeatureVector(
        "powpularityS", "1.0", "c3","2.0", "original","7.0");


    // Test feature vectors returned (without re-ranking)
    query.setFields("*,score,features:[fv store=test]");
    queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("1", queryResponse.getResults().get(0).get("id").toString());
    assertEquals("2", queryResponse.getResults().get(1).get("id").toString());
    assertEquals("3", queryResponse.getResults().get(2).get("id").toString());
    assertEquals("4", queryResponse.getResults().get(3).get("id").toString());
    assertEquals("5", queryResponse.getResults().get(4).get("id").toString());
    assertEquals("6", queryResponse.getResults().get(5).get("id").toString());
    assertEquals("7", queryResponse.getResults().get(6).get("id").toString());
    assertEquals("8", queryResponse.getResults().get(7).get("id").toString());

    assertEquals(original_result0_score, queryResponse.getResults().get(0).get("score"));
    assertEquals(original_result1_score, queryResponse.getResults().get(1).get("score"));
    assertEquals(original_result2_score, queryResponse.getResults().get(2).get("score"));
    assertEquals(original_result3_score, queryResponse.getResults().get(3).get("score"));
    assertEquals(original_result4_score, queryResponse.getResults().get(4).get("score"));
    assertEquals(original_result5_score, queryResponse.getResults().get(5).get("score"));
    assertEquals(original_result6_score, queryResponse.getResults().get(6).get("score"));
    assertEquals(original_result7_score, queryResponse.getResults().get(7).get("score"));

    assertEquals(result7_features,
        queryResponse.getResults().get(0).get("features").toString());
    assertEquals(result6_features,
        queryResponse.getResults().get(1).get("features").toString());
    assertEquals(result5_features,
        queryResponse.getResults().get(2).get("features").toString());
    assertEquals(result4_features,
        queryResponse.getResults().get(3).get("features").toString());
    assertEquals(result3_features,
        queryResponse.getResults().get(4).get("features").toString());
    assertEquals(result2_features,
        queryResponse.getResults().get(5).get("features").toString());
    assertEquals(result1_features,
        queryResponse.getResults().get(6).get("features").toString());
    assertEquals(result0_features,
        queryResponse.getResults().get(7).get("features").toString());

    // Test feature vectors returned (with re-ranking)
    query.setFields("*,score,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");
    queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("8", queryResponse.getResults().get(0).get("id").toString());
    assertEquals(result0_features,
        queryResponse.getResults().get(0).get("features").toString());
    assertEquals("7", queryResponse.getResults().get(1).get("id").toString());
    assertEquals(result1_features,
        queryResponse.getResults().get(1).get("features").toString());
    assertEquals("6", queryResponse.getResults().get(2).get("id").toString());
    assertEquals(result2_features,
        queryResponse.getResults().get(2).get("features").toString());
    assertEquals("5", queryResponse.getResults().get(3).get("id").toString());
    assertEquals(result3_features,
        queryResponse.getResults().get(3).get("features").toString());
    assertEquals("4", queryResponse.getResults().get(4).get("id").toString());
    assertEquals(result4_features,
        queryResponse.getResults().get(4).get("features").toString());
    assertEquals("3", queryResponse.getResults().get(5).get("id").toString());
    assertEquals(result5_features,
        queryResponse.getResults().get(5).get("features").toString());
    assertEquals("2", queryResponse.getResults().get(6).get("id").toString());
    assertEquals(result6_features,
        queryResponse.getResults().get(6).get("features").toString());
    assertEquals("1", queryResponse.getResults().get(7).get("id").toString());
    assertEquals(result7_features,
        queryResponse.getResults().get(7).get("features").toString());
  }

  private void setupSolrCluster(int numShards, int numReplicas, int numServers, int maxShardsPerNode) throws Exception {
    JettyConfig jc = buildJettyConfig("/solr");
    jc = JettyConfig.builder(jc).withServlets(extraServlets).build();
    solrCluster = new MiniSolrCloudCluster(numServers, tmpSolrHome.toPath(), jc);
    File configDir = tmpSolrHome.toPath().resolve("collection1/conf").toFile();
    solrCluster.uploadConfigSet(configDir.toPath(), "conf1");

    solrCluster.getSolrClient().setDefaultCollection(COLLECTION);

    createCollection(COLLECTION, "conf1", numShards, numReplicas, maxShardsPerNode);
    indexDocuments(COLLECTION);
    for (JettySolrRunner solrRunner : solrCluster.getJettySolrRunners()) {
      if (!solrRunner.getCoreContainer().getCores().isEmpty()){
        String coreName = solrRunner.getCoreContainer().getCores().iterator().next().getName();
        restTestHarness = new RestTestHarness(() -> solrRunner.getBaseUrl().toString() + "/" + coreName, solrCluster.getSolrClient().getHttpClient()
            , solrRunner.getCoreContainer()
            .getResourceLoader());
        break;
      }
    }
    loadModelsAndFeatures();
  }


  private void createCollection(String name, String config, int numShards, int numReplicas, int maxShardsPerNode)
      throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(name, config, numShards, numReplicas);
    create.setMaxShardsPerNode(maxShardsPerNode);
    response = create.process(solrCluster.getSolrClient());

    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
  }


  void indexDocument(String collection, String id, String title, String description, int popularity)
    throws Exception{
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("title", title);
    doc.setField("description", description);
    doc.setField("popularity", popularity);
    solrCluster.getSolrClient().add(collection, doc);
  }

  private void indexDocuments(final String collection)
       throws Exception {
    final int collectionSize = 8;
    for (int docId = 1; docId <= collectionSize;  docId++) {
      final int popularity = docId;
      indexDocument(collection, String.valueOf(docId), "a1", "bloom", popularity);
    }
    solrCluster.getSolrClient().commit(collection);
  }


  private void loadModelsAndFeatures() throws Exception {
    final String featureStore = "test";
    final String[] featureNames = new String[] {"powpularityS","c3", "original"};
    final String jsonModelParams = "{\"weights\":{\"powpularityS\":1.0,\"c3\":1.0,\"original\":0.1}}";

    loadFeature(
            featureNames[0],
            SolrFeature.class.getName(),
            featureStore,
            "{\"q\":\"{!func}pow(popularity,2)\"}"
    );
    loadFeature(
            featureNames[1],
            ValueFeature.class.getName(),
            featureStore,
            "{\"value\":2}"
    );
    loadFeature(
            featureNames[2],
            OriginalScoreFeature.class.getName(),
            featureStore,
            null
    );

    loadModel(
             "powpularityS-model",
             LinearModel.class.getName(),
             featureNames,
             featureStore,
             jsonModelParams
    );
    reloadCollection(COLLECTION);
    // The feature/model managed resources are PUT to a single core and persisted to ZK, then
    // reloadCollection reloads every core from ZK. The subsequent queries are routed by
    // CloudSolrClient to any replica, so a replica that has not finished re-registering its
    // managed model/feature store yet would answer with "cannot find model" or an empty feature
    // vector (the long-standing SOLR-12028 flake). Block until every replica can actually serve
    // the model and feature store through the live query path before querying.
    waitForModelAndFeaturesOnAllReplicas("powpularityS-model", featureStore);
  }

  private void waitForModelAndFeaturesOnAllReplicas(String modelName, String featureStore)
      throws Exception {
    // Probe every replica directly (distrib=false) until each one can both resolve the LTR model
    // and emit a non-empty feature vector. The in-memory managed model/feature store reachable
    // outside the request path is not the instance the query consults after a core reload, so it
    // is not a reliable readiness signal. The live query path is the ground truth for both failure
    // modes here: a missing model surfaces as a 400 and an unready feature store surfaces as an
    // empty feature vector.
    //
    // The managed model/feature resources are PUT to one core and persisted to the shared ZK
    // configset, then reloadCollection reloads every core to re-read them. A core that reloads
    // before the just-persisted resource is visible to it re-reads a stale configset and then
    // stays stale until it is reloaded again (the long-standing SOLR-12028 propagation flake).
    // Reload is itself the propagation mechanism, so when a replica is found not-ready we re-issue
    // the (idempotent) reload to repair it, throttled so we do not hammer the cluster.
    final List<Replica> replicas = new ArrayList<>(
        solrCluster.getSolrClient().getZkStateReader().getClusterState()
            .getCollection(COLLECTION).getReplicas());
    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    long nextReloadNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    String notReady;
    do {
      notReady = probeReplicasForModel(replicas, modelName, featureStore);
      if (notReady == null) {
        return;
      }
      if (System.nanoTime() >= nextReloadNanos) {
        reloadCollection(COLLECTION);
        nextReloadNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      }
      Thread.sleep(200);
    } while (System.nanoTime() < deadlineNanos);
    fail("Timed out waiting for LTR model '" + modelName + "' and feature store '" + featureStore
        + "' to be queryable on replica " + notReady);
  }

  /** Returns the description of the first replica not yet serving the model/features, else null. */
  private String probeReplicasForModel(List<Replica> replicas, String modelName, String featureStore)
      throws Exception {
    for (Replica replica : replicas) {
      try (Http2SolrClient client =
          new Http2SolrClient.Builder(replica.getBaseUrl()).build()) {
        final SolrQuery probe = new SolrQuery("{!func}sub(8,field(popularity))");
        probe.setRequestHandler("/query");
        probe.setParam("distrib", "false");
        probe.setParam("rows", "1");
        probe.setFields("id,score,features:[fv store=" + featureStore + "]");
        probe.add("rq", "{!ltr model=" + modelName + " reRankDocs=1}");
        try {
          final QueryResponse resp = client.query(replica.getName(), probe);
          if (resp.getResults().isEmpty()) {
            // This shard slice holds no doc to extract features from; nothing to verify here.
            continue;
          }
          final Object fv = resp.getResults().get(0).get("features");
          if (fv == null || fv.toString().isEmpty()) {
            return replica.getName() + " @ " + replica.getBaseUrl() + " (empty feature vector)";
          }
        } catch (final Exception e) {
          return replica.getName() + " @ " + replica.getBaseUrl() + " (" + e.getMessage() + ")";
        }
      }
    }
    return null;
  }

  private void reloadCollection(String collection) throws Exception {
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(solrCluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
  }

  @AfterClass
  public static void after() throws Exception {
    if (null != tmpSolrHome) {
      FileUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    System.clearProperty("managed.schema.mutable");
  }

}
