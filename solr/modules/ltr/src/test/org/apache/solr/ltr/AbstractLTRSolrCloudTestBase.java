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
package org.apache.solr.ltr;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.OriginalScoreFeature;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.AfterClass;

public abstract class AbstractLTRSolrCloudTestBase extends TestRerankBase {

  protected MiniSolrCloudCluster solrCluster;

  protected abstract int numberOfShards();

  protected abstract int numberOfReplicas();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setupTestInit("solrconfig-ltr.xml", "schema.xml", true);
    System.setProperty("solr.index.updatelog.enabled", "true");

    final int numShards = numberOfShards();
    final int numReplicas = numberOfReplicas();
    setupSolrCluster(numShards, numReplicas, numShards * numReplicas);
  }

  @Override
  public void tearDown() throws Exception {
    restTestHarness = null;
    if (solrCluster != null) {
      solrCluster.shutdown();
      solrCluster = null;
    }
    super.tearDown();
  }

  protected void setupSolrCluster(int numShards, int numReplicas, int numServers) throws Exception {
    solrCluster = new MiniSolrCloudCluster(numServers, tmpSolrHome, JettyConfig.builder().build());
    Path configDir = tmpSolrHome.resolve("collection1/conf");
    solrCluster.uploadConfigSet(configDir, "conf1");

    createCollection(COLLECTION, "conf1", numShards, numReplicas);
    indexDocuments(COLLECTION);
    for (JettySolrRunner solrRunner : solrCluster.getJettySolrRunners()) {
      if (!solrRunner.getCoreContainer().getCores().isEmpty()) {
        String coreName = solrRunner.getCoreContainer().getCores().iterator().next().getName();
        restTestHarness = solrRunner.getRestClient(coreName);
        break;
      }
    }
    loadModelsAndFeatures();
  }

  private void createCollection(String name, String config, int numShards, int numReplicas)
      throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(name, config, numShards, numReplicas);
    response = create.process(solrCluster.getSolrClient());

    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response);
    }
    solrCluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }

  private void indexDocument(
      String collection, String id, String title, String description, int popularity)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("title", title);
    doc.setField("description", description);
    doc.setField("popularity", popularity);
    if (popularity != 1) {
      // check that empty values will be read as default
      doc.setField("dvIntField", popularity);
      doc.setField("dvLongField", popularity);
      doc.setField("dvFloatField", ((float) popularity) / 10);
      doc.setField("dvDoubleField", ((double) popularity) / 10);
      doc.setField("dvStrNumField", popularity % 2 == 0 ? "F" : "T");
      doc.setField("dvStrBoolField", popularity % 2 == 0 ? "T" : "F");
    }
    solrCluster.getSolrClient().add(collection, doc);
  }

  private void indexDocuments(final String collection) throws Exception {
    final int collectionSize = 8;
    // put documents in random order to check that advanceExact is working correctly
    List<Integer> docIds = IntStream.rangeClosed(1, collectionSize).boxed().collect(toList());
    Collections.shuffle(docIds, random());

    int docCounter = 1;
    for (int docId : docIds) {
      final int popularity = docId;
      indexDocument(collection, String.valueOf(docId), "a1", "bloom", popularity);
      // maybe commit in the middle in order to check that everything works fine for multi-segment
      // case
      if (docCounter == collectionSize / 2 && random().nextBoolean()) {
        solrCluster.getSolrClient().commit(collection);
      }
      docCounter++;
    }
    solrCluster.getSolrClient().commit(collection, true, true);
  }

  private void loadModelsAndFeatures() throws Exception {
    final String featureStore = "test";
    final String[] featureNames =
        new String[] {
          "powpularityS",
          "c3",
          "original",
          "dvIntFieldFeature",
          "dvLongFieldFeature",
          "dvFloatFieldFeature",
          "dvDoubleFieldFeature",
          "dvStrNumFieldFeature",
          "dvStrBoolFieldFeature"
        };
    final String jsonModelParams =
        "{\"weights\":{\"powpularityS\":1.0,\"c3\":1.0,\"original\":0.1,"
            + "\"dvIntFieldFeature\":0.1,\"dvLongFieldFeature\":0.1,"
            + "\"dvFloatFieldFeature\":0.1,\"dvDoubleFieldFeature\":0.1,\"dvStrNumFieldFeature\":0.1,\"dvStrBoolFieldFeature\":0.1}}";

    loadFeature(
        featureNames[0],
        SolrFeature.class.getName(),
        featureStore,
        "{\"q\":\"{!func}pow(popularity,2)\"}");
    loadFeature(featureNames[1], ValueFeature.class.getName(), featureStore, "{\"value\":2}");
    loadFeature(featureNames[2], OriginalScoreFeature.class.getName(), featureStore, null);
    loadFeature(
        featureNames[3],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvIntField\"}");
    loadFeature(
        featureNames[4],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvLongField\"}");
    loadFeature(
        featureNames[5],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvFloatField\"}");
    loadFeature(
        featureNames[6],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvDoubleField\",\"defaultValue\":-4.0}");
    loadFeature(
        featureNames[7],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvStrNumField\",\"defaultValue\":-5}");
    loadFeature(
        featureNames[8],
        FieldValueFeature.class.getName(),
        featureStore,
        "{\"field\":\"dvStrBoolField\"}");

    loadModel(
        "powpularityS-model",
        LinearModel.class.getName(),
        featureNames,
        featureStore,
        jsonModelParams);
    reloadCollection(COLLECTION);
  }

  private void reloadCollection(String collection) throws Exception {
    CollectionAdminRequest.Reload reloadRequest =
        CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(solrCluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
  }

  @AfterClass
  public static void after() throws Exception {
    if (null != tmpSolrHome) {
      PathUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
  }
}
