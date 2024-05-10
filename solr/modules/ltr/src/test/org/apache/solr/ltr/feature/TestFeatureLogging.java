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
package org.apache.solr.ltr.feature;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.store.FeatureStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFeatureLogging extends TestRerankBase {

  @Before
  public void setup() throws Exception {
    setuptest(true);
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testGeneratedFeatures() throws Exception {
    loadFeature("c1", ValueFeature.class.getName(), "test1", "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getName(), "test1", "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getName(), "test1", "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getName(), "test1", "{\"field\":\"popularity\"}");
    loadFeature(
        "nomatch", SolrFeature.class.getName(), "test1", "{\"q\":\"{!terms f=title}foobarbat\"}");
    loadFeature(
        "yesmatch", SolrFeature.class.getName(), "test1", "{\"q\":\"{!terms f=popularity}2\"}");

    loadModel(
        "sum1",
        LinearModel.class.getName(),
        new String[] {"c1", "c2", "c3"},
        "test1",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "c1", "1.0",
            "c2", "2.0",
            "c3", "3.0",
            "pop", "2.0",
            "nomatch", "0.0",
            "yesmatch", "1.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "c1", "1.0",
            "c2", "2.0",
            "c3", "3.0",
            "pop", "2.0",
            "yesmatch", "1.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "id,popularity,[fv logAll=true]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'id':'7', 'popularity':2,  '[fv]':'" + docs0fv_default_csv + "'}");

    query.remove("fl");
    query.add("fl", "[fv logAll=true]");
    query.add("rows", "3");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
  }

  @Test
  public void testDefaultStoreFeatureExtraction() throws Exception {
    loadFeature(
        "defaultf1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature("store8f1", ValueFeature.class.getName(), "store8", "{\"value\":2.0}");
    loadFeature("store9f1", ValueFeature.class.getName(), "store9", "{\"value\":3.0}");
    loadModel(
        "store9m1",
        LinearModel.class.getName(),
        new String[] {"store9f1"},
        "store9",
        "{\"weights\":{\"store9f1\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");

    // No store specified, use default store for extraction
    query.add("fl", "fv:[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("defaultf1", "1.0")
            + "'}");

    // Store specified, use store for extraction
    query.remove("fl");
    query.add("fl", "fv:[fv store=store8]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("store8f1", "2.0")
            + "'}");

    // Store specified + model specified, use store for extraction
    query.add("rq", "{!ltr reRankDocs=3 model=store9m1}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("store8f1", "2.0")
            + "'}");

    // No store specified + model specified, use model store for extraction
    query.remove("fl");
    query.add("fl", "fv:[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("store9f1", "3.0")
            + "'}");
  }

  @Test
  public void featureExtraction_nonExistentFeatureStore_shouldThrowException() throws Exception {
    loadFeature("store9f1", ValueFeature.class.getName(), "store9", "{\"value\":3.0}");
    loadModel(
        "store9m1",
        LinearModel.class.getName(),
        new String[] {"store9f1"},
        "store9",
        "{\"weights\":{\"store9f1\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");

    // Non-existent store specified, should throw exception
    query.add("fl", "fv:[fv store=nonExistentFeatureStore]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='Missing feature store: nonExistentFeatureStore'");
    assertJQ("/query" + query.toQueryString(), "/error/code==400");
  }

  @Test
  public void featureExtraction_nonExistentFeatureStoreReRanking_shouldThrowException()
      throws Exception {
    loadFeature("store9f1", ValueFeature.class.getName(), "store9", "{\"value\":3.0}");
    loadModel(
        "store9m1",
        LinearModel.class.getName(),
        new String[] {"store9f1"},
        "store9",
        "{\"weights\":{\"store9f1\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");

    // Non-existent store specified + model specified, should throw exception
    query.add("fl", "fv:[fv store=nonExistentFeatureStore]");
    query.add("rq", "{!ltr reRankDocs=3 model=store9m1}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='Missing feature store: nonExistentFeatureStore'");
    assertJQ("/query" + query.toQueryString(), "/error/code==400");
  }

  @Test
  public void featureExtraction_noFeatureAndModelStore_shouldThrowException() throws Exception {
    loadFeature("store9f1", ValueFeature.class.getName(), "store9", "{\"value\":3.0}");
    loadModel(
        "store9m1",
        LinearModel.class.getName(),
        new String[] {"store9f1"},
        "store9",
        "{\"weights\":{\"store9f1\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");

    // No store specified + no model specified + empty default store, should throw exception
    query.add("fl", "fv:[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='Empty feature store. If no ranking query (rq) is passed and DEFAULT feature store is empty, an existing feature store name must be passed for feature extraction.'");
    assertJQ("/query" + query.toQueryString(), "/error/code==400");
  }

  @Test
  public void testGeneratedGroup() throws Exception {
    loadFeature("c1", ValueFeature.class.getName(), "testgroup", "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getName(), "testgroup", "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getName(), "testgroup", "{\"value\":3.0}");
    loadFeature(
        "pop", FieldValueFeature.class.getName(), "testgroup", "{\"field\":\"popularity\"}");

    loadModel(
        "sumgroup",
        LinearModel.class.getName(),
        new String[] {"c1", "c2", "c3"},
        "testgroup",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "*,[fv]");
    query.add("debugQuery", "on");

    query.remove("fl");
    query.add("fl", "fv:[fv logAll=true]");
    query.add("rows", "3");
    query.add("group", "true");
    query.add("group.field", "title");

    query.add("rq", "{!ltr reRankDocs=3 model=sumgroup}");

    final String docs0fv_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "c1", "1.0",
            "c2", "2.0",
            "c3", "3.0",
            "pop", "5.0");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ(
        "/query" + query.toQueryString(),
        "/grouped/title/groups/[0]/doclist/docs/[0]/=={'fv':'" + docs0fv_csv + "'}");
  }

  @Test
  public void testSparseDenseFeatures() throws Exception {
    loadFeature(
        "match", SolrFeature.class.getName(), "test4", "{\"q\":\"{!terms f=title}different\"}");
    loadFeature("c4", ValueFeature.class.getName(), "test4", "{\"value\":1.0}");

    loadModel(
        "sum4",
        LinearModel.class.getName(),
        new String[] {"match"},
        "test4",
        "{\"weights\":{\"match\":1.0}}");

    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector("match", "1.0", "c4", "1.0");
    final String docs1fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("c4", "1.0");

    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector("match", "1.0", "c4", "1.0");
    final String docs1fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector("match", "0.0", "c4", "1.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);
    final String docs1fv_default_csv =
        chooseDefaultFeatureVector(docs1fv_dense_csv, docs1fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("rows", "10");
    query.add("rq", "{!ltr reRankDocs=10 model=sum4}");

    // csv - no feature format specified i.e. use default
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4 logAll=true]");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[0]/fv/=='" + docs0fv_default_csv + "'");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[1]/fv/=='" + docs1fv_default_csv + "'");

    // csv - sparse feature format check
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4 format=sparse logAll=true]");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[0]/fv/=='" + docs0fv_sparse_csv + "'");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[1]/fv/=='" + docs1fv_sparse_csv + "'");

    // csv - dense feature format check
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4 format=dense logAll=true]");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[0]/fv/=='" + docs0fv_dense_csv + "'");
    assertJQ(
        "/query" + query.toQueryString(), "/response/docs/[1]/fv/=='" + docs1fv_dense_csv + "'");
  }

  @Test
  public void testNoReranking_defaultStoreDefaultLogAll_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // No store specified, use default store for logging
    // No logAll specified, use default: logAll=true
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "defaultStoreFeature1", "1.0", "defaultStoreFeature2", "4.0")
            + "'}");
  }

  @Test
  public void testNoReranking_defaultStoreLogAllTrue_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // No store specified, use default store for logging
    // logAll=true, return all the features in the default store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv logAll=true]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "defaultStoreFeature1", "1.0", "defaultStoreFeature2", "4.0")
            + "'}");
  }

  @Test
  public void testNoReranking_defaultStoreLogAllFalse_shouldRaiseException() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // No store specified, use default store for logging
    // logAll=false, exception since no model used
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv logAll=false]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='you can only log all features from the store \\'null\\' passed in input in the logger'");
  }

  @Test
  public void testNoReranking_definedStoreDefaultLogAll_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // Store specified, used store for logging
    // No logAll specified, use default: logAll=true
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeA]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "storeAFeature1", "2.0", "storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testNoReranking_definedStoreLogAllTrue_shouldPrintAllFeature() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // Store specified, used store for logging
    // logAll=true, return all the features in the defined store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeA logAll=true]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "storeAFeature1", "2.0", "storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testNoReranking_definedStoreLogAllFalse_shouldRaiseException() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");

    // Store specified, used store for logging
    // logAll=false, exception since no model used
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeA logAll=false]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='you can only log all features from the store \\'storeA\\' passed in input in the logger'");
  }

  @Test
  public void testReranking_defaultStoreDefaultLogAll_shouldPrintModelFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // No store specified, use model store for logging
    // No logAll specified, use default: logAll=false
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testReranking_defaultStoreLogAllTrue_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // No store specified, use model store for logging
    // logAll=true, return all the features in the model store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv logAll=true]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "storeAFeature1", "2.0", "storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testReranking_defaultStoreLogAllFalse_shouldPrintModelFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // No store specified, use model store for logging
    // logAll=false, only model features returned
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv logAll=false]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testReranking_differentStoreDefaultLogAll_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadFeature("storeBFeature1", ValueFeature.class.getName(), "storeB", "{\"value\":3.0}");
    loadFeature("storeBFeature2", ValueFeature.class.getName(), "storeB", "{\"value\":7.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // Store specified, used store for logging
    // No logAll specified, use default: logAll=true
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeB]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "storeBFeature1", "3.0", "storeBFeature2", "7.0")
            + "'}");
  }

  @Test
  public void testReranking_differentStoreLogAllTrue_shouldPrintAllFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadFeature("storeBFeature1", ValueFeature.class.getName(), "storeB", "{\"value\":3.0}");
    loadFeature("storeBFeature2", ValueFeature.class.getName(), "storeB", "{\"value\":7.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // Store specified, used store for logging
    // logAll=true, return all the features in the defined store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeB logAll=true]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector(
                "storeBFeature1", "3.0", "storeBFeature2", "7.0")
            + "'}");
  }

  @Test
  public void testReranking_differentStoreLogAllFalse_shouldRaiseException() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadFeature("storeBFeature1", ValueFeature.class.getName(), "storeB", "{\"value\":3.0}");
    loadFeature("storeBFeature2", ValueFeature.class.getName(), "storeB", "{\"value\":7.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // Store specified, used store for logging
    // logAll=false, exception since the defined store is different from the model store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeB logAll=false]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='the feature store \\'storeB\\' in the logger is different from the model feature store \\'storeA\\', you can only log all the features from the store'");
  }

  @Test
  public void testReranking_modelStoreDefaultLogAll_shouldPrintModelFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // Store specified, used store for logging
    // No logAll specified, use default: logAll=false since we pass the same store as the model
    // store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeA]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("storeAFeature2", "6.0")
            + "'}");
  }

  @Test
  public void testReranking_modelStoreLogAllFalse_shouldPrintModelFeatures() throws Exception {
    loadFeature(
        "defaultStoreFeature1",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature(
        "defaultStoreFeature2",
        ValueFeature.class.getName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":4.0}");
    loadFeature("storeAFeature1", ValueFeature.class.getName(), "storeA", "{\"value\":2.0}");
    loadFeature("storeAFeature2", ValueFeature.class.getName(), "storeA", "{\"value\":6.0}");
    loadModel(
        "modelA",
        LinearModel.class.getName(),
        new String[] {"storeAFeature2"},
        "storeA",
        "{\"weights\":{\"storeAFeature2\":6.0}}");

    // Store specified, used store for logging
    // logAll=false, only model features returned since the defined store is the same as the model
    // store
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");
    query.add("fl", "fv:[fv store=storeA logAll=false]");
    query.add("rq", "{!ltr reRankDocs=3 model=modelA}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"
            + FeatureLoggerTestUtils.toFeatureVector("storeAFeature2", "6.0")
            + "'}");
  }
}
