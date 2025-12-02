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

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFeatureVectorCache extends TestRerankBase {
  SolrCore core = null;
  List<String> docs;

  @Before
  public void before() throws Exception {
    setupFeatureVectorCacheTest(false);

    this.docs = new ArrayList<>();
    docs.add(adoc("id", "1", "title", "w2", "description", "w2", "popularity", "2"));
    docs.add(adoc("id", "2", "title", "w1", "description", "w1", "popularity", "0"));
    for (String doc : docs) {
      assertU(doc);
    }
    assertU(commit());

    loadFeatures("featurevectorcache_features.json");
    loadModels("featurevectorcache_linear_model.json");

    core = solrClientTestRule.getCoreContainer().getCore(DEFAULT_TEST_CORENAME);
  }

  @After
  public void after() throws Exception {
    core.close();
    aftertest();
  }

  private static CounterSnapshot.CounterDataPointSnapshot getFeatureVectorCacheInserts(
      SolrCore core) {
    return SolrMetricTestUtils.getCacheSearcherOpsInserts(core, "featureVectorCache");
  }

  private static double getFeatureVectorCacheLookups(SolrCore core) {
    return SolrMetricTestUtils.getCacheSearcherTotalLookups(core, "featureVectorCache");
  }

  private static CounterSnapshot.CounterDataPointSnapshot getFeatureVectorCacheHits(SolrCore core) {
    return SolrMetricTestUtils.getCacheSearcherOpsHits(core, "featureVectorCache");
  }

  @Test
  public void testFeatureVectorCache_loggingDefaultStoreNoReranking() throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "value_feature_3",
            "3.0",
            "efi_feature",
            "3.0",
            "match_w1_title",
            "0.0",
            "popularity_value",
            "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "value_feature_3",
            "3.0",
            "efi_feature",
            "3.0",
            "popularity_value",
            "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "[fv efi.efi_feature=3]");

    // No caching, we want to see lookups, insertions and no hits
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size(), getFeatureVectorCacheLookups(core), 0);
    assertEquals(0, getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size(), getFeatureVectorCacheHits(core).getValue(), 0);
  }

  @Test
  public void testFeatureVectorCache_loggingExplicitStoreNoReranking() throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector("match_w1_title", "0.0", "value_feature_2", "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector("value_feature_2", "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "[fv store=store1 efi.efi_feature=3]");

    // No caching, we want to see lookups, insertions and no hits
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size(), getFeatureVectorCacheLookups(core), 0);
    assertEquals(0, getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size(), getFeatureVectorCacheHits(core).getValue(), 0);
  }

  @Test
  public void testFeatureVectorCache_loggingModelStoreAndRerankingWithDifferentEfi()
      throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "efi_feature",
            "3.0",
            "match_w1_title",
            "0.0",
            "popularity_value",
            "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1", "1.0", "efi_feature", "3.0", "popularity_value", "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "id,score,fv:[fv efi.efi_feature=3]");
    query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

    // No caching, we want to see lookups, insertions and no hits since the efis are different
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(0, getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits and same scores as before
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 4, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheHits(core).getValue(), 0);
  }

  @Test
  public void testFeatureVectorCache_loggingModelStoreAndRerankingWithSameEfi() throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "efi_feature",
            "4.0",
            "match_w1_title",
            "0.0",
            "popularity_value",
            "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1", "1.0", "efi_feature", "4.0", "popularity_value", "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "id,score,fv:[fv efi.efi_feature=4]");
    query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

    // No caching for reranking but logging should hit since we have the same feature store and efis
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size(), getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits and same scores
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size(), getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 4, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size() * 3, getFeatureVectorCacheHits(core).getValue(), 0);
  }

  @Test
  public void testFeatureVectorCache_loggingAllFeatureStoreAndReranking() throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "value_feature_3",
            "3.0",
            "efi_feature",
            "3.0",
            "match_w1_title",
            "0.0",
            "popularity_value",
            "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "value_feature_1",
            "1.0",
            "value_feature_3",
            "3.0",
            "efi_feature",
            "3.0",
            "popularity_value",
            "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "id,score,fv:[fv logAll=true efi.efi_feature=3]");
    query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

    // No caching, we want to see lookups, insertions and no hits since the efis are different
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(0, getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits and same scores
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 4, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheHits(core).getValue(), 0);
  }

  @Test
  public void testFeatureVectorCache_loggingExplicitStoreAndReranking() throws Exception {
    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector("match_w1_title", "0.0", "value_feature_2", "2.0");
    final String docs0fv_sparse_csv =
        FeatureLoggerTestUtils.toFeatureVector("value_feature_2", "2.0");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "id,score,fv:[fv store=store1 efi.efi_feature=3]");
    query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

    // No caching, we want to see lookups, insertions and no hits since the efis are different
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheLookups(core), 0);
    assertEquals(0, getFeatureVectorCacheHits(core).getValue(), 0);

    query.add("sort", "popularity desc");
    // Caching, we want to see hits and same scores
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={"
            + "'id':'1',"
            + "'score':3.4,"
            + "'fv':'"
            + docs0fv_default_csv
            + "'}");
    assertEquals(docs.size() * 2, getFeatureVectorCacheInserts(core).getValue(), 0);
    assertEquals(docs.size() * 4, getFeatureVectorCacheLookups(core), 0);
    assertEquals(docs.size() * 2, getFeatureVectorCacheHits(core).getValue(), 0);
  }
}
