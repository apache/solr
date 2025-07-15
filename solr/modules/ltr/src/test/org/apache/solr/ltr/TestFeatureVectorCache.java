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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TestFeatureVectorCache extends TestRerankBase {
    SolrCore core = null;

    @Before
    public void before() throws Exception {
        setupFeatureVectorCachetest(false);

        assertU(adoc("id", "1", "title", "w2", "description", "w2", "popularity", "2"));
        assertU(adoc("id", "2", "title", "w1", "description", "w1", "popularity", "0"));
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

    private static Map<String, Object> lookupFilterCacheMetrics(SolrCore core) {
        return ((MetricsMap)
                ((SolrMetricManager.GaugeWrapper<?>)
                        core.getCoreMetricManager()
                                .getRegistry()
                                .getMetrics()
                                .get("CACHE.searcher.featureVectorCache"))
                        .getGauge())
                .getValue();
    }

    @Test
    public void testFeatureVectorCache_loggingDefaultStoreNoReranking() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "value_feature_3", "3.0", "efi_feature", "3.0",
                "match_w1_title", "0.0", "popularity_value", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "value_feature_3", "3.0", "efi_feature", "3.0",
                "popularity_value", "2.0");

        final String docs0fv_default_csv =
                chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "[fv efi.efi_feature=3]");

        // No caching, we want to see lookups, an insertions and no hits
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(2, (long) filterCacheMetrics.get("lookups"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(2, (long) filterCacheMetrics.get("hits"));
    }

    @Test
    public void testFeatureVectorCache_loggingExplicitStoreNoReranking() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "match_w1_title", "0.0", "value_feature_2", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_2", "2.0");

        final String docs0fv_default_csv =
                chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "[fv store=store1 efi.efi_feature=3]");

        // No caching, we want to see lookups, an insertions and no hits
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(2, (long) filterCacheMetrics.get("lookups"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(2, (long) filterCacheMetrics.get("hits"));
    }

    @Test
    public void testFeatureVectorCache_loggingModelStoreRerankingDifferentEfi() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "efi_feature", "3.0",
                "match_w1_title", "0.0", "popularity_value", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "efi_feature", "3.0", "popularity_value", "2.0");

        final String docs0fv_default_csv =
                chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "id,score,fv:[fv efi.efi_feature=3]");
        query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

        // No caching, we want to see lookups, an insertions and no hits since the efi are different
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits and same score
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(8, (long) filterCacheMetrics.get("lookups"));
        assertEquals(4, (long) filterCacheMetrics.get("hits"));
    }

    @Test
    public void testFeatureVectorCache_loggingModelStoreRerankingSameEfi() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "efi_feature", "4.0",
                "match_w1_title", "0.0", "popularity_value", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
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
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(2, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits and same score
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(2, (long) filterCacheMetrics.get("inserts"));
        assertEquals(8, (long) filterCacheMetrics.get("lookups"));
        assertEquals(6, (long) filterCacheMetrics.get("hits"));
    }

    @Test
    public void testFeatureVectorCache_loggingAllStoreReranking() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "value_feature_3", "3.0", "efi_feature", "3.0",
                "match_w1_title", "0.0", "popularity_value", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_1", "1.0", "value_feature_3", "3.0", "efi_feature", "3.0",
                "popularity_value", "2.0");

        final String docs0fv_default_csv =
                chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "id,score,fv:[fv logAll=true efi.efi_feature=3]");
        query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

        // No caching, we want to see lookups, an insertions and no hits since the efi are different
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits and same score
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(8, (long) filterCacheMetrics.get("lookups"));
        assertEquals(4, (long) filterCacheMetrics.get("hits"));
    }

    @Test
    public void testFeatureVectorCache_loggingExplicitStoreReranking() throws Exception {
        final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(
                "match_w1_title", "0.0", "value_feature_2", "2.0");
        final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
                "value_feature_2", "2.0");

        final String docs0fv_default_csv =
                chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "id,score,fv:[fv store=store1 efi.efi_feature=3]");
        query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

        // No caching, we want to see lookups, an insertions and no hits since the efi are different
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        Map<String, Object> filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(4, (long) filterCacheMetrics.get("lookups"));
        assertEquals(0, (long) filterCacheMetrics.get("hits"));

        query.add("sort", "popularity desc");
        // Caching, we want to see hits and same score
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={" +
                        "'id':'1'," +
                        "'score':3.4," +
                        "'fv':'" + docs0fv_default_csv + "'}");
        filterCacheMetrics = lookupFilterCacheMetrics(core);
        assertEquals(4, (long) filterCacheMetrics.get("inserts"));
        assertEquals(8, (long) filterCacheMetrics.get("lookups"));
        assertEquals(4, (long) filterCacheMetrics.get("hits"));
    }
}