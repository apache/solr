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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFeatureVectorCache extends TestRerankBase {
    @Before
    public void before() throws Exception {
        setupFeatureVectorCachetest(false);

        assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity", "1"));
        assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity", "2"));
        assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity", "3"));
        assertU(commit());

        loadFeatures("featurevectorcache_features.json");
        loadModels("featurevectorcache_linear_model.json");
    }

    @After
    public void after() throws Exception {
        aftertest();
    }

    @Test
    public void testFeatureVectorCacheLogging() throws Exception {
        final String doc1_feature_vector =
                FeatureLoggerTestUtils.toFeatureVector(
                        "value_feature_1", "1.0",
                        "efi_feature", "3.0",
                        "match_w1_title", "1.0",
                        "popularity_value", "1.0");

        final String doc3_feature_vector =
                FeatureLoggerTestUtils.toFeatureVector(
                        "value_feature_1", "1.0",
                        "efi_feature", "3.0",
                        "match_w1_title", "0.0",
                        "popularity_value", "3.0");

        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "[fv format=dense efi.efi_feature=3]");

        // Feature vectors without caching
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + doc1_feature_vector + "'}");

        // Feature vectors with caching
        query.add("sort", "popularity desc");
        assertJQ(
                "/query" + query.toQueryString(),
                "/response/docs/[0]/=={'[fv]':'" + doc3_feature_vector + "'}");
    }

    @Test
    public void testFeatureVectorCacheRerank() throws Exception {
        final SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.add("rows", "3");
        query.add("fl", "*,score");
        query.add("rq", "{!ltr reRankDocs=3 model=featurevectorcache_linear_model efi.efi_feature=4}");

        // Feature vectors without caching
        assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==4.2");

        // Feature vectors with caching
        assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==3.1");
    }
}