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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestPrefetchingFieldValueFeature extends TestRerankBase {

  private static final String[] FIELDS = {
      "popularity",
      "storedIntField", "storedLongField", "storedFloatField", "storedDoubleField",
      "storedStringField", "storedStrNumField", "storedStrBoolField", "storedDateField"
  };

  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "popularity", "1", "title", "w1",
        "storedStrNumField", "1", "storedStrBoolField", "F", "storedDateField", "1970-01-01T00:00:01.000Z",
        "description", "w1"));
    assertU(adoc("id", "2", "popularity", "2", "title", "w2 2asd asdd didid",
        "storedStrNumField", "2", "storedStrBoolField", "T", "storedDateField", "1970-01-01T00:00:02.000Z",
        "description", "w2 2asd asdd didid"));
    assertU(adoc("id", "3", "popularity", "3", "title", "w3",
        "storedStrNumField", "3", "storedStrBoolField", "F", "storedDateField", "1970-01-01T00:00:03.000Z",
        "description", "w3"));
    assertU(adoc("id", "4", "popularity", "4", "title", "w4",
        "storedStrNumField", "4", "storedStrBoolField", "T", "storedDateField", "1970-01-01T00:00:04.000Z",
        "description", "w4"));
    assertU(adoc("id", "5", "popularity", "5", "title", "w5",
        "storedStrNumField", "5", "storedStrBoolField", "F", "storedDateField", "1970-01-01T00:00:05.000Z",
        "description", "w5"));
    assertU(adoc("id", "6", "popularity", "6", "title", "w1 w2",
        "storedStrNumField", "6", "storedStrBoolField", "T", "storedDateField", "1970-01-01T00:00:06.000Z",
        "description", "w1 w2"));
    assertU(adoc("id", "7", "popularity", "7", "title", "w1 w2 w3 w4 w5",
        "storedStrNumField", "7", "storedStrBoolField", "F", "storedDateField", "1970-01-01T00:00:07.000Z",
        "description", "w1 w2 w3 w4 w5 w8"));
    assertU(adoc("id", "8", "popularity", "8", "title", "w1 w1 w1 w2 w2 w8",
        "storedStrNumField", "8", "storedStrBoolField", "T", "storedDateField", "1970-01-01T00:00:08.000Z",
        "description", "w1 w1 w1 w2 w2"));

    // a document without the popularity and the dv fields
    assertU(adoc("id", "42", "title", "NO popularity or isTrendy", "description", "NO popularity or isTrendy"));

    assertU(commit());

    for (String field : FIELDS) {
      loadFeature(field, PrefetchingFieldValueFeature.class.getName(),
          "{\"field\":\"" + field + "\"}");
    }
    loadModel("model", LinearModel.class.getName(), FIELDS,
        "{\"weights\":{\"popularity\":1.0,\"storedIntField\":1.0,\"storedLongField\":1.0," +
        "\"storedFloatField\":1.0,\"storedDoubleField\":1.0," +
        "\"storedStringField\":1.0,\"storedStrNumField\":1.0,\"storedStrBoolField\":1.0,\"storedDateField\":1.0}}");
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testRanking() throws Exception {
    // just a basic sanity check that we can work with the PrefetchingFieldValueFeature
    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    query.add("rq", "{!ltr model=model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");

    query.setQuery("*:*");
    query.remove("rows");
    query.add("rows", "8");
    query.remove("rq");
    query.add("rq", "{!ltr model=model reRankDocs=8}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='5'");
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "storedIntField" + "\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;

    loadFeature("storedLongField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "storedLongField" + "\"}");
    loadFeature("storedFloatField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "storedFloatField" + "\"}");

    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields
        .containsAll(List.of("storedIntField", "storedLongField", "storedFloatField")));
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "storedIntField" + "\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;

    loadFeature("storedLongField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore + "-other",
        "{\"field\":\"" + "storedLongField" + "\"}");

    assertEquals(ObservingPrefetchingFieldValueFeature.allPrefetchFields.size(), 1);
    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields.contains("storedLongField"));
  }

  @Test
  public void testThatPrefetchFieldsAreOnlyUpdatedAfterLoadingAllFeatures() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    ObservingPrefetchingFieldValueFeature.updateCount = 0;
    final String fstore = "feature-store-for-test";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "storedIntField" + "\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);
    assertEquals(1, ObservingPrefetchingFieldValueFeature.updateCount);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    ObservingPrefetchingFieldValueFeature.updateCount = 0;

    loadFeatures("prefetching_features.json");

    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields
        .containsAll(List.of("storedIntField", "storedLongField", "storedFloatField", "storedDoubleField")));
    assertEquals(1, ObservingPrefetchingFieldValueFeature.updateCount);
  }

  /**
   * This class is used to track the content of the prefetchFields and the updates so that we can assert,
   * that the logic to store them works as expected.
   */
  final public static class ObservingPrefetchingFieldValueFeature extends PrefetchingFieldValueFeature {
    public static Set<String> allPrefetchFields;
    public static int updateCount = 0;

    public ObservingPrefetchingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    public void setPrefetchFields(Set<String> fields) {
      super.setPrefetchFields(fields);
      // needed because all feature instances are updated. We just want to track updates with different fields, not all
      // updates of the instances
      if (fields != allPrefetchFields) {
        updateCount++;
      }
      allPrefetchFields = getPrefetchFields();
    }

    @Override
    public Feature.FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request,
                                              Query originalQuery, Map<String, String[]> efi) throws IOException {
      return new PrefetchingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }
  }
}
