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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.DefaultValueFieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.FieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.NumericDocValuesFieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.SortedDocValuesFieldValueFeatureScorer;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFieldValueFeature extends TestRerankBase {

  private static final float FIELD_VALUE_FEATURE_DEFAULT_VAL = 0.0f;

  private static final String[] FIELDS = {
      "popularity",
      "dvIntPopularity", "dvLongPopularity",
      "dvFloatPopularity", "dvDoublePopularity",
      "dvStringPopularity", "dvBoolPopularity"
  };

  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "popularity", "1", "title", "w1",
        "dvStringPopularity", "1", "dvBoolPopularity", "F",
        "description", "w1", "isTrendy", "true"));
    assertU(adoc("id", "2", "popularity", "2", "title", "w2 2asd asdd didid",
        "dvStringPopularity", "2", "dvBoolPopularity", "T",
        "description", "w2 2asd asdd didid"));
    assertU(adoc("id", "3", "popularity", "3", "title", "w3",
        "dvStringPopularity", "3", "dvBoolPopularity", "F",
        "description", "w3", "isTrendy", "true"));
    assertU(adoc("id", "4", "popularity", "4", "title", "w4",
        "dvStringPopularity", "4", "dvBoolPopularity", "T",
        "description", "w4", "isTrendy", "false"));
    assertU(adoc("id", "5", "popularity", "5", "title", "w5",
        "dvStringPopularity", "5", "dvBoolPopularity", "F",
        "description", "w5", "isTrendy", "true"));
    assertU(adoc("id", "6", "popularity", "6", "title", "w1 w2",
        "dvStringPopularity", "6", "dvBoolPopularity", "T",
        "description", "w1 w2", "isTrendy", "false"));
    assertU(adoc("id", "7", "popularity", "7", "title", "w1 w2 w3 w4 w5",
        "dvStringPopularity", "7", "dvBoolPopularity", "F",
        "description", "w1 w2 w3 w4 w5 w8", "isTrendy", "true"));
    assertU(adoc("id", "8", "popularity", "8", "title", "w1 w1 w1 w2 w2 w8",
        "dvStringPopularity", "8", "dvBoolPopularity", "T",
        "description", "w1 w1 w1 w2 w2", "isTrendy", "false"));

    // a document without the popularity and the dv fields
    assertU(adoc("id", "42", "title", "NO popularity", "description", "NO popularity"));

    assertU(commit());

    for (String field : FIELDS) {
      loadFeature(field, FieldValueFeature.class.getName(),
          "{\"field\":\"" + field + "\"}");
    }
   loadModel("model", LinearModel.class.getName(), FIELDS,
       "{\"weights\":{\"popularity\":1.0,\"dvIntPopularity\":1.0,\"dvLongPopularity\":1.0," +
           "\"dvFloatPopularity\":1.0,\"dvDoublePopularity\":1.0," +
           "\"dvStringPopularity\":1.0,\"dvBoolPopularity\":1.0}}");
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testRanking() throws Exception {
    SolrQuery query = new SolrQuery();
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
  public void testIfADocumentDoesntHaveAFieldDefaultValueIsReturned() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("fl", "*, score");
    query.add("rows", "4");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");

    query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=model reRankDocs=4}");
    query.add("fl", "[fv]");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'popularity=0.0,dvIntPopularity=0.0,dvLongPopularity=0.0," +
            "dvFloatPopularity=0.0,dvDoublePopularity=0.0," +
            "dvStringPopularity=0.0,dvBoolPopularity=0.0'}");
  }

  @Test
  public void testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned() throws Exception {
    for (String field : FIELDS) {
      final String fstore = "testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned"+field;

      loadFeature(field+"42", FieldValueFeature.class.getName(), fstore,
              "{\"field\":\""+field+"\",\"defaultValue\":\"42.0\"}");

      SolrQuery query = new SolrQuery();
      query.setQuery("id:42");
      query.add("fl", "*, score");
      query.add("rows", "4");

      loadModel(field+"-model42", LinearModel.class.getName(),
              new String[] {field+"42"}, fstore, "{\"weights\":{\""+field+"42\":1.0}}");

      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");
      query = new SolrQuery();
      query.setQuery("id:42");
      query.add("rq", "{!ltr model="+field+"-model42 reRankDocs=4}");
      query.add("fl", "[fv]");
      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ("/query" + query.toQueryString(),
              "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector(field+"42","42.0")+"'}");
    }
  }

  @Test
  public void testIfADocumentDoesntHaveAFieldTheDefaultValueFromSchemaIsReturned() throws Exception {
    final String[] fieldsWithDefaultValues = {"dvIntField", "dvLongField", "dvFloatField"};

    double fieldCounter = -1.0;
    for (String field : fieldsWithDefaultValues) {
      final String fstore = "testIfADocumentDoesntHaveAFieldTheDefaultValueFromSchemaIsReturned"+field;

      assertU(adoc("id", "21"));
      assertU(commit());

      loadFeature(field, FieldValueFeature.class.getName(), fstore,
              "{\"field\":\""+field+"\"}");
      loadModel(field+"-model", LinearModel.class.getName(),
          new String[] {field}, fstore, "{\"weights\":{\"" + field + "\":1.0}}");

      SolrQuery query = new SolrQuery();
      query.setQuery("id:21");
      query.add("rq", "{!ltr model="+field+"-model reRankDocs=4}");
      query.add("fl", "[fv]");

      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ("/query" + query.toQueryString(),
              "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector(field, String.valueOf(fieldCounter))+"'}");

      fieldCounter--;
    }
  }

  @Test
  public void testThatFieldValueFeatureScorerIsUsedAndDefaultIsReturned() throws Exception {
    // this tests the case that we create a feature for a non-existent field
    // using a different fstore to avoid a clash with the other tests
    final String fstore = "testThatFieldValueFeatureScorerIsUsedAndDefaultIsReturned";
    loadFeature("not-existing-field", ObservingFieldValueFeature.class.getName(), fstore,
            "{\"field\":\"cowabunga\"}");

    loadModel("not-existing-field-model", LinearModel.class.getName(),
            new String[] {"not-existing-field"}, fstore, "{\"weights\":{\"not-existing-field\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=not-existing-field-model reRankDocs=4}");
    query.add("fl", "[fv]");
    ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils
                    .toFeatureVector("not-existing-field",Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL))+"'}");
    assertEquals(FieldValueFeatureScorer.class.getName(), ObservingFieldValueFeature.usedScorerClass);
  }

  @Test
  public void testThatDefaultFieldValueScorerIsUsedAndDefaultIsReturned() throws Exception {
    // this tests the case that no document contains docValues for the provided existing field
    final String fstore = "testThatDefaultFieldValueScorerIsUsedAndDefaultIsReturned";
    loadFeature("dvTestField", ObservingFieldValueFeature.class.getName(), fstore,
            "{\"field\":\"dvTestField\"}");

    loadModel("dvTestField-model", LinearModel.class.getName(),
            new String[] {"dvTestField"}, fstore, "{\"weights\":{\"dvTestField\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=dvTestField-model reRankDocs=4}");
    query.add("fl", "[fv]");

    ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils
                    .toFeatureVector("dvTestField",Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL))+"'}");
    assertEquals(DefaultValueFieldValueFeatureScorer.class.getName(), ObservingFieldValueFeature.usedScorerClass);
  }

  @Test
  public void testBooleanValue() throws Exception {
    final String fstore = "test_boolean_store";
    loadFeature("trendy", FieldValueFeature.class.getName(), fstore,
            "{\"field\":\"isTrendy\"}");

    loadModel("trendy-model", LinearModel.class.getName(),
            new String[] {"trendy"}, fstore, "{\"weights\":{\"trendy\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("id:4");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","0.0")+"'}");

    query = new SolrQuery();
    query.setQuery("id:5");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","1.0")+"'}");

    // check default value is false
    query = new SolrQuery();
    query.setQuery("id:2");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(),
            "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector("trendy","0.0")+"'}");
  }

  @Test
  public void testThatExceptionIsThrownForUnsupportedType() throws Exception {
    final String fstore = "test_store";

    assertU(adoc("id", "21", "title", "multivalued not supported", "dvUnsupportedField", "wow value"));
    assertU(commit());

    loadFeature("dvUnsupportedField", FieldValueFeature.class.getName(), fstore,
            "{\"field\":\"dvUnsupportedField\"}");

    loadModel("dvUnsupportedField-model", LinearModel.class.getName(),
            new String[] {"dvUnsupportedField"}, fstore, "{\"weights\":{\"dvUnsupportedField\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("id:21");
    query.add("rq", "{!ltr model=dvUnsupportedField-model reRankDocs=4}");
    query.add("fl", "[fv]");

    assertJQ("/query" + query.toQueryString(),
        "/error/msg/=='java.lang.IllegalArgumentException: Doc values type SORTED_SET of field dvUnsupportedField is not supported!'");
  }

  @Test
  public void testThatCorrectFieldValueFeatureIsUsedForDocValueTypes() throws Exception {
    final String[][] fieldsWithDifferentTypes = {
        new String[]{"dvIntPopularity", NumericDocValuesFieldValueFeatureScorer.class.getName()},
        new String[]{"dvStringPopularity", SortedDocValuesFieldValueFeatureScorer.class.getName()},
        new String[]{"noDocValuesField", FieldValueFeatureScorer.class.getName()}
    };

    for (String[] fieldAndScorerClass : fieldsWithDifferentTypes) {
      String field = fieldAndScorerClass[0];
      final String fstore = "testThatCorrectFieldValueFeatureIsUsedForDocValueTypes"+field;

      assertU(adoc("id", "21", field, "1"));
      assertU(commit());

      loadFeature(field, ObservingFieldValueFeature.class.getName(), fstore,
          "{\"field\":\""+field+"\"}");
      loadModel(field+"-model", LinearModel.class.getName(),
          new String[] {field}, fstore, "{\"weights\":{\"" + field + "\":1.0}}");

      SolrQuery query = new SolrQuery();
      query.setQuery("id:21");
      query.add("rq", "{!ltr model="+field+"-model reRankDocs=4}");
      query.add("fl", "[fv]");

      ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ("/query" + query.toQueryString(),
          "/response/docs/[0]/=={'[fv]':'"+FeatureLoggerTestUtils.toFeatureVector(field, "1.0")+"'}");
      assertEquals(fieldAndScorerClass[1], ObservingFieldValueFeature.usedScorerClass);
    }
  }

  @Test
  public void testParamsToMap() throws Exception {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<String,Object>();
    params.put("field", "field"+random().nextInt(10));
    doTestParamsToMap(FieldValueFeature.class.getName(), params);
  }

  @Test
  public void testThatStringValuesAreCorrectlyParsed() throws Exception {
    final String[][] inputsAndTests = {
      new String[]{"T", "/response/docs/[0]/=={'[fv]':'" +
          FeatureLoggerTestUtils.toFeatureVector("dvStrNumField", "1.0")+"'}"},
      new String[]{"F", "/response/docs/[0]/=={'[fv]':'" +
          FeatureLoggerTestUtils.toFeatureVector("dvStrNumField", "0.0")+"'}"},
      new String[]{"-7324.427", "/response/docs/[0]/=={'[fv]':'" +
          FeatureLoggerTestUtils.toFeatureVector("dvStrNumField", "-7324.427")+"'}"},
      new String[]{"532", "/response/docs/[0]/=={'[fv]':'" +
          FeatureLoggerTestUtils.toFeatureVector("dvStrNumField", "532.0")+"'}"},
      new String[]{"notanumber", "/error/msg/=='org.apache.solr.ltr.feature.FeatureException: " +
          "Cannot parse value notanumber of field dvStrNumField to float.'"}
    };

    final String fstore = "testThatStringValuesAreCorrectlyParsed";
    loadFeature("dvStrNumField", FieldValueFeature.class.getName(), fstore,
        "{\"field\":\"" + "dvStrNumField" + "\"}");
    loadModel("dvStrNumField-model", LinearModel.class.getName(),
        new String[]{"dvStrNumField"}, fstore, "{\"weights\":{\"" + "dvStrNumField" + "\":1.0}}");

    for (String[] inputAndTest : inputsAndTests) {
      assertU(adoc("id", "21", "dvStrNumField", inputAndTest[0]));
      assertU(commit());

      SolrQuery query = new SolrQuery();
      query.setQuery("id:21");
      query.add("rq", "{!ltr model=" + "dvStrNumField" + "-model reRankDocs=4}");
      query.add("fl", "[fv]");

      assertJQ("/query" + query.toQueryString(), inputAndTest[1]);
    }
  }

  /**
   * This class is used to track which specific FieldValueFeature is used so that we can test, whether the
   * fallback mechanism works correctly.
   */
  public static class ObservingFieldValueFeature extends FieldValueFeature {
    static String usedScorerClass;

    public ObservingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    @Override
    public Feature.FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request,
                                              Query originalQuery, Map<String, String[]> efi) throws IOException {
      return new ObservingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }

    public class ObservingFieldValueFeatureWeight extends FieldValueFeatureWeight {
      public ObservingFieldValueFeatureWeight(IndexSearcher searcher, SolrQueryRequest request,
                                              Query originalQuery, Map<String, String[]> efi) {
        super(searcher, request, originalQuery, efi);
      }

      @Override
      public FeatureScorer scorer(LeafReaderContext context) throws IOException {
         FeatureScorer scorer = super.scorer(context);
         usedScorerClass = scorer.getClass().getName();
         return scorer;
      }
    }
  }
}
