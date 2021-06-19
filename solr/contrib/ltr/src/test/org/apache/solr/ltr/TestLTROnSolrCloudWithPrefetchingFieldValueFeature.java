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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ltr.feature.PrefetchingFieldValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.Test;

import java.util.List;

public class TestLTROnSolrCloudWithPrefetchingFieldValueFeature extends TestLTROnSolrCloudBase {

  private static final String STORED_FEATURE_STORE_NAME = "stored-feature-store";
  private static final String[] STORED_FIELD_NAMES = new String[]{"storedIntField", "storedLongField",
      "storedFloatField", "storedDoubleField", "storedStrNumField", "storedStrBoolField"};
  private static final String[] STORED_FEATURE_NAMES = new String[]{"storedIntFieldFeature", "storedLongFieldFeature",
      "storedFloatFieldFeature", "storedDoubleFieldFeature", "storedStrNumFieldFeature", "storedStrBoolFieldFeature"};
  private static final String STORED_MODEL_WEIGHTS = "{\"weights\":{\"storedIntFieldFeature\":0.1,\"storedLongFieldFeature\":0.1," +
      "\"storedFloatFieldFeature\":0.1,\"storedDoubleFieldFeature\":0.1," +
      "\"storedStrNumFieldFeature\":0.1,\"storedStrBoolFieldFeature\":0.1}}";

  private static final String DV_FEATURE_STORE_NAME = "dv-feature-store";
  private static final String[] DV_FIELD_NAMES = new String[]{"dvIntPopularity", "dvLongPopularity",
      "dvFloatPopularity", "dvDoublePopularity"};
  private static final String[] DV_FEATURE_NAMES = new String[]{"dvIntPopularityFeature", "dvLongPopularityFeature",
      "dvFloatPopularityFeature", "dvDoublePopularityFeature"};
  private static final String DV_MODEL_WEIGHTS = "{\"weights\":{\"dvIntPopularityFeature\":1.0,\"dvLongPopularityFeature\":1.0," +
      "\"dvFloatPopularityFeature\":1.0,\"dvDoublePopularityFeature\":1.0}}";

  private static final String FEATURE_STORE_NAME_PARTLY_INVALID = "partly-invalid-feature-store";
  private static final String[] FIELD_NAMES_PARTLY_INVALID = new String[]{"popularity", "storedIntField",
      "storedDoubleField", "field", "notExisting"};
  private static final String[] FEATURE_NAMES_PARTLY_INVALID = new String[]{"popularityFeature", "storedIntFieldFeature",
      "storedDoubleFieldFeature", "fieldFeature", "notExistingFeature"};
  private static final String MODEL_WEIGHTS_PARTLY_INVALID = "{\"weights\":{\"popularityFeature\":1.0," +
      "\"storedIntFieldFeature\":1.0,\"storedDoubleFieldFeature\":1.0,\"fieldFeature\":1.0,\"notExistingFeature\":1.0}}";

  @Test
  public void testRanking() throws Exception {
    // just a basic sanity check that we can work with the PrefetchingFieldValueFeature
    final SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.add("fl", "*,score");
    query.add("rows", "4");

    // Normal term match
    assertOrderOfTopDocuments(query, 8, List.of(1, 2, 3, 4));

    query.add("rq", "{!ltr model=stored-fields-model reRankDocs=8}");

    // reRanked match
    assertOrderOfTopDocuments(query, 8, List.of(8, 7, 6, 5));

    query.set("rows", "8");
    assertOrderOfTopDocuments(query, 8, List.of(8, 7, 6, 5, 4, 3, 2, 1));
  }

  @Test
  public void testDelegationToFieldValueFeature() throws Exception {
    assertU(adoc("id", "21", "popularity", "21", "title", "docValues"));
    assertU(adoc("id", "22", "popularity", "22", "title", "docValues"));
    assertU(adoc("id", "23", "popularity", "23", "title", "docValues"));
    assertU(adoc("id", "24", "popularity", "24", "title", "docValues"));
    assertU(commit());

    // only use fields that are not stored but have docValues
    // the PrefetchingFieldValueWeight should delegate the work to a FieldValueFeatureScorer
    final SolrQuery query = new SolrQuery("{!func}sub(24,field(popularity))");
    query.setRequestHandler("/query");
    query.add("fl", "*,score");
    query.add("fq", "title:docValues");
    query.add("rows", "4");

    // Normal term match
    assertOrderOfTopDocuments(query, 4, List.of(21, 22, 23, 24));

    // check that the PrefetchingFieldValueFeature delegates the work for docValue fields (reRanking works)
    query.add("rq", "{!ltr model=doc-value-model reRankDocs=4}");

    assertOrderOfTopDocuments(query, 4, List.of(24, 23, 22, 21));
  }

  @Test
  public void testBehaviorForInvalidAndEmptyFields() throws Exception {
    assertU(adoc("id", "41", "popularity", "41", "storedIntField", "100", "storedDoubleField", "10", "field", "neither stored nor docValues"));
    assertU(adoc("id", "42", "popularity", "42", "storedIntField", "200", "field", "neither stored nor docValues"));
    assertU(adoc("id", "43", "popularity", "43", "storedIntField", "300", "field", "neither stored nor docValues"));
    assertU(adoc("id", "44", "popularity", "44", "storedIntField", "400", "field", "neither stored nor docValues"));
    assertU(adoc("id", "45", "popularity", "45", "storedIntField", "500", "storedDoubleField", "50","field", "neither stored nor docValues"));
    assertU(adoc("id", "46", "popularity", "46", "storedIntField", "600","field", "neither stored nor docValues"));
    assertU(commit());

    final SolrQuery query = new SolrQuery("{!func}sub(46,field(popularity))");
    query.setRequestHandler("/query");
    query.add("fl", "*,score");
    query.add("fq", "field:docValues");

    // Normal term match
    assertOrderOfTopDocuments(query, 6, List.of(41, 42, 43, 44, 45, 46));

    // use a model and features with invalid fields
    // field has no docValues and is not stored, notExisting does not exist, storedDoubleField is empty for some docs
    query.set("fl", "*,score,features:[fv]");
    query.remove("rows");
    query.add("rq", "{!ltr model=partly-invalid-model reRankDocs=6}");

    // reRanked match, for invalid fields the default value should be returned
    assertOrderOfTopDocuments(query, 6, List.of(46, 45, 44, 43, 42, 41));
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/features==" +
        "'popularityFeature=46.0,storedIntFieldFeature=600.0,storedDoubleFieldFeature=0.0,fieldFeature=0.0,notExistingFeature=0.0'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/features==" +
        "'popularityFeature=45.0,storedIntFieldFeature=500.0,storedDoubleFieldFeature=50.0,fieldFeature=0.0,notExistingFeature=0.0'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/features==" +
        "'popularityFeature=44.0,storedIntFieldFeature=400.0,storedDoubleFieldFeature=0.0,fieldFeature=0.0,notExistingFeature=0.0'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/features==" +
        "'popularityFeature=43.0,storedIntFieldFeature=300.0,storedDoubleFieldFeature=0.0,fieldFeature=0.0,notExistingFeature=0.0'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[4]/features==" +
        "'popularityFeature=42.0,storedIntFieldFeature=200.0,storedDoubleFieldFeature=0.0,fieldFeature=0.0,notExistingFeature=0.0'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[5]/features==" +
        "'popularityFeature=41.0,storedIntFieldFeature=100.0,storedDoubleFieldFeature=10.0,fieldFeature=0.0,notExistingFeature=0.0'");
  }

  @Override
  void indexDocument(String collection, String id, String title, String description, int popularity)
    throws Exception{
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("title", title);
    doc.setField("description", description);
    doc.setField("popularity", popularity);
    // check that empty values will be read as default
    if (popularity != 1) {
      doc.setField("storedIntField", popularity);
      doc.setField("storedLongField", popularity);
      doc.setField("storedFloatField", ((float) popularity) / 10);
      doc.setField("storedDoubleField", ((double) popularity) / 10);
      doc.setField("storedStrNumField", popularity % 2 == 0 ? "F" : "T");
      doc.setField("storedStrBoolField", popularity % 2 == 0 ? "T" : "F");
    }
    solrCluster.getSolrClient().add(collection, doc);
  }

  @Override
  void loadModelsAndFeatures() throws Exception {
    setUpStoredFieldModelAndFeatures();
    setUpDocValueModelAndFeatures();
    setUpModelAndFeaturesForInvalidFields();
    reloadCollection(COLLECTION);
  }

  private void setUpStoredFieldModelAndFeatures() throws Exception {
    for (int i = 0; i < STORED_FEATURE_NAMES.length; i++) {
      loadFeature(
          STORED_FEATURE_NAMES[i],
          PrefetchingFieldValueFeature.class.getName(),
          STORED_FEATURE_STORE_NAME,
          "{\"field\":\"" + STORED_FIELD_NAMES[i] + "\"}"
      );
    }
    loadModel(
        "stored-fields-model",
        LinearModel.class.getName(),
        STORED_FEATURE_NAMES,
        STORED_FEATURE_STORE_NAME,
        STORED_MODEL_WEIGHTS
    );
  }

  public void setUpDocValueModelAndFeatures() throws Exception {
    for (int i = 0; i < DV_FEATURE_NAMES.length; i++) {
      loadFeature(
          DV_FEATURE_NAMES[i],
          PrefetchingFieldValueFeature.class.getName(),
          DV_FEATURE_STORE_NAME,
          "{\"field\":\"" + DV_FIELD_NAMES[i] + "\"}"
      );
    }
    loadModel("doc-value-model",
        LinearModel.class.getName(),
        DV_FEATURE_NAMES,
        DV_FEATURE_STORE_NAME,
        DV_MODEL_WEIGHTS);
  }

  public void setUpModelAndFeaturesForInvalidFields() throws Exception {
    for (int i = 0; i < FEATURE_NAMES_PARTLY_INVALID.length; i++) {
      loadFeature(
          FEATURE_NAMES_PARTLY_INVALID[i],
          PrefetchingFieldValueFeature.class.getName(),
          FEATURE_STORE_NAME_PARTLY_INVALID,
          "{\"field\":\"" + FIELD_NAMES_PARTLY_INVALID[i] + "\"}"
      );
    }
    loadModel("partly-invalid-model",
        LinearModel.class.getName(),
        FEATURE_NAMES_PARTLY_INVALID,
        FEATURE_STORE_NAME_PARTLY_INVALID,
        MODEL_WEIGHTS_PARTLY_INVALID);
  }
}
