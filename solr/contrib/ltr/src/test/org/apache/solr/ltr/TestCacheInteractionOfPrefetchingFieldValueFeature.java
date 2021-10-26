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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.misc.document.LazyDocument;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ltr.feature.PrefetchingFieldValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class TestCacheInteractionOfPrefetchingFieldValueFeature extends TestLTROnSolrCloudBase {
  private final String LAZY_FIELD_LOADING_CONFIG_KEY = "solr.query.enableLazyFieldLoading";

  private static final String FEATURE_STORE_NAME = "test";
  private static final int NUM_FEATURES = 6;
  private static final String[] FIELD_NAMES = new String[]{"storedIntField", "storedLongField",
      "storedFloatField", "storedDoubleField", "storedStrNumField", "storedStrBoolField"};
  private static final String[] FEATURE_NAMES = new String[]{"storedIntFieldFeature", "storedLongFieldFeature",
      "storedFloatFieldFeature", "storedDoubleFieldFeature", "storedStrNumFieldFeature", "storedStrBoolFieldFeature"};
  private static final String MODEL_WEIGHTS = "{\"weights\":{\"storedIntFieldFeature\":0.1,\"storedLongFieldFeature\":0.1," +
      "\"storedFloatFieldFeature\":0.1,\"storedDoubleFieldFeature\":0.1," +
      "\"storedStrNumFieldFeature\":0.1,\"storedStrBoolFieldFeature\":0.1}}";

  @Override
  void setupSolrCluster(int numShards, int numReplicas) throws Exception {
    // we do not want to test the scoring / ranking but the interaction with the cache
    // because the scoring itself behaves just like the FieldValueFeature
    // so just one shard, replica and node serve the purpose
    setupSolrCluster(1, 1, 1);
  }

  private SolrQuery composeSolrQuery() {
    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.setParam("rows", "8");
    query.setFields("id,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");
    return query;
  }

  @Test
  public void testSimpleQuery() throws Exception {
    // test the normal behavior of the PrefetchingFieldValueFeature
    // fields of all PrefetchingFieldValueFeatures are collected and read at once from the index
    // we assert, that all field values for these fields are present at the document from the start

    // build query and assert correct result size & order
    QueryResponse response = queryAndMakeAssertions(false);
    // get information about loaded fields and assert that all fields were loaded at once
    final Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;
    assertGroupedFieldLoading(response, loadedFields);
  }

  @Test
  public void testSimpleQueryBreakPrefetching() throws Exception {
    // test the difference to the behavior of the FieldValueFeature
    // to do so, simulate the behavior of the FieldValueFeature by breaking the prefetching
    // each PrefetchingFieldValueFeature only reads its field from the index
    // we assert, that only the currently used field is read for the document

    // NOTE: this test does not test the new logic of the PrefetchingFieldValueFeature but should illustrate
    // the difference to the FieldValueFeature

    // build query and assert correct result size & order
    QueryResponse response = queryAndMakeAssertions(true);
    // get information about loaded fields and assert that all fields were loaded separately
    final Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;
    assertSeparateFieldLoading(response, loadedFields);
  }

  public QueryResponse queryAndMakeAssertions(boolean breakPrefetching) throws Exception {
    ObservingPrefetchingFieldValueFeature.setBreakPrefetching(breakPrefetching);
    ObservingPrefetchingFieldValueFeature.loadedFields = new HashMap<>();

    String lazyLoadingEnabled = String.valueOf(random().nextBoolean());
    System.setProperty(LAZY_FIELD_LOADING_CONFIG_KEY, lazyLoadingEnabled);

    // needed to clear cache because we make assertions on its content
    reloadCollection(COLLECTION);

    SolrQuery query = composeSolrQuery();

    QueryResponse queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);

    Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;

    assertEquals(loadedFields.size(), queryResponse.getResults().size());
    assertTheResponse(queryResponse);
    return queryResponse;
  }

  private void assertSeparateFieldLoading(QueryResponse queryResponse, Map<String, List<List<String>>> loadedFields) {
    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        // doc with id 1 has no values set for 3 of the 6 feature fields
        // so for this doc NUM_FEATURES - 3 means that all the fields were loaded after each other
        assertEquals(NUM_FEATURES - 3, loadedFields.get(docId).stream()
            .filter(fieldLoadedList -> fieldLoadedList.size() == 1)
            .count());
      } else {
        // each single field used for a feature gets loaded separately
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            .filter(fieldLoadedList -> fieldLoadedList.size() == 1)
            .count());
      }
    }
  }

  private void assertGroupedFieldLoading(QueryResponse queryResponse, Map<String, List<List<String>>> loadedFields) {
    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            // doc with id 1 has no values set for 3 of the 6 feature fields
            // so for this doc NUM_FEATURES - 3 means that all the fields were loaded at once
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES - 3)
            .count());
      } else {
        // all the fields were loaded at once
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES)
            .count());
      }
    }
  }

  private void assertTheResponse(QueryResponse queryResponse) {
    assertEquals(8, queryResponse.getResults().getNumFound());
    assertEquals("8", queryResponse.getResults().get(0).get("id").toString());
    assertEquals(expectedFeatures[7],
        queryResponse.getResults().get(0).get("features").toString());
    assertEquals("7", queryResponse.getResults().get(1).get("id").toString());
    assertEquals(expectedFeatures[6],
        queryResponse.getResults().get(1).get("features").toString());
    assertEquals("6", queryResponse.getResults().get(2).get("id").toString());
    assertEquals(expectedFeatures[5],
        queryResponse.getResults().get(2).get("features").toString());
    assertEquals("5", queryResponse.getResults().get(3).get("id").toString());
    assertEquals(expectedFeatures[4],
        queryResponse.getResults().get(3).get("features").toString());
    assertEquals("4", queryResponse.getResults().get(4).get("id").toString());
    assertEquals(expectedFeatures[3],
        queryResponse.getResults().get(4).get("features").toString());
    assertEquals("3", queryResponse.getResults().get(5).get("id").toString());
    assertEquals(expectedFeatures[2],
        queryResponse.getResults().get(5).get("features").toString());
    assertEquals("2", queryResponse.getResults().get(6).get("id").toString());
    assertEquals(expectedFeatures[1],
        queryResponse.getResults().get(6).get("features").toString());
    assertEquals("1", queryResponse.getResults().get(7).get("id").toString());
    assertEquals(expectedFeatures[0],
      queryResponse.getResults().get(7).get("features").toString());
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

  private static final String[] expectedFeatures = createExpectations();

  private static String[] createExpectations() {
    // doc 1 has no values for some fields so we have to set the defaults
    final List<String> expectedFeatures = new ArrayList<>(Collections.singleton(
        "storedIntFieldFeature=-1.0,storedLongFieldFeature=-2.0,storedFloatFieldFeature=-3.0," +
            "storedDoubleFieldFeature=0.0,storedStrNumFieldFeature=0.0,storedStrBoolFieldFeature=0.0"));
    expectedFeatures.addAll(IntStream.rangeClosed(2, 8).boxed()
        .map(docId ->
            String.format(Locale.ROOT, "storedIntFieldFeature=%s.0,storedLongFieldFeature=%s.0,storedFloatFieldFeature=0.%s," +
                    "storedDoubleFieldFeature=0.%s,storedStrNumFieldFeature=%s,storedStrBoolFieldFeature=%s",
                docId, docId, docId, docId, docId % 2 == 0 ? "0.0" : "1.0", docId % 2 == 0 ? "1.0" : "0.0"))
        .collect(toList()));
    return expectedFeatures.toArray(String[]::new);
  }

  @Override
  void loadModelsAndFeatures() throws Exception {
    for (int i = 0 ; i < FEATURE_NAMES.length; i++) {
      loadFeature(
          FEATURE_NAMES[i],
          ObservingPrefetchingFieldValueFeature.class.getName(),
          FEATURE_STORE_NAME,
          "{\"field\":\"" + FIELD_NAMES[i] + "\"}"
      );
    }
    loadModel(
        "powpularityS-model",
        LinearModel.class.getName(),
        FEATURE_NAMES,
        FEATURE_STORE_NAME,
        MODEL_WEIGHTS
    );
    reloadCollection(COLLECTION);
  }

  public void setUpDocValueModelAndFeatures() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "popularity", "1", "title", "w1",
        "storedStrNumField", "1", "storedStrBoolField", "F", "description", "w1"));
    assertU(adoc("id", "2", "popularity", "2", "title", "w2 2asd asdd didid",
        "storedStrNumField", "2", "storedStrBoolField", "T", "description", "w2 2asd asdd didid"));
    assertU(adoc("id", "3", "popularity", "3", "title", "w3",
        "storedStrNumField", "3", "storedStrBoolField", "F", "description", "w3"));
    assertU(adoc("id", "4", "popularity", "4", "title", "w4",
        "storedStrNumField", "4", "storedStrBoolField", "T", "description", "w4"));
    assertU(adoc("id", "5", "popularity", "5", "title", "w5",
        "storedStrNumField", "5", "storedStrBoolField", "F", "description", "w5"));
    assertU(adoc("id", "6", "popularity", "6", "title", "w1 w2",
        "storedStrNumField", "6", "storedStrBoolField", "T", "description", "w1 w2"));
    assertU(adoc("id", "7", "popularity", "7", "title", "w1 w2 w3 w4 w5",
        "storedStrNumField", "7", "storedStrBoolField", "F", "description", "w1 w2 w3 w4 w5 w8"));
    assertU(adoc("id", "8", "popularity", "8", "title", "w1 w1 w1 w2 w2 w8",
        "storedStrNumField", "8", "storedStrBoolField", "T", "description", "w1 w1 w1 w2 w2"));

    // a document without the popularity and the stored fields
    assertU(adoc("id", "42", "title", "NO popularity or isTrendy", "description", "NO popularity or isTrendy"));

    assertU(commit());

    for (String field : FIELD_NAMES) {
      loadFeature(field, PrefetchingFieldValueFeature.class.getName(),
          "{\"field\":\"" + field + "\"}");
    }
    loadModel("model", LinearModel.class.getName(), FIELD_NAMES,
        "{\"weights\":{\"storedIntField\":1.0,\"storedLongField\":1.0," +
            "\"storedFloatField\":1.0,\"storedDoubleField\":1.0," +
            "\"storedStringField\":1.0,\"storedStrNumField\":1.0,\"storedStrBoolField\":1.0}}");
    reloadCollection(COLLECTION);
  }

  /**
   * This class is used to track which fields of the documents were loaded.
   * It is used to assert that the prefetching mechanism works as intended.
   */
  final public static class ObservingPrefetchingFieldValueFeature extends PrefetchingFieldValueFeature {
    // this boolean can be used to mimic the behavior of the original FieldValueFeature
    // this is used to compare the behavior of the two Features so that the purpose of the Prefetching on becomes clear
    private static final AtomicBoolean breakPrefetching = new AtomicBoolean(false);
    public static Map<String, List<List<String>>> loadedFields = new HashMap<>();

    public ObservingPrefetchingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    public void setPrefetchFields(SortedSet<String> fields, boolean keepFeaturesFinal) {
      if(breakPrefetching.get()) {
        // only prefetch own field (and id needed for map-building below)
        super.setPrefetchFields(new TreeSet<>(Set.of(getField(), "id")), keepFeaturesFinal);
      } else {
        // also prefetch all fields that all other PrefetchingFieldValueFeatures need
        fields.add("id");
        super.setPrefetchFields(fields, keepFeaturesFinal);
      }
    }

    public ObservingPrefetchingFieldValueFeature clone() {
      ObservingPrefetchingFieldValueFeature foo = new ObservingPrefetchingFieldValueFeature(this.name, this.paramsToMap());
      foo.setField(this.getField());
      foo.setIndex(this.getIndex());
      return foo;
    }

    public static void setBreakPrefetching(boolean disable) {
      breakPrefetching.set(disable);
    }

    @Override
    public FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores,
                                     SolrQueryRequest request, Query originalQuery, Map<String,String[]> efi)
        throws IOException {
      return new ObservingPrefetchingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }

    public class ObservingPrefetchingFieldValueFeatureWeight extends PrefetchingFieldValueFeatureWeight {

      public ObservingPrefetchingFieldValueFeatureWeight(IndexSearcher searcher, SolrQueryRequest request,
                                                         Query originalQuery, Map<String, String[]> efi) {
        super(searcher, request, originalQuery, efi);
      }

      @Override
      public FeatureScorer scorer(LeafReaderContext context) throws IOException {
        if (schemaField != null && !schemaField.stored() && schemaField.hasDocValues()) {
          return super.scorer(context);
        }
        return new ObservingPrefetchingFieldValueFeatureScorer(this, context,
            DocIdSetIterator.all(DocIdSetIterator.NO_MORE_DOCS));
      }

      public class ObservingPrefetchingFieldValueFeatureScorer extends PrefetchingFieldValueFeatureScorer {
        public ObservingPrefetchingFieldValueFeatureScorer(FeatureWeight weight,
                                                  LeafReaderContext context, DocIdSetIterator itr) {
          super(weight, context, itr);
        }

        @Override
        public float score() throws IOException {
          // same 2 lines as super.score(), but we need the document for cache inspection
          final Document document = fetchDocument();
          float fieldValue = super.parseStoredFieldValue(document.getField(getField()));

          List<String> loadedLazyFields = document.getFields().stream()
              .filter(field -> field instanceof LazyDocument.LazyField)
              .map(indexableField -> (LazyDocument.LazyField) indexableField)
              .filter(LazyDocument.LazyField::hasBeenLoaded)  // collect lazy fields that were loaded (value was read)
              .map(LazyDocument.LazyField::name)
              .filter(fieldName -> !fieldName.equals("id")) // ignore id because it is not assigned to a feature
              .collect(Collectors.toList());

          List<String> loadedStoredFields = document.getFields().stream()
              .filter(field -> field instanceof StoredField)
              .map(indexableField -> (StoredField) indexableField)
              .map(Field::name)  // collect stored fields that were loaded
              .filter(fieldName -> !fieldName.equals("id")) // ignore id because it is not assigned to a feature
              .collect(Collectors.toList());

          loadedLazyFields.addAll(loadedStoredFields);
          String id = document.get("id");
          handleLoadedFields(id, loadedLazyFields);
          return fieldValue;

        }

        private synchronized void handleLoadedFields(String id, List<String> loadedLazyFields) {
          // for each document id we create a history of the fields that were loaded
          // each inner list represents a state of loaded fields at the point when a feature value was calculated
          // so each feature adds one state to this list
          Map<String, List<List<String>>> presentLoadedFields = loadedFields;
          List<List<String>> maybePresentList = presentLoadedFields.getOrDefault(id, new ArrayList<>());
          maybePresentList.add(loadedLazyFields);
          presentLoadedFields.put(id, maybePresentList);
          loadedFields = presentLoadedFields;
        }
      }
    }
  }
}
