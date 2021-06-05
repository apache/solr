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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.misc.document.LazyDocument;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.feature.PrefetchingFieldValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class TestLTROnSolrCloudWithPrefetchingFieldValueFeature extends TestRerankBase {

  private MiniSolrCloudCluster solrCluster;
  String solrconfig = "solrconfig-ltr.xml";
  String solrconfigLazy = "solrconfig-ltr-lazy.xml";
  String schema = "schema.xml";

  SortedMap<ServletHolder,String> extraServlets = null;

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
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    restTestHarness.close();
    restTestHarness = null;
    solrCluster.shutdown();
    super.tearDown();
  }

  public void afterSetUp(String configName) throws Exception {
    extraServlets = setupTestInit(configName, schema, true);
    System.setProperty("enable.update.log", "true");

    // we do not want to test the scoring / ranking but the interaction with the cache
    // because the scoring itself behaves just like the FieldValueFeature
    // so just one shard, replica and node serve the purpose
    setupSolrCluster(1, 1, 1);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    ObservingPrefetchingFieldValueFeature.setBreakPrefetching(false);
    ObservingPrefetchingFieldValueFeature.loadedFields = new HashMap<>();
    afterSetUp(solrconfig);

    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.setParam("rows", "8");
    query.setFields("id,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");

    QueryResponse queryResponse = solrCluster.getSolrClient().query(COLLECTION,query);

    Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;

    assertEquals(loadedFields.size(), queryResponse.getResults().size());
    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            // doc with id 1 has no values set for 3 of the 6 feature fields
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES - 3)
            .count());
      } else {
        // all the fields were loaded at once
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES)
            .count());
      }
    }
    assertTheResponse(queryResponse);
  }

  @Test
  public void testSimpleQueryLazy() throws Exception {
    ObservingPrefetchingFieldValueFeature.setBreakPrefetching(false);
    ObservingPrefetchingFieldValueFeature.loadedFields = new HashMap<>();
    afterSetUp(solrconfigLazy);

    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.setParam("rows", "8");
    query.setFields("id,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");

    QueryResponse queryResponse = solrCluster.getSolrClient().query(COLLECTION,query);

    Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;

    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            // doc with id 1 has no values set for 3 of the 6 feature fields
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES - 3)
            .count());
      } else {
        // all the fields were loaded at once
        assertEquals(NUM_FEATURES, loadedFields.get(docId).stream()
            .filter(fieldLoadedList -> fieldLoadedList.size() == NUM_FEATURES)
            .count());
      }
    }
    assertTheResponse(queryResponse);
  }

  @Test
  public void testSimpleQueryBreakPrefetching() throws Exception {
    ObservingPrefetchingFieldValueFeature.setBreakPrefetching(true);
    ObservingPrefetchingFieldValueFeature.loadedFields = new HashMap<>();
    afterSetUp(solrconfig);

    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.setParam("rows", "8");
    query.setFields("id,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");

    QueryResponse queryResponse =
        solrCluster.getSolrClient().query(COLLECTION,query);

    Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;

    assertEquals(loadedFields.size(), queryResponse.getResults().size());
    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        // doc with id 1 has no values set for 3 of the 6 feature fields
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
    assertTheResponse(queryResponse);
  }

  @Test
  public void testSimpleQueryLazyBreakPrefetching() throws Exception {
    ObservingPrefetchingFieldValueFeature.setBreakPrefetching(true);
    ObservingPrefetchingFieldValueFeature.loadedFields = new HashMap<>();
    afterSetUp(solrconfigLazy);

    SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setRequestHandler("/query");
    query.setParam("rows", "8");
    query.setFields("id,features:[fv]");
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=8}");

    QueryResponse queryResponse = solrCluster.getSolrClient().query(COLLECTION,query);

    Map<String, List<List<String>>> loadedFields = ObservingPrefetchingFieldValueFeature.loadedFields;

    for (SolrDocument doc : queryResponse.getResults()) {
      String docId = (String) doc.getFirstValue("id");
      if (docId.equals("1")) {
        // doc with id 1 has no values set for 3 of the 6 feature fields
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
    assertTheResponse(queryResponse);
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

  private void setupSolrCluster(int numShards, int numReplicas, int numServers) throws Exception {
    JettyConfig jc = buildJettyConfig("/solr");
    jc = JettyConfig.builder(jc).withServlets(extraServlets).build();
    solrCluster = new MiniSolrCloudCluster(numServers, tmpSolrHome.toPath(), jc);
    File configDir = tmpSolrHome.toPath().resolve("collection1/conf").toFile();
    solrCluster.uploadConfigSet(configDir.toPath(), "conf1");

    solrCluster.getSolrClient().setDefaultCollection(COLLECTION);

    createCollection(COLLECTION, "conf1", numShards, numReplicas);
    indexDocuments(COLLECTION);
    for (JettySolrRunner solrRunner : solrCluster.getJettySolrRunners()) {
      if (!solrRunner.getCoreContainer().getCores().isEmpty()){
        String coreName = solrRunner.getCoreContainer().getCores().iterator().next().getName();
        restTestHarness = new RestTestHarness(() -> solrRunner.getBaseUrl().toString() + "/" + coreName);
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
      fail("Could not create collection. Response" + response.toString());
    }
    solrCluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }

  void indexDocument(String collection, String id, String title, String description, int popularity)
    throws Exception{
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", id);
    doc.setField("title", title);
    doc.setField("description", description);
    doc.setField("popularity", popularity);
    if (popularity != 1) {
      // check that empty values will be read as default
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

  private void indexDocuments(final String collection)
       throws Exception {
    final int collectionSize = 8;
    // put documents in random order to check that advanceExact is working correctly
    List<Integer> docIds = IntStream.rangeClosed(1, collectionSize).boxed().collect(toList());
    Collections.shuffle(docIds, random());

    int docCounter = 1;
    for (int docId : docIds) {
      final int popularity = docId;
      indexDocument(collection, String.valueOf(docId), "a1", "bloom", popularity);
      // maybe commit in the middle in order to check that everything works fine for multi-segment case
      if (docCounter == collectionSize / 2 && random().nextBoolean()) {
        solrCluster.getSolrClient().commit(collection);
      }
      docCounter++;
    }
    solrCluster.getSolrClient().commit(collection, true, true);
  }

  private void loadModelsAndFeatures() throws Exception {
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

  /**
   * This class is used to track which fields of the documents were loaded.
   * It is used to assert that the prefetching mechanism works as intended.
   */
  final public static class ObservingPrefetchingFieldValueFeature extends PrefetchingFieldValueFeature {
    private String field;
    private Set<String> prefetchFields;
    // this boolean can be used to mimic the behavior of the original FieldVAlueFeature
    // this is used to compare the behavior of the two Features so that the purpose of the Prefetching on becomes clear
    private static final AtomicBoolean breakPrefetching = new AtomicBoolean(false);
    public static Map<String, List<List<String>>> loadedFields = new HashMap<>();

    public ObservingPrefetchingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    public void setField(String field) {
      this.field = field;
      super.setField(field);
    }

    public void setPrefetchFields(Set<String> fields) {
      if(breakPrefetching.get()) {
        prefetchFields = Set.of(field, "id"); // only prefetch own field (and id needed for map-building below)
        super.setPrefetchFields(Set.of(field, "id"));
      } else {
        fields.add("id");
        prefetchFields = fields; // also prefetch all fields that all other PrefetchingFieldValueFeatures need
        super.setPrefetchFields(fields);
      }
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
      private final SchemaField schemaField;
      private final SolrDocumentFetcher docFetcher;

      public ObservingPrefetchingFieldValueFeatureWeight(IndexSearcher searcher, SolrQueryRequest request,
                                                         Query originalQuery, Map<String, String[]> efi) {
        super(searcher, request, originalQuery, efi);
        if (searcher instanceof SolrIndexSearcher) {
          schemaField = ((SolrIndexSearcher) searcher).getSchema().getFieldOrNull(field);
        } else {
          schemaField = null;
        }
        this.docFetcher = request.getSearcher().getDocFetcher();
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

        LeafReaderContext context = null;
        public ObservingPrefetchingFieldValueFeatureScorer(FeatureWeight weight,
                                                  LeafReaderContext context, DocIdSetIterator itr) {
          super(weight, context, itr);
          this.context = context;
        }

        @Override
        public float score() throws IOException {
          try {
            final Document document = docFetcher.doc(context.docBase + itr.docID(), prefetchFields);

            float fieldValue = super.score();

            List<String> loadedLazyFields = document.getFields().stream()
                .filter(field -> field instanceof LazyDocument.LazyField)
                .map(indexableField -> (LazyDocument.LazyField) indexableField)
                .filter(LazyDocument.LazyField::hasBeenLoaded)
                .map(LazyDocument.LazyField::name)
                .filter(fieldName -> !fieldName.equals("id"))
                .collect(Collectors.toList());

            List<String> loadedStoredFields = document.getFields().stream()
                .filter(field -> field instanceof StoredField)
                .map(indexableField -> (StoredField) indexableField)
                .map(Field::name)
                .filter(fieldName -> !fieldName.equals("id"))
                .collect(Collectors.toList());

            loadedLazyFields.addAll(loadedStoredFields);
            String id = document.get("id");
            // this can be null if
            if (id != null) {
              handleLoadedFields(id, loadedLazyFields);
            }

            return fieldValue;

          } catch (final IOException e) {
            throw new FeatureException(
                e.toString() + ": " +
                    "Unable to extract feature for "
                    + name, e);
          }
        }

        private synchronized void handleLoadedFields(String id, List<String> loadedLazyFields) {
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
