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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public class TestPrefetchingFieldValueFeature extends TestRerankBase {
  @Before
  public void before() throws Exception {
    setuptest(false);
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;

    loadFeature("storedLongField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedLongField\"}");
    loadFeature("storedFloatField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedFloatField\"}");

    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields
        .containsAll(List.of("storedIntField", "storedLongField", "storedFloatField")));
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;

    loadFeature("storedLongField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore + "-other",
        "{\"field\":\"storedLongField\"}");

    assertEquals(ObservingPrefetchingFieldValueFeature.allPrefetchFields.size(), 1);
    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields.contains("storedLongField"));
  }

  @Test
  public void testThatPrefetchFieldsAreOnlyUpdatedAfterLoadingAllFeatures() throws Exception {
    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    ObservingPrefetchingFieldValueFeature.updateCount = 0;
    final String fstore = "feature-store-for-test";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of("storedIntField"), ObservingPrefetchingFieldValueFeature.allPrefetchFields);
    assertEquals(1, ObservingPrefetchingFieldValueFeature.updateCount);

    ObservingPrefetchingFieldValueFeature.allPrefetchFields = null;
    ObservingPrefetchingFieldValueFeature.updateCount = 0;

    loadFeatures("prefetching_features.json");

    assertTrue(ObservingPrefetchingFieldValueFeature.allPrefetchFields
        .containsAll(List.of("storedIntField", "storedLongField", "storedFloatField", "storedDoubleField")));
    assertEquals(1, ObservingPrefetchingFieldValueFeature.updateCount);
  }

  @Test
  public void testThatPrefetchFieldsAreReturnedInOutputOfFeatureStore() throws Exception {
    final String fstore = "feature-store-for-test";

    // load initial features
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");
    loadFeature("storedLongField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedLongField\"}");

    assertJQ(ManagedFeatureStore.REST_END_POINT,"/featureStores==['"+fstore+"']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/name=='storedIntField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/prefetchFields==['storedIntField','storedLongField']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/name=='storedLongField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/prefetchFields==['storedIntField','storedLongField']");

    // add another feature to test that new field is added to prefetchFields
    loadFeature("storedDoubleField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedDoubleField\"}");

    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/name=='storedIntField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/prefetchFields==['storedDoubleField','storedIntField','storedLongField']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/name=='storedLongField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/prefetchFields==['storedDoubleField','storedIntField','storedLongField']");
  }

  @Test
  public void testParamsToMapWithoutPrefetchFields() {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
    params.put("field", "field"+random().nextInt(10));

    // create a feature from the parameters
    final PrefetchingFieldValueFeature featureA = (PrefetchingFieldValueFeature) Feature.getInstance(solrResourceLoader,
        PrefetchingFieldValueFeature.class.getName(), "featureName", params);

    // turn the feature back into parameters
    final LinkedHashMap<String,Object> paramsB = featureA.paramsToMap();

    // create feature B from feature A's parameters
    final PrefetchingFieldValueFeature featureB = (PrefetchingFieldValueFeature) Feature.getInstance(solrResourceLoader,
        PrefetchingFieldValueFeature.class.getName(), "featureName", paramsB);

    // do not call equals() because of mismatch between prefetchFields and params
    // (featureA has null for prefetchFields-param, featureB has empty set)
    assertEquals(featureA.getName(), featureB.getName());
    assertEquals(featureA.getField(), featureB.getField());
    assertTrue(CollectionUtils.isEmpty(featureA.getPrefetchFields()) // featureA has null
        && CollectionUtils.isEmpty(featureB.getPrefetchFields())); // featureB has empty set
  }

  @Test
  public void testCompleteParamsToMap() throws Exception {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
    params.put("field", "field"+random().nextInt(10));
    params.put("prefetchFields", Set.of("field_1", "field_2"));
    doTestParamsToMap(PrefetchingFieldValueFeature.class.getName(), params);
  }

  /**
   * This class is used to track the content of the prefetchFields and the updates so that we can assert,
   * that the logic to store them works as expected.
   */
  final public static class ObservingPrefetchingFieldValueFeature extends PrefetchingFieldValueFeature {
    public static SortedSet<String> allPrefetchFields;
    public static int updateCount = 0;

    public ObservingPrefetchingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    public void setPrefetchFields(SortedSet<String> fields) {
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
