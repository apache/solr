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
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
