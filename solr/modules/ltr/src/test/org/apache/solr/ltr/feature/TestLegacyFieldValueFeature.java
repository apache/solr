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

import java.util.Map;
import org.junit.Test;

@Deprecated
public class TestLegacyFieldValueFeature extends TestFieldValueFeature {

  @Override
  protected String getFieldValueFeatureClassName() {
    return LegacyFieldValueFeature.class.getName();
  }

  @Override
  protected String getObservingFieldValueFeatureClassName() {
    return LegacyObservingFieldValueFeature.class.getName();
  }

  @Deprecated
  public static final class LegacyObservingFieldValueFeature
      extends TestFieldValueFeature.ObservingFieldValueFeature {

    public LegacyObservingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params, false);
    }
  }

  @Override
  protected String storedDvIsTrendy_FieldValueFeatureScorer_className() {
    return FieldValueFeature.FieldValueFeatureWeight.FieldValueFeatureScorer.class.getName();
  }

  @Test
  public void test_LegacyFieldValueFeature_behavesDifferentlyThan_FieldValueFeature() throws Exception {
    // the field storedDvIsTrendy has stored=true and docValues=true
    final String field = "storedDvIsTrendy";
    final String fieldValue = "1";
    String fstore = "test_LegacyFieldValueFeature_behavesDifferentlyThan_FieldValueFeature" + field;

    assertU(adoc("id", "21", field, fieldValue));
    assertU(commit());
    // demonstrate that & how the FieldValueFeature & LegacyFieldValueFeature implementations differ

    // the LegacyFieldValueFeature does not use docValues
    loadAndQuery(getObservingFieldValueFeatureClassName(), field, fstore);

    String legacyScorerClass = ObservingFieldValueFeature.usedScorerClass;
    assertEquals(
        FieldValueFeature.FieldValueFeatureWeight.FieldValueFeatureScorer.class.getName(),
        legacyScorerClass);

    // the FieldValueFeature does use docValues
    loadAndQuery(super.getObservingFieldValueFeatureClassName(), field, fstore);

    String scorerClass = ObservingFieldValueFeature.usedScorerClass;
    assertEquals(
        FieldValueFeature.FieldValueFeatureWeight.SortedDocValuesFieldValueFeatureScorer.class.getName(),
        scorerClass);
  }

  void loadAndQuery(String featureClassName, String field, String fstore) throws Exception {
    final String modelName = field + "-model-" + featureClassName;
    final String featureStoreName = fstore + featureClassName;

    loadFeatureAndModel(featureClassName, field, featureStoreName, modelName);

    query(field, modelName);
  }
}
