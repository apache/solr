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
  public void test_storedDvIsTrendy_FieldValueFeatureScorer_className_differs() throws Exception {
    final String className = super.storedDvIsTrendy_FieldValueFeatureScorer_className();
    final String legacyClassName = storedDvIsTrendy_FieldValueFeatureScorer_className();
    // demonstrate that & how the FieldValueFeature & LegacyFieldValueFeature implementations differ
    assertNotEquals(className, legacyClassName);
    assertEquals(className.replace("SortedDocValues", ""), legacyClassName);
  }
}
