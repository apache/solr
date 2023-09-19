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

/**
 * This feature returns the value of a field in the current document. The field must have
 * stored="true" or docValues="true" properties. Example configuration:
 *
 * <pre>
 * {
 *   "name":  "rawHits",
 *   "class": "org.apache.solr.ltr.feature.LegacyFieldValueFeature",
 *   "params": {
 *     "field": "hits"
 *   }
 * }
 * </pre>
 *
 * <p>There are 4 different types of FeatureScorers that a FieldValueFeatureWeight may use. The
 * chosen scorer depends on the field attributes.
 *
 * <p>FieldValueFeatureScorer (FVFS): used for stored=true, no matter if docValues=true or
 * docValues=false
 *
 * <p>NumericDocValuesFVFS: used for stored=false and docValues=true, if docValueType == NUMERIC
 *
 * <p>SortedDocValuesFVFS: used for stored=false and docValues=true, if docValueType == SORTED
 *
 * <p>DefaultValueFVFS: used for stored=false and docValues=true, a fallback scorer that is used on
 * segments where no document has a value set in the field of this feature
 *
 * <p>Matches {@link FieldValueFeature} behaviour prior to 9.4 i.e. DocValues are not used when
 * docValues=true is combined with stored=true.
 */
@Deprecated(since = "9.4")
public class LegacyFieldValueFeature extends FieldValueFeature {

  public LegacyFieldValueFeature(String name, Map<String, Object> params) {
    super(name, params);
    this.useDocValuesForStored = false;
  }
}
