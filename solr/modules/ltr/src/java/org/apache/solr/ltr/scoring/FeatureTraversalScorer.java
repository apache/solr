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
package org.apache.solr.ltr.scoring;

import java.io.IOException;
import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.extraction.FeatureExtractor;
import org.apache.solr.ltr.model.LTRScoringModel;

/** This class is responsible for extracting features and using them to score the document. */
public abstract class FeatureTraversalScorer extends Scorer {
  protected int targetDoc = -1;
  protected int activeDoc = -1;
  protected FeatureExtractor featureExtractor;
  protected LTRScoringQuery.FeatureInfo[] allFeaturesInStore;
  protected LTRScoringModel ltrScoringModel;
  protected Feature.FeatureWeight[] extractedFeatureWeights;
  protected LTRScoringQuery.ModelWeight modelWeight;

  public int getTargetDoc() {
    return targetDoc;
  }

  public int getActiveDoc() {
    return activeDoc;
  }

  public void reset() {
    for (int i = 0; i < extractedFeatureWeights.length; ++i) {
      int featId = extractedFeatureWeights[i].getIndex();
      float value = extractedFeatureWeights[i].getDefaultValue();
      // need to set default value everytime as the default value is used in 'dense'
      // mode even if used=false
      allFeaturesInStore[featId].setValue(value);
      allFeaturesInStore[featId].setIsDefaultValue(true);
    }
  }

  public void fillFeaturesInfo() throws IOException {
    // Initialize features to their default values and set isDefaultValue to true.
    reset();
    featureExtractor.fillFeaturesInfo();
  }

  @Override
  public float score() throws IOException {
    // Initialize features to their default values and set isDefaultValue to true.
    reset();
    featureExtractor.fillFeaturesInfo();
    modelWeight.normalizeFeatures();
    return ltrScoringModel.score(modelWeight.getModelFeatureValuesNormalized());
  }

  @Override
  public float getMaxScore(int upTo) {
    return Float.POSITIVE_INFINITY;
  }
}
