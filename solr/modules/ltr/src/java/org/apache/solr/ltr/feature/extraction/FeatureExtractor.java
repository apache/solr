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
package org.apache.solr.ltr.feature.extraction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.solr.ltr.FeatureLogger;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory;
import org.apache.solr.ltr.scoring.FeatureTraversalScorer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrCache;

/** The class used to extract features for LTR feature logging. */
public abstract class FeatureExtractor {
  protected final FeatureTraversalScorer traversalScorer;
  SolrQueryRequest request;
  Feature.FeatureWeight[] extractedFeatureWeights;
  LTRScoringQuery.FeatureInfo[] allFeaturesInStore;
  LTRScoringModel ltrScoringModel;
  FeatureLogger logger;
  Map<String, String[]> efi;

  FeatureExtractor(
      FeatureTraversalScorer traversalScorer,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi) {
    this.traversalScorer = traversalScorer;
    this.request = request;
    this.extractedFeatureWeights = extractedFeatureWeights;
    this.allFeaturesInStore = allFeaturesInStore;
    this.ltrScoringModel = ltrScoringModel;
    this.efi = efi;
  }

  protected float[] initFeatureVector(LTRScoringQuery.FeatureInfo[] featuresInfos) {
    float[] featureVector = new float[featuresInfos.length];
    for (int i = 0; i < featuresInfos.length; i++) {
      if (featuresInfos[i] != null) {
        featureVector[i] = featuresInfos[i].getValue();
      }
    }
    return featureVector;
  }

  protected abstract float[] extractFeatureVector() throws IOException;

  public void fillFeaturesInfo() throws IOException {
    if (traversalScorer.getActiveDoc() == traversalScorer.getTargetDoc()) {
      SolrCache<Integer, float[]> featureVectorCache = null;
      float[] featureVector;

      if (request != null) {
        featureVectorCache = request.getSearcher().getFeatureVectorCache();
      }
      if (featureVectorCache != null) {
        int fvCacheKey =
            computeFeatureVectorCacheKey(traversalScorer.getDocInfo().getOriginalDocId());
        featureVector = featureVectorCache.get(fvCacheKey);
        if (featureVector == null) {
          featureVector = extractFeatureVector();
          featureVectorCache.put(fvCacheKey, featureVector);
        }
      } else {
        featureVector = extractFeatureVector();
      }

      for (int i = 0; i < extractedFeatureWeights.length; i++) {
        int featureId = extractedFeatureWeights[i].getIndex();
        float featureValue = featureVector[featureId];
        if (!Float.isNaN(featureValue)
            && featureValue != extractedFeatureWeights[i].getDefaultValue()) {
          allFeaturesInStore[featureId].setValue(featureValue);
          allFeaturesInStore[featureId].setIsDefaultValue(false);
        }
      }
    }
  }

  private int computeFeatureVectorCacheKey(int docId) {
    int prime = 31;
    int result = docId;
    if (Objects.equals(
            ltrScoringModel.getName(),
            LTRFeatureLoggerTransformerFactory.DEFAULT_LOGGING_MODEL_NAME)
        || (logger != null && logger.isLogFeatures() && logger.isLoggingAll())) {
      result = (prime * result) + ltrScoringModel.getFeatureStoreName().hashCode();
    } else {
      result = (prime * result) + ltrScoringModel.getName().hashCode();
    }
    result = (prime * result) + addEfisHash(result, prime, efi);
    return result;
  }

  private int addEfisHash(int result, int prime, Map<String, String[]> efi) {
    if (efi != null) {
      TreeMap<String, String[]> sorted = new TreeMap<>(efi);
      for (final Map.Entry<String, String[]> entry : sorted.entrySet()) {
        final String key = entry.getKey();
        final String[] values = entry.getValue();
        result = (prime * result) + key.hashCode();
        result = (prime * result) + Arrays.hashCode(values);
      }
    }
    return result;
  }
}
