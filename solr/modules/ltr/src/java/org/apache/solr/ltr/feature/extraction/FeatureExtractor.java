package org.apache.solr.ltr.feature;

import org.apache.solr.ltr.FeatureLogger;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrCache;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public abstract class FeatureExtractor {
  protected final LTRScoringQuery.ModelWeight.ModelScorer.FeatureTraversalScorer traversalScorer;
  SolrQueryRequest request;
  Feature.FeatureWeight[] extractedFeatureWeights;
  LTRScoringQuery.FeatureInfo[] allFeaturesInStore;
  LTRScoringModel ltrScoringModel;
  FeatureLogger logger;
  Map<String, String[]> efi;

  FeatureExtractor(
      LTRScoringQuery.ModelWeight.ModelScorer.FeatureTraversalScorer traversalScorer,
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
        int fvCacheKey = computeFeatureVectorCacheKey(traversalScorer.docID());
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
