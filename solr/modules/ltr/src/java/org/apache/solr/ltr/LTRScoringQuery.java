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
package org.apache.solr.ltr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.scoring.FeatureTraversalScorer;
import org.apache.solr.ltr.scoring.MultiFeaturesScorer;
import org.apache.solr.ltr.scoring.SingleFeatureScorer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SolrDefaultScorerSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ranking query that is run, reranking results using the LTRScoringModel algorithm */
public class LTRScoringQuery extends Query implements Accountable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(LTRScoringQuery.class);

  // contains a description of the model
  private final LTRScoringModel ltrScoringModel;
  private final LTRThreadModule ltrThreadMgr;

  // limits the number of threads per query, so that multiple requests can be serviced
  // simultaneously
  private final Semaphore querySemaphore;

  // feature logger to output the features.
  private FeatureLogger logger;
  // Map of external parameters, such as query intent, that can be used by
  // features
  private Map<String, String[]> efi;
  // Original solr query used to fetch matching documents
  private Query originalQuery;
  // Original solr request
  private SolrQueryRequest request;

  private Feature.FeatureWeight[] extractedFeatureWeights;

  public LTRScoringQuery(LTRScoringModel ltrScoringModel) {
    this(ltrScoringModel, Collections.<String, String[]>emptyMap(), null);
  }

  public LTRScoringQuery(
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> externalFeatureInfo,
      LTRThreadModule ltrThreadMgr) {
    this.ltrScoringModel = ltrScoringModel;
    this.efi = externalFeatureInfo;
    this.ltrThreadMgr = ltrThreadMgr;
    if (this.ltrThreadMgr != null) {
      this.querySemaphore = this.ltrThreadMgr.createQuerySemaphore();
    } else {
      this.querySemaphore = null;
    }
  }

  public LTRScoringModel getScoringModel() {
    return ltrScoringModel;
  }

  public String getScoringModelName() {
    return ltrScoringModel.getName();
  }

  public void setFeatureLogger(FeatureLogger logger) {
    this.logger = logger;
  }

  public FeatureLogger getFeatureLogger() {
    return logger;
  }

  public void setOriginalQuery(Query originalQuery) {
    this.originalQuery = originalQuery;
  }

  public Query getOriginalQuery() {
    return originalQuery;
  }

  public Map<String, String[]> getExternalFeatureInfo() {
    return efi;
  }

  public void setExternalFeatureInfo(Map<String, String[]> efi) {
    this.efi = efi;
  }

  public void setRequest(SolrQueryRequest request) {
    this.request = request;
  }

  public SolrQueryRequest getRequest() {
    return request;
  }

  public Feature.FeatureWeight[] getExtractedFeatureWeights() {
    return extractedFeatureWeights;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = (prime * result) + ((ltrScoringModel == null) ? 0 : ltrScoringModel.hashCode());
    result = (prime * result) + ((originalQuery == null) ? 0 : originalQuery.hashCode());
    if (efi == null) {
      result = (prime * result) + 0;
    } else {
      for (final Map.Entry<String, String[]> entry : efi.entrySet()) {
        final String key = entry.getKey();
        final String[] values = entry.getValue();
        result = (prime * result) + key.hashCode();
        result = (prime * result) + Arrays.hashCode(values);
      }
    }
    result = (prime * result) + this.toString().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  private boolean equalsTo(LTRScoringQuery other) {
    if (ltrScoringModel == null) {
      if (other.ltrScoringModel != null) {
        return false;
      }
    } else if (!ltrScoringModel.equals(other.ltrScoringModel)) {
      return false;
    }
    if (originalQuery == null) {
      if (other.originalQuery != null) {
        return false;
      }
    } else if (!originalQuery.equals(other.originalQuery)) {
      return false;
    }
    if (efi == null) {
      if (other.efi != null) {
        return false;
      }
    } else {
      if (other.efi == null || efi.size() != other.efi.size()) {
        return false;
      }
      for (final Map.Entry<String, String[]> entry : efi.entrySet()) {
        final String key = entry.getKey();
        final String[] otherValues = other.efi.get(key);
        if (otherValues == null || !Arrays.equals(otherValues, entry.getValue())) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public ModelWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Collection<Feature> modelFeatures = ltrScoringModel.getFeatures();
    final Collection<Feature> allFeatures = ltrScoringModel.getAllFeatures();
    int modelFeatSize = modelFeatures.size();

    Collection<Feature> features;
    if (logger != null && logger.isLoggingAll()) {
      features = allFeatures;
    } else {
      features = modelFeatures;
    }
    this.extractedFeatureWeights = new Feature.FeatureWeight[features.size()];
    final Feature.FeatureWeight[] modelFeaturesWeights = new Feature.FeatureWeight[modelFeatSize];
    List<Feature.FeatureWeight> featureWeights = new ArrayList<>(features.size());

    if (querySemaphore == null) {
      createWeights(searcher, scoreMode.needsScores(), featureWeights, features);
    } else {
      createWeightsParallel(searcher, scoreMode.needsScores(), featureWeights, features);
    }
    int i = 0, j = 0;
    if (logger != null && logger.isLoggingAll()) {
      for (final Feature.FeatureWeight fw : featureWeights) {
        extractedFeatureWeights[i++] = fw;
      }
      for (final Feature f : modelFeatures) {
        // we can lookup by featureid because all features will be extracted
        modelFeaturesWeights[j++] = extractedFeatureWeights[f.getIndex()];
      }
    } else {
      for (final Feature.FeatureWeight fw : featureWeights) {
        extractedFeatureWeights[i++] = fw;
        modelFeaturesWeights[j++] = fw;
      }
    }
    return new ModelWeight(modelFeaturesWeights, allFeatures.size());
  }

  private void createWeights(
      IndexSearcher searcher,
      boolean needsScores,
      List<Feature.FeatureWeight> featureWeights,
      Collection<Feature> features)
      throws IOException {
    final SolrQueryRequest req = getRequest();
    // since the feature store is a linkedhashmap order is preserved
    for (final Feature f : features) {
      try {
        Feature.FeatureWeight fw = f.createWeight(searcher, needsScores, req, originalQuery, efi);
        featureWeights.add(fw);
      } catch (final Exception e) {
        throw new RuntimeException(
            "Exception from createWeight for " + f.toString() + " " + e.getMessage(), e);
      }
    }
  }

  private class CreateWeightCallable implements Callable<Feature.FeatureWeight> {
    private final Feature f;
    private final IndexSearcher searcher;
    private final boolean needsScores;
    private final SolrQueryRequest req;

    public CreateWeightCallable(
        Feature f, IndexSearcher searcher, boolean needsScores, SolrQueryRequest req) {
      this.f = f;
      this.searcher = searcher;
      this.needsScores = needsScores;
      this.req = req;
    }

    @Override
    public Feature.FeatureWeight call() throws Exception {
      try {
        Feature.FeatureWeight fw = f.createWeight(searcher, needsScores, req, originalQuery, efi);
        return fw;
      } catch (final Exception e) {
        throw new RuntimeException(
            "Exception from createWeight for " + f.toString() + " " + e.getMessage(), e);
      } finally {
        querySemaphore.release();
        ltrThreadMgr.releaseLTRSemaphore();
      }
    }
  } // end of call CreateWeightCallable

  private void createWeightsParallel(
      IndexSearcher searcher,
      boolean needsScores,
      List<Feature.FeatureWeight> featureWeights,
      Collection<Feature> features)
      throws RuntimeException {

    final SolrQueryRequest req = getRequest();
    List<Future<Feature.FeatureWeight>> futures = new ArrayList<>(features.size());
    try {
      for (final Feature f : features) {
        CreateWeightCallable callable = new CreateWeightCallable(f, searcher, needsScores, req);
        RunnableFuture<Feature.FeatureWeight> runnableFuture = new FutureTask<>(callable);

        // always acquire before the ltrSemaphore is acquired, to guarantee a that
        // the current query is within the limit for max. threads
        querySemaphore.acquire();

        ltrThreadMgr.acquireLTRSemaphore(); // may block and/or interrupt
        ltrThreadMgr.execute(runnableFuture); // releases semaphore when done
        futures.add(runnableFuture);
      }
      // Loop over futures to get the feature weight objects
      for (final Future<Feature.FeatureWeight> future : futures) {
        featureWeights.add(future.get()); // future.get() will block if the job is still running
      }
    } catch (Exception e) { // To catch InterruptedException and ExecutionException
      log.info("Error while creating weights in LTR: InterruptedException", e);
      throw new RuntimeException("Error while creating weights in LTR: " + e.getMessage(), e);
    }
  }

  @Override
  public String toString(String field) {
    return field;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(efi)
        + RamUsageEstimator.sizeOfObject(ltrScoringModel)
        + RamUsageEstimator.sizeOfObject(
            originalQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED);
  }

  public static class FeatureInfo {
    private final String name;
    private float value;
    private boolean isDefaultValue;

    FeatureInfo(String name, float value, boolean isDefaultValue) {
      this.name = name;
      this.value = value;
      this.isDefaultValue = isDefaultValue;
    }

    public void setValue(float value) {
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public float getValue() {
      return value;
    }

    public boolean isDefaultValue() {
      return isDefaultValue;
    }

    public void setIsDefaultValue(boolean isDefaultValue) {
      this.isDefaultValue = isDefaultValue;
    }
  }

  public class ModelWeight extends Weight {

    // List of the model's features used for scoring. This is a subset of the
    // features used for logging.
    private final Feature.FeatureWeight[] modelFeatureWeights;
    private final float[] modelFeatureValuesNormalized;

    // Array of all the features in the feature store of reference
    private final FeatureInfo[] allFeaturesInStore;

    /*
     * @param modelFeatureWeights
     *     - should be the same size as the number of features used by the model
     * @param extractedFeatureWeights
     *     - if features are requested from the same store as model feature store,
     *       this will be the size of total number of features in the model feature store
     *       else, this will be the size of the modelFeatureWeights
     * @param allFeaturesSize
     *     - total number of feature in the feature store used by this model
     */
    public ModelWeight(Feature.FeatureWeight[] modelFeatureWeights, int allFeaturesSize) {
      super(LTRScoringQuery.this);
      this.modelFeatureWeights = modelFeatureWeights;
      this.modelFeatureValuesNormalized = new float[modelFeatureWeights.length];
      this.allFeaturesInStore = new FeatureInfo[allFeaturesSize];
      setFeaturesInfo(extractedFeatureWeights);
    }

    private void setFeaturesInfo(Feature.FeatureWeight[] extractedFeatureWeights) {
      for (int i = 0; i < extractedFeatureWeights.length; ++i) {
        String featName = extractedFeatureWeights[i].getName();
        int featId = extractedFeatureWeights[i].getIndex();
        float value = extractedFeatureWeights[i].getDefaultValue();
        allFeaturesInStore[featId] = new FeatureInfo(featName, value, true);
      }
    }

    public FeatureInfo[] getAllFeaturesInStore() {
      return allFeaturesInStore;
    }

    // for test use
    Feature.FeatureWeight[] getModelFeatureWeights() {
      return modelFeatureWeights;
    }

    // for test use
    public float[] getModelFeatureValuesNormalized() {
      return modelFeatureValuesNormalized;
    }

    /**
     * Goes through all the stored feature values, and calculates the normalized values for all the
     * features that will be used for scoring. Then calculate and return the model's score.
     */
    public void normalizeFeatures() {
      int pos = 0;
      for (final Feature.FeatureWeight feature : modelFeatureWeights) {
        final int featureId = feature.getIndex();
        FeatureInfo fInfo = allFeaturesInStore[featureId];
        modelFeatureValuesNormalized[pos] = fInfo.getValue();
        pos++;
      }
      ltrScoringModel.normalizeFeaturesInPlace(modelFeatureValuesNormalized);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {

      final Explanation[] explanations = new Explanation[this.allFeaturesInStore.length];
      for (final Feature.FeatureWeight feature : extractedFeatureWeights) {
        explanations[feature.getIndex()] = feature.explain(context, doc);
      }
      final List<Explanation> featureExplanations = new ArrayList<>();
      for (int idx = 0; idx < modelFeatureWeights.length; ++idx) {
        final Feature.FeatureWeight f = modelFeatureWeights[idx];
        Explanation e = ltrScoringModel.getNormalizerExplanation(explanations[f.getIndex()], idx);
        featureExplanations.add(e);
      }
      final ModelScorer bs = modelScorer(context);
      bs.iterator().advance(doc);

      final float finalScore = bs.score();

      return ltrScoringModel.explain(context, doc, finalScore, featureExplanations);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      return new SolrDefaultScorerSupplier(modelScorer(context));
    }

    public ModelScorer modelScorer(LeafReaderContext context) throws IOException {

      final List<Feature.FeatureWeight.FeatureScorer> featureScorers =
          new ArrayList<Feature.FeatureWeight.FeatureScorer>(extractedFeatureWeights.length);
      for (final Feature.FeatureWeight featureWeight : extractedFeatureWeights) {
        final Feature.FeatureWeight.FeatureScorer scorer = featureWeight.featureScorer(context);
        if (scorer != null) {
          featureScorers.add(scorer);
        }
      }
      // Always return a ModelScorer, even if no features match, because we
      // always need to call
      // score on the model for every document, since 0 features matching could
      // return a
      // non 0 score for a given model.
      ModelScorer mscorer = new ModelScorer(featureScorers);
      return mscorer;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    public class ModelScorer extends Scorer {
      private final DocInfo docInfo;
      private final FeatureTraversalScorer featureTraversalScorer;

      public DocInfo getDocInfo() {
        return docInfo;
      }

      public ModelScorer(List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
        docInfo = new DocInfo();
        for (final Feature.FeatureWeight.FeatureScorer subScorer : featureScorers) {
          subScorer.setDocInfo(docInfo);
        }
        if (featureScorers.size() <= 1) {
          // future enhancement: allow the use of dense features in other cases
          featureTraversalScorer =
              new SingleFeatureScorer(
                  ModelWeight.this,
                  request,
                  extractedFeatureWeights,
                  allFeaturesInStore,
                  ltrScoringModel,
                  efi,
                  featureScorers);
        } else {
          featureTraversalScorer =
              new MultiFeaturesScorer(
                  ModelWeight.this,
                  request,
                  extractedFeatureWeights,
                  allFeaturesInStore,
                  ltrScoringModel,
                  efi,
                  featureScorers);
        }
      }

      @Override
      public Collection<ChildScorable> getChildren() throws IOException {
        return featureTraversalScorer.getChildren();
      }

      @Override
      public int docID() {
        return featureTraversalScorer.docID();
      }

      @Override
      public float score() throws IOException {
        return featureTraversalScorer.score();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return Float.POSITIVE_INFINITY;
      }

      @Override
      public DocIdSetIterator iterator() {
        return featureTraversalScorer.iterator();
      }

      public void fillFeaturesInfo() throws IOException {
        featureTraversalScorer.fillFeaturesInfo();
      }

      public void setSolrDocID(int solrDocID) throws IOException {
        featureTraversalScorer.setSolrDocID(solrDocID);
      }
    }
  }
}
