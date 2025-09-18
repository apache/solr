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
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
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
  private final Map<String, String[]> efi;
  // Original solr query used to fetch matching documents
  private Query originalQuery;
  // Original solr request
  private SolrQueryRequest request;

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

  public void setRequest(SolrQueryRequest request) {
    this.request = request;
  }

  public SolrQueryRequest getRequest() {
    return request;
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
    final Feature.FeatureWeight[] extractedFeatureWeights =
        new Feature.FeatureWeight[features.size()];
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
    return new ModelWeight(modelFeaturesWeights, extractedFeatureWeights, allFeatures.size());
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
    private final Feature.FeatureWeight[] extractedFeatureWeights;

    // List of all the feature names, values - used for both scoring and logging
    /*
     *  What is the advantage of using a hashmap here instead of an array of objects?
     *     A set of arrays was used earlier and the elements were accessed using the featureId.
     *     With the updated logic to create weights selectively,
     *     the number of elements in the array can be fewer than the total number of features.
     *     When [features] are not requested, only the model features are extracted.
     *     In this case, the indexing by featureId, fails. For this reason,
     *     we need a map which holds just the features that were triggered by the documents in the result set.
     *
     */
    private final FeatureInfo[] featuresInfo;

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
    public ModelWeight(
        Feature.FeatureWeight[] modelFeatureWeights,
        Feature.FeatureWeight[] extractedFeatureWeights,
        int allFeaturesSize) {
      super(LTRScoringQuery.this);
      this.extractedFeatureWeights = extractedFeatureWeights;
      this.modelFeatureWeights = modelFeatureWeights;
      this.modelFeatureValuesNormalized = new float[modelFeatureWeights.length];
      this.featuresInfo = new FeatureInfo[allFeaturesSize];
      setFeaturesInfo();
    }

    private void setFeaturesInfo() {
      for (int i = 0; i < extractedFeatureWeights.length; ++i) {
        String featName = extractedFeatureWeights[i].getName();
        int featId = extractedFeatureWeights[i].getIndex();
        float value = extractedFeatureWeights[i].getDefaultValue();
        featuresInfo[featId] = new FeatureInfo(featName, value, true);
      }
    }

    public FeatureInfo[] getFeaturesInfo() {
      return featuresInfo;
    }

    // for test use
    Feature.FeatureWeight[] getModelFeatureWeights() {
      return modelFeatureWeights;
    }

    // for test use
    float[] getModelFeatureValuesNormalized() {
      return modelFeatureValuesNormalized;
    }

    // for test use
    Feature.FeatureWeight[] getExtractedFeatureWeights() {
      return extractedFeatureWeights;
    }

    /**
     * Goes through all the stored feature values, and calculates the normalized values for all the
     * features that will be used for scoring. Then calculate and return the model's score.
     */
    private float makeNormalizedFeaturesAndScore() {
      int pos = 0;
      for (final Feature.FeatureWeight feature : modelFeatureWeights) {
        final int featureId = feature.getIndex();
        FeatureInfo fInfo = featuresInfo[featureId];
        modelFeatureValuesNormalized[pos] = fInfo.getValue();
        pos++;
      }
      ltrScoringModel.normalizeFeaturesInPlace(modelFeatureValuesNormalized);
      return ltrScoringModel.score(modelFeatureValuesNormalized);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {

      final Explanation[] explanations = new Explanation[this.featuresInfo.length];
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

    protected void reset() {
      for (int i = 0; i < extractedFeatureWeights.length; ++i) {
        int featId = extractedFeatureWeights[i].getIndex();
        float value = extractedFeatureWeights[i].getDefaultValue();
        // need to set default value everytime as the default value is used in 'dense'
        // mode even if used=false
        featuresInfo[featId].setValue(value);
        featuresInfo[featId].setIsDefaultValue(true);
      }
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
      ModelScorer mscorer = new ModelScorer(this, featureScorers);
      return mscorer;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    public class ModelScorer extends Scorer {
      private final DocInfo docInfo;
      private final Scorer featureTraversalScorer;

      public DocInfo getDocInfo() {
        return docInfo;
      }

      public ModelScorer(Weight weight, List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
        docInfo = new DocInfo();
        for (final Feature.FeatureWeight.FeatureScorer subScorer : featureScorers) {
          subScorer.setDocInfo(docInfo);
        }
        if (featureScorers.size() <= 1) {
          // future enhancement: allow the use of dense features in other cases
          featureTraversalScorer = new DenseModelScorer(weight, featureScorers);
        } else {
          featureTraversalScorer = new SparseModelScorer(weight, featureScorers);
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

      private class SparseModelScorer extends Scorer {
        private final DisiPriorityQueue subScorers;
        private final List<DisiWrapper> wrappers;
        private final ScoringQuerySparseIterator itr;

        private int targetDoc = -1;
        private int activeDoc = -1;

        private SparseModelScorer(
            Weight unusedWeight, List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
          if (featureScorers.size() <= 1) {
            throw new IllegalArgumentException("There must be at least 2 subScorers");
          }
          subScorers = DisiPriorityQueue.ofMaxSize(featureScorers.size());
          wrappers = new ArrayList<>();
          for (final Scorer scorer : featureScorers) {
            final DisiWrapper w = new DisiWrapper(scorer, false /* impacts */);
            subScorers.add(w);
            wrappers.add(w);
          }

          itr = new ScoringQuerySparseIterator(wrappers);
        }

        @Override
        public int docID() {
          return itr.docID();
        }

        @Override
        public float score() throws IOException {
          final DisiWrapper topList = subScorers.topList();
          // If target doc we wanted to advance to match the actual doc
          // the underlying features advanced to, perform the feature
          // calculations,
          // otherwise just continue with the model's scoring process with empty
          // features.
          reset();
          if (activeDoc == targetDoc) {
            for (DisiWrapper w = topList; w != null; w = w.next) {
              final Feature.FeatureWeight.FeatureScorer subScorer =
                  (Feature.FeatureWeight.FeatureScorer) w.scorer;
              Feature.FeatureWeight scFW = subScorer.getWeight();
              final int featureId = scFW.getIndex();
              featuresInfo[featureId].setValue(subScorer.score());
              if (featuresInfo[featureId].getValue() != scFW.getDefaultValue()) {
                featuresInfo[featureId].setIsDefaultValue(false);
              }
            }
          }
          return makeNormalizedFeaturesAndScore();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return Float.POSITIVE_INFINITY;
        }

        @Override
        public DocIdSetIterator iterator() {
          return itr;
        }

        @Override
        public final Collection<ChildScorable> getChildren() {
          final ArrayList<ChildScorable> children = new ArrayList<>();
          for (final DisiWrapper scorer : subScorers) {
            children.add(new ChildScorable(scorer.scorer, "SHOULD"));
          }
          return children;
        }

        private class ScoringQuerySparseIterator extends DocIdSetIterator {

          public ScoringQuerySparseIterator(Collection<DisiWrapper> wrappers) {
            // Initialize all wrappers to start at -1
            for (DisiWrapper wrapper : wrappers) {
              wrapper.doc = -1;
            }
          }

          @Override
          public int docID() {
            // Return the target document ID (mimicking DisjunctionDISIApproximation behavior)
            return targetDoc;
          }

          @Override
          public final int nextDoc() throws IOException {
            // Mimic DisjunctionDISIApproximation behavior
            if (targetDoc == -1) {
              // First call - initialize all iterators
              DisiWrapper top = subScorers.top();
              if (top != null && top.doc == -1) {
                // Need to advance all iterators to their first document
                DisiWrapper current = subScorers.top();
                while (current != null) {
                  current.doc = current.iterator.nextDoc();
                  current = subScorers.updateTop();
                }
                top = subScorers.top();
                activeDoc = top == null ? NO_MORE_DOCS : top.doc;
              }
              targetDoc = activeDoc;
              return targetDoc;
            }

            if (activeDoc == targetDoc) {
              // Advance the underlying disjunction
              DisiWrapper top = subScorers.top();
              if (top == null) {
                activeDoc = NO_MORE_DOCS;
              } else {
                // Advance the top iterator and rebalance the queue
                top.doc = top.iterator.nextDoc();
                top = subScorers.updateTop();
                activeDoc = top == null ? NO_MORE_DOCS : top.doc;
              }
            } else if (activeDoc < targetDoc) {
              // Need to catch up to targetDoc + 1
              activeDoc = advanceInternal(targetDoc + 1);
            }
            return ++targetDoc;
          }

          @Override
          public final int advance(int target) throws IOException {
            // Mimic DisjunctionDISIApproximation behavior
            if (activeDoc < target) {
              activeDoc = advanceInternal(target);
            }
            targetDoc = target;
            return targetDoc;
          }

          private int advanceInternal(int target) throws IOException {
            // Advance the underlying disjunction to the target
            DisiWrapper top;
            do {
              top = subScorers.top();
              if (top == null) {
                return NO_MORE_DOCS;
              }
              if (top.doc >= target) {
                return top.doc;
              }
              top.doc = top.iterator.advance(target);
              top = subScorers.updateTop();
              if (top == null) {
                return NO_MORE_DOCS;
              }
            } while (top.doc < target);
            return top.doc;
          }

          @Override
          public long cost() {
            // Calculate cost from all wrappers
            long cost = 0;
            for (DisiWrapper wrapper : wrappers) {
              cost += wrapper.iterator.cost();
            }
            return cost;
          }
        }
      }

      private class DenseModelScorer extends Scorer {
        private int activeDoc = -1; // The doc that our scorer's are actually at
        private int targetDoc = -1; // The doc we were most recently told to go to
        private int freq = -1;
        private final List<Feature.FeatureWeight.FeatureScorer> featureScorers;

        private DenseModelScorer(
            Weight unusedWeight, List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
          this.featureScorers = featureScorers;
        }

        @Override
        public int docID() {
          return targetDoc;
        }

        @Override
        public float score() throws IOException {
          reset();
          freq = 0;
          if (targetDoc == activeDoc) {
            for (final Scorer scorer : featureScorers) {
              if (scorer.docID() == activeDoc) {
                freq++;
                Feature.FeatureWeight.FeatureScorer featureScorer =
                    (Feature.FeatureWeight.FeatureScorer) scorer;
                Feature.FeatureWeight scFW = featureScorer.getWeight();
                final int featureId = scFW.getIndex();
                featuresInfo[featureId].setValue(scorer.score());
                if (featuresInfo[featureId].getValue() != scFW.getDefaultValue()) {
                  featuresInfo[featureId].setIsDefaultValue(false);
                }
              }
            }
          }
          return makeNormalizedFeaturesAndScore();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return Float.POSITIVE_INFINITY;
        }

        @Override
        public final Collection<ChildScorable> getChildren() {
          final ArrayList<ChildScorable> children = new ArrayList<>();
          for (final Scorer scorer : featureScorers) {
            children.add(new ChildScorable(scorer, "SHOULD"));
          }
          return children;
        }

        @Override
        public DocIdSetIterator iterator() {
          return new DenseIterator();
        }

        private class DenseIterator extends DocIdSetIterator {

          @Override
          public int docID() {
            return targetDoc;
          }

          @Override
          public int nextDoc() throws IOException {
            if (activeDoc <= targetDoc) {
              activeDoc = NO_MORE_DOCS;
              for (final Scorer scorer : featureScorers) {
                if (scorer.docID() != NO_MORE_DOCS) {
                  activeDoc = Math.min(activeDoc, scorer.iterator().nextDoc());
                }
              }
            }
            return ++targetDoc;
          }

          @Override
          public int advance(int target) throws IOException {
            if (activeDoc < target) {
              activeDoc = NO_MORE_DOCS;
              for (final Scorer scorer : featureScorers) {
                if (scorer.docID() != NO_MORE_DOCS) {
                  activeDoc = Math.min(activeDoc, scorer.iterator().advance(target));
                }
              }
            }
            targetDoc = target;
            return target;
          }

          @Override
          public long cost() {
            long sum = 0;
            for (int i = 0; i < featureScorers.size(); i++) {
              sum += featureScorers.get(i).iterator().cost();
            }
            return sum;
          }
        }
      }
    }
  }
}
