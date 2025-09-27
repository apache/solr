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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.extraction.MultiFeaturesExtractor;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.request.SolrQueryRequest;

/**
 * This class is responsible for extracting more than one feature and using them to score the
 * document.
 */
public class MultiFeaturesScorer extends FeatureTraversalScorer {
  private final DisiPriorityQueue subScorers;
  private final List<DisiWrapper> wrappers;
  private final MultiFeaturesIterator multiFeaturesIteratorIterator;

  public MultiFeaturesScorer(
      LTRScoringQuery.ModelWeight modelWeight,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi,
      List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
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

    multiFeaturesIteratorIterator = new MultiFeaturesIterator(wrappers);
    this.extractedFeatureWeights = extractedFeatureWeights;
    this.allFeaturesInStore = allFeaturesInStore;
    this.ltrScoringModel = ltrScoringModel;
    this.featureExtractor =
        new MultiFeaturesExtractor(
            this,
            request,
            extractedFeatureWeights,
            allFeaturesInStore,
            ltrScoringModel,
            efi,
            subScorers);
    this.modelWeight = modelWeight;
  }

  @Override
  public int docID() {
    return multiFeaturesIteratorIterator.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return multiFeaturesIteratorIterator;
  }

  @Override
  public final Collection<ChildScorable> getChildren() {
    final ArrayList<ChildScorable> children = new ArrayList<>();
    for (final DisiWrapper scorer : subScorers) {
      children.add(new ChildScorable(scorer.scorer, "SHOULD"));
    }
    return children;
  }

  private class MultiFeaturesIterator extends DocIdSetIterator {

    public MultiFeaturesIterator(Collection<DisiWrapper> wrappers) {
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
