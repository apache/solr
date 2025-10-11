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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.extraction.SingleFeatureExtractor;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.request.SolrQueryRequest;

/** This class is responsible for extracting a single feature and using it to score the document. */
public class SingleFeatureScorer extends FeatureTraversalScorer {
  private int targetDoc = -1;
  private int activeDoc = -1;
  private int solrDocID = -1;
  private final List<Feature.FeatureWeight.FeatureScorer> featureScorers;

  public SingleFeatureScorer(
      LTRScoringQuery.ModelWeight modelWeight,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi,
      List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
    this.featureScorers = featureScorers;
    this.extractedFeatureWeights = extractedFeatureWeights;
    this.allFeaturesInStore = allFeaturesInStore;
    this.ltrScoringModel = ltrScoringModel;
    this.featureExtractor =
        new SingleFeatureExtractor(
            this,
            request,
            extractedFeatureWeights,
            allFeaturesInStore,
            ltrScoringModel,
            efi,
            featureScorers);
    this.modelWeight = modelWeight;
  }

  @Override
  public int getActiveDoc() {
    return activeDoc;
  }

  @Override
  public int getTargetDoc() {
    return targetDoc;
  }

  @Override
  public int getSolrDocID() {
    return solrDocID;
  }

  @Override
  public void setSolrDocID(int solrDocID) {
    this.solrDocID = solrDocID;
  }

  @Override
  public int docID() {
    return targetDoc;
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
    return new SingleFeatureIterator();
  }

  private class SingleFeatureIterator extends DocIdSetIterator {

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
