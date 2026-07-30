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
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.scoring.FeatureTraversalScorer;
import org.apache.solr.request.SolrQueryRequest;

/** The class used to extract a single feature for LTR feature logging. */
public class SingleFeatureExtractor extends FeatureExtractor {
  List<Feature.FeatureWeight.FeatureScorer> featureScorers;

  public SingleFeatureExtractor(
      FeatureTraversalScorer singleFeatureScorer,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi,
      List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
    super(
        singleFeatureScorer,
        request,
        extractedFeatureWeights,
        allFeaturesInStore,
        ltrScoringModel,
        efi);
    this.featureScorers = featureScorers;
  }

  @Override
  protected float[] extractFeatureVector() throws IOException {
    float[] featureVector = initFeatureVector(allFeaturesInStore);
    for (final Scorer scorer : featureScorers) {
      if (scorer.docID() == traversalScorer.getActiveDoc()) {
        Feature.FeatureWeight.FeatureScorer featureScorer =
            (Feature.FeatureWeight.FeatureScorer) scorer;
        Feature.FeatureWeight scFW = featureScorer.getWeight();
        final int featureId = scFW.getIndex();
        float featureValue = scorer.score();
        featureVector[featureId] = featureValue;
      }
    }
    return featureVector;
  }
}
