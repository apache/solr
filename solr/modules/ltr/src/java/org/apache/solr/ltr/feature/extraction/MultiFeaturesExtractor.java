package org.apache.solr.ltr.feature.extraction;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.scoring.FeatureTraversalScorer;
import org.apache.solr.request.SolrQueryRequest;
import java.io.IOException;
import java.util.Map;

public class MultiFeaturesExtractor extends FeatureExtractor {
  DisiPriorityQueue subScorers;

  public MultiFeaturesExtractor(
      FeatureTraversalScorer multiFeaturesScorer,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi,
      DisiPriorityQueue subScorers) {
    super(multiFeaturesScorer, request, extractedFeatureWeights, allFeaturesInStore, ltrScoringModel, efi);
    this.subScorers = subScorers;
  }

  @Override
  protected float[] extractFeatureVector() throws IOException {
    final DisiWrapper topList = subScorers.topList();
    float[] featureVector = initFeatureVector(allFeaturesInStore);
    for (DisiWrapper w = topList; w != null; w = w.next) {
      final Feature.FeatureWeight.FeatureScorer subScorer =
          (Feature.FeatureWeight.FeatureScorer) w.scorer;
      Feature.FeatureWeight feature = subScorer.getWeight();
      final int featureId = feature.getIndex();
      float featureValue = subScorer.score();
      featureVector[featureId] = featureValue;
    }
    return featureVector;
  }
}
