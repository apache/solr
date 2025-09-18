package org.apache.solr.ltr.feature;

import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.request.SolrQueryRequest;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SingleFeatureExtractor extends FeatureExtractor {
  List<Feature.FeatureWeight.FeatureScorer> featureScorers;

  public SingleFeatureExtractor(
      LTRScoringQuery.ModelWeight.ModelScorer.FeatureTraversalScorer singleFeatureScorer,
      SolrQueryRequest request,
      Feature.FeatureWeight[] extractedFeatureWeights,
      LTRScoringQuery.FeatureInfo[] allFeaturesInStore,
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> efi,
      List<Feature.FeatureWeight.FeatureScorer> featureScorers) {
    super(singleFeatureScorer, request, extractedFeatureWeights, allFeaturesInStore, ltrScoringModel, efi);
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
