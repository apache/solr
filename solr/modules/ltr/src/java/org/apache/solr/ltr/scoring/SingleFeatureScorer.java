package org.apache.solr.ltr.scoring;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.extraction.SingleFeatureExtractor;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.request.SolrQueryRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SingleFeatureScorer extends FeatureTraversalScorer {
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
    this.featureExtractor = new SingleFeatureExtractor(this, request, extractedFeatureWeights, allFeaturesInStore, ltrScoringModel, efi, featureScorers);
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
