package com.flipkart.neo.solr.ltr.transformers;

import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.solr.ltr.transformers.ScoreMetaSerializer;
import com.flipkart.solr.ltr.transformers.ScoreMetaTransformerFactory;

public class NeoScoreMetaTransformerFactory extends ScoreMetaTransformerFactory<ScoreMeta> {
  private ScoreMetaSerializer<ScoreMeta> scoreMetaSerializer;

  /**
   * scoreMetaSerializer should be set while loading collection.
   * @param scoreMetaSerializer scoreMetaSerializer
   */
  public void setScoreMetaSerializer(ScoreMetaSerializer<ScoreMeta> scoreMetaSerializer) {
    this.scoreMetaSerializer = scoreMetaSerializer;
  }

  @Override
  protected ScoreMetaSerializer<ScoreMeta> provideScoreMetaSerializer() {
    return scoreMetaSerializer;
  }
}
