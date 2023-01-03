package com.flipkart.solr.ltr.transformers;

public interface ScoreMetaSerializer<ScoreMeta> {
  String serialize(ScoreMeta scoreMeta);
}
