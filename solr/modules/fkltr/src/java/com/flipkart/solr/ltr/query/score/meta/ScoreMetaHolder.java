package com.flipkart.solr.ltr.query.score.meta;

public interface ScoreMetaHolder<ScoreMeta> {
  ScoreMeta getScoreMeta(int docId);
  void putScoreMeta(int docId, ScoreMeta scoreMeta);
}
