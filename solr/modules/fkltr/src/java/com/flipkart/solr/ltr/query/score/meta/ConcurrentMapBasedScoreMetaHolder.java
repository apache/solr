package com.flipkart.solr.ltr.query.score.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentMapBasedScoreMetaHolder<ScoreMeta> implements ScoreMetaHolder<ScoreMeta> {
  private Map<Integer, ScoreMeta> docIdToScoreMetaMap = new ConcurrentHashMap<>();

  public ScoreMeta getScoreMeta(int docId) {
    return docIdToScoreMetaMap.get(docId);
  }

  public void putScoreMeta(int docId, ScoreMeta scoreMeta) {
    docIdToScoreMetaMap.put(docId, scoreMeta);
  }
}
