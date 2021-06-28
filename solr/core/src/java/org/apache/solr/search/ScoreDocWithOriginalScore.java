package org.apache.solr.search;

import org.apache.lucene.search.ScoreDoc;

public class ScoreDocWithOriginalScore extends ScoreDoc {

  public final float originalScore;

  // FIXME optimize, do not create a copy each time of the doc and throw the old just away....this is not gentle for the gc
  public ScoreDocWithOriginalScore(ScoreDoc scoreDoc) {
    super(scoreDoc.doc, scoreDoc.score);
    this.originalScore = scoreDoc.score;
  }

  // FIXME optimize
  public static ScoreDocWithOriginalScore[] makeListFromScoreDocs(ScoreDoc[] scoreDocs) {
    ScoreDocWithOriginalScore[] withOriginalScores = new ScoreDocWithOriginalScore[scoreDocs.length];
    for (int i = 0; i < scoreDocs.length; i++) {
      if (scoreDocs[i] instanceof ScoreDocWithOriginalScore) {
        withOriginalScores[i] = (ScoreDocWithOriginalScore) scoreDocs[i];
      } else {
        withOriginalScores[i] = new ScoreDocWithOriginalScore(scoreDocs[i]);
      }
    }
    return withOriginalScores;
  }

  // A convenience method for debugging.
  @Override
  public String toString() {
    return "doc=" + doc + " score=" + score + " originalScore=" + originalScore + " shardIndex=" + shardIndex;
  }
}