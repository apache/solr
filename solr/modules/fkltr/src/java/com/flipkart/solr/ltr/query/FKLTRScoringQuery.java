package com.flipkart.solr.ltr.query;

import java.util.Objects;
import java.util.StringJoiner;

import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;

/**
 * The ranking query that encapsulates the context for reRanking.
 */
public class FKLTRScoringQuery<RescoringContext extends BaseRescoringContext, ScoreMeta> {
  /** context for this query which is used in reranking */
  private RescoringContext rescoringContext;

  /** Query execution specific config. This does not take part in equalsAndHashcode */
  private boolean filterCollectorEnabled;

  /** Query execution specific data, holds the scoreMeta against docId. This does not take part in equalsAndHashcode */
  private ScoreMetaHolder<ScoreMeta> scoreMetaHolder;

  public RescoringContext getRescoringContext() {
    return rescoringContext;
  }

  public void setRescoringContext(RescoringContext rescoringContext) {
    this.rescoringContext = rescoringContext;
  }

  public boolean isFilterCollectorEnabled() {
    return filterCollectorEnabled;
  }

  public void setFilterCollectorEnabled(boolean filterCollectorEnabled) {
    this.filterCollectorEnabled = filterCollectorEnabled;
  }

  public ScoreMetaHolder<ScoreMeta> getScoreMetaHolder() {
    return scoreMetaHolder;
  }

  public void setScoreMetaHolder(ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    this.scoreMetaHolder = scoreMetaHolder;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FKLTRScoringQuery<?, ?> that = (FKLTRScoringQuery<?, ?>) o;
    return Objects.equals(rescoringContext, that.rescoringContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rescoringContext);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", FKLTRScoringQuery.class.getSimpleName() + "[", "]")
        .add("rescoringContext=" + rescoringContext)
        .add("filterCollectorEnabled=" + filterCollectorEnabled)
        .toString();
  }
}
