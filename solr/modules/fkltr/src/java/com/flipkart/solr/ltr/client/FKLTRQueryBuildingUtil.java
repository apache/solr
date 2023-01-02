package com.flipkart.solr.ltr.client;

import com.flipkart.solr.ltr.search.FKLTRQParserPlugin;

/**
 * Util class that helps in creating fkltr rerankQuery. Output of this is what you put against rq.
 */
public abstract class FKLTRQueryBuildingUtil<RescoringContext> {
  private static final String OPENING_BRACES_PLUS_PARSER = "{!"+ FKLTRQParserPlugin.NAME + " ";
  private static final String CLOSING_BRACES = "}";
  private static final String EQUALS = "=";
  private static final String SINGLE_SPACE = " ";

  @SuppressWarnings("unused")
  public Query newQuery(int reRankDocs, RescoringContext rescoringContext) {
    return new Query(reRankDocs, rescoringContext);
  }

  @SuppressWarnings("unused")
  public Query newQuery(int reRankDocs, RescoringContext rescoringContext, boolean filterCollectorEnabled) {
    return new Query(reRankDocs, rescoringContext, filterCollectorEnabled);
  }
  
  public class Query {
    private final int reRankDocs;
    private final RescoringContext rescoringContext;
    private final boolean filterCollectorEnabled;

    private Query(int reRankDocs, RescoringContext rescoringContext) {
      this.reRankDocs = reRankDocs;
      this.rescoringContext = rescoringContext;
      this.filterCollectorEnabled = true;
    }

    private Query(int reRankDocs, RescoringContext rescoringContext, boolean filterCollectorEnabled) {
      this.reRankDocs = reRankDocs;
      this.rescoringContext = rescoringContext;
      this.filterCollectorEnabled = filterCollectorEnabled;
    }

    public RescoringContext getRescoringContext() {
      return rescoringContext;
    }

    /**
     * @return queryString to be put against rq parameter.
     */
    @Override
    public String toString() {
      return OPENING_BRACES_PLUS_PARSER +
          FKLTRQParserPlugin.RERANK_DOCS + EQUALS + reRankDocs + SINGLE_SPACE +
          FKLTRQParserPlugin.RESCORING_CONTEXT + EQUALS + getRescoringContextString(rescoringContext) + SINGLE_SPACE +
          FKLTRQParserPlugin.FILTER_COLLECTOR_ENABLED + EQUALS + filterCollectorEnabled +
          CLOSING_BRACES;
    }
  }

  protected abstract String getRescoringContextString(RescoringContext rescoringContext);
}
