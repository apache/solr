package com.flipkart.solr.ltr.search;

import java.util.Optional;
import java.util.Set;

import com.flipkart.solr.ltr.query.BaseRescoringContext;
import com.flipkart.solr.ltr.utils.SolrQueryRequestContextUtils;
import com.flipkart.solr.ltr.scorer.FKLTRDocumentScorer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import com.flipkart.solr.ltr.query.FKLTRScoringQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.RankQuery;

/**
 * Plug into solr a reRanking module.
 *
 * Learning to Rank Query Parser Syntax: rq={!fkltr reRankDocs=300 rescoringContext={}}
 *
 */
public abstract class FKLTRQParserPlugin<RescoringContext extends BaseRescoringContext, ScoreMeta>
    extends QParserPlugin {
  public static final String NAME = "fkltr";

  private static Query defaultQuery = new MatchAllDocsQuery();

  /** query parser plugin: default number of documents to rerank **/
  private static final int DEFAULT_RERANK_DOCS = 200;

  /** by default filterCollector optimisation is enabled **/
  private static final boolean DEFAULT_FILTER_COLLECTOR_ENABLED = true;

  /**
   * query parser plugin:the param that will select the number of documents to rerank
   **/
  public static final String RERANK_DOCS = "reRankDocs";

  /**
   * rescorer specific params.
   */
  public static final String RESCORING_CONTEXT = "rescoringContext";

  public static final String FILTER_COLLECTOR_ENABLED = "filterCollectorEnabled";

  protected abstract FKLTRDocumentScorer<RescoringContext, ScoreMeta> provideFKLTRDocumentScorer();

  @Override
  public QParser createParser(String qStr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new FKLTRQParser(qStr, localParams, params, req, provideFKLTRDocumentScorer());
  }

  public class FKLTRQParser extends QParser {
    private final FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer;

    FKLTRQParser(String qstr, SolrParams localParams, SolrParams params,
                 SolrQueryRequest req, FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer) {
      super(qstr, localParams, params, req);
      this.fkltrDocumentScorer = fkltrDocumentScorer;
    }

    @Override
    public Query parse() {
      final FKLTRScoringQuery<RescoringContext, ScoreMeta> scoringQuery = new FKLTRScoringQuery<>();

      SolrQueryRequestContextUtils.setScoringQuery(req, scoringQuery);

      int reRankDocs = localParams.getInt(RERANK_DOCS, DEFAULT_RERANK_DOCS);
      if (reRankDocs <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Must rerank at least 1 document");
      }

      String rescoringContextString = localParams.get(RESCORING_CONTEXT);
      if (rescoringContextString == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "requires rescoringContextString for reranking");
      }
      scoringQuery.setRescoringContext(parseRescoringContext(rescoringContextString));

      scoringQuery.setFilterCollectorEnabled(
          localParams.getBool(FILTER_COLLECTOR_ENABLED, DEFAULT_FILTER_COLLECTOR_ENABLED));

      return new FKLTRQuery(scoringQuery, reRankDocs, fkltrDocumentScorer);
    }
  }

  protected abstract RescoringContext parseRescoringContext(String contextString);

  /**
   * A learning to rank Query, will encapsulate the user and supply context, and delegate to it the rescoring
   * of the documents.
   **/
  public class FKLTRQuery extends AbstractReRankQuery {
    private final FKLTRScoringQuery<RescoringContext, ScoreMeta> scoringQuery;
    private final FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer;

    FKLTRQuery(FKLTRScoringQuery<RescoringContext, ScoreMeta> scoringQuery, int reRankDocs,
               FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer) {
      super(defaultQuery, reRankDocs, new FKLTRRescorer<>(fkltrDocumentScorer, scoringQuery));
      this.scoringQuery = scoringQuery;
      this.fkltrDocumentScorer = fkltrDocumentScorer;
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + (mainQuery.hashCode() + scoringQuery.hashCode() + reRankDocs);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      return equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(FKLTRQuery other) {
      return (mainQuery.equals(other.mainQuery)
          && scoringQuery.equals(other.scoringQuery) && (reRankDocs == other.reRankDocs));
    }

    @Override
    public RankQuery wrap(Query _mainQuery) {
      super.wrap(_mainQuery);
      return this;
    }

    @Override
    public String toString(String field) {
      return "{!fkltr mainQuery='" + mainQuery.toString() + "' scoringQuery='"
          + scoringQuery.toString() + "' reRankDocs=" + reRankDocs + "}";
    }

    @Override
    protected Query rewrite(Query rewrittenMainQuery) {
      return new FKLTRQuery(scoringQuery, reRankDocs, fkltrDocumentScorer).wrap(rewrittenMainQuery);
    }

//    @Override
    public Optional<String> getCustomGroupField() {
      return scoringQuery.getRescoringContext().getCustomGroupField();
    }

//    @Override
    public Set<String> getGroupsRequired() {
      return scoringQuery.getRescoringContext().getGroupsRequired();
    }

//    @Override
    public int getLimitPerGroup() {
      return scoringQuery.getRescoringContext().getLimitPerGroup();
    }

//    @Override
    public boolean isFilterCollectorEnabled() {
      return scoringQuery.isFilterCollectorEnabled();
    }
  }
}
