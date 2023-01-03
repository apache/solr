package com.flipkart.solr.ltr.utils;

import com.flipkart.solr.ltr.query.FKLTRScoringQuery;
import org.apache.solr.request.SolrQueryRequest;

public class SolrQueryRequestContextUtils {

  /** key prefix to reduce possibility of clash with other code's key choices **/
  private static final String FKLTR_PREFIX = "fkltr.";

  /** key of the scoring query in the request context **/
  private static final String SCORING_QUERY = FKLTR_PREFIX + "scoring_query";

  /** scoring query accessors **/

  public static void setScoringQuery(SolrQueryRequest req, FKLTRScoringQuery scoringQuery) {
    req.getContext().put(SCORING_QUERY, scoringQuery);
  }

  public static FKLTRScoringQuery getScoringQuery(SolrQueryRequest req) {
    return (FKLTRScoringQuery) req.getContext().get(SCORING_QUERY);
  }

}