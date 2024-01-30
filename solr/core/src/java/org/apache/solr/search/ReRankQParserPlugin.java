/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryRescorer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 *
 *  Syntax: q=*:*&rq={!rerank reRankQuery=$rqq reRankDocs=300 reRankWeight=3}
 *
 */

public class ReRankQParserPlugin extends QParserPlugin {

  public static final String NAME = "rerank";
  private static Query defaultQuery = new MatchAllDocsQuery();

  public static final String RERANK_QUERY = "reRankQuery";

  public static final String RERANK_DOCS = "reRankDocs";
  public static final int RERANK_DOCS_DEFAULT = 200;

  public static final String RERANK_WEIGHT = "reRankWeight";
  public static final double RERANK_WEIGHT_DEFAULT = 2.0d;

  public static final String RERANK_OPERATOR = "reRankOperator";
  public static final String RERANK_OPERATOR_DEFAULT = "add";

  public static final String RERANK_SCALE = "reRankScale";
  public static final String RERANK_MAIN_SCALE = "reRankMainScale";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public QParser createParser(
      String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ReRankQParser(query, localParams, params, req);
  }

  private static class ReRankQParser extends QParser {

    public ReRankQParser(
        String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      String reRankQueryString = localParams.get(RERANK_QUERY);
      if (StrUtils.isBlank(reRankQueryString)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, RERANK_QUERY + " parameter is mandatory");
      }
      QParser reRankParser = QParser.getParser(reRankQueryString, req);
      Query reRankQuery = reRankParser.parse();

      int reRankDocs = localParams.getInt(RERANK_DOCS, RERANK_DOCS_DEFAULT);
      reRankDocs = Math.max(1, reRankDocs);

      double reRankWeight = localParams.getDouble(RERANK_WEIGHT, RERANK_WEIGHT_DEFAULT);

      ReRankOperator reRankOperator =
          ReRankOperator.get(localParams.get(RERANK_OPERATOR, RERANK_OPERATOR_DEFAULT));

      String mainScale = localParams.get(RERANK_MAIN_SCALE);
      String reRankScale = localParams.get(RERANK_SCALE);
      boolean debugQuery = params.getBool(CommonParams.DEBUG_QUERY, false);

      if (!debugQuery) {
        String[] debugParams = params.getParams(CommonParams.DEBUG);
        if (debugParams != null) {
          for (String debugParam : debugParams) {
            if ("true".equals(debugParam)) {
              debugQuery = true;
              break;
            }
          }
        }
      }

      double reRankScaleWeight = reRankWeight;

      ReRankScaler reRankScaler =
          new ReRankScaler(
              mainScale,
              reRankScale,
              reRankScaleWeight,
              reRankOperator,
              new ReRankQueryRescorer(reRankQuery, 1, ReRankOperator.REPLACE),
              debugQuery);

      if (reRankScaler.scaleScores()) {
        // Scaler applies the weighting instead of the rescorer
        reRankWeight = 1;
      }

      return new ReRankQuery(
          reRankQuery, reRankDocs, reRankWeight, reRankOperator, reRankScaler, debugQuery);
    }
  }

  private static final class ReRankQueryRescorer extends QueryRescorer {

    final BiFloatFunction scoreCombiner;

    @FunctionalInterface
    interface BiFloatFunction {
      float func(float a, float b);
    }

    public ReRankQueryRescorer(
        Query reRankQuery, double reRankWeight, ReRankOperator reRankOperator) {
      super(reRankQuery);
      switch (reRankOperator) {
        case ADD:
          scoreCombiner = (score, second) -> (float) (score + reRankWeight * second);
          break;
        case MULTIPLY:
          scoreCombiner = (score, second) -> (float) (score * reRankWeight * second);
          break;
        case REPLACE:
          scoreCombiner = (score, second) -> (float) (reRankWeight * second);
          break;
        default:
          scoreCombiner = null;
          throw new IllegalArgumentException("Unexpected: reRankOperator=" + reRankOperator);
      }
    }

    @Override
    protected float combine(
        float firstPassScore, boolean secondPassMatches, float secondPassScore) {
      float score = firstPassScore;
      if (secondPassMatches) {
        return scoreCombiner.func(score, secondPassScore);
      }
      return score;
    }
  }

  private static final class ReRankQuery extends AbstractReRankQuery {
    private final Query reRankQuery;
    private final double reRankWeight;
    private final boolean debugQuery;

    @Override
    public int hashCode() {
      return 31 * classHash()
          + mainQuery.hashCode()
          + reRankQuery.hashCode()
          + (int) reRankWeight
          + reRankDocs
          + reRankOperator.hashCode()
          + reRankScaler.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(ReRankQuery rrq) {
      return mainQuery.equals(rrq.mainQuery)
          && reRankQuery.equals(rrq.reRankQuery)
          && reRankWeight == rrq.reRankWeight
          && reRankDocs == rrq.reRankDocs
          && reRankOperator.equals(rrq.reRankOperator)
          && reRankScaler.equals(rrq.reRankScaler);
    }

    public ReRankQuery(
        Query reRankQuery,
        int reRankDocs,
        double reRankWeight,
        ReRankOperator reRankOperator,
        ReRankScaler reRankScaler,
        boolean debugQuery) {
      super(
          defaultQuery,
          reRankDocs,
          new ReRankQueryRescorer(reRankQuery, reRankWeight, reRankOperator),
          reRankScaler,
          reRankOperator);
      this.reRankQuery = reRankQuery;
      this.reRankWeight = reRankWeight;
      this.debugQuery = debugQuery;
    }

    @Override
    public String toString(String s) {
      final StringBuilder sb =
          new StringBuilder(100); // default initialCapacity of 16 won't be enough
      sb.append("{!").append(NAME);
      sb.append(" mainQuery='").append(mainQuery.toString()).append("' ");
      sb.append(RERANK_QUERY).append("='").append(reRankQuery.toString()).append("' ");
      sb.append(RERANK_DOCS).append('=').append(reRankDocs).append(' ');
      if (reRankScaler.scaleScores()) {
        // The reRankScaler applies the weight
        sb.append(RERANK_WEIGHT)
            .append('=')
            .append(reRankScaler.getReRankScaleWeight())
            .append(' ');
      } else {
        sb.append(RERANK_WEIGHT).append('=').append(reRankWeight).append(' ');
      }
      if (reRankScaler.getReRankScalerExplain().getReRankScale() != null) {
        sb.append(RERANK_SCALE)
            .append('=')
            .append(reRankScaler.getReRankScalerExplain().getReRankScale())
            .append(' ');
      }
      if (reRankScaler.getReRankScalerExplain().getMainScale() != null) {
        sb.append(RERANK_MAIN_SCALE)
            .append('=')
            .append(reRankScaler.getReRankScalerExplain().getMainScale())
            .append(' ');
      }
      sb.append(RERANK_OPERATOR).append('=').append(reRankOperator.toLower()).append('}');
      return sb.toString();
    }

    @Override
    protected Query rewrite(Query rewrittenMainQuery) throws IOException {
      return new ReRankQuery(
              reRankQuery, reRankDocs, reRankWeight, reRankOperator, reRankScaler, debugQuery)
          .wrap(rewrittenMainQuery);
    }

    @Override
    public boolean getCache() {
      if (reRankScaler.scaleScores() && debugQuery) {
        // Caching breaks explain when reRankScaling is used.
        return false;
      } else {
        return super.getCache();
      }
    }
  }
}
