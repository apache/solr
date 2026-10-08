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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/**
 * Creates a query-time join query backed by Lucene's {@link JoinUtil#createJoinQuery(String, Query,
 * Query, IndexSearcher, ScoreMode, OrdinalMap)} using global ordinals.
 *
 * <p>It joins documents matching a subordinate from-query to documents matching a target to-query
 * (the {@code which} parameter) on the same single-valued string docValues field.
 *
 * <p>Local parameters:
 *
 * <ul>
 *   <li>{@code joinField} - single-valued docValues string field to join on. Required.
 *   <li>{@code which} - subquery defining the target "to" documents. Required.
 *   <li>{@code v} - subquery defining the "from" documents (or specified as the query body).
 *       Required.
 *   <li>{@code score} - optional score mode: {@code none}, {@code avg}, {@code max}, {@code min},
 *       {@code total}/{@code sum}. Defaults to {@code none}.
 * </ul>
 *
 * <p>Example: {@code q={!globalOrdinalsJoin joinField=sku_id_s which="type:parent"
 * score=max}color:blue}
 */
public class GlobalOrdinalsJoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "globalOrdinalsJoin";
  public static final String JOIN_FIELD = "joinField";
  public static final String WHICH = "which";
  public static final String SCORE = "score";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String joinField = localParams.get(JOIN_FIELD);
        if (joinField == null || joinField.isBlank()) {
          throw new SyntaxError("'" + JOIN_FIELD + "' is required for '" + NAME + "' query parser");
        }

        final SchemaField sf = req.getSchema().getFieldOrNull(joinField);
        if (sf == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "joinField '" + joinField + "' does not exist in schema");
        }
        if (!sf.hasDocValues()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "joinField '" + joinField + "' must have docValues enabled");
        }
        if (sf.multiValued()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "joinField '" + joinField + "' must be single-valued");
        }
        if (sf.getType().getNumberType() != null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "joinField '"
                  + joinField
                  + "' must be a String field, but has numeric type "
                  + sf.getType().getNumberType());
        }

        final String whichStr = localParams.get(WHICH);
        if (whichStr == null || whichStr.isBlank()) {
          throw new SyntaxError("'" + WHICH + "' is required for '" + NAME + "' query parser");
        }

        final String fromQueryStr = localParams.get(CommonParams.VALUE);
        final String effectiveFromStr =
            (fromQueryStr != null && !fromQueryStr.isBlank()) ? fromQueryStr : qstr;
        if (effectiveFromStr == null || effectiveFromStr.isBlank()) {
          throw new SyntaxError("from query is required for '" + NAME + "' query parser");
        }

        final ScoreMode scoreMode;
        final String scoreParam = getParam(SCORE);
        if (scoreParam == null || scoreParam.isBlank()) {
          scoreMode = ScoreMode.None;
        } else {
          scoreMode = ScoreModeParser.parse(scoreParam);
        }

        final Query fromQuery = subQuery(effectiveFromStr, null).getQuery();
        final Query toQuery = subQuery(whichStr, null).getQuery();

        return createJoinQuery(fromQuery, toQuery, joinField, scoreMode);
      }
    };
  }

  /**
   * Helper method to create a {@link GlobalOrdinalsJoinQuery}.
   *
   * @param fromQuery the query defining matching docs on the "from" side
   * @param toQuery the query defining candidate docs on the "to" side
   * @param joinField the single-valued string docValues field name
   * @param scoreMode scoring mode
   * @return a {@link GlobalOrdinalsJoinQuery} instance
   */
  public static Query createJoinQuery(
      Query fromQuery, Query toQuery, String joinField, ScoreMode scoreMode) {
    return new GlobalOrdinalsJoinQuery(fromQuery, toQuery, joinField, scoreMode);
  }

  /** Query representing a join based on global ordinals across segments. */
  public static class GlobalOrdinalsJoinQuery extends Query {
    protected final Query fromQuery;
    protected final Query toQuery;
    protected final String joinField;
    protected final ScoreMode scoreMode;

    public GlobalOrdinalsJoinQuery(
        Query fromQuery, Query toQuery, String joinField, ScoreMode scoreMode) {
      this.fromQuery = Objects.requireNonNull(fromQuery, "fromQuery must not be null");
      this.toQuery = Objects.requireNonNull(toQuery, "toQuery must not be null");
      this.joinField = Objects.requireNonNull(joinField, "joinField must not be null");
      this.scoreMode = scoreMode == null ? ScoreMode.None : scoreMode;
    }

    public Query getFromQuery() {
      return fromQuery;
    }

    public Query getToQuery() {
      return toQuery;
    }

    public String getJoinField() {
      return joinField;
    }

    public ScoreMode getScoreMode() {
      return scoreMode;
    }

    @Override
    public Weight createWeight(
        IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
        throws IOException {
      OrdinalMap ordinalMap = null;
      if (searcher.getIndexReader().leaves().size() > 1) {
        final SortedDocValues sdv;
        if (searcher instanceof SolrIndexSearcher sis) {
          sdv = sis.getSlowAtomicReader().getSortedDocValues(joinField);
        } else {
          sdv = MultiDocValues.getSortedValues(searcher.getIndexReader(), joinField);
        }
        if (sdv == null) {
          return new MatchNoDocsQuery("No join values for " + joinField)
              .createWeight(searcher, scoreMode, boost);
        }
        if (sdv instanceof MultiDocValues.MultiSortedDocValues multi) {
          ordinalMap = multi.mapping;
        }
      }
      final Query jq =
          JoinUtil.createJoinQuery(
              joinField, fromQuery, toQuery, searcher, this.scoreMode, ordinalMap);
      return jq.rewrite(searcher).createWeight(searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
      return "GlobalOrdinalsJoinQuery [fromQuery="
          + fromQuery
          + ", toQuery="
          + toQuery
          + ", joinField="
          + joinField
          + ", scoreMode="
          + scoreMode
          + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = classHash();
      result = prime * result + Objects.hashCode(fromQuery);
      result = prime * result + Objects.hashCode(toQuery);
      result = prime * result + Objects.hashCode(joinField);
      result = prime * result + Objects.hashCode(scoreMode);
      return result;
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(GlobalOrdinalsJoinQuery other) {
      return Objects.equals(fromQuery, other.fromQuery)
          && Objects.equals(toQuery, other.toQuery)
          && Objects.equals(joinField, other.joinField)
          && Objects.equals(scoreMode, other.scoreMode);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }
  }
}
