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

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;

/**
 *
 */
public class QueryUtils {

  /** return true if this query has no positive components */
  public static boolean isNegative(Query q) {
    if (!(q instanceof BooleanQuery)) return false;
    BooleanQuery bq = (BooleanQuery)q;
    Collection<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return false;
    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return false;
    }
    return true;
  }

  /**
   * Recursively unwraps the specified query to determine whether it is capable of producing a score
   * that varies across different documents. Returns true if this query is not capable of producing a
   * varying score (i.e., it is a constant score query).
   */
  public static boolean isConstantScoreQuery(Query q) {
    return isConstantScoreQuery(q, null);
  }

  private static Map<Query, Void> lazyInitSeen(Map<Query, Void> seen, Query add) {
    if (seen == null) {
      seen = new IdentityHashMap<>();
    }
    seen.put(add, null);
    return seen;
  }

  /**
   * Returns true if the specified query is guaranteed to assign the same score to all docs; otherwise false
   * @param q query to be evaluated
   * @param seen used to detect possible loops in nested query input
   */
  private static boolean isConstantScoreQuery(Query q, Map<Query, Void> seen) {
    for (;;) {
      final Query unwrapped;
      if (q instanceof BoostQuery) {
        unwrapped = ((BoostQuery) q).getQuery();
        // NOTE: BoostQuery class and its inner query are final, so there's no risk of direct loops
      } else if (q instanceof WrappedQuery) {
        unwrapped = ((WrappedQuery) q).getWrappedQuery();
        // NOTE: Neither WrappedQuery class nor its inner query are final, so there is a risk of direct loops
        // TODO: Only the queries we explicitly check for in this method are relevant wrt detecting loops, and
        //  only `WrappedQuery` currently presents a risk in that respect; we may be able to avoid this risk
        //  by more tightly restricting the `WrappedQuery` API (e.g., making the get/set methods `final`)?
        if (unwrapped == q) {
          throw new IllegalStateException("recursive query");
        }
      } else if (q instanceof ConstantScoreQuery) {
        return true;
      } else if (q instanceof MatchAllDocsQuery) {
        return true;
      } else if (q instanceof MatchNoDocsQuery) {
        return true;
      } else if (q instanceof Filter || q instanceof SolrConstantScoreQuery) {
        // TODO: this clause will be replaced with `q instanceof DocSetQuery`, pending SOLR-12336
        return true;
      } else if (q instanceof BooleanQuery) {
        // do actual method call recursion for BooleanQuery
        boolean addedQ = false;
        for (BooleanClause c : (BooleanQuery) q) {
          if (c.isScoring()) {
            // NOTE: there's no purpose to checking `q == c.getQuery()` because BooleanQuery is final, with
            // a builder that prevents direct loops. But as long as it is in principle possible to have a
            // query that at some level wraps its own ancestor, we have to add `q` in order to detect deeper
            // loops here.
            if (!isConstantScoreQuery(c.getQuery(), addedQ ? seen : lazyInitSeen(seen, q))) {
              return false;
            }
            addedQ = true;
          }
        }
        // BooleanQuery with no scoring clauses capable of producing a score is itself a constant-score query
        return true;
      } else {
        return false;
      }
      // For wrapping queries that only wrap one query, we can use a loop instead of true recursion
      lazyInitSeen(seen, q);
      q = unwrapped;
    }
  }

  /** Returns the original query if it was already a positive query, otherwise
   * return the negative of the query (i.e., a positive query).
   * <p>
   * Example: both id:10 and id:-10 will return id:10
   * <p>
   * The caller can tell the sign of the original by a reference comparison between
   * the original and returned query.
   * @param q Query to create the absolute version of
   * @return Absolute version of the Query
   */
  public static Query getAbs(Query q) {
    if (q instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) q;
      Query subQ = bq.getQuery();
      Query absSubQ = getAbs(subQ);
      if (absSubQ == subQ) return q;
      return new BoostQuery(absSubQ, bq.getBoost());
    }

    if (q instanceof WrappedQuery) {
      Query subQ = ((WrappedQuery)q).getWrappedQuery();
      Query absSubQ = getAbs(subQ);
      if (absSubQ == subQ) return q;
      return new WrappedQuery(absSubQ);
    }

    if (!(q instanceof BooleanQuery)) return q;
    BooleanQuery bq = (BooleanQuery)q;

    Collection<BooleanClause> clauses = bq.clauses();
    if (clauses.size()==0) return q;


    for (BooleanClause clause : clauses) {
      if (!clause.isProhibited()) return q;
    }

    if (clauses.size()==1) {
      // if only one clause, dispense with the wrapping BooleanQuery
      Query negClause = clauses.iterator().next().getQuery();
      // we shouldn't need to worry about adjusting the boosts since the negative
      // clause would have never been selected in a positive query, and hence would
      // not contribute to a score.
      return negClause;
    } else {
      BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
      // ignore minNrShouldMatch... it doesn't make sense for a negative query

      // the inverse of -a -b is a OR b
      for (BooleanClause clause : clauses) {
        newBqB.add(clause.getQuery(), BooleanClause.Occur.SHOULD);
      }
      return newBqB.build();
    }
  }

  /** Makes negative queries suitable for querying by
   * lucene.
   */
  public static Query makeQueryable(Query q) {
    if (q instanceof WrappedQuery) {
      return makeQueryable(((WrappedQuery)q).getWrappedQuery());
    }
    return isNegative(q) ? fixNegativeQuery(q) : q;
  }

  /** Fixes a negative query by adding a MatchAllDocs query clause.
   * The query passed in *must* be a negative query.
   */
  public static Query fixNegativeQuery(Query q) {
    float boost = 1f;
    if (q instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) q;
      boost = bq.getBoost();
      q = bq.getQuery();
    }
    BooleanQuery bq = (BooleanQuery) q;
    BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
    newBqB.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    for (BooleanClause clause : bq) {
      newBqB.add(clause);
    }
    newBqB.add(new MatchAllDocsQuery(), Occur.MUST);
    BooleanQuery newBq = newBqB.build();
    return new BoostQuery(newBq, boost);
  }

  /** @lucene.experimental throw exception if max boolean clauses are exceeded */
  public static BooleanQuery build(BooleanQuery.Builder builder, QParser parser) {
    int configuredMax = parser != null ? parser.getReq().getCore().getSolrConfig().booleanQueryMaxClauseCount : IndexSearcher.getMaxClauseCount();
    BooleanQuery bq = builder.build();
    if (bq.clauses().size() > configuredMax) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Too many clauses in boolean query: encountered=" + bq.clauses().size() + " configured in solrconfig.xml via maxBooleanClauses=" + configuredMax);
    }
    return bq;
  }

  /**
   * Combines a scoring query with a non-scoring (filter) query.
   * If both parameters are null then return a {@link MatchAllDocsQuery}.
   * If only {@code scoreQuery} is present then return it.
   * If only {@code filterQuery} is present then return it wrapped with constant scoring.
   * If neither are null then we combine with a BooleanQuery.
   */
  public static Query combineQueryAndFilter(Query scoreQuery, Query filterQuery) {
    // check for *:* is simple and avoids needless BooleanQuery wrapper even though BQ.rewrite optimizes this away
    if (scoreQuery == null || scoreQuery instanceof MatchAllDocsQuery) {
      if (filterQuery == null) {
        return new MatchAllDocsQuery(); // default if nothing -- match everything
      } else {
        return new ConstantScoreQuery(filterQuery);
      }
    } else {
      if (filterQuery == null || filterQuery instanceof MatchAllDocsQuery) {
        return scoreQuery;
      } else {
        return new BooleanQuery.Builder()
            .add(scoreQuery, Occur.MUST)
            .add(filterQuery, Occur.FILTER)
            .build();
      }
    }
  }
}
