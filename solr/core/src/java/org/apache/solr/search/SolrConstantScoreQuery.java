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
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.   This Solr extension also supports
 * weighting of a SolrFilter.
 *
 * Experimental and subject to change.
 */
public class SolrConstantScoreQuery extends Query implements ExtendedQuery {
  private Query query;
  boolean cache = true;  // cache by default
  int cost;

  public SolrConstantScoreQuery(Query query) {
    Objects.requireNonNull(query);
    this.query = query;
  }

  public SolrConstantScoreQuery() {}

  /** Returns the encapsulated filter */
  public Query getQuery() {
    return query;
  }

  @Override
  public void setCache(boolean cache) {
    this.cache = cache;
  }

  @Override
  public boolean getCache() {
    return cache;
  }

  @Override
  public void setCost(int cost) {
    this.cost = cost;
  }

  @Override
  public int getCost() {
    return cost;
  }

  protected class ConstantWeight extends ConstantScoreWeight {
    private ScoreMode scoreMode;
    private Weight wrappedWeight;

    public ConstantWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      super(SolrConstantScoreQuery.this, boost);
      this.scoreMode = scoreMode;
      if (query != null) // Not a DocSetQuery
        this.wrappedWeight = query.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer scorer = wrappedWeight.scorer(context);
      if (scorer == null) return null;
      DocIdSetIterator disi = scorer.iterator();
      return disi == null ? null : new ConstantScoreScorer(this, score(), scoreMode, disi);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      final Scorer scorer = scorer(context);
      final boolean match = (scorer != null && scorer.iterator().advance(doc) == doc);
      if (match) {
        assert scorer.score() == 0f;
        return Explanation.match(0f, "Match on id " + doc);
      } else {
        return Explanation.match(0f, "No match on id " + doc);
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new SolrConstantScoreQuery.ConstantWeight(searcher, scoreMode, boost);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    return ExtendedQueryBase.getOptionsString(this) + "ConstantScore(" + query + ")";
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           Objects.equals(query, ((SolrConstantScoreQuery) other).query);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return 31 * classHash() + query.hashCode();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
