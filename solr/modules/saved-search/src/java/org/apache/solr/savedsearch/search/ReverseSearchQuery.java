/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.savedsearch.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.monitor.QCEVisitor;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.solr.savedsearch.SavedSearchDataValues;
import org.apache.solr.savedsearch.SavedSearchDecoder;
import org.apache.solr.savedsearch.cache.SavedSearchCache;

class ReverseSearchQuery extends Query {

  private final ReverseSearchContext reverseSearchContext;
  private final Query presearchQuery;

  public ReverseSearchQuery(ReverseSearchContext reverseSearchContext, Query presearchQuery) {
    this.reverseSearchContext = reverseSearchContext;
    this.presearchQuery = presearchQuery;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    presearchQuery.visit(visitor);
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = presearchQuery.rewrite(indexSearcher);
    if (rewritten != presearchQuery) {
      return new ReverseSearchQuery(reverseSearchContext, presearchQuery.rewrite(indexSearcher));
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public String toString(String field) {
    return this.getClass().getSimpleName()
        + "(presearchQuery="
        + presearchQuery.toString(field)
        + ")";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ReverseSearchWeight(
        this, reverseSearchContext, presearchQuery.createWeight(searcher, scoreMode, boost));
  }

  static class ReverseSearchWeight extends Weight {

    private final ReverseSearchContext reverseSearchContext;
    private final Weight presearchWeight;

    ReverseSearchWeight(
        Query query, ReverseSearchContext reverseSearchContext, Weight presearchWeight) {
      super(query);
      this.reverseSearchContext = reverseSearchContext;
      this.presearchWeight = presearchWeight;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return presearchWeight.explain(context, doc);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer scorer = presearchWeight.scorer(context);
      if (scorer == null) {
        return null;
      }
      return new ReverseSearchScorer(scorer, reverseSearchContext, context);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
      return presearchWeight.count(context);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      return super.bulkScorer(context);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer == null) {
        return null;
      }
      return new ScorerSupplier() {

        @Override
        public Scorer get(long leadCost) {
          return scorer;
        }

        @Override
        public long cost() {
          return Long.MAX_VALUE;
        }
      };
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return presearchWeight.matches(context, doc);
    }
  }

  static class ReverseSearchScorer extends Scorer {
    private static final float MATCH_COST = 100.0f;

    private final Scorer presearchScorer;
    private final SavedSearchCache savedSearchCache;
    private final SavedSearchDecoder queryDecoder;
    private final SolrMatcherSink matcherSink;
    private final Metadata metadata;
    private final SavedSearchDataValues dataValues;

    ReverseSearchScorer(
        Scorer presearchScorer,
        ReverseSearchContext reverseSearchContext,
        LeafReaderContext leafReaderContext)
        throws IOException {
      super(presearchScorer.getWeight());
      this.presearchScorer = presearchScorer;
      this.savedSearchCache = reverseSearchContext.queryCache;
      this.queryDecoder = reverseSearchContext.queryDecoder;
      this.matcherSink = reverseSearchContext.solrMatcherSink;
      this.metadata = new Metadata();
      matcherSink.captureMetadata(metadata);
      this.dataValues = new SavedSearchDataValues(leafReaderContext);
    }

    @Override
    public DocIdSetIterator iterator() {
      return presearchScorer.iterator();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return presearchScorer.getMaxScore(upTo);
    }

    @Override
    public float score() throws IOException {
      return presearchScorer.score();
    }

    @Override
    public int docID() {
      return presearchScorer.docID();
    }

    @Override
    public Weight getWeight() {
      return presearchScorer.getWeight();
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return presearchScorer.advanceShallow(target);
    }

    @Override
    public float smoothingScore(int docId) throws IOException {
      return presearchScorer.smoothingScore(docId);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      presearchScorer.setMinCompetitiveScore(minScore);
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
      return presearchScorer.getChildren();
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {

      return new TwoPhaseIterator(presearchScorer.iterator()) {

        private String lastQueryId;

        @Override
        public boolean matches() throws IOException {
          metadata.queriesRun++;
          int doc = approximation.docID();
          dataValues.advanceTo(doc);
          var entry = getEntry(dataValues);
          var queryId = dataValues.getQueryId();
          var originalMatchQuery = entry.getMatchQuery();

          var matchQuery = new ConstantScoreQuery(originalMatchQuery);

          boolean isMatch = matcherSink.matchQuery(queryId, matchQuery, entry.getMetadata());
          if (isMatch && !queryId.equals(lastQueryId)) {
            lastQueryId = queryId;
            return true;
          }
          return false;
        }

        private QCEVisitor getEntry(SavedSearchDataValues dataValues) throws IOException {
          var versionedEntry =
              savedSearchCache == null
                  ? null
                  : savedSearchCache.computeIfStale(dataValues, queryDecoder);
          return (versionedEntry == null || versionedEntry.version != dataValues.getVersion())
              ? queryDecoder.getComponent(dataValues, dataValues.getCacheId())
              : versionedEntry.entry;
        }

        @Override
        public float matchCost() {
          return MATCH_COST;
        }
      };
    }
  }

  static class ReverseSearchContext {

    private final SavedSearchCache queryCache;
    private final SavedSearchDecoder queryDecoder;
    private final SolrMatcherSink solrMatcherSink;

    ReverseSearchContext(
        SavedSearchCache queryCache,
        SavedSearchDecoder queryDecoder,
        SolrMatcherSink solrMatcherSink) {
      this.queryCache = queryCache;
      this.queryDecoder = queryDecoder;
      this.solrMatcherSink = solrMatcherSink;
    }
  }

  static class Metadata {

    static final Metadata IDENTITY = new Metadata();

    private int queriesRun;

    int getQueriesRun() {
      return queriesRun;
    }

    Metadata add(Metadata metadata) {
      Metadata result = new Metadata();
      result.queriesRun = this.queriesRun + metadata.queriesRun;
      return result;
    }
  }
}
