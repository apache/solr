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

package org.apache.solr.monitor.search;

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
import org.apache.solr.monitor.MonitorDataValues;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.search.ExtendedQueryBase;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

class ReverseSearchQuery extends ExtendedQueryBase {

    private final ReverseSearchContext reverseSearchContext;
    private Query inner;

    public ReverseSearchQuery(ReverseSearchContext reverseSearchContext, Query inner) {
        this.reverseSearchContext = reverseSearchContext;
        this.inner = inner;
    }

    @Override
    public boolean getCache() {
        return false;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        inner.visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReverseSearchQuery that = (ReverseSearchQuery) o;
        return Objects.equals(reverseSearchContext, that.reverseSearchContext) && Objects.equals(inner, that.inner);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewritten = inner.rewrite(indexSearcher);
        if (rewritten != inner){
            return new ReverseSearchQuery(reverseSearchContext, inner.rewrite(indexSearcher));
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reverseSearchContext, inner);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ReverseSearchWeight(this, reverseSearchContext, inner.createWeight(searcher, scoreMode, boost));
    }

    static class ReverseSearchWeight extends Weight {

        private final ReverseSearchContext reverseSearchContext;
        private final Weight innerWeight;

        ReverseSearchWeight(Query query, ReverseSearchContext reverseSearchContext, Weight innerWeight) {
            super(query);
            this.reverseSearchContext = reverseSearchContext;
            this.innerWeight = innerWeight;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return innerWeight.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return new ReverseSearchScorer(innerWeight.scorer(context), reverseSearchContext, context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return innerWeight.count(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            return super.bulkScorer(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            ReverseSearchWeight weight = this;
            return new ScorerSupplier() {

                @Override
                public Scorer get(long leadCost) throws IOException {
                    return weight.scorer(context);
                }

                @Override
                public long cost() {
                    return Long.MAX_VALUE;
                }
            };
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return innerWeight.matches(context, doc);
        }
    }

    static class ReverseSearchScorer extends Scorer {
        private static final float MATCH_COST = 100.0f;

        private final Scorer inner;
        private final MonitorQueryCache monitorQueryCache;
        private final SolrMonitorQueryDecoder queryDecoder;
        private final SolrMatcherSink matcherSink;
        private final MonitorDataValues dataValues = new MonitorDataValues();

        ReverseSearchScorer(Scorer inner, ReverseSearchContext reverseSearchContext, LeafReaderContext leafReaderContext) throws IOException {
            super(inner.getWeight());
            this.inner = inner;
            this.monitorQueryCache = reverseSearchContext.queryCache;
            this.queryDecoder = reverseSearchContext.queryDecoder;
            this.matcherSink = reverseSearchContext.solrMatcherSink;
            this.dataValues.update(leafReaderContext);
        }

        @Override
        public DocIdSetIterator iterator() {
            return inner.iterator();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return inner.getMaxScore(upTo);
        }

        @Override
        public float score() throws IOException {
            return inner.score();
        }

        @Override
        public int docID() {
            return inner.docID();
        }

        @Override
        public Weight getWeight() {
            return inner.getWeight();
        }

        @Override
        public int advanceShallow(int target) throws IOException {
            return inner.advanceShallow(target);
        }

        @Override
        public float smoothingScore(int docId) throws IOException {
            return inner.smoothingScore(docId);
        }

        @Override
        public void setMinCompetitiveScore(float minScore) throws IOException {
            inner.setMinCompetitiveScore(minScore);
        }

        @Override
        public Collection<ChildScorable> getChildren() throws IOException {
            return inner.getChildren();
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {

            return new TwoPhaseIterator(inner.iterator()) {

                private String lastQueryId;

                @Override
                public boolean matches() throws IOException {
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

                private QCEVisitor getEntry(MonitorDataValues dataValues) throws IOException {
                    var versionedEntry =
                            monitorQueryCache == null
                                    ? null
                                    : monitorQueryCache.computeIfStale(dataValues, queryDecoder);
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

        private final MonitorQueryCache queryCache;
        private final SolrMonitorQueryDecoder queryDecoder;
        private final SolrMatcherSink solrMatcherSink;

        ReverseSearchContext(
                MonitorQueryCache queryCache,
                SolrMonitorQueryDecoder queryDecoder,
                SolrMatcherSink solrMatcherSink) {
            this.queryCache = queryCache;
            this.queryDecoder = queryDecoder;
            this.solrMatcherSink = solrMatcherSink;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReverseSearchContext that = (ReverseSearchContext) o;
            return Objects.equals(queryCache, that.queryCache) && Objects.equals(queryDecoder, that.queryDecoder) && Objects.equals(solrMatcherSink, that.solrMatcherSink);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryCache, queryDecoder, solrMatcherSink);
        }
    }
}
