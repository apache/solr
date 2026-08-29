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
package org.apache.solr.search.join.auxindexjoin;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.join.auxindexjoin.AuxIndexManager.JoinSegmentReference;

final class JoinIndexWeight extends Weight {
  final IndexSearcher maybeStaleJoinSearcher;
  final Map<String, JoinSegmentReference> existingJoinSegments;
  private final ScoreMode scoreMode;
  private final float boost;
  private final IndexReader toReader;
  private final Future<FromLeafJoinContext>[] fromColumnFutures;

  /**
   * [toSegmentOrd][fromSegmentOrd] -> the pair column resolved at construction time; null where the
   * from segment has no cached matches, the pair maps no from-side values, or no cached match falls
   * into the pair's from-doc range. Pair columns record the sidecar segment name, not a leaf
   * context, so they stay valid across join reader refreshes.
   */
  // private final PairColumn[][] pairColumnsByTo;

  JoinIndexWeight(
      AuxIndexJoinQuery auxIndexJoinQuery,
      IndexSearcher maybeStaleJoinSearcher,
      Map<String, JoinSegmentReference> existingJoinSegments,
      IndexReader toReader,
      ScoreMode scoreMode,
      float boost,
      Future<FromLeafJoinContext>[] foreignColsFutures) {
    super(auxIndexJoinQuery);
    this.maybeStaleJoinSearcher = maybeStaleJoinSearcher;
    this.existingJoinSegments = existingJoinSegments;
    this.scoreMode = scoreMode;
    this.boost = boost;
    this.toReader = toReader;
    this.fromColumnFutures = foreignColsFutures;
  }

  private AuxIndexJoinQuery aiJQuery() {
    return (AuxIndexJoinQuery) this.parentQuery;
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    ScorerSupplier supplier = scorerSupplier(context);
    if (supplier != null) {
      Scorer scorer = supplier.get(1);
      if (scorer.iterator().advance(doc) == doc) {
        return Explanation.match(scorer.score(), aiJQuery().toString());
      }
    }
    return Explanation.noMatch(aiJQuery().toString());
  }

  @Override
  public int count(LeafReaderContext context) throws IOException {
    // cost() is a range-size upper bound, not an exact match count, so counting has to
    // fall back to actually driving the two-phase iterator
    return super.count(context);
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext tolrc) throws IOException {
    IndexSearcher joinSearcher = aiJQuery().joinIndex.acquire();
    try {
      JoinIndexScorerSupplier ctx = null;
      try {
        ctx =
            new JoinIndexScorerSupplier(
                tolrc,
                aiJQuery().fromField,
                aiJQuery().fromQuery,
                aiJQuery().fromSearcher,
                aiJQuery().toField,
                this.toReader,
                this.existingJoinSegments,
                this.maybeStaleJoinSearcher,
                joinSearcher,
                aiJQuery().joinIndex,
                this.fromColumnFutures,
                scoreMode,
                boost);
      } catch (ExecutionException e) { // TODO review exception
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return ctx.isEmpty() ? null : ctx;
    } finally {
      aiJQuery().joinIndex.release(joinSearcher);
    }
  }

  @Override
  public boolean isCacheable(LeafReaderContext lrc) {
    // matches depend on the from-side searcher and the external join index, which the
    // query cache cannot see
    return false;
  }
}
