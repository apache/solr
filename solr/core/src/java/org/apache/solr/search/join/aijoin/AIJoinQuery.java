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
package org.apache.solr.search.join.aijoin;

import static org.apache.solr.search.join.aijoin.AIJoinUtil.cacheImpl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.join.aijoin.AIJoinIndex.JoinSegmentReference;

/**
 * Joins the from-side index to the to-side index this query is executed against, resolving
 * from-side docs matching {@code fromQuery} to to-side docs through the auxiliary join index
 * managed by {@link AIJoinIndex}: there, each (from-segment, to-segment) pair owns a SORTED_NUMERIC
 * column named by both sides' persistent keys, whose doc number is the from-side doc id and whose
 * value is the matching to-side doc id. Pair columns missing from the join index are built on
 * demand at weight creation, so no explicit build step exists; obtain instances via {@link
 * AIJoinIndex#newJoinQuery}. Matches score a constant.
 */
class AIJoinQuery extends Query {

  final AIJoinIndex joinIndex;
  final String fromField;
  final Query fromQuery;
  protected final IndexSearcher fromSearcher;
  final String toField;
  private final ExecutorService fromExecutorService;

  AIJoinQuery(
      AIJoinIndex joinIndex,
      String fromField,
      Query fromQuery,
      IndexSearcher fromSearcher,
      String toField,
      ExecutorService fromExecutorService) {
    this.joinIndex = Objects.requireNonNull(joinIndex, "joinIndex");
    this.fromField = Objects.requireNonNull(fromField, "fromField");
    this.fromQuery = Objects.requireNonNull(fromQuery, "fromQuery");
    this.fromSearcher = Objects.requireNonNull(fromSearcher, "fromSearcher");
    this.toField = Objects.requireNonNull(toField, "toField");
    this.fromExecutorService = fromExecutorService;
  }

  private AIJoinUtil.CacheAndCount computeDocIdSet(Weight fromWeight, LeafReaderContext ctx)
      throws IOException {
    // TODO figure out how to steal cached from side filters
    //    if (fromWeight!=null && fromWeight.getClass().getSimpleName().contains("Caching") ){
    //      System.out.println("fromWeight is CachingWeight");
    //    }
    ScorerSupplier supplier = fromWeight.scorerSupplier(ctx);
    if (supplier == null) {
      return null; // NO matches ???
    }
    // TODO handle already cached WeightWrapper
    BulkScorer scorer = supplier.bulkScorer();
    return cacheImpl(scorer, ctx.reader().maxDoc(), ctx.reader().getLiveDocs());
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    // the from-side selection rewrites against the from-side searcher, not against the (to-side)
    // searcher this query is executed with
    Query rewrittenFrom = fromQuery.rewrite(fromSearcher);
    if (rewrittenFrom != fromQuery) {
      return new AIJoinQuery(
          joinIndex, fromField, rewrittenFrom, fromSearcher, toField, fromExecutorService);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    //    Set<String> matchingFromKeys =
    //        AIJoinUtil.matchingFromSideKeys(cachedFromSearcher, fromQuery, fromField);
    Set<String> neededPairs = new HashSet<>();
    for (LeafReaderContext toContext : searcher.getIndexReader().leaves()) {
      String toKey = AIJoinUtil.getSideKey(toContext, toField);
      for (LeafReaderContext fromCtx : fromSearcher.getLeafContexts()) {
        String fromKey = AIJoinUtil.getSideKey(fromCtx, fromField);
        neededPairs.add(fromKey + "_" + toKey);
      }
    }

    joinIndex.onCreateWeight(neededPairs, fromSearcher, searcher); // ignoring fields

    // build any pair among neededPairs that isn't in the join index yet, up front, so this
    // weight's existingJoinSegments below is already complete instead of leaving the gaps to be
    // discovered lazily, one to-segment at a time, once scoring starts
    //
    // DON'T write'em upfront
    //
    // WAS:
    // joinIndex.ensureJoinSegments(neededPairs, fromSearcher, fromField, searcher, toField);
    //
    // TODO instead, look for needed pairs, grab from segments fields,
    // submit tasks loading from side components
    // fromOrd[fromDoc#] and term dict fromOrd[fromVal],
    // pass this futures array downstream
    Predicate<String> isNeeded = neededPairs::contains;

    Map<String, JoinSegmentReference> existingJoinSegments;
    IndexSearcher joinSearcher = this.joinIndex.acquire();
    try {
      existingJoinSegments = AIJoinIndex.extractExistingJoinColumns(joinSearcher, isNeeded);
    } finally {
      this.joinIndex.release(joinSearcher);
    }
    // let's submit to from searcher executor tasks yielding DocIdSets
    // then, ToLeafContexts will get these DocIdSets and iterate them.
    final Weight fromWeight =
        this.fromSearcher.createWeight(this.fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    @SuppressWarnings("unchecked")
    Future<AIJoinUtil.CacheAndCount>[] fromDocIdSetFutures =
        (Future<AIJoinUtil.CacheAndCount>[])
            new Future<?>[this.fromSearcher.getLeafContexts().size()];
    for (LeafReaderContext ctx : this.fromSearcher.getLeafContexts()) {
      fromDocIdSetFutures[ctx.ord] =
          this.fromExecutorService.submit(() -> computeDocIdSet(fromWeight, ctx));
    }
    return new AIJoinWeight(
        this,
        joinSearcher,
        existingJoinSegments,
        searcher.getIndexReader(),
        scoreMode,
        boost,
        fromDocIdSetFutures);
  }

  @Override
  public String toString(String field) {
    return "AIJoinQuery(" + fromField + " -> " + toField + ", from: " + fromQuery + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo((AIJoinQuery) other);
  }

  private boolean equalsTo(AIJoinQuery other) {
    // the join index and the from searcher compare by identity: a reopened from reader sees
    // different ordinal spaces, so queries over different searcher instances must not be
    // considered equal
    return joinIndex == other.joinIndex
        && fromSearcher == other.fromSearcher
        && fromField.equals(other.fromField)
        && fromQuery.equals(other.fromQuery)
        && toField.equals(other.toField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        classHash(),
        System.identityHashCode(joinIndex),
        fromField,
        fromQuery,
        System.identityHashCode(fromSearcher),
        toField);
  }
}
