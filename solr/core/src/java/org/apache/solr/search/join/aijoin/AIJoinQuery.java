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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
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
  private final IndexSearcher fromSearcher;
  final String toField;
  IndexSearcher cachedFromSearcher;

  AIJoinQuery(
      AIJoinIndex joinIndex,
      String fromField,
      Query fromQuery,
      IndexSearcher fromSearcher,
      String toField) {
    this.joinIndex = Objects.requireNonNull(joinIndex, "joinIndex");
    this.fromField = Objects.requireNonNull(fromField, "fromField");
    this.fromQuery = Objects.requireNonNull(fromQuery, "fromQuery");
    this.fromSearcher = Objects.requireNonNull(fromSearcher, "fromSearcher");
    this.toField = Objects.requireNonNull(toField, "toField");
    this.cachedFromSearcher = wrapFromSearcher(fromSearcher);
  }

  // presumabily keep it in AIJoinIndex
  private static IndexSearcher wrapFromSearcher(IndexSearcher fromSearcher) {
    IndexSearcher cachedFromSearcher = new IndexSearcher(fromSearcher.getIndexReader());
    cachedFromSearcher.setQueryCache(
        new LRUQueryCache(
            fromSearcher.getLeafContexts().size() + 1,
            fromSearcher.getIndexReader().maxDoc() / 8 * 2));
    cachedFromSearcher.setQueryCachingPolicy(
        new QueryCachingPolicy() {
          @Override
          public boolean shouldCache(Query query) {
            return true;
          }

          @Override
          public void onUse(Query query) {}
        });
    return cachedFromSearcher;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    // the from-side selection rewrites against the from-side searcher, not against the (to-side)
    // searcher this query is executed with
    Query rewrittenFrom = fromQuery.rewrite(fromSearcher);
    if (rewrittenFrom != fromQuery) {
      return new AIJoinQuery(joinIndex, fromField, rewrittenFrom, fromSearcher, toField);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    // narrow the join index lookup to pairs this weight could possibly need: from-segments with
    // at least one live match for fromQuery (mirrors the per-to-segment filtering in
    // ToLeafJoinContext#createFromItersTasks, without needing a to-segment to preposition
    // against), crossed with every to-segment of the searcher this weight is created against --
    // this hash will be shared across all to-segments' scorers
    Set<String> matchingFromKeys =
        AIJoinUtil.matchingFromSideKeys(cachedFromSearcher, fromQuery, fromField);
    Set<String> neededPairs = new HashSet<>();
    for (LeafReaderContext toContext : searcher.getIndexReader().leaves()) {
      String toKey = AIJoinUtil.getSideKey(toContext, toField);
      for (String fromKey : matchingFromKeys) {
        neededPairs.add(fromKey + "_" + toKey);
      }
    }

    joinIndex.onCreateWeight(neededPairs, fromSearcher, searcher); // ignoring fields

    // build any pair among neededPairs that isn't in the join index yet, up front, so this
    // weight's existingJoinSegments below is already complete instead of leaving the gaps to be
    // discovered lazily, one to-segment at a time, once scoring starts
    joinIndex.ensureJoinSegments(neededPairs, cachedFromSearcher, fromField, searcher, toField);

    Predicate<String> isNeeded = neededPairs::contains;

    Map<String, JoinSegmentReference> existingJoinSegments;
    IndexSearcher joinSearcher = this.joinIndex.acquire();
    try {
      existingJoinSegments = AIJoinIndex.extractExistingJoinColumns(joinSearcher, isNeeded);
    } finally {
      this.joinIndex.release(joinSearcher);
    }

    return new AIJoinWeight(
        this, joinSearcher, existingJoinSegments, searcher.getIndexReader(), scoreMode, boost);
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
