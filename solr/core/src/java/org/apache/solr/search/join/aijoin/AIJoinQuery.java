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
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.join.aijoin.AIJoinIndex.JoinSegmentReference;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
  public Weight createWeight(IndexSearcher toSideSearcher, ScoreMode scoreMode, float boost)
      throws IOException {
    @NonNull Map<String, AIJoinIndex.SegmentsTuple> neededPairs =
        getRequiredColumNames(toSideSearcher);

    joinIndex.onCreateWeight(neededPairs.keySet(), fromSearcher, toSideSearcher); // ignoring fields
    //
    // DON'T write'em upfront
    //
    // WAS:
    // joinIndex.ensureJoinSegments(neededPairs, fromSearcher, fromField, toSideSearcher, toField);
    Predicate<String> isNeeded = neededPairs::containsKey;

    Map<String, JoinSegmentReference> existingJoinSegments;
    IndexSearcher joinSearcher = this.joinIndex.acquire();
    try {
      existingJoinSegments = AIJoinIndex.extractExistingJoinColumns(joinSearcher, isNeeded);
    } finally {
      this.joinIndex.release(joinSearcher);
    }
    int pairsNeeded = neededPairs.size();
    neededPairs.keySet().removeAll(existingJoinSegments.keySet());
    IntHashSet fromOrdsToLoad = new IntHashSet(neededPairs.size());
    neededPairs.values().stream()
        .mapToInt(AIJoinIndex.SegmentsTuple::fromLeafOrd)
        .forEach(fromOrdsToLoad::add);
    if (AIJoinUtil.diagnosticsEnabled(log)) {
      // pairsMissing > 0 on a repeat query means those pairs were never persisted by a previous
      // run (writeBatch never captured them), so their from-segments' FK columns get reloaded
      // here; pairsClaimed counts missing pairs some build already claimed/completed in-process,
      // i.e. reloads that are pure waste
      AIJoinUtil.logDiagnostic(
          log,
          "AIJOIN evt=weight pairsNeeded={} pairsExisting={} pairsMissing={} pairsClaimed={}"
              + " fkOrdsToLoad={} missingPairs={}",
          pairsNeeded,
          existingJoinSegments.size(),
          neededPairs.size(),
          joinIndex.countClaimedBuilds(neededPairs.keySet()),
          fromOrdsToLoad.size(),
          neededPairs.keySet());
    }
    Future<FromLeafJoinContext>[] fromFutures = loadFromSide(fromOrdsToLoad);
    // TODO this might produce too many small tasks
    return new AIJoinWeight(
        this,
        joinSearcher,
        existingJoinSegments,
        toSideSearcher.getIndexReader(),
        scoreMode,
        boost,
        fromFutures);
  }

  @SuppressWarnings("unchecked")
  private Future<FromLeafJoinContext>[] loadFromSide(IntHashSet fromLeafsToLoad)
      throws IOException {
    Future<FromLeafJoinContext>[] futures =
        (Future<FromLeafJoinContext>[]) (new Future<?>[this.fromSearcher.getLeafContexts().size()]);
    final Weight fromWeight =
        this.fromSearcher.createWeight(this.fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    for (LeafReaderContext ctx : this.fromSearcher.getLeafContexts()) {
      futures[ctx.ord] =
          this.fromExecutorService.submit(
              () -> {
                try {
                  AIJoinUtil.CacheAndCount docset = computeDocIdSet(fromWeight, ctx);
                  boolean loadFk =
                      docset != null && docset.count() > 0 && fromLeafsToLoad.contains(ctx.ord);
                  if (AIJoinUtil.diagnosticsEnabled(log)) {
                    AIJoinUtil.logDiagnostic(
                        log,
                        "AIJOIN evt=fromLeaf fromSeg={} ord={} fromMatches={} missingPair={}"
                            + " fkLoaded={}",
                        AIJoinUtil.segmentName(ctx),
                        ctx.ord,
                        docset == null ? -1 : docset.count(),
                        fromLeafsToLoad.contains(ctx.ord),
                        loadFk);
                  }
                  if (loadFk) {
                    // waste case: noone to-seg read FK,
                    return new FromLeafJoinContext(docset, new ForeignKeyColumn(ctx, fromField));
                  } else {
                    return new FromLeafJoinContext(docset, null);
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }

    return futures;
  }

  private @NonNull Map<String, AIJoinIndex.SegmentsTuple> getRequiredColumNames(
      IndexSearcher searcher) {
    Map<String, AIJoinIndex.SegmentsTuple> neededPairs = new HashMap<>();
    for (LeafReaderContext toContext : searcher.getIndexReader().leaves()) {
      String toKey = AIJoinUtil.getSideKey(toContext, toField);
      for (LeafReaderContext fromCtx : fromSearcher.getLeafContexts()) {
        String fromKey = AIJoinUtil.getSideKey(fromCtx, fromField);
        neededPairs.put(
            fromKey + "_" + toKey, new AIJoinIndex.SegmentsTuple(fromCtx.ord, toContext.ord));
      }
    }
    return neededPairs;
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
