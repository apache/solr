package org.apache.solr.search.join.aijoin;

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
import org.apache.solr.search.join.aijoin.AIJoinIndex.JoinSegmentReference;

final class AIJoinWeight extends Weight {
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

  AIJoinWeight(
      AIJoinQuery aiJoinQuery,
      IndexSearcher maybeStaleJoinSearcher,
      Map<String, JoinSegmentReference> existingJoinSegments,
      IndexReader toReader,
      ScoreMode scoreMode,
      float boost,
      Future<FromLeafJoinContext>[] foreignColsFutures) {
    super(aiJoinQuery);
    this.maybeStaleJoinSearcher = maybeStaleJoinSearcher;
    this.existingJoinSegments = existingJoinSegments;
    this.scoreMode = scoreMode;
    this.boost = boost;
    this.toReader = toReader;
    this.fromColumnFutures = foreignColsFutures;
  }

  private AIJoinQuery aiJQuery() {
    return (AIJoinQuery) this.parentQuery;
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
      ToLeafJoinContext ctx = null;
      try {
        ctx =
            new ToLeafJoinContext(
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
                this.fromColumnFutures);
      } catch (ExecutionException e) { // TODO review exception
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return ctx.scorerSupplier(scoreMode, boost);
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
