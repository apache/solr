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

import static org.apache.solr.search.OrdMapRegenerator.getRegenKeepAliveNanos;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.solr.common.MapWriter;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.util.IOFunction;

/**
 * A regenerator capable of lazily warming entries where possible (passing stale entries along
 * unmodified, with metadata to allow using the stale entry to reconstruct new entries on-demand,
 * without actually needing to run the query again over segments that still exist in the index).
 *
 * <p>Cache entries with cross-doc/cross-segment dependencies (e.g., {@link JoinQuery}) cannot be
 * warmed lazily, but this regenerator may still be configured to eagerly warm such queries.
 *
 * <p>Especially because this regenerator is designed to be used in conjunction with {@code
 * autowarm="100%"}, it can be important to prevent unused queries from being carried forward
 * indefinitely. Accordingly, this class extends {@link MetaCacheRegenerator}, wrapping entries in
 * the associated cache to record (and update) each entry's last access time, allowing to drop (not
 * warm) entries that haven't been accessed recently enough.
 *
 * <p>There are 4 configuration parameters (configured by attributes in the element configuring the
 * associated cache):
 *
 * <ul>
 *   <li>{@code regenKeepAlive}: max time since last access for cache entries to warm lazily
 *   <li>{@code eagerKeepAlive}: max time since last access for cache entries to warm eagerly
 *   <li>{@code preferLazy}: determines warming behavior for cache entries that are capable of being
 *       warmed lazily, but that also meet the criteria configured for {@code eagerKeepAlive}. If
 *       {@code false} (the default), such entries will be warmed eagerly (though still leveraging
 *       the stale cache entry to make warming more efficient). If set to {@code "true"}, no eager
 *       evaluation will be performed at warming time; the stale entry will be carried forward to
 *       the new cache as-is, and used lazily to reconstruct a new cache entry on-demand, if needed.
 *   <li>{@code overlapThreshold}: (default {@code 0.5}) determines the minimum segment overlap
 *       threshold that allows cache entries to be carried forward lazily. Below this threshold, the
 *       costs (heap consumed) are considered to outweigh the potential benefits of partial cache
 *       entry reconstruction.
 * </ul>
 */
public class KeepAliveRegenerator<M extends MetaEntry<Query, DocSet, M>>
    extends MetaCacheRegenerator<Query, DocSet, M> {

  private static final int DEFAULT_REGEN_KEEPALIVE_MINUTES = 2;
  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS =
      TimeUnit.MINUTES.toNanos(DEFAULT_REGEN_KEEPALIVE_MINUTES);

  private static final double DEFAULT_OVERLAP_THRESHOLD = 0.5;

  private final long regenKeepAliveNanos;
  private final long eagerKeepAliveNanos;
  private final boolean preferLazy;
  private final double overlapThreshold;

  public KeepAliveRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS, 0, false);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  static boolean autowarmOn(CacheConfig config) {
    return new SolrCacheBase.AutoWarmCountRef(
            (String) config.toMap(new HashMap<>()).get("autowarmCount"))
        .isAutoWarmingOn();
  }

  public KeepAliveRegenerator(SolrConfig solrConfig, CacheConfig cacheConfig) {
    this(solrConfig, cacheConfig, new LongAdder(), new DoubleAdder());
  }

  private KeepAliveRegenerator(
      SolrConfig solrConfig,
      CacheConfig cacheConfig,
      LongAdder partialHits,
      DoubleAdder partialHitsRatio) {
    super(autowarmOn(cacheConfig), getWrapFunction(partialHits, partialHitsRatio));
    Map<String, Object> cacheConfigArgs = cacheConfig.toMap(Collections.emptyMap());
    this.regenKeepAliveNanos =
        getRegenKeepAliveNanos("regenKeepAlive", solrConfig, cacheConfigArgs, null);
    this.eagerKeepAliveNanos =
        getRegenKeepAliveNanos("eagerKeepAlive", solrConfig, cacheConfigArgs, "0");
    this.preferLazy = "true".equals(cacheConfigArgs.get("preferLazy"));
    this.partialHits = partialHits;
    this.partialHitsRatio = partialHitsRatio;
    String tmp = (String) cacheConfigArgs.get("overlapThreshold");
    this.overlapThreshold = tmp == null ? DEFAULT_OVERLAP_THRESHOLD : Double.parseDouble(tmp);
  }

  private KeepAliveRegenerator(
      long regenKeepAliveNanos, long eagerKeepAliveNanos, boolean preferLazy) {
    this(regenKeepAliveNanos, eagerKeepAliveNanos, preferLazy, new LongAdder(), new DoubleAdder());
  }

  private KeepAliveRegenerator(
      long regenKeepAliveNanos,
      long eagerKeepAliveNanos,
      boolean preferLazy,
      LongAdder partialHits,
      DoubleAdder partialHitsRatio) {
    super(true, getWrapFunction(partialHits, partialHitsRatio));
    this.regenKeepAliveNanos = regenKeepAliveNanos;
    this.eagerKeepAliveNanos = eagerKeepAliveNanos;
    this.preferLazy = preferLazy;
    this.partialHits = partialHits;
    this.partialHitsRatio = partialHitsRatio;
    this.overlapThreshold = DEFAULT_OVERLAP_THRESHOLD;
  }

  @SuppressWarnings({"unchecked", "UnnecessaryLambda"})
  private static <M> BiFunction<SegmentMap, DocSet, M> getWrapFunction(
      LongAdder partialHits, DoubleAdder partialHitsRatio) {
    return (segMap, v) -> (M) new KeepAliveSegAwareValue(segMap, v, partialHits, partialHitsRatio);
  }

  static boolean isCrossDoc(Query q) {
    boolean[] ret = new boolean[1];
    q.visit(
        new QueryVisitor() {
          @Override
          public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            return isCrossDoc(parent);
          }

          @Override
          public void visitLeaf(Query query) {
            isCrossDoc(query);
          }

          private QueryVisitor isCrossDoc(Query q) {
            if (ret[0]) {
              return EMPTY_VISITOR;
            } else if (q.getClass().getSimpleName().endsWith("JoinQuery")) {
              ret[0] = true;
              return EMPTY_VISITOR;
            } else {
              return this;
            }
          }

          @Override
          public void consumeTerms(Query query, Term... terms) {
            isCrossDoc(query);
          }

          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunAutomaton> automaton) {
            isCrossDoc(query);
          }

          @Override
          public boolean acceptField(String field) {
            return !ret[0];
          }
        });
    return ret[0];
  }

  private static class KeepAliveSegAwareValue
      implements MetaEntry<Query, DocSet, KeepAliveSegAwareValue> {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(KeepAliveSegAwareValue.class)
            + RamUsageEstimator.shallowSizeOfInstance(AtomicReference.class)
            + RamUsageEstimator.shallowSizeOfInstance(AbstractMap.SimpleImmutableEntry.class)
            + RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class);

    private final AtomicReference<
            AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>>>
        ref;
    private final LongAdder partialHits;
    private final DoubleAdder partialHitsRatio;
    private final long ramBytesUsed;
    private long accessTimestampNanos;

    public KeepAliveSegAwareValue(
        SegmentMap segMap, DocSet val, LongAdder partialHits, DoubleAdder partialHitsRatio) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = System.nanoTime();
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    public KeepAliveSegAwareValue(
        SegmentMap segMap,
        DocSet val,
        long accessTimestampNanos,
        LongAdder partialHits,
        DoubleAdder partialHitsRatio) {
      CompletableFuture<DocSet> f = new CompletableFuture<>();
      f.complete(val);
      ref = new AtomicReference<>(new AbstractMap.SimpleImmutableEntry<>(segMap, f));
      ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      this.accessTimestampNanos = accessTimestampNanos;
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    public KeepAliveSegAwareValue(
        KeepAliveSegAwareValue template, LongAdder partialHits, DoubleAdder partialHitsRatio) {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> entry =
          template.ref.get();
      ref = new AtomicReference<>(template.ref.get());
      DocSet val = entry.getValue().getNow(null);
      if (val == null) {
        // warming a value that's not yet loaded; just use the old `ramBytesUsed`
        ramBytesUsed = template.ramBytesUsed();
      } else {
        ramBytesUsed = BASE_RAM_BYTES_USED + val.ramBytesUsed();
      }
      this.accessTimestampNanos = template.accessTimestampNanos;
      this.partialHits = partialHits;
      this.partialHitsRatio = partialHitsRatio;
    }

    @Override
    public KeepAliveSegAwareValue metaClone(DocSet val) {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public DocSet get(
        SegmentMap segMap, Query key, IOFunction<? super Query, ? extends DocSet> mappingFunction)
        throws IOException {
      AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref = this.ref.get();
      accessTimestampNanos = System.nanoTime();
      try {
        if (ref.getKey() == segMap) {
          return ref.getValue().get();
        } else if (mappingFunction == null) {
          return null;
        }
        CompletableFuture<DocSet> f = new CompletableFuture<>();
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> newRef =
            new AbstractMap.SimpleImmutableEntry<>(segMap, f);
        AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> witness =
            this.ref.compareAndExchange(ref, newRef);
        if (witness == ref) {
          // we compute
          Query frankenstein =
              new FrankensteinQuery(key, ref.getKey().segments, ref.getValue().get());
          DocSet computed = mappingFunction.apply(frankenstein);
          f.complete(computed);
          partialHits.increment();
          partialHitsRatio.add(ref.getKey().getOverlap(segMap.key));
          return computed;
        } else if (witness.getKey() == segMap) {
          return witness.getValue().get();
        } else {
          return mappingFunction.apply(key);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new RuntimeException(ex);
        }
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  private final LongAdder partialHits;
  private final DoubleAdder partialHitsRatio;
  private final LongAdder priorPartialHits = new LongAdder();
  private final DoubleAdder priorPartialHitsRatio = new DoubleAdder();

  @Override
  public void postWarm() {
    priorPartialHits.add(partialHits.sumThenReset());
    priorPartialHitsRatio.add(partialHitsRatio.sumThenReset());
  }

  @Override
  public void appendMetrics(MapWriter.EntryWriter map) throws IOException {
    final long partialHitCount = partialHits.sum();
    final double currentPartialHitRatio = partialHitsRatio.sum();
    final long cumPartialHitCount = priorPartialHits.sum() + partialHitCount;
    final double cumCurrentPartialHitsRatio = priorPartialHitsRatio.sum() + currentPartialHitRatio;
    map.put("partialHits", partialHitCount);
    map.put("partialHitsRatio", currentPartialHitRatio);
    map.put(
        "partialRatioPerHit",
        partialHitCount == 0 ? 1.0 : (currentPartialHitRatio / partialHitCount));
    map.put("cumulative_partialHits", cumPartialHitCount);
    map.put("cumulative_partialHitsRatio", cumCurrentPartialHitsRatio);
    map.put(
        "cumulative_partialRatioPerHit",
        cumPartialHitCount == 0 ? 1.0 : (cumCurrentPartialHitsRatio / cumPartialHitCount));
  }

  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    DirectoryReader in = newSearcher.getIndexReader();
    IndexReader.CacheHelper cacheHelper = in.getReaderCacheHelper();
    if (cacheHelper == null) {
      return false;
    }

    final List<LeafReaderContext> leaves = in.leaves();
    final int size = leaves.size();

    if (size < 2) {
      // we don't need OrdinalMaps for these trivial cases
      return false;
    }

    KeepAliveSegAwareValue metaEntry = (KeepAliveSegAwareValue) oldVal;
    final long extantTimestamp = metaEntry.accessTimestampNanos;
    long lastAccessAgo = System.nanoTime() - extantTimestamp;
    if (lastAccessAgo > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final Query query = (Query) oldKey;

    @SuppressWarnings("unchecked")
    SolrCache<Query, KeepAliveSegAwareValue> c =
        (SolrCache<Query, KeepAliveSegAwareValue>) newCache;

    if (metaEntry.ref.get().getKey().registerOverlap(newSearcher.getSegmentMap()) < overlapThreshold
        || isCrossDoc(query)) {
      if (lastAccessAgo < eagerKeepAliveNanos) {
        // if we meet the criterion for eager warming, do it here (not leveraging segment-aware)
        c.computeIfAbsent(
            query,
            (q) -> {
              SegmentMap segMap = newSearcher.getSegmentMap();
              DocSet docSet = newSearcher.getDocSetNC(query, null);
              return new KeepAliveSegAwareValue(
                  segMap, docSet, extantTimestamp, partialHits, partialHitsRatio);
            });
      }
      return true;
    }

    if (!preferLazy && lastAccessAgo < eagerKeepAliveNanos) {
      // if we're doing eager warming, do it here even for segment-aware entries. We can
      // still benefit from the old cache entry.
      c.computeIfAbsent(
          query,
          (q) -> {
            AbstractMap.SimpleImmutableEntry<SegmentMap, CompletableFuture<DocSet>> ref =
                metaEntry.ref.get();
            Query frankenstein;
            try {
              frankenstein =
                  new FrankensteinQuery(query, ref.getKey().segments, ref.getValue().get());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (cause instanceof IOException) {
                throw (IOException) cause;
              } else {
                throw new RuntimeException(e);
              }
            }
            DocSet docSet = newSearcher.getDocSetNC(frankenstein, null);
            return new KeepAliveSegAwareValue(
                newSearcher.getSegmentMap(),
                docSet,
                extantTimestamp,
                partialHits,
                partialHitsRatio);
          });
      return true;
    }

    // the lazy way. Just pass the stale cache entry along in case it's queried later
    c.computeIfAbsent(
        query, (q) -> new KeepAliveSegAwareValue(metaEntry, partialHits, partialHitsRatio));
    return true;
  }

  private static class FrankensteinQuery extends Query {

    private final Query backing;
    private final Map<IndexReader.CacheKey, SegmentMap.Segment> segs;
    private final DocSet stale;

    FrankensteinQuery(
        Query backing, Map<IndexReader.CacheKey, SegmentMap.Segment> segs, DocSet stale) {
      this.backing = backing;
      this.segs = segs;
      this.stale = stale;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      final Weight backingWeight = backing.createWeight(searcher, scoreMode, boost);
      return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
          return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final SegmentMap.Segment segment =
              segs.get(context.reader().getCoreCacheHelper().getKey());
          if (segment == null) {
            return backingWeight.scorer(context);
          } else {
            // backed by existing (partially stale) DocSet
            final int docBase = segment.docBase;
            final DocIdSetIterator disi;
            if (stale instanceof SortedIntDocSet) {
              final int[] docs = ((SortedIntDocSet) stale).getDocs();
              final int first = Arrays.binarySearch(docs, docBase);
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int idx = (first < 0 ? ~first : first) - 1;
                    int id = -1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++idx >= docs.length || (id = docs[idx]) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      while (nextDoc() < target) {
                        // advance
                      }
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else if (stale instanceof BitDocSet) {
              final FixedBitSet docs = ((BitDocSet) stale).getBits();
              disi =
                  new DocIdSetIterator() {
                    final int limit = segment.maxDoc + docBase;
                    int id = docBase - 1;

                    @Override
                    public int docID() {
                      return id == NO_MORE_DOCS ? NO_MORE_DOCS : id - docBase;
                    }

                    @Override
                    public int nextDoc() {
                      if (++id >= limit || (id = docs.nextSetBit(id)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public int advance(int target) {
                      if (target >= limit || (id = docs.nextSetBit(target)) >= limit) {
                        return id = NO_MORE_DOCS;
                      } else {
                        return id - docBase;
                      }
                    }

                    @Override
                    public long cost() {
                      return 0;
                    }
                  };
            } else {
              throw new IllegalStateException();
            }
            return new ConstantScoreScorer(this, 1f, scoreMode, disi);
          }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public Query rewrite(IndexReader reader) throws IOException {
      Query rewrittenBacking = backing.rewrite(reader);
      return rewrittenBacking == backing
          ? this
          : new FrankensteinQuery(rewrittenBacking, segs, stale);
    }

    @Override
    public String toString(String field) {
      return FrankensteinQuery.class.getSimpleName() + "{" + backing + "}";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      backing.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this
          || (obj instanceof FrankensteinQuery
              && ((FrankensteinQuery) obj).backing.equals(backing));
    }

    @Override
    public int hashCode() {
      return FrankensteinQuery.class.hashCode() ^ backing.hashCode();
    }
  }
}
