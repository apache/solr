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

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A lightweight metadata representation of the segment layout of a particular view ({@link
 * SolrIndexSearcher} over an index. This may be used, e.g., to capture the context of stale cache
 * entries, where portions of cached values may still be valid over newer views of the index.
 *
 * <p>The metadata contained here can be used to leverage stale cache entries in the efficient
 * reconstruction of new cache entries by "re-mapping" still-valid segment ranges of the old cache
 * entry onto the new cache entry (thereby avoiding the need to actually <i>compute</i> the new
 * cached value for that segment range.
 */
public class SegmentMap {

  private static final AtomicLong IDS = new AtomicLong();

  /**
   * Generates a unique (classloader-scoped) String id to be associated with generated segment maps.
   * This enables scoping of objects and data structures that are associated with a given segment
   * map.
   *
   * <p>This may be too cute in its construction of ids, but since we have sequentially-assigned
   * ids, it's preferable that ids vary in their <i>lower</i> indexes (to avoid having to compare
   * many character indexes). {@link Character#MAX_RADIX} and reversing the string accomplishes
   * this.
   */
  // TODO: could we just use
  //  SolrIndexSearcher.getTopReaderContext().reader().getReaderCacheHelper().getKey() and use
  //  simple {@link Object} for scoping instead? ... and perhaps fallback to String when such is
  //  not available?
  private static String mintId() {
    long raw = IDS.getAndIncrement();
    String inOrder = Long.toUnsignedString(raw, Character.MAX_RADIX);
    return new StringBuilder(inOrder).reverse().toString();
  }

  public static SegmentMap generateSegmentMap(SolrIndexSearcher searcher) {
    final List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map.Entry<IndexReader.CacheKey, Segment>[] segs = new Map.Entry[leafContexts.size()];
    int i = 0;
    for (LeafReaderContext ctx : leafContexts) {
      LeafReader r = ctx.reader();
      IndexReader.CacheKey coreKey = r.getCoreCacheHelper().getKey();
      segs[i++] =
          new AbstractMap.SimpleImmutableEntry<>(
              coreKey,
              new Segment(
                  coreKey,
                  r.getReaderCacheHelper().getKey(),
                  ctx.docBase,
                  r.numDocs(),
                  r.maxDoc()));
    }
    DirectoryReader r = searcher.getIndexReader();
    return new SegmentMap(
        r.getReaderCacheHelper().getKey(), Map.ofEntries(segs), r.numDocs(), r.maxDoc());
  }

  /**
   * Uniquely identifies this {@link SegmentMap} within the context of the associated classloader.
   * This enables scoping of objects and data structures that are associated with a given segment
   * map.
   */
  public final String id;

  /**
   * The reader cache key of the top-level {@link DirectoryReader} associated with this {@link
   * SegmentMap}.
   */
  public final IndexReader.CacheKey key;

  /**
   * Map of {@link Segment} instances for all segments associated with this {@link SegmentMap}.
   * Keyed on the segment core cache key.
   */
  public final Map<IndexReader.CacheKey, Segment> segments;

  /**
   * Number of docs (respecting deletes) in the top-level index associated with this {@link
   * SegmentMap}
   */
  public final int numDocs;

  /**
   * Number of docs (irrespective of deletes) in the top-level index associated with this {@link
   * SegmentMap}
   */
  public final int maxDoc;

  private final ConcurrentHashMap<IndexReader.CacheKey, Double> overlap = new ConcurrentHashMap<>();

  private SegmentMap(
      IndexReader.CacheKey key,
      Map<IndexReader.CacheKey, Segment> segments,
      int numDocs,
      int maxDoc) {
    this.id = mintId();
    this.key = key;
    this.segments = segments;
    this.numDocs = numDocs;
    this.maxDoc = maxDoc;
  }

  /**
   * Computes and caches the degree of overlapping segments between this {@link SegmentMap} and the
   * specified {@link SegmentMap} (as a proportion of {@code newer.maxDoc}).
   *
   * <p>This method should be called on older {@link SegmentMap}s, passing newer {@link
   * SegmentMap}s. This allows cached overlaps to be more effectively GC'd when one of the
   * associated {@link SegmentMap}s is no longer referenced.
   *
   * <p>The returned value may be used by the caller to make decisions, e.g., about whether the
   * benefit of preserving a stale cache value is worth allowing the stale entry to continue
   * consuming heap memory.
   */
  public double registerOverlap(SegmentMap newer) {
    return overlap.computeIfAbsent(
        newer.key,
        (k) -> {
          int count = 0;
          final Map<IndexReader.CacheKey, Segment> newerSegments = newer.segments;
          for (Segment s : segments.values()) {
            if (newerSegments.containsKey(s.coreKey)) {
              count += s.maxDoc;
            }
          }
          return (double) count / newer.maxDoc;
        });
  }

  /**
   * Returns the cached overlap (if available) between this {@link SegmentMap} and the {@link
   * SegmentMap} associated with the specified top-level reader cache key. If no cached value is
   * available, this returns {@code 0}.
   *
   * @see #registerOverlap(SegmentMap)
   */
  public double getOverlap(IndexReader.CacheKey newSearcher) {
    return overlap.getOrDefault(newSearcher, 0d);
  }

  public static class Segment {
    /** core cache key, associated with this segment, irrespective of deletes */
    public final IndexReader.CacheKey coreKey;

    /** reader cache key, associated with this segment, respecting deletes. */
    public final IndexReader.CacheKey readerKey;

    /**
     * The global doc id of the first doc in this segment, in the context of the top-level index of
     * the associated {@link SegmentMap}. This is linked to the associated {@link SegmentMap}.
     */
    public final int docBase;

    /**
     * The number of live docs (respecting deletes) in this segment, in the context of the top-level
     * index of the associated {@link SegmentMap}. This is linked to the {@link #readerKey}.
     */
    public final int numDocs;

    /**
     * The total number of docs (irrespective of deletes) in this segment. This is linked to the
     * {@link #coreKey}.
     */
    public final int maxDoc;

    private Segment(
        IndexReader.CacheKey coreKey,
        IndexReader.CacheKey readerKey,
        int docBase,
        int numDocs,
        int maxDoc) {
      this.coreKey = coreKey;
      this.readerKey = readerKey;
      this.docBase = docBase;
      this.numDocs = numDocs;
      this.maxDoc = maxDoc;
    }
  }
}
