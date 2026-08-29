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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;

final class AuxIndexJoinMergePolicy extends MergePolicy {
  @Override
  public MergePolicy.MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    MergeSpecification spec = null;
    for (SegmentCommitInfo info : segmentInfos) {
      if (merging.contains(info)) {
        continue;
      }
      Set<String> pairFieldNames = JoinIndexUtils.pairFieldNames(JoinIndexUtils.readFieldInfos(info));
      if (!pairFieldNames.isEmpty()
          && pendingPairRemovals.containsAll(
              pairFieldNames)) { // todo sweep pending removals as well
        if (spec == null) {
          spec = new MergeSpecification();
        }
        spec.add(new DropSegmentMerge(List.of(info)));
      }
    }
    return spec;
  }

  // counts merges that actually dropped a fully-dead segment; test-only observability, see
  // droppedSegmentCount()
  private final AtomicInteger droppedSegmentCount = new AtomicInteger();

  /**
   * A merge over a single dead segment whose contents are reported as fully deleted, so {@link
   * IndexWriter} drops it instead of rewriting it -- see {@link #wrapForMerge}. Non-static so it
   * can report back to the outer policy's {@link #droppedSegmentCount}.
   */
  private final class DropSegmentMerge extends OneMerge {
    DropSegmentMerge(List<SegmentCommitInfo> segments) {
      super(segments);
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) {
      return new FilterCodecReader(reader) {
        @Override
        public CacheHelper getCoreCacheHelper() {
          return reader.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null; // we are altering live docs
        }

        @Override
        public Bits getLiveDocs() {
          return new Bits.MatchNoBits(reader.maxDoc());
        }

        @Override
        public int numDocs() {
          return 0;
        }
      };
    }

    @Override
    public void mergeFinished(boolean success, boolean segmentDropped) throws IOException {
      if (segmentDropped) {
        droppedSegmentCount.incrementAndGet();
      }
      super.mergeFinished(success, segmentDropped);
    }
  }

  /** Test-only: how many sidecar segments this policy has actually reaped so far. */
  int droppedSegmentCount() {
    return droppedSegmentCount.get();
  }

  /** Test-only: how many dead pair field names are currently queued for the next reap. */
  int pendingPairRemovalsCount() {
    return pendingPairRemovals.size();
  }

  @Override
  public MergePolicy.MergeSpecification findForcedMerges(
      SegmentInfos segmentInfos,
      int maxSegmentCount,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge,
      MergeContext mergeContext)
      throws IOException {
    return null;
  }

  @Override
  public MergePolicy.MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
    return null;
  }

  // caps how many distinct (from-searcher, to-searcher) pairs we remember snapshots for; a
  // best-effort bound since this only anchors a heuristic reap, never correctness
  private static final int MAX_TRACKED_SEARCHER_PAIRS = 256;
  private final ConcurrentHashMap<Map.Entry<Object, Object>, Set<String>>
      lastNeededPairsBySearcherPair = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Map.Entry<Object, Object>> trackedSearcherPairsOrder =
      new ConcurrentLinkedQueue<>();

  // pair field names seen in an earlier snapshot but missing from a later one for the same
  // (from-searcher, to-searcher) pair -- i.e. no longer needed -- queued here for findMerges to
  // reap; also size-capped, same reasoning
  private static final int MAX_PENDING_PAIR_REMOVALS = 4096;
  private final Set<String> pendingPairRemovals = ConcurrentHashMap.newKeySet();
  private final ConcurrentLinkedQueue<String> pendingPairRemovalsOrder =
      new ConcurrentLinkedQueue<>();

  // how often onCreateWeight actually bothers to sample searcher state; calls arriving sooner
  // than this after the last accepted sample are skipped outright, since sampling is only a
  // heuristic hint feeding findMerges' reap decision, not a correctness requirement. Zero (or
  // negative) disables throttling entirely. Defaults to one minute; see #setSweepInterval.
  private volatile long samplingIntervalNanos = TimeUnit.MINUTES.toNanos(1);

  // Long.MIN_VALUE marks "never sampled yet" so the very first call always goes through,
  // regardless of what System.nanoTime()'s arbitrary origin happens to be.
  private final AtomicLong nextSampleAtNanos = new AtomicLong(Long.MIN_VALUE);

  /**
   * Configures how often {@link #onCreateWeight} actually samples searcher state for the dead-pair
   * reaper; calls arriving sooner than this after the last accepted sample are skipped, since
   * sampling is only a heuristic hint, not a correctness requirement. Defaults to one minute. Pass
   * zero (or a non-positive value) to sample on every call.
   */
  void setSweepInterval(long duration, TimeUnit unit) {
    this.samplingIntervalNanos = unit.toNanos(duration);
  }

  /**
   * Approximates "has enough time passed since the last sample" with {@link System#nanoTime()} --
   * the JDK's cheapest monotonic timer, since it need not track wall-clock time -- gated by a
   * single CAS so that under concurrent callers exactly one wins a given interval and the rest
   * skip, without any lock.
   */
  private boolean shouldSample() {
    long interval = samplingIntervalNanos;
    if (interval <= 0) {
      return true;
    }
    long now = System.nanoTime();
    long next = nextSampleAtNanos.get();
    // subtraction (not direct comparison) so this stays correct across nanoTime() overflow, per
    // its javadoc
    if (next != Long.MIN_VALUE && now - next < 0) {
      return false;
    }
    return nextSampleAtNanos.compareAndSet(next, now + interval);
  }

  void onCreateWeight(Set<String> neededPairs, IndexSearcher fromSearcher, IndexSearcher searcher)
      throws IOException {
    if (!shouldSample()) {
      return;
    }
    Object fromKey = JoinIndexUtils.directoryKey(JoinIndexUtils.directory(fromSearcher.getIndexReader()));
    Object toKey = JoinIndexUtils.directoryKey(JoinIndexUtils.directory(searcher.getIndexReader()));
    Map.Entry<Object, Object> searcherKey = Map.entry(fromKey, toKey);

    Set<String> currentSnapshot = Set.copyOf(neededPairs);
    Set<String> previousSnapshot =
        AuxIndexJoinMergePolicy.putBounded(
            lastNeededPairsBySearcherPair,
            trackedSearcherPairsOrder,
            searcherKey,
            currentSnapshot,
            MAX_TRACKED_SEARCHER_PAIRS);
    if (previousSnapshot != null) {
      for (String pairFieldName : previousSnapshot) {
        if (!currentSnapshot.contains(pairFieldName)) {
          AuxIndexJoinMergePolicy.addBounded(
              pendingPairRemovals,
              pendingPairRemovalsOrder,
              pairFieldName,
              MAX_PENDING_PAIR_REMOVALS);
        }
      }
    }
  }

  /**
   * Puts {@code key} -> {@code value}, evicting the oldest key(s) once {@code map} exceeds {@code
   * maxSize}. Approximate under races (an eviction can drop a key concurrently re-inserted, or the
   * map can briefly exceed {@code maxSize}) -- acceptable since callers only use this as a soft cap
   * on a best-effort cache.
   */
  static <K, V> V putBounded(
      ConcurrentHashMap<K, V> map,
      ConcurrentLinkedQueue<K> insertionOrder,
      K key,
      V value,
      int maxSize) {
    V previous = map.put(key, value);
    if (previous == null) {
      insertionOrder.add(key);
      while (map.size() > maxSize) {
        K oldest = insertionOrder.poll();
        if (oldest == null) {
          break;
        }
        map.remove(oldest);
      }
    }
    return previous;
  }

  /** Same eviction policy as {@link AuxIndexJoinMergePolicy#putBounded}, for a plain set. */
  static <T> void addBounded(
      Set<T> set, ConcurrentLinkedQueue<T> insertionOrder, T value, int maxSize) {
    if (set.add(value)) {
      insertionOrder.add(value);
      while (set.size() > maxSize) {
        T oldest = insertionOrder.poll();
        if (oldest == null) {
          break;
        }
        set.remove(oldest);
      }
    }
  }
}
