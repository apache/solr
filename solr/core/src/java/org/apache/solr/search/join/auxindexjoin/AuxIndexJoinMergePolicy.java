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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
final class AuxIndexJoinMergePolicy extends MergePolicy {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** How many sidecar segments one {@link DocAlignedMerge} folds into one, by default. */
  static final int DEFAULT_MERGE_SEGMENTS_AT_ONCE = 10;

  /**
   * How many pair columns a sidecar segment may already carry and still be compacted again, by
   * default. Merged segments grow wider, not longer, so this is what makes compaction converge
   * instead of rewriting the same big segment forever.
   */
  static final int DEFAULT_MAX_PAIRS_PER_SEGMENT = 1024;

  /**
   * Cap on the merges proposed in one round. A backlog of hundreds of segments is worked off over
   * the following rounds (every flush and every finished merge triggers one) rather than handed to
   * the scheduler at once, where it would stall the writing thread -- which here is a query thread
   * building a pair column.
   */
  private static final int MAX_MERGES_PER_ROUND = 4;

  private volatile int mergeSegmentsAtOnce = DEFAULT_MERGE_SEGMENTS_AT_ONCE;
  private volatile int maxPairsPerSegment = DEFAULT_MAX_PAIRS_PER_SEGMENT;

  /**
   * Last round's pair field names by segment key, so a steady stream of commits doesn't re-read
   * every segment's FieldInfos -- a compound-file open and mmap per segment per round -- when only
   * the newest segment is new. Rebuilt on every round, which is also what evicts gone segments.
   * Needs no synchronization: {@link #findMerges} is always called under the {@link IndexWriter}
   * monitor, as its contract states.
   */
  private Map<String, Set<String>> pairFieldNamesBySegment = Map.of();

  /** Test/ops knobs, mirroring {@link #setSweepInterval}: see the DEFAULT_ constants above. */
  void setCompaction(int mergeSegmentsAtOnce, int maxPairsPerSegment) {
    this.mergeSegmentsAtOnce = mergeSegmentsAtOnce;
    this.maxPairsPerSegment = maxPairsPerSegment;
  }

  @Override
  public MergePolicy.MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    Map<String, Set<String>> pairNamesThisRound = HashMap.newHashMap(segmentInfos.asList().size());
    MergeSpecification spec = null;
    List<SegmentCommitInfo> compactable = new ArrayList<>();
    int dead = 0;
    for (SegmentCommitInfo info : segmentInfos) {
      Set<String> pairFieldNames = pairFieldNames(info, pairNamesThisRound);
      if (merging.contains(info)) {
        continue;
      }
      if (!pairFieldNames.isEmpty()
          && pendingPairRemovals.containsAll(
              pairFieldNames)) { // todo sweep pending removals as well
        dead++;
        spec = added(spec, new DropSegmentMerge(List.of(info)));
      } else if (isCompactable(info, pairFieldNames)) {
        compactable.add(info);
      }
    }
    this.pairFieldNamesBySegment = pairNamesThisRound;
    int aligned = 0;
    if (compactable.size() >= mergeSegmentsAtOnce) {
      // by maxDoc, so a merge groups sidecar segments of comparable length: the union is as long
      // as its longest input, and every shorter input pays a bit of sparse-docvalues overhead for
      // the docs it doesn't reach
      compactable.sort(Comparator.comparingInt(info -> info.info.maxDoc()));
      for (int from = 0;
          from + mergeSegmentsAtOnce <= compactable.size() && aligned < MAX_MERGES_PER_ROUND;
          from += mergeSegmentsAtOnce) {
        List<SegmentCommitInfo> group =
            List.copyOf(compactable.subList(from, from + mergeSegmentsAtOnce));
        aligned++;
        spec = added(spec, new DocAlignedMerge(group, alignedMergeCount::incrementAndGet));
      }
    }
    if (spec != null) {
      log.info(
          "AUXIJOIN sidecar compaction: {} doc-aligned merge(s) of {} segments each and {} dead "
              + "segment(s) dropped, out of {} segments ({} compactable, {} merging, {} pairs "
              + "pending removal), on {}",
          aligned,
          mergeSegmentsAtOnce,
          dead,
          segmentInfos.size(),
          compactable.size(),
          merging.size(),
          pendingPairRemovals.size(),
          mergeTrigger);
    } else if (log.isDebugEnabled()) {
      log.debug(
          "AUXIJOIN sidecar: nothing to do on {}, {} segments ({} compactable, {} merging, {} pairs "
              + "pending removal)",
          mergeTrigger,
          segmentInfos.size(),
          compactable.size(),
          merging.size(),
          pendingPairRemovals.size());
    }
    return spec;
  }

  private static MergeSpecification added(MergeSpecification spec, OneMerge merge) {
    if (spec == null) {
      spec = new MergeSpecification();
    }
    spec.add(merge);
    return spec;
  }

  /**
   * Whether this segment may be folded into a {@link DocAlignedMerge}: it must carry pairs worth
   * keeping, must not be so wide already that rewriting it buys nothing, and must have no deletions
   * -- the sidecar never deletes, and doc-aligning a segment that somehow did would misreport its
   * live docs for the padded tail.
   */
  private boolean isCompactable(SegmentCommitInfo info, Set<String> pairFieldNames) {
    return !pairFieldNames.isEmpty()
        && pairFieldNames.size() <= maxPairsPerSegment
        && !info.hasDeletions()
        && info.info.maxDoc() > 0;
  }

  /** The segment's pair field names, from last round's cache when it was already read. */
  private Set<String> pairFieldNames(SegmentCommitInfo info, Map<String, Set<String>> thisRound)
      throws IOException {
    // fieldInfosGen changes whenever the segment's field set can have changed, so it belongs in
    // the key; segment names are never reused within an index
    String key = info.info.name + ":" + info.getFieldInfosGen();
    Set<String> cached = pairFieldNamesBySegment.get(key);
    if (cached == null) {
      cached = JoinIndexUtils.pairFieldNames(JoinIndexUtils.readFieldInfos(info));
      JoinIndexUtils.logDiagnostic(
          log, "AUXIJOIN evt=readFieldInfos segment={} pairs={}", key, cached.size());
    }
    thisRound.put(key, cached);
    return cached;
  }

  // counts merges that actually dropped a fully-dead segment; test-only observability, see
  // droppedSegmentCount()
  private final AtomicInteger droppedSegmentCount = new AtomicInteger();

  // counts doc-aligned merges that actually produced a compacted segment, see alignedMergeCount()
  private final AtomicInteger alignedMergeCount = new AtomicInteger();

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
      return DocAlignedMerge.dropAllDocs(reader);
    }

    @Override
    public void mergeFinished(boolean success, boolean segmentDropped) throws IOException {
      if (segmentDropped) {
        int dropped = droppedSegmentCount.incrementAndGet();
        log.info("AUXIJOIN sidecar: reaped dead segment {} ({} so far)", segments, dropped);
      }
      super.mergeFinished(success, segmentDropped);
    }
  }

  /** Test-only: how many sidecar segments this policy has actually reaped so far. */
  int droppedSegmentCount() {
    return droppedSegmentCount.get();
  }

  /** Test-only: how many doc-aligned compactions have actually completed so far. */
  int alignedMergeCount() {
    return alignedMergeCount.get();
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
    Object fromKey =
        JoinIndexUtils.directoryKey(JoinIndexUtils.directory(fromSearcher.getIndexReader()));
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
    int queued = 0;
    if (previousSnapshot != null) {
      for (String pairFieldName : previousSnapshot) {
        if (!currentSnapshot.contains(pairFieldName)) {
          AuxIndexJoinMergePolicy.addBounded(
              pendingPairRemovals,
              pendingPairRemovalsOrder,
              pairFieldName,
              MAX_PENDING_PAIR_REMOVALS);
          queued++;
        }
      }
    }
    JoinIndexUtils.logDiagnostic(
        log,
        "AUXIJOIN evt=sweepSample needed={} wasNeeded={} queuedForRemoval={} pendingTotal={}",
        currentSnapshot.size(),
        previousSnapshot == null ? -1 : previousSnapshot.size(),
        queued,
        pendingPairRemovals.size());
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
