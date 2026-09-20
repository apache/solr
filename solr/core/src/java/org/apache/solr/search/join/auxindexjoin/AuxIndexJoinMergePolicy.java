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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
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

  /**
   * How many sidecar segments one {@link DocAlignedMerge} folds into one, by default.
   *
   * <p>This is a floor as much as a batch size -- with fewer eligible segments than this, no
   * compaction is proposed at all -- so it has to sit below the segment count the sidecar actually
   * runs at, not above it. At 10 it never fired once in a 41-minute run: the sidecar sat at 7-16
   * segments, of which at most 9 were ever eligible at the same time (the rest merging, deleted, or
   * over {@link #DEFAULT_MAX_PAIRS_PER_SEGMENT}), so every round logged "nothing to do" while pairs
   * queued for removal piled up until the heap gave out.
   */
  static final int DEFAULT_MERGE_SEGMENTS_AT_ONCE = 4;

  /**
   * How many pair columns a sidecar segment may already carry and still be compacted again, by
   * default. Merged segments grow wider, not longer, so this is what makes compaction converge
   * instead of rewriting the same big segment forever.
   */
  static final int DEFAULT_MAX_PAIRS_PER_SEGMENT = 1024;

  /**
   * How many bytes a purge has to reclaim before it is worth rewriting the segment for, by default.
   *
   * <p>A purge pays the segment's whole size to drop what is dead in it, so it has to earn the
   * trip; the question is what "earning it" should measure. A share of the columns -- one dead in
   * ten, say -- reads well and is exactly wrong for this index: dead columns land in whatever
   * segment compaction last folded them into, so the widest segments dilute their own dead share
   * below any fixed percentage and become the least likely to ever be cleaned, though they are
   * where the bytes are. A 41-minute run ended with a 96MB sidecar of which 74MB was one segment
   * carrying, at roughly 8% dead, some 6MB nobody could read: under the share rule every round
   * logged "too little dead to be worth rewriting", and the queue never drained below 197 names.
   *
   * <p>Bytes rank those the way an operator would. At 1MB the 74MB segment above is rewritten to
   * win 6MB back, while a 1.3MB segment at the same 8% offers 100KB and is left alone until more of
   * it dies. The estimate assumes columns in a segment cost about the same, which is only roughly
   * true -- it decides an order of magnitude, not a byte count.
   */
  static final long DEFAULT_MIN_RECLAIMABLE_BYTES_TO_PURGE = 1L << 20;

  /**
   * Cap on the merges proposed in one round. A backlog of hundreds of segments is worked off over
   * the following rounds (every flush and every finished merge triggers one) rather than handed to
   * the scheduler at once, where it would stall the writing thread -- which here is a query thread
   * building a pair column.
   */
  private static final int MAX_MERGES_PER_ROUND = 4;

  private volatile int mergeSegmentsAtOnce = DEFAULT_MERGE_SEGMENTS_AT_ONCE;
  private volatile int maxPairsPerSegment = DEFAULT_MAX_PAIRS_PER_SEGMENT;
  private volatile long minReclaimableBytesToPurge = DEFAULT_MIN_RECLAIMABLE_BYTES_TO_PURGE;

  /**
   * One segment worth purging: what is dead in it, and how wide it is -- the two numbers that
   * decide whether rewriting it pays, and which of several candidates pays best.
   */
  private record PurgeCandidate(
      SegmentCommitInfo info, Set<String> dead, int width, long sizeInBytes) {

    /** The share of this segment's columns that a purge would reclaim. */
    double deadShare() {
      return width == 0 ? 0 : (double) dead.size() / width;
    }

    /** What that share is worth in bytes, columns taken to cost about the same as each other. */
    long reclaimableBytes() {
      return (long) (sizeInBytes * deadShare());
    }

    boolean earnsRewrite(long minReclaimableBytes) {
      return reclaimableBytes() >= minReclaimableBytes;
    }
  }

  /**
   * Last round's pair field names by segment key, so a steady stream of commits doesn't re-read
   * every segment's FieldInfos -- a compound-file open and mmap per segment per round -- when only
   * the newest segment is new. Rebuilt on every round, which is also what evicts gone segments.
   * Written only by {@link #findMerges}, which is always called under the {@link IndexWriter}
   * monitor, as its contract states; volatile because {@link #onCreateWeight} reads it from query
   * threads to know what columns the sidecar actually holds.
   */
  private volatile Map<String, Set<String>> pairFieldNamesBySegment = Map.of();

  /**
   * Compaction knobs, fed from {@link AuxIndexJoinConfig} the way {@link #setSweepInterval} is: see
   * the DEFAULT_ constants above for what each one trades off.
   */
  void setCompaction(int mergeSegmentsAtOnce, int maxPairsPerSegment) {
    this.mergeSegmentsAtOnce = mergeSegmentsAtOnce;
    this.maxPairsPerSegment = maxPairsPerSegment;
  }

  /**
   * How many bytes a purge must reclaim before it rewrites a segment for that alone; see {@link
   * #DEFAULT_MIN_RECLAIMABLE_BYTES_TO_PURGE}. Zero purges on any death at all.
   */
  void setMinReclaimableBytesToPurge(long minReclaimableBytesToPurge) {
    this.minReclaimableBytesToPurge = minReclaimableBytesToPurge;
  }

  @Override
  public MergePolicy.MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    Map<String, Set<String>> pairNamesThisRound = HashMap.newHashMap(segmentInfos.asList().size());
    MergeSpecification spec = null;
    List<SegmentCommitInfo> compactable = new ArrayList<>();
    // one snapshot for the whole round, so every merge it proposes reaps a consistent set and no
    // merge drains a name whose column another merge was going to drop
    Set<String> reapSnapshot = Set.copyOf(pendingPairRemovals);
    Map<SegmentCommitInfo, PurgeCandidate> reapableBySegment = new LinkedHashMap<>();
    int dead = 0;
    for (SegmentCommitInfo info : segmentInfos) {
      Set<String> pairFieldNames = pairFieldNames(info, pairNamesThisRound);
      if (merging.contains(info)) {
        continue;
      }
      if (!pairFieldNames.isEmpty() && reapSnapshot.containsAll(pairFieldNames)) {
        dead++;
        spec = added(spec, new DropSegmentMerge(List.of(info), pairFieldNames));
        continue;
      }
      Set<String> reapable = intersection(pairFieldNames, reapSnapshot);
      if (!reapable.isEmpty() && isRewritable(info)) {
        reapableBySegment.put(
            info, new PurgeCandidate(info, reapable, pairFieldNames.size(), info.sizeInBytes()));
      }
      if (isCompactable(info, pairFieldNames)) {
        compactable.add(info);
      }
    }
    this.pairFieldNamesBySegment = pairNamesThisRound;
    int aligned = 0;
    Set<SegmentCommitInfo> taken = new HashSet<>();
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
        // no threshold here, unlike the purges below: these segments are being rewritten anyway,
        // so dropping whatever of them is dead costs nothing extra
        Set<String> reaped = new HashSet<>();
        for (SegmentCommitInfo info : group) {
          PurgeCandidate candidate = reapableBySegment.get(info);
          if (candidate != null) {
            reaped.addAll(candidate.dead());
          }
        }
        taken.addAll(group);
        aligned++;
        spec =
            added(
                spec,
                new DocAlignedMerge(
                    group, reaped, alignedMergeCount::incrementAndGet, this::drainReaped));
      }
    }
    // Reaping on its own account. Compaction alone cannot be relied on to carry it: it needs a
    // quorum of eligible segments, and it passes over exactly the segments most worth purging --
    // isCompactable excludes anything already wider than maxPairsPerSegment. A segment holding dead
    // columns is worth rewriting for that reason alone, so purge it as a merge of one. This
    // converges: the merge drains those names, so the next round finds nothing reapable there.
    //
    // A purge exists only to drop columns, so unlike a compaction it has to earn its rewrite: it
    // costs the segment's whole size to reclaim the bytes that are dead in it. Below the threshold
    // the names simply stay queued and the next death in that segment is what pays for the trip --
    // unless the queue itself has grown dangerous, at which point I/O is the cheaper currency and
    // the threshold is waived.
    boolean queueUnderPressure = pendingPairRemovals.size() >= PENDING_PURGE_HIGH_WATER;
    List<PurgeCandidate> candidates = new ArrayList<>(reapableBySegment.values());
    // most bytes reclaimed first: the per-round merge budget is small, so spend it where it buys
    // the most, not on whichever segment happens to come first in the index
    candidates.sort(Comparator.comparingLong(PurgeCandidate::reclaimableBytes).reversed());
    int purged = 0;
    int notWorthIt = 0;
    PurgeCandidate bestDeclined = null;
    for (PurgeCandidate candidate : candidates) {
      if (aligned + purged >= MAX_MERGES_PER_ROUND) {
        break;
      }
      if (taken.contains(candidate.info())) {
        continue; // a compaction above is already dropping these
      }
      if (!queueUnderPressure && !candidate.earnsRewrite(minReclaimableBytesToPurge)) {
        notWorthIt++;
        // candidates are sorted by what they reclaim, so the first one declined is the best of
        // them: report it, or "too little dead" is a verdict with no evidence attached
        if (bestDeclined == null) {
          bestDeclined = candidate;
        }
        JoinIndexUtils.logDiagnostic(
            log,
            "AUXIJOIN evt=purgeDeclined segment={} dead={} of {} columns ({}%),"
                + " reclaims={} of {} bytes, threshold={}",
            candidate.info().info.name,
            candidate.dead().size(),
            candidate.width(),
            Math.round(candidate.deadShare() * 100),
            candidate.reclaimableBytes(),
            candidate.sizeInBytes(),
            minReclaimableBytesToPurge);
        continue;
      }
      purged++;
      spec =
          added(
              spec,
              new DocAlignedMerge(
                  List.of(candidate.info()),
                  candidate.dead(),
                  purgedSegmentCount::incrementAndGet,
                  this::drainReaped));
    }
    if (spec != null) {
      if (log.isInfoEnabled()) {
        log.info(
            "AUXIJOIN sidecar compaction: {} doc-aligned merge(s) of {} segments each, {} column "
                + "purge(s) and {} dead segment(s) dropped, out of {} segments ({} compactable, {} "
                + "merging, {} pairs pending removal, {} segment(s) reapable now, {} of those too "
                + "little dead to be worth rewriting{}{}), on {}",
            aligned,
            mergeSegmentsAtOnce,
            purged,
            dead,
            segmentInfos.size(),
            compactable.size(),
            merging.size(),
            pendingPairRemovals.size(),
            reapableBySegment.size(),
            notWorthIt,
            bestDeclined == null
                ? ""
                : String.format(
                    Locale.ROOT,
                    " -- the best of them, %s, offers %d of %d bytes (%d of %d columns) against a"
                        + " %d byte threshold",
                    bestDeclined.info().info.name,
                    bestDeclined.reclaimableBytes(),
                    bestDeclined.sizeInBytes(),
                    bestDeclined.dead().size(),
                    bestDeclined.width(),
                    minReclaimableBytesToPurge),
            queueUnderPressure ? ", threshold waived: queue over high water" : "",
            mergeTrigger);
      }
    } else if (log.isDebugEnabled()) {
      log.debug(
          "AUXIJOIN sidecar: nothing to do on {}, {} segments ({} compactable, {} merging, {} pairs"
              + " pending removal)",
          mergeTrigger,
          segmentInfos.size(),
          compactable.size(),
          merging.size(),
          pendingPairRemovals.size());
    }
    return spec;
  }

  private static Set<String> intersection(Set<String> pairFieldNames, Set<String> reapSnapshot) {
    if (pairFieldNames.isEmpty() || reapSnapshot.isEmpty()) {
      return Set.of();
    }
    Set<String> both = new HashSet<>();
    // iterate the smaller side; a fat segment carries a thousand pairs, the queue up to four
    Set<String> smaller;
    Set<String> larger;
    if (pairFieldNames.size() <= reapSnapshot.size()) {
      smaller = pairFieldNames;
      larger = reapSnapshot;
    } else {
      smaller = reapSnapshot;
      larger = pairFieldNames;
    }
    for (String name : smaller) {
      if (larger.contains(name)) {
        both.add(name);
      }
    }
    return both;
  }

  /**
   * Whether a {@link DocAlignedMerge} may rewrite this segment at all -- weaker than {@link
   * #isCompactable}, which additionally declines segments too wide to be worth folding into
   * another. Those are still worth purging dead columns from.
   */
  private static boolean isRewritable(SegmentCommitInfo info) {
    // no deletions: PaddedToMaxDoc refuses to align a segment carrying them, and the sidecar never
    // deletes, so this is belt and braces
    return !info.hasDeletions() && info.info.maxDoc() > 0;
  }

  /**
   * Forgets the pair names a merge just dropped the columns of. Called from {@code
   * OneMerge#mergeFinished} on success only: on failure those columns are still in the index, and
   * dropping their names here would strand them, never to be reaped again.
   */
  private void drainReaped(Set<String> reaped) {
    pendingPairRemovals.removeAll(reaped);
    pendingPairRemovalsOrder.removeAll(reaped);
    reapedPairCount.addAndGet(reaped.size());
    if (log.isInfoEnabled()) {
      log.info(
          "AUXIJOIN sidecar: reaped {} dead pair column(s), {} still pending",
          reaped.size(),
          pendingPairRemovals.size());
    }
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

  // counts columns the liveness sweep condemned, as opposed to the snapshot diff; test-only
  // observability, see strandedColumnCount()
  private final AtomicInteger strandedColumnCount = new AtomicInteger();

  // counts merges that actually dropped a fully-dead segment; test-only observability, see
  // droppedSegmentCount()
  private final AtomicInteger droppedSegmentCount = new AtomicInteger();

  // counts doc-aligned merges that actually produced a compacted segment, see alignedMergeCount()
  private final AtomicInteger alignedMergeCount = new AtomicInteger();

  // counts single-segment rewrites that existed only to drop dead columns, see purgedSegmentCount()
  private final AtomicInteger purgedSegmentCount = new AtomicInteger();

  // counts pair names actually reclaimed -- queued as dead and then dropped from the index by a
  // merge that committed, see reapedPairCount()
  private final AtomicInteger reapedPairCount = new AtomicInteger();

  /**
   * A merge over a single dead segment whose contents are reported as fully deleted, so {@link
   * IndexWriter} drops it instead of rewriting it -- see {@link #wrapForMerge}. Non-static so it
   * can report back to the outer policy's {@link #droppedSegmentCount}.
   */
  private final class DropSegmentMerge extends OneMerge {

    /** The segment's pairs -- all of them dead, which is why it is being dropped. */
    private final Set<String> reapedPairs;

    DropSegmentMerge(List<SegmentCommitInfo> segments, Set<String> reapedPairs) {
      super(segments);
      this.reapedPairs = Set.copyOf(reapedPairs);
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
        // the columns went with the segment, so their queue entries can go too -- otherwise the
        // pending set only ever grows, which is what exhausted the heap
        drainReaped(reapedPairs);
      }
      super.mergeFinished(success, segmentDropped);
    }
  }

  /**
   * Test-only: how many columns the liveness sweep found stranded -- their side gone, and no sample
   * able to say so.
   */
  int strandedColumnCount() {
    return strandedColumnCount.get();
  }

  /** Test-only: how many sidecar segments this policy has actually reaped so far. */
  int droppedSegmentCount() {
    return droppedSegmentCount.get();
  }

  /** Test-only: how many doc-aligned compactions have actually completed so far. */
  int alignedMergeCount() {
    return alignedMergeCount.get();
  }

  /** Test-only: how many segments have been rewritten purely to drop dead pair columns. */
  int purgedSegmentCount() {
    return purgedSegmentCount.get();
  }

  /**
   * Test-only: how many dead pair names have been fully reclaimed so far -- queued as dead, their
   * columns dropped by a merge that committed, and their names taken off the queue. Distinct from
   * {@link #pendingPairRemovalsCount()}, which is what is still owed: that one settles back to zero
   * as reaping keeps up, so a test wanting to prove reaping happened has to watch this instead.
   */
  int reapedPairCount() {
    return reapedPairCount.get();
  }

  /** Test-only: how many dead pair field names are currently queued for the next reap. */
  int pendingPairRemovalsCount() {
    return pendingPairRemovals.size();
  }

  /**
   * Test-only: queues a pair name for reaping, exactly as a sample that stopped needing it would --
   * so a test can pin down what gets reaped without staging two searcher generations to imply it.
   */
  boolean queueForRemoval(String pairFieldName) {
    return addBounded(
        pendingPairRemovals, pendingPairRemovalsOrder, pairFieldName, MAX_PENDING_PAIR_REMOVALS);
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

  /** A sample's needed-pair set, tagged with the index generations it was taken from. */
  private record NeededPairs(Set<String> pairs, long fromVersion, long toVersion) {}

  /**
   * {@link DirectoryReader#getVersion()} is unavailable for this reader, so it cannot be ordered.
   */
  private static final long UNKNOWN_VERSION = Long.MIN_VALUE;

  private final ConcurrentHashMap<Map.Entry<Object, Object>, NeededPairs>
      lastNeededPairsBySearcherPair = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<Map.Entry<Object, Object>> trackedSearcherPairsOrder =
      new ConcurrentLinkedQueue<>();

  // pair field names seen in an earlier snapshot but missing from a later one for the same
  // (from-searcher, to-searcher) pair -- i.e. no longer needed -- queued here for findMerges to
  // reap; also size-capped, same reasoning
  private static final int MAX_PENDING_PAIR_REMOVALS = 4096;

  /**
   * Pending removals past which purges ignore {@link #MAX_PENDING_PAIR_REMOVALS}. That threshold
   * trades heap for I/O -- every name it declines to act on pins a column nobody can read again --
   * and the trade stops being sensible once the queue is itself what threatens the process. Half
   * the queue's cap leaves room to work the backlog off before it starts evicting names, which
   * would strand their columns for good.
   */
  private static final int PENDING_PURGE_HIGH_WATER = MAX_PENDING_PAIR_REMOVALS / 2;

  /**
   * Marks a field name seen on the from side of more than one index; see {@link #onCreateWeight}.
   */
  private static final Object AMBIGUOUS_FROM_INDEX = new Object();

  /** The from-side index observed per join field, or {@link #AMBIGUOUS_FROM_INDEX} once several. */
  private final Map<String, Object> fromIndexByField = new ConcurrentHashMap<>();

  /** Fields already warned about above, so an ambiguous setup doesn't warn on every sample. */
  private final Set<String> warnedAmbiguousFromFields = ConcurrentHashMap.newKeySet();

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

  void onCreateWeight(
      Set<String> neededPairs,
      String fromField,
      IndexSearcher fromSearcher,
      String toField,
      IndexSearcher searcher)
      throws IOException {
    if (!shouldSample()) {
      return;
    }
    Object fromKey =
        JoinIndexUtils.directoryKey(JoinIndexUtils.directory(fromSearcher.getIndexReader()));
    Object toKey = JoinIndexUtils.directoryKey(JoinIndexUtils.directory(searcher.getIndexReader()));
    Map.Entry<Object, Object> searcherKey = Map.entry(fromKey, toKey);

    NeededPairs current =
        new NeededPairs(
            Set.copyOf(neededPairs), readerVersion(fromSearcher), readerVersion(searcher));
    NeededPairs previous = lastNeededPairsBySearcherPair.get(searcherKey);
    // A pair is queued because an earlier sample needed it and this one doesn't. That only means
    // the pair is dead if this sample sees at least as much of the index as the earlier one did:
    // the key here is the directory, stable across reopens, so consecutive samples come from
    // different queries that may hold different searcher generations. Let an older generation
    // speak and it reports every segment opened since as missing -- queueing live columns for
    // removal, which JoinIndexScorerSupplier then has to rebuild mid-query.
    if (previous != null && seesLessThan(current, previous)) {
      JoinIndexUtils.logDiagnostic(
          log,
          "AUXIJOIN evt=sweepSkipped needed={} wasNeeded={} fromVer={}<{} toVer={}<{}"
              + " pendingTotal={}",
          current.pairs().size(),
          previous.pairs().size(),
          current.fromVersion(),
          previous.fromVersion(),
          current.toVersion(),
          previous.toVersion(),
          pendingPairRemovals.size());
      return;
    }
    AuxIndexJoinMergePolicy.putBounded(
        lastNeededPairsBySearcherPair,
        trackedSearcherPairsOrder,
        searcherKey,
        current,
        MAX_TRACKED_SEARCHER_PAIRS);
    int queued = 0;
    if (previous != null) {
      for (String pairFieldName : previous.pairs()) {
        if (!current.pairs().contains(pairFieldName)) {
          AuxIndexJoinMergePolicy.addBounded(
              pendingPairRemovals,
              pendingPairRemovalsOrder,
              pairFieldName,
              MAX_PENDING_PAIR_REMOVALS);
          queued++;
        }
      }
    }
    int stranded =
        queueColumnsWithDeadSideKeys(fromField, fromSearcher, fromKey, toField, searcher);
    JoinIndexUtils.logDiagnostic(
        log,
        "AUXIJOIN evt=sweepSample needed={} wasNeeded={} queuedForRemoval={} strandedFound={}"
            + " pendingTotal={}",
        current.pairs().size(),
        previous == null ? -1 : previous.pairs().size(),
        queued,
        stranded,
        pendingPairRemovals.size());
  }

  /**
   * Queues every column the sidecar holds whose side key names a segment that no longer exists --
   * the deaths the snapshot diff above structurally cannot see.
   *
   * <p>That diff only ever reports a pair as dead by watching a query need it and then stop needing
   * it, both within one process. Everything a restart inherits therefore lives forever: no query in
   * the new process ever needs a column whose from-segment was merged away while Solr was down, so
   * no snapshot ever holds its name to miss it later. A 96MB sidecar carrying a 74MB segment from
   * the previous day's run is what that looks like on disk. Here the reader itself is the evidence
   * instead: {@code fromSearcher} and {@code searcher} enumerate exactly which side keys still
   * exist, so any stored pair naming one they don't is provably unreadable, restart or no restart.
   *
   * <p>Attribution is by field name, since a sidecar may serve several joins: only pairs whose two
   * keys belong to this query's {@code fromField} and {@code toField} are judged here, and the
   * from-side half is judged only while a single from-index has been seen using that field name --
   * two cores joining into this one on the same field would otherwise each declare the other's
   * columns dead. The to side needs no such guard: the sidecar lives under the to-core's dataDir,
   * so every to key in it names a segment of the index {@code searcher} is reading.
   *
   * <p>Runs under the same sampling throttle as the snapshot diff, and reads the column names
   * {@link #findMerges} last saw on disk -- so after a restart the reclaim lands on the first
   * sample once the sidecar has been committed to at least once, not on the very first query. Like
   * everything else the reaper decides, this is recoverable rather than authoritative: a column
   * built moments ago by a concurrent query holding a newer searcher than this sample's is
   * invisible to these readers and can be condemned, costing a rebuild through {@code
   * JoinIndexScorerSupplier#refreshJoinTasksReferences} -- never a wrong answer.
   *
   * @return how many names this sweep newly queued
   */
  private int queueColumnsWithDeadSideKeys(
      String fromField,
      IndexSearcher fromSearcher,
      Object fromDirectoryKey,
      String toField,
      IndexSearcher toSearcher) {
    Map<String, Set<String>> stored = pairFieldNamesBySegment;
    if (stored.isEmpty()) {
      return 0;
    }
    Set<String> liveFrom = JoinIndexUtils.liveSideKeys(fromSearcher, fromField);
    Set<String> liveTo = JoinIndexUtils.liveSideKeys(toSearcher, toField);
    boolean judgeFromSide = fromSideIsUnambiguous(fromField, fromDirectoryKey);
    int queued = 0;
    for (Set<String> namesInSegment : stored.values()) {
      for (String pairFieldName : namesInSegment) {
        String[] sideKeys = JoinIndexUtils.splitPairFieldName(pairFieldName);
        if (sideKeys == null
            || !JoinIndexUtils.isKeyOf(sideKeys[0], fromField)
            || !JoinIndexUtils.isKeyOf(sideKeys[1], toField)) {
          continue; // another join's column: this query knows nothing about its sides
        }
        boolean toSideGone = !liveTo.contains(sideKeys[1]);
        boolean fromSideGone = judgeFromSide && !liveFrom.contains(sideKeys[0]);
        if ((toSideGone || fromSideGone) && queueForRemoval(pairFieldName)) {
          queued++;
          strandedColumnCount.incrementAndGet();
          JoinIndexUtils.logDiagnostic(
              log,
              "AUXIJOIN evt=strandedColumn pair={} fromSideGone={} toSideGone={}",
              pairFieldName,
              fromSideGone,
              toSideGone);
        }
      }
    }
    return queued;
  }

  /**
   * Whether from keys of {@code fromField} can be trusted to come from {@code fromDirectoryKey}'s
   * index -- true until a second index is seen joining on that same field name, and false from then
   * on, since neither can then tell which of them wrote a given column.
   */
  private boolean fromSideIsUnambiguous(String fromField, Object fromDirectoryKey) {
    Object seen =
        fromIndexByField.merge(
            fromField,
            fromDirectoryKey,
            (previous, current) -> previous.equals(current) ? previous : AMBIGUOUS_FROM_INDEX);
    if (Objects.equals(seen, AMBIGUOUS_FROM_INDEX)) {
      if (warnedAmbiguousFromFields.add(fromField)) { // once per field, not once per sample
        log.warn(
            "AUXIJOIN sidecar: more than one index joins on field {}, so stranded columns of that "
                + "field can no longer be told apart by their from key and stay until their to "
                + "side dies",
            fromField);
      }
      return false;
    }
    return true;
  }

  /**
   * Whether {@code sample} was taken from an older view of either side than {@code previous} -- so
   * a pair it does not need may simply be one it cannot see yet. Unknown versions compare as
   * neither older nor newer, leaving the sample to be trusted as before.
   */
  private static boolean seesLessThan(NeededPairs sample, NeededPairs previous) {
    return isOlder(sample.fromVersion(), previous.fromVersion())
        || isOlder(sample.toVersion(), previous.toVersion());
  }

  private static boolean isOlder(long version, long than) {
    return version != UNKNOWN_VERSION && than != UNKNOWN_VERSION && version < than;
  }

  /**
   * The index generation behind a searcher, or {@link #UNKNOWN_VERSION} when it is not reading a
   * {@link DirectoryReader} and so carries no orderable version.
   */
  private static long readerVersion(IndexSearcher searcher) {
    return searcher.getIndexReader() instanceof DirectoryReader dr
        ? dr.getVersion()
        : UNKNOWN_VERSION;
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

  /**
   * Same eviction policy as {@link AuxIndexJoinMergePolicy#putBounded}, for a plain set.
   *
   * @return whether the value was not already there
   */
  static <T> boolean addBounded(
      Set<T> set, ConcurrentLinkedQueue<T> insertionOrder, T value, int maxSize) {
    if (!set.add(value)) {
      return false;
    }
    insertionOrder.add(value);
    while (set.size() > maxSize) {
      T oldest = insertionOrder.poll();
      if (oldest == null) {
        break;
      }
      set.remove(oldest);
    }
    return true;
  }
}
