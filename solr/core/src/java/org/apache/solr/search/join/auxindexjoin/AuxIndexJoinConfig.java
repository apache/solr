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

import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;

/**
 * Holds the configuration used to create an {@link AuxIndexManager}. Every setter returns {@link
 * AuxIndexJoinConfig} to allow chaining settings conveniently, for example:
 *
 * <pre class="prettyprint">
 * AuxIndexJoinConfig config = new AuxIndexJoinConfig().setBlockingRefresh(false);
 * AuxIndexManager joinIndex = new AuxIndexManager(joinDir, config);
 * </pre>
 *
 * <p>Once passed to {@link AuxIndexManager#AuxIndexManager(Directory, AuxIndexJoinConfig)}, changes
 * to this object no longer affect the created {@link AuxIndexManager} instance.
 *
 * @lucene.experimental
 *     <p>This is experimental API and subject to change.
 */
public final class AuxIndexJoinConfig {

  private boolean singleFieldPerSegment = false;
  private boolean blockingRefresh = true;
  private boolean useFromSideThreads = true;
  private boolean wipeOnVersionMismatch = true;
  private long sweepSamplingIntervalNanos = TimeUnit.MINUTES.toNanos(1);
  private int mergeSegmentsAtOnce = AuxIndexJoinMergePolicy.DEFAULT_MERGE_SEGMENTS_AT_ONCE;
  private int maxPairsPerSegment = AuxIndexJoinMergePolicy.DEFAULT_MAX_PAIRS_PER_SEGMENT;
  private long minReclaimableBytesToPurge =
      AuxIndexJoinMergePolicy.DEFAULT_MIN_RECLAIMABLE_BYTES_TO_PURGE;

  /** Sole constructor, using the default settings documented on each setter. */
  public AuxIndexJoinConfig() {}

  /**
   * Whether each pair column is flushed into its own sidecar segment, rather than batching every
   * pair column built in the same round into one segment. Default is {@code false}: many columns
   * per segment, traded off against a longer sweep to reclaim any that become dead.
   */
  public AuxIndexJoinConfig setSingleFieldPerSegment(boolean singleFieldPerSegment) {
    this.singleFieldPerSegment = singleFieldPerSegment;
    return this;
  }

  /** Returns the current value set via {@link #setSingleFieldPerSegment}. */
  public boolean getSingleFieldPerSegment() {
    return singleFieldPerSegment;
  }

  /**
   * Whether writing a batch of pair columns blocks until the sidecar's {@link
   * org.apache.lucene.search.SearcherManager} is refreshed past it, so the freshly built pairs are
   * visible to the caller that triggered the build. Default is {@code true}.
   */
  public AuxIndexJoinConfig setBlockingRefresh(boolean blockingRefresh) {
    this.blockingRefresh = blockingRefresh;
    return this;
  }

  /** Returns the current value set via {@link #setBlockingRefresh}. */
  public boolean getBlockingRefresh() {
    return blockingRefresh;
  }

  /**
   * Whether loading the from-side leaves that feed a join build is parallelized across the caller's
   * executor threads. When {@code true} (the default) each from-side segment is loaded on a
   * separate executor thread; when {@code false} they are loaded sequentially on the calling
   * thread. Set to {@code false} to bound the load to a single thread, e.g. when the executor is
   * contended or to keep queries deterministic.
   */
  public AuxIndexJoinConfig setUseFromSideThreads(boolean useFromSideThreads) {
    this.useFromSideThreads = useFromSideThreads;
    return this;
  }

  /** Returns the current value set via {@link #setUseFromSideThreads}. */
  public boolean getUseFromSideThreads() {
    return useFromSideThreads;
  }

  /**
   * Whether opening the join index wipes a pre-existing on-disk index whose commit user data
   * records a different Solr major version than the current one (see {@code
   * AuxIndexManager#wipeIfIncompatibleVersion}). Default is {@code true}: doc-id mappings are
   * version-specific and stale pairs are rebuilt lazily, so keeping them risks reading corrupt
   * joins after an upgrade. Disable only when the same process is guaranteed to own the index.
   */
  public AuxIndexJoinConfig setWipeOnVersionMismatch(boolean wipeOnVersionMismatch) {
    this.wipeOnVersionMismatch = wipeOnVersionMismatch;
    return this;
  }

  /** Returns the current value set via {@link #setWipeOnVersionMismatch}. */
  public boolean getWipeOnVersionMismatch() {
    return wipeOnVersionMismatch;
  }

  /**
   * How often {@link AuxIndexManager#onCreateWeight} actually samples searcher state for the
   * dead-pair reaper; calls arriving sooner than this after the last accepted sample are skipped,
   * since sampling is only a heuristic hint feeding the reap decision, not a correctness
   * requirement. Default is one minute. Pass zero (or a non-positive value) to sample on every
   * call.
   */
  public AuxIndexJoinConfig setSweepSamplingInterval(long duration, TimeUnit unit) {
    this.sweepSamplingIntervalNanos = unit.toNanos(duration);
    return this;
  }

  /** Returns the current value set via {@link #setSweepSamplingInterval}, in nanoseconds. */
  public long getSweepSamplingIntervalNanos() {
    return sweepSamplingIntervalNanos;
  }

  /**
   * How many sidecar segments one compaction folds into a single doc-aligned one. This is a floor
   * as much as a batch size: with fewer eligible segments than this, {@code
   * AuxIndexJoinMergePolicy} proposes no compaction at all, so it has to sit below the segment
   * count the sidecar actually runs at. Default is 4.
   */
  public AuxIndexJoinConfig setMergeSegmentsAtOnce(int mergeSegmentsAtOnce) {
    this.mergeSegmentsAtOnce = mergeSegmentsAtOnce;
    return this;
  }

  /** Returns the current value set via {@link #setMergeSegmentsAtOnce}. */
  public int getMergeSegmentsAtOnce() {
    return mergeSegmentsAtOnce;
  }

  /**
   * How many pair columns a sidecar segment may already carry and still be compacted again. Merged
   * segments grow wider, not longer, so this is what makes compaction converge instead of rewriting
   * the same big segment forever. Default is 1024.
   */
  public AuxIndexJoinConfig setMaxPairsPerSegment(int maxPairsPerSegment) {
    this.maxPairsPerSegment = maxPairsPerSegment;
    return this;
  }

  /** Returns the current value set via {@link #setMaxPairsPerSegment}. */
  public int getMaxPairsPerSegment() {
    return maxPairsPerSegment;
  }

  /**
   * How many bytes a purge has to reclaim before it rewrites a segment just to drop the columns
   * that are dead in it. Ranking by bytes rather than by share of columns is what keeps the widest
   * segments -- which dilute their own dead share, and hold most of the sidecar -- from being the
   * least likely to ever be cleaned. Default is 1MB. Zero purges on any death at all; compaction is
   * not subject to it, having decided to rewrite anyway.
   */
  public AuxIndexJoinConfig setMinReclaimableBytesToPurge(long minReclaimableBytesToPurge) {
    this.minReclaimableBytesToPurge = minReclaimableBytesToPurge;
    return this;
  }

  /** Returns the current value set via {@link #setMinReclaimableBytesToPurge}. */
  public long getMinReclaimableBytesToPurge() {
    return minReclaimableBytesToPurge;
  }
}
