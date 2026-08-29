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
 */
public final class AuxIndexJoinConfig {

  private boolean singleFieldPerSegment = false;
  private boolean blockingRefresh = true;
  private long sweepSamplingIntervalNanos = TimeUnit.MINUTES.toNanos(1);

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
}
