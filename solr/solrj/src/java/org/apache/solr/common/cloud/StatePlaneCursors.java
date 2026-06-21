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
package org.apache.solr.common.cloud;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-shard {@code (epoch, seq)} reader cursors for the StateUpdates delta plane (PR-3 reader).
 *
 * <p>The reader tracks the last applied delta position <b>per shard</b> — NOT a single global ZK
 * {@code Stat.version} (D2). Higher epoch always wins; within an epoch, higher seq wins.
 *
 * <p>{@link #getGeneration()} is a monotone count of applied delta batches, decoupled from ZK
 * {@code Stat.version}. It is surfaced as a SEPARATE version domain via
 * {@link DocCollection#getStatePlaneGeneration()} (never via {@code getStateUpdatesZkVersion()},
 * which stays pure legacy domain) so that the {@code updateWatchedCollection} clobber guard compares
 * delta-plane progress against delta-plane progress only — and is not misled by a ZK
 * {@code Stat.version} that permanently outruns logical seq after the first ring compaction fold.
 */
public final class StatePlaneCursors {

  // shard -> {epoch, seq}
  private final Map<String, long[]> cursors = new ConcurrentHashMap<>();

  // Monotone count of applied delta batches. Never regresses; decoupled from ZK Stat.version (D2).
  private final AtomicInteger generation = new AtomicInteger(0);

  public StatePlaneCursors() {}

  /** Returns the cursor for {@code shard} as {@code {epoch, seq}}; {@code {0, 0}} if never advanced. */
  public long[] get(String shard) {
    long[] c = cursors.get(shard);
    return c == null ? new long[] {0L, 0L} : new long[] {c[0], c[1]};
  }

  /** True if this shard has ever been advanced (distinguishes a real {0,0} from "never seen"). */
  public boolean isInitialized(String shard) {
    return cursors.containsKey(shard);
  }

  /**
   * Advance the {@code shard} cursor to {@code (epoch, seq)} iff it is strictly ahead of the current
   * position (higher epoch, or same epoch and higher seq). A stale/duplicate position is ignored, so
   * a re-fired watch cannot regress an already-applied cursor. Bumps {@link #getGeneration()} when it
   * actually moves.
   */
  public void advance(String shard, int epoch, long seq) {
    long[] prev = cursors.get(shard);
    if (prev == null || epoch > prev[0] || (epoch == prev[0] && seq > prev[1])) {
      cursors.put(shard, new long[] {epoch, seq});
      generation.incrementAndGet();
    }
  }

  public boolean isEmpty() {
    return cursors.isEmpty();
  }

  public int getGeneration() {
    return generation.get();
  }

  /** Deep copy of these cursors (positions + generation). */
  public StatePlaneCursors copy() {
    StatePlaneCursors c = new StatePlaneCursors();
    cursors.forEach((shard, pos) -> c.cursors.put(shard, new long[] {pos[0], pos[1]}));
    c.generation.set(generation.get());
    return c;
  }

  /**
   * Adopt another set of cursors, advancing each shard to {@code other}'s position when it is ahead
   * and carrying {@code other}'s generation forward when higher. Used by the structure-refresh
   * clobber guard so a fresh {@code state.json} fetch cannot drop newer delta-plane state.
   */
  public void adoptIfAhead(StatePlaneCursors other) {
    if (other == null || other.isEmpty()) {
      return;
    }
    other.cursors.forEach((shard, pos) -> advance(shard, (int) pos[0], pos[1]));
    int og = other.generation.get();
    if (og > generation.get()) {
      generation.set(og);
    }
  }
}
