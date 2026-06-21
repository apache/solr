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

/**
 * Centralized tuning knobs for the StateUpdates delta plane (PR-5: snapshot compaction + bounded
 * delta retention). Each value is parsed from a clearly-named system property with a sane default.
 *
 * <p>Compaction (writer) vs. catch-up (reader) split:
 * <ul>
 *   <li><b>{@link #COMPACT_AFTER_COUNT_SYSPROP}</b> / <b>{@link #COMPACT_AFTER_MILLIS_SYSPROP}</b> —
 *       writer-side: when to fold a shard's delta prefix into its per-shard snapshot.
 *   <li><b>{@link #MAX_DELTA_FETCH_SYSPROP}</b> / <b>{@link #READER_CATCHUP_LIMIT_SYSPROP}</b> —
 *       reader-side: hard cap on deltas folded per ring read, and how far behind the ring head a
 *       reader may fall before it must force a snapshot catch-up instead of an incremental fold.
 * </ul>
 *
 * <p>Relationship to the legacy {@code solr.statePlane.ringCap}
 * ({@link StatePlaneWriter#RING_CAP_SYSPROP}): {@code ringCap} remains a <b>hard safety cap</b> on
 * ring depth (a blind-trim backstop that never folds into the snapshot). {@link #compactAfterCount()}
 * is the real compaction trigger and should normally be {@code <= ringCap}; with compaction active a
 * ring is folded into its snapshot well before {@code ringCap} is ever reached, so the blind trim
 * only fires on a misconfiguration where {@code compactAfterCount > ringCap}.
 */
public final class StateDeltaConfig {

    private StateDeltaConfig() {}

    /** int: fold a shard's delta prefix into its snapshot once the live delta count reaches this. */
    public static final String COMPACT_AFTER_COUNT_SYSPROP = "solr.stateDeltas.compactAfterCount";
    /**
     * long (millis): also compact a shard when this much wall time has elapsed since its last
     * compaction, even if the delta count is still under {@link #COMPACT_AFTER_COUNT_SYSPROP}.
     * {@code 0} (default) disables the time-based trigger.
     */
    public static final String COMPACT_AFTER_MILLIS_SYSPROP = "solr.stateDeltas.compactAfterMillis";
    /** int: hard cap on how many deltas a reader folds per ring read before forcing a catch-up. */
    public static final String MAX_DELTA_FETCH_SYSPROP = "solr.stateDeltas.maxDeltaFetch";
    /**
     * int: when a reader is more than this many seqs behind the ring head it must force a snapshot
     * catch-up instead of an incremental fold. {@code 0} disables (always fold incrementally).
     */
    public static final String READER_CATCHUP_LIMIT_SYSPROP = "solr.stateDeltas.readerCatchupLimit";

    /** Default compaction count trigger; aligns with {@code DEFAULT_RING_CAP=64}. */
    public static final int DEFAULT_COMPACT_AFTER_COUNT = 64;
    /** Default time-based compaction trigger — disabled (count-only). */
    public static final long DEFAULT_COMPACT_AFTER_MILLIS = 0L;
    /** Default hard cap on deltas folded per ring read. */
    public static final int DEFAULT_MAX_DELTA_FETCH = 1024;
    /** Default reader catch-up threshold (force snapshot catch-up beyond this many seqs behind). */
    public static final int DEFAULT_READER_CATCHUP_LIMIT = 256;

    public static int compactAfterCount() {
        return Integer.getInteger(COMPACT_AFTER_COUNT_SYSPROP, DEFAULT_COMPACT_AFTER_COUNT);
    }

    public static long compactAfterMillis() {
        return Long.getLong(COMPACT_AFTER_MILLIS_SYSPROP, DEFAULT_COMPACT_AFTER_MILLIS);
    }

    public static int maxDeltaFetch() {
        return Integer.getInteger(MAX_DELTA_FETCH_SYSPROP, DEFAULT_MAX_DELTA_FETCH);
    }

    public static int readerCatchupLimit() {
        return Integer.getInteger(READER_CATCHUP_LIMIT_SYSPROP, DEFAULT_READER_CATCHUP_LIMIT);
    }
}
