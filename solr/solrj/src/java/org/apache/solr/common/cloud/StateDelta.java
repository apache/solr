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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A bounded state change for one or more replicas in a single shard.
 * Validated at construction: {@code shortState} values must be in [1..6].
 * LEADER raw short state (1) is preserved; collapsing to ACTIVE is a read-side concern.
 * demotedReplicaIds is always a {@link List} (never {@code int[]}), per D13.
 * demotedShortState defaults to ACTIVE(2) per D14 (deposed leader → ACTIVE).
 */
public final class StateDelta implements Comparable<StateDelta> {

    /** A single (replicaId, shortState) promotion within a delta. */
    public static final class Entry {
        public final int replicaId;
        public final int shortState;

        public Entry(int replicaId, int shortState) {
            validateShortState(shortState);
            this.replicaId = replicaId;
            this.shortState = shortState;
        }

        static void validateShortState(int shortState) {
            if (shortState < 1 || shortState > 6) {
                throw new IllegalArgumentException("shortState must be 1..6, got: " + shortState);
            }
        }
    }

    public final int epoch;
    public final long seq;
    /** Promotion entries for this delta. */
    public final List<Entry> entries;
    /** Replica ids demoted in this delta (D13: {@code List<Integer>}, never int[]). */
    public final List<Integer> demotedReplicaIds;
    /** Short state assigned to demoted replicas; ACTIVE(2) by design (D14). */
    public final int demotedShortState;

    public StateDelta(int epoch, long seq, List<Entry> entries,
                      List<Integer> demotedReplicaIds, int demotedShortState) {
        Entry.validateShortState(demotedShortState);
        this.epoch = epoch;
        this.seq = seq;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.demotedReplicaIds = Collections.unmodifiableList(new ArrayList<>(demotedReplicaIds));
        this.demotedShortState = demotedShortState;
    }

    /**
     * Total ordering by (epoch, seq). Ties are equal (same log position, duplicate detection).
     */
    @Override
    public int compareTo(StateDelta o) {
        int c = Integer.compare(this.epoch, o.epoch);
        return c != 0 ? c : Long.compare(this.seq, o.seq);
    }

    /**
     * Returns true when this delta is stale relative to the given cursor.
     * Stale means {@code (epoch, seq) <= (cursorEpoch, cursorSeq)}.
     */
    public boolean isStale(int cursorEpoch, long cursorSeq) {
        if (this.epoch != cursorEpoch) return this.epoch < cursorEpoch;
        return this.seq <= cursorSeq;
    }
}
