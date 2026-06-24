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
import java.util.Objects;

/**
 * One state-plane transition for a single shard. The durable record carries the collection
 * incarnation and shard that produced it so readers can fail closed instead of applying a
 * transition to the wrong collection recreation or shard path.
 */
public final class StateDelta implements Comparable<StateDelta> {
    public static final int ACTIVE = 2;

    /** Collection incarnation id that produced this delta. */
    public final String collectionId;
    /** Shard name that produced this delta. */
    public final String shardId;
    public final int epoch;
    public final long seq;
    public final List<Entry> entries;
    public final List<Integer> demotedReplicaIds;
    /** shortState to assign to demoted leaders. Defaults to ACTIVE(2), D14. */
    public final int demotedShortState;

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
                throw new IllegalArgumentException("shortState must be in [1..6], got " + shortState);
            }
        }
    }

    public StateDelta(String collectionId, String shardId, int epoch, long seq, List<Entry> entries,
                      List<Integer> demotedIds, int demotedShortState) {
        Entry.validateShortState(demotedShortState);
        this.collectionId = collectionId;
        this.shardId = shardId;
        this.epoch = epoch;
        this.seq = seq;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.demotedReplicaIds = Collections.unmodifiableList(new ArrayList<>(demotedIds));
        this.demotedShortState = demotedShortState;
    }

    public void validateIdentity(DocCollection dc, String shard) {
        if (collectionId == null) {
            throw new IllegalStateException("state-plane delta is missing collectionId");
        }
        if (shardId == null) {
            throw new IllegalStateException("state-plane delta is missing shardId");
        }
        if (dc.getId() == null || !collectionId.equals(String.valueOf(dc.getId()))) {
            throw new IllegalStateException("state-plane collectionId mismatch delta="
                    + collectionId + " current=" + dc.getId());
        }
        if (!Objects.equals(shardId, shard)) {
            throw new IllegalStateException("state-plane shardId mismatch delta=" + shardId + " current=" + shard);
        }
    }

    @Override
    public int compareTo(StateDelta o) {
        int c = Integer.compare(this.epoch, o.epoch);
        if (c != 0) return c;
        return Long.compare(this.seq, o.seq);
    }

    /** True if this delta is at or behind the supplied per-shard cursor. */
    public boolean isStale(int cursorEpoch, long cursorSeq) {
        if (this.epoch < cursorEpoch) return true;
        return this.epoch == cursorEpoch && this.seq <= cursorSeq;
    }
}
