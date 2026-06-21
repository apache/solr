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
 * An ordered ring of {@link StateDelta} entries for one shard, stored as a single ZK node.
 *
 * <p>{@code lastSeq} is the authoritative high-water mark (D2: decoupled from ZK {@code Stat.version},
 * which is a CAS token only — it permanently outruns logical seq after the first compaction fold).
 *
 * <p>{@code writerId} is a writer-side fence + diagnostics identifier (D4). Readers MUST NOT
 * validate {@code writerId}; readers trust {@code (epoch, seq)} ordering only.
 */
public final class ShardStateLog {

    public final int epoch;
    public final long baseSeq;
    /** High-water mark — authoritative seq, correct even on an empty post-fold ring (D2). */
    public final long lastSeq;
    /** Overseer election id for writer-side fencing (D4). Readers must not validate. */
    public final String writerId;
    public final List<StateDelta> entries;

    public ShardStateLog(int epoch, long baseSeq, long lastSeq, String writerId,
                         List<StateDelta> entries) {
        this.epoch = epoch;
        this.baseSeq = baseSeq;
        this.lastSeq = lastSeq;
        this.writerId = writerId;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    /**
     * Returns {@link #lastSeq} — the authoritative high-water mark.
     * Correct on an empty post-fold ring (D2: seq decoupled from Stat.version).
     */
    public long maxSeq() {
        return lastSeq;
    }

    /**
     * Applies all entries with {@code (epoch, seq) > (cursorEpoch, cursorSeq)} to {@code dc},
     * calling {@code dc.updateState(replicaId, shortState)} for each qualifying entry.
     * Demotions are applied before promotions within each delta (reader ordering invariant).
     * Idempotent: the same delta applied twice is suppressed by cursor advancement.
     */
    public void applyAfter(int cursorEpoch, long cursorSeq, DocCollection dc) {
        List<StateDelta> sorted = new ArrayList<>(entries);
        Collections.sort(sorted);
        int curEpoch = cursorEpoch;
        long curSeq = cursorSeq;
        for (StateDelta delta : sorted) {
            if (delta.isStale(curEpoch, curSeq)) continue;
            // Demotions first, then promotions (ordering invariant for leader-flap correctness)
            for (Integer demotedId : delta.demotedReplicaIds) {
                dc.updateState(demotedId, delta.demotedShortState);
            }
            for (StateDelta.Entry entry : delta.entries) {
                dc.updateState(entry.replicaId, entry.shortState);
            }
            // Advance cursor to suppress double-apply of the same (epoch, seq)
            curEpoch = delta.epoch;
            curSeq = delta.seq;
        }
    }
}
