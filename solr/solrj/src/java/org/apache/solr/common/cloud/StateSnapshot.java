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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-shard state snapshot (D1: per-shard, not per-collection — avoids the 1 MB per-collection
 * ceiling and cross-shard rebase storms).
 *
 * <p>LEADER raw short state (1) is preserved through encode → decode → reconstruct.
 * Collapsing shortState=1 to ACTIVE is a read-side concern ({@code published=true}), not done here.
 */
public final class StateSnapshot {

    /** Collection identity (D4): the numeric collection id, or {@code -1} when unknown/legacy-seeded. */
    public final long collectionId;
    /** Collection identity (D4): the collection name, or {@code null} when unknown/legacy-seeded. */
    public final String collectionName;
    public final int epoch;
    public final String shard;
    /**
     * Seq cursor at snapshot time — the explicit {@code upToSeq}: deltas with seq &lt;= this are
     * already folded into this snapshot. Exposed both as {@code baseSeq} (historical name) and via
     * {@link #upToSeq()} (D4). The two are identical by construction; {@code reconstruct()} semantics
     * are unchanged.
     */
    public final long baseSeq;
    /** Maps replicaId → raw shortState. LEADER is preserved as shortState=1. */
    public final Map<Integer, Integer> replicaStates;

    /** Back-compat constructor without explicit collection identity (collectionId=-1, name=null). */
    public StateSnapshot(int epoch, String shard, long baseSeq, Map<Integer, Integer> replicaStates) {
        this(-1L, null, epoch, shard, baseSeq, replicaStates);
    }

    /**
     * Full constructor carrying collection identity (D4). {@code baseSeq} IS the {@code upToSeq} — the
     * seq up to which deltas are folded into {@code replicaStates}.
     */
    public StateSnapshot(long collectionId, String collectionName, int epoch, String shard,
                         long baseSeq, Map<Integer, Integer> replicaStates) {
        this.collectionId = collectionId;
        this.collectionName = collectionName;
        this.epoch = epoch;
        this.shard = shard;
        this.baseSeq = baseSeq;
        this.replicaStates = Collections.unmodifiableMap(new HashMap<>(replicaStates));
    }

    /** The seq up to which deltas are already folded into this snapshot (== {@link #baseSeq}). */
    public long upToSeq() {
        return baseSeq;
    }

    /**
     * Reconstructs the final {@code Map&lt;replicaId, shortState&gt;} by applying {@code deltas}
     * on top of this snapshot. Honors ordering, idempotency, and staleness.
     *
     * <ul>
     *   <li>Ordering: deltas sorted by (epoch, seq) before application.
     *   <li>Stale: deltas with (epoch,seq) &lt;= (snapshot.epoch, snapshot.baseSeq) are skipped.
     *   <li>Idempotent: duplicate (epoch,seq) pairs are suppressed via cursor advancement.
     *   <li>LEADER (shortState=1) is preserved as-is through reconstruction.
     * </ul>
     */
    public Map<Integer, Integer> reconstruct(List<StateDelta> deltas) {
        Map<Integer, Integer> result = new HashMap<>(replicaStates);
        List<StateDelta> sorted = new ArrayList<>(deltas);
        Collections.sort(sorted);
        int curEpoch = epoch;
        long curSeq = baseSeq;
        for (StateDelta delta : sorted) {
            if (delta.isStale(curEpoch, curSeq)) continue;
            // Demotions first, then promotions
            for (Integer demotedId : delta.demotedReplicaIds) {
                result.put(demotedId, delta.demotedShortState);
            }
            for (StateDelta.Entry entry : delta.entries) {
                result.put(entry.replicaId, entry.shortState);
            }
            // Advance cursor for idempotency on duplicate (epoch,seq)
            curEpoch = delta.epoch;
            curSeq = delta.seq;
        }
        return Collections.unmodifiableMap(result);
    }
}
