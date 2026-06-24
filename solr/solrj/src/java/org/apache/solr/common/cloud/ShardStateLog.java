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
 * Bounded per-shard CAS ring of {@link StateDelta} entries stored in one ZK node.
 *
 * <p>{@code writerId} is a writer-side fence and diagnostic identifier. Readers do not validate
 * writer ids; they validate the durable collection/shard identity and then advance by {@code (epoch, seq)}.
 */
public final class ShardStateLog {
    /** Collection incarnation id for this shard log. */
    public final String collectionId;
    /** Shard name for this log. */
    public final String shardId;
    public final int epoch;
    public final long baseSeq;
    public final long lastSeq;
    public final String writerId;
    public final List<StateDelta> entries;

    public ShardStateLog(String collectionId, String shardId, int epoch, long baseSeq, long lastSeq,
                         String writerId, List<StateDelta> entries) {
        this.collectionId = collectionId;
        this.shardId = shardId;
        this.epoch = epoch;
        this.baseSeq = baseSeq;
        this.lastSeq = lastSeq;
        this.writerId = writerId;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    }

    public long maxSeq() {
        return lastSeq;
    }

    public void validateIdentity(DocCollection dc, String shard) {
        if (collectionId == null) {
            throw new IllegalStateException("state-plane ring is missing collectionId");
        }
        if (shardId == null) {
            throw new IllegalStateException("state-plane ring is missing shardId");
        }
        if (dc.getId() == null || !collectionId.equals(String.valueOf(dc.getId()))) {
            throw new IllegalStateException("state-plane ring collectionId mismatch ring="
                    + collectionId + " current=" + dc.getId());
        }
        if (!Objects.equals(shardId, shard)) {
            throw new IllegalStateException("state-plane ring shardId mismatch ring=" + shardId + " current=" + shard);
        }
    }

    public void applyAfter(int cursorEpoch, long cursorSeq, DocCollection dc) {
        validateIdentity(dc, shardId);
        List<StateDelta> sorted = new ArrayList<>(entries);
        Collections.sort(sorted);
        int curEpoch = cursorEpoch;
        long curSeq = cursorSeq;
        for (StateDelta delta : sorted) {
            delta.validateIdentity(dc, shardId);
            if (delta.isStale(curEpoch, curSeq)) continue;
            for (Integer demotedId : delta.demotedReplicaIds) {
                dc.updateState(demotedId, delta.demotedShortState);
            }
            for (StateDelta.Entry entry : delta.entries) {
                dc.updateState(entry.replicaId, entry.shortState);
            }
            curEpoch = delta.epoch;
            curSeq = delta.seq;
        }
    }
}
