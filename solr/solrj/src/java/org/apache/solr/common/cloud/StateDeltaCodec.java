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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;

/**
 * Fixed-order javabin (no POJO binding, D13) codec for {@link StateDelta},
 * {@link ShardStateLog}, and {@link StateSnapshot}.
 *
 * <p>Wire format — each POJO encoded as a fixed-order {@code List&lt;Object&gt;} of primitives:
 * <ul>
 *   <li>StateDelta: {@code [epoch, seq, entries_list, demotedReplicaIds, demotedShortState]}
 *       where {@code entries_list} = {@code [[replicaId, shortState], ...]}
 *   <li>ShardStateLog: {@code [epoch, baseSeq, lastSeq, writerId, delta_tuples_list]}
 *   <li>StateSnapshot: {@code [epoch, shard, baseSeq, flat_states, collectionId, collectionName]}
 *       where {@code flat_states} = {@code [id1, state1, id2, state2, ...]}. {@code collectionId}/
 *       {@code collectionName} are appended LAST (D4) so older 4-element snapshots still decode.
 * </ul>
 *
 * <p>Deserialized lists are read via the {@link List} interface — javabin produces fastutil
 * {@code ObjectArrayList}, not {@code ArrayList}; never cast to {@code ArrayList} (D13).
 */
public final class StateDeltaCodec {

    private StateDeltaCodec() {}

    // ---- StateDelta ----

    public static byte[] encodeStateDelta(StateDelta delta) throws IOException {
        return encode(deltaToTuple(delta));
    }

    public static StateDelta decodeStateDelta(byte[] bytes) throws IOException {
        return tupleToStateDelta((List<?>) decode(bytes));
    }

    // ---- ShardStateLog ----

    public static byte[] encodeShardStateLog(ShardStateLog log) throws IOException {
        return encode(logToTuple(log));
    }

    public static ShardStateLog decodeShardStateLog(byte[] bytes) throws IOException {
        return tupleToShardStateLog((List<?>) decode(bytes));
    }

    // ---- StateSnapshot ----

    public static byte[] encodeStateSnapshot(StateSnapshot snapshot) throws IOException {
        return encode(snapshotToTuple(snapshot));
    }

    public static StateSnapshot decodeStateSnapshot(byte[] bytes) throws IOException {
        return tupleToStateSnapshot((List<?>) decode(bytes));
    }

    // ---- Internal tuple builders ----

    static List<Object> deltaToTuple(StateDelta delta) {
        List<Object> tuple = new ArrayList<>(5);
        tuple.add(delta.epoch);
        tuple.add(delta.seq);
        List<Object> entriesOut = new ArrayList<>(delta.entries.size());
        for (StateDelta.Entry e : delta.entries) {
            List<Object> pair = new ArrayList<>(2);
            pair.add(e.replicaId);
            pair.add(e.shortState);
            entriesOut.add(pair);
        }
        tuple.add(entriesOut);
        tuple.add(new ArrayList<>(delta.demotedReplicaIds));
        tuple.add(delta.demotedShortState);
        return tuple;
    }

    static StateDelta tupleToStateDelta(List<?> tuple) {
        int epoch = num(tuple.get(0)).intValue();
        long seq = num(tuple.get(1)).longValue();
        List<?> rawEntries = (List<?>) tuple.get(2);
        List<StateDelta.Entry> entries = new ArrayList<>(rawEntries.size());
        for (Object rawEntry : rawEntries) {
            List<?> pair = (List<?>) rawEntry;
            entries.add(new StateDelta.Entry(num(pair.get(0)).intValue(), num(pair.get(1)).intValue()));
        }
        List<?> rawDemoted = (List<?>) tuple.get(3);
        List<Integer> demotedIds = new ArrayList<>(rawDemoted.size());
        for (Object id : rawDemoted) {
            demotedIds.add(num(id).intValue());
        }
        int demotedShortState = num(tuple.get(4)).intValue();
        return new StateDelta(epoch, seq, entries, demotedIds, demotedShortState);
    }

    static List<Object> logToTuple(ShardStateLog log) {
        List<Object> tuple = new ArrayList<>(5);
        tuple.add(log.epoch);
        tuple.add(log.baseSeq);
        tuple.add(log.lastSeq);
        tuple.add(log.writerId);
        List<Object> deltaList = new ArrayList<>(log.entries.size());
        for (StateDelta delta : log.entries) {
            deltaList.add(deltaToTuple(delta));
        }
        tuple.add(deltaList);
        return tuple;
    }

    static ShardStateLog tupleToShardStateLog(List<?> tuple) {
        int epoch = num(tuple.get(0)).intValue();
        long baseSeq = num(tuple.get(1)).longValue();
        long lastSeq = num(tuple.get(2)).longValue();
        String writerId = tuple.get(3) == null ? null : tuple.get(3).toString();
        List<?> rawDeltas = (List<?>) tuple.get(4);
        List<StateDelta> entries = new ArrayList<>(rawDeltas.size());
        for (Object rawDelta : rawDeltas) {
            entries.add(tupleToStateDelta((List<?>) rawDelta));
        }
        return new ShardStateLog(epoch, baseSeq, lastSeq, writerId, entries);
    }

    static List<Object> snapshotToTuple(StateSnapshot snap) {
        // Fixed order: [epoch, shard, baseSeq(==upToSeq), flat_states, collectionId, collectionName].
        // collectionId/collectionName are appended LAST (D4) so an older 4-element snapshot still
        // decodes (back-compat) and a newer reader/writer can read identity when present.
        List<Object> tuple = new ArrayList<>(6);
        tuple.add(snap.epoch);
        tuple.add(snap.shard);
        tuple.add(snap.baseSeq);
        // flat list: [id1, state1, id2, state2, ...] — avoids Map key type issues on deserialization
        List<Object> flat = new ArrayList<>(snap.replicaStates.size() * 2);
        for (Map.Entry<Integer, Integer> e : snap.replicaStates.entrySet()) {
            flat.add(e.getKey());
            flat.add(e.getValue());
        }
        tuple.add(flat);
        tuple.add(snap.collectionId);
        tuple.add(snap.collectionName);
        return tuple;
    }

    static StateSnapshot tupleToStateSnapshot(List<?> tuple) {
        int epoch = num(tuple.get(0)).intValue();
        String shard = tuple.get(1).toString();
        long baseSeq = num(tuple.get(2)).longValue();
        List<?> flat = (List<?>) tuple.get(3);
        Map<Integer, Integer> states = new HashMap<>(flat.size() / 2 + 1);
        for (int i = 0; i < flat.size(); i += 2) {
            states.put(num(flat.get(i)).intValue(), num(flat.get(i + 1)).intValue());
        }
        // Back-compat: older snapshots have no identity fields.
        long collectionId = tuple.size() > 4 && tuple.get(4) != null ? num(tuple.get(4)).longValue() : -1L;
        String collectionName = tuple.size() > 5 && tuple.get(5) != null ? tuple.get(5).toString() : null;
        return new StateSnapshot(collectionId, collectionName, epoch, shard, baseSeq, states);
    }

    // ---- Wire I/O ----

    private static byte[] encode(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // marshal writes VERSION header and flushes daos internally
        new JavaBinCodec().marshal(obj, new FastOutputStream(baos));
        return baos.toByteArray();
    }

    public static Object decode(byte[] bytes) throws IOException {
        // unmarshal reads and validates the VERSION header, then returns the root object
        return new JavaBinCodec().unmarshal(bytes);
    }

    private static Number num(Object o) {
        return (Number) o;
    }
}
