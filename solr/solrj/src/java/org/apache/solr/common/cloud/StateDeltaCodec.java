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

/** Fixed-order javabin codec for state-plane records. */
public final class StateDeltaCodec {
    private StateDeltaCodec() {}

    public static byte[] encodeStateDelta(StateDelta delta) throws IOException {
        return encode(deltaToTuple(delta));
    }

    public static StateDelta decodeStateDelta(byte[] bytes) throws IOException {
        return tupleToStateDelta((List<?>) decode(bytes));
    }

    public static byte[] encodeShardStateLog(ShardStateLog log) throws IOException {
        return encode(logToTuple(log));
    }

    public static ShardStateLog decodeShardStateLog(byte[] bytes) throws IOException {
        return tupleToShardStateLog((List<?>) decode(bytes));
    }

    public static byte[] encodeStateSnapshot(StateSnapshot snapshot) throws IOException {
        return encode(snapshotToTuple(snapshot));
    }

    public static StateSnapshot decodeStateSnapshot(byte[] bytes) throws IOException {
        return tupleToStateSnapshot((List<?>) decode(bytes));
    }

    // StateDelta: [collectionId, shardId, epoch, seq, entries_list, demotedReplicaIds, demotedShortState]
    static List<Object> deltaToTuple(StateDelta delta) {
        List<Object> tuple = new ArrayList<>(7);
        tuple.add(delta.collectionId);
        tuple.add(delta.shardId);
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
        String collectionId = tuple.get(0) == null ? null : tuple.get(0).toString();
        String shardId = tuple.get(1) == null ? null : tuple.get(1).toString();
        int epoch = num(tuple.get(2)).intValue();
        long seq = num(tuple.get(3)).longValue();
        List<StateDelta.Entry> entries = new ArrayList<>();
        for (Object o : (List<?>) tuple.get(4)) {
            List<?> pair = (List<?>) o;
            entries.add(new StateDelta.Entry(num(pair.get(0)).intValue(), num(pair.get(1)).intValue()));
        }
        List<Integer> demotedIds = new ArrayList<>();
        for (Object o : (List<?>) tuple.get(5)) {
            demotedIds.add(num(o).intValue());
        }
        int demotedShortState = num(tuple.get(6)).intValue();
        return new StateDelta(collectionId, shardId, epoch, seq, entries, demotedIds, demotedShortState);
    }

    // ShardStateLog: [collectionId, shardId, epoch, baseSeq, lastSeq, writerId, delta_tuples_list]
    static List<Object> logToTuple(ShardStateLog log) {
        List<Object> tuple = new ArrayList<>(7);
        tuple.add(log.collectionId);
        tuple.add(log.shardId);
        tuple.add(log.epoch);
        tuple.add(log.baseSeq);
        tuple.add(log.lastSeq);
        tuple.add(log.writerId);
        List<Object> entriesOut = new ArrayList<>(log.entries.size());
        for (StateDelta delta : log.entries) {
            entriesOut.add(deltaToTuple(delta));
        }
        tuple.add(entriesOut);
        return tuple;
    }

    static ShardStateLog tupleToShardStateLog(List<?> tuple) {
        String collectionId = tuple.get(0) == null ? null : tuple.get(0).toString();
        String shardId = tuple.get(1) == null ? null : tuple.get(1).toString();
        int epoch = num(tuple.get(2)).intValue();
        long baseSeq = num(tuple.get(3)).longValue();
        long lastSeq = num(tuple.get(4)).longValue();
        String writerId = tuple.get(5) == null ? null : tuple.get(5).toString();
        List<StateDelta> entries = new ArrayList<>();
        for (Object o : (List<?>) tuple.get(6)) {
            entries.add(tupleToStateDelta((List<?>) o));
        }
        return new ShardStateLog(collectionId, shardId, epoch, baseSeq, lastSeq, writerId, entries);
    }

    // StateSnapshot: [collectionId, collectionName, epoch, shard, baseSeq, flat_replica_state_pairs]
    static List<Object> snapshotToTuple(StateSnapshot snap) {
        List<Object> tuple = new ArrayList<>(6);
        tuple.add(snap.collectionId);
        tuple.add(snap.collectionName);
        tuple.add(snap.epoch);
        tuple.add(snap.shard);
        tuple.add(snap.baseSeq);
        List<Object> flat = new ArrayList<>(snap.replicaStates.size() * 2);
        for (Map.Entry<Integer, Integer> e : snap.replicaStates.entrySet()) {
            flat.add(e.getKey());
            flat.add(e.getValue());
        }
        tuple.add(flat);
        return tuple;
    }

    static StateSnapshot tupleToStateSnapshot(List<?> tuple) {
        long collectionId = num(tuple.get(0)).longValue();
        String collectionName = tuple.get(1) == null ? null : tuple.get(1).toString();
        int epoch = num(tuple.get(2)).intValue();
        String shard = tuple.get(3) == null ? null : tuple.get(3).toString();
        long baseSeq = num(tuple.get(4)).longValue();
        Map<Integer, Integer> states = new HashMap<>();
        List<?> flat = (List<?>) tuple.get(5);
        for (int i = 0; i < flat.size(); i += 2) {
            states.put(num(flat.get(i)).intValue(), num(flat.get(i + 1)).intValue());
        }
        return new StateSnapshot(collectionId, collectionName, epoch, shard, baseSeq, states);
    }

    private static byte[] encode(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new JavaBinCodec().marshal(obj, new FastOutputStream(baos));
        return baos.toByteArray();
    }

    public static Object decode(byte[] bytes) throws IOException {
        return new JavaBinCodec().unmarshal(bytes);
    }

    private static Number num(Object o) {
        return (Number) o;
    }
}
