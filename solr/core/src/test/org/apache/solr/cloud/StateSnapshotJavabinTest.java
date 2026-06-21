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
package org.apache.solr.cloud;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StateDeltaCodec;
import org.apache.solr.common.cloud.StateSnapshot;
import org.junit.Test;

/**
 * Javabin round-trip and reconstruction tests for {@link StateSnapshot} (per-shard, D1).
 * Also covers LEADER raw-state preservation and out-of-order delta reconstruction.
 */
public class StateSnapshotJavabinTest extends SolrTestCaseJ4 {

    @Test
    public void roundTrip() throws Exception {
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2); // ACTIVE
        states.put(2, 1); // LEADER (raw shortState=1)
        states.put(3, 4); // RECOVERING
        StateSnapshot snap = new StateSnapshot(7, "shard1", 42L, states);

        byte[] bytes = StateDeltaCodec.encodeStateSnapshot(snap);
        StateSnapshot decoded = StateDeltaCodec.decodeStateSnapshot(bytes);

        assertEquals(7, decoded.epoch);
        assertEquals("shard1", decoded.shard);
        assertEquals(42L, decoded.baseSeq);
        assertEquals(3, decoded.replicaStates.size());
        assertEquals(2, (int) decoded.replicaStates.get(1)); // ACTIVE
        assertEquals(1, (int) decoded.replicaStates.get(2)); // LEADER preserved as 1
        assertEquals(4, (int) decoded.replicaStates.get(3)); // RECOVERING
    }

    @Test
    public void leaderRawStatePreservedThroughEncodeDecodeReconstruct() throws Exception {
        // Snapshot: replica 2 is LEADER (shortState=1)
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2); // ACTIVE
        states.put(2, 1); // LEADER
        StateSnapshot snap = new StateSnapshot(1, "shard1", 0L, states);

        byte[] bytes = StateDeltaCodec.encodeStateSnapshot(snap);
        StateSnapshot decoded = StateDeltaCodec.decodeStateSnapshot(bytes);

        // Verify LEADER=1 survived encode/decode
        assertEquals(1, (int) decoded.replicaStates.get(2));

        // Now reconstruct: replica 3 promoted to LEADER, replica 2 demoted to ACTIVE
        StateDelta promote = new StateDelta(1, 1L,
                Collections.singletonList(new StateDelta.Entry(3, 1 /* LEADER */)),
                Arrays.asList(2), // demote replica 2
                2 /* ACTIVE, D14 */);

        Map<Integer, Integer> result = decoded.reconstruct(Collections.singletonList(promote));
        assertEquals("demoted replica 2 must be ACTIVE(2)", 2, (int) result.get(2));
        assertEquals("new leader replica 3 must be LEADER(1)", 1, (int) result.get(3));
        assertEquals("unaffected replica 1 stays ACTIVE(2)", 2, (int) result.get(1));
    }

    @Test
    public void reconstructOutOfOrderDeltas() throws Exception {
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2); // ACTIVE
        StateSnapshot snap = new StateSnapshot(1, "shard1", 0L, states);

        // Deltas delivered out of order
        StateDelta d2 = new StateDelta(1, 2L,
                Collections.singletonList(new StateDelta.Entry(1, 4 /* RECOVERING */)),
                Collections.emptyList(), 2);
        StateDelta d1 = new StateDelta(1, 1L,
                Collections.singletonList(new StateDelta.Entry(2, 5 /* DOWN */)),
                Collections.emptyList(), 2);

        // reconstruct must sort by (epoch, seq) before applying
        Map<Integer, Integer> result = snap.reconstruct(Arrays.asList(d2, d1));
        assertEquals("replica 1 applied in order → RECOVERING", 4, (int) result.get(1));
        assertEquals("replica 2 applied in order → DOWN", 5, (int) result.get(2));
    }

    @Test
    public void reconstructIsIdempotent() throws Exception {
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2); // ACTIVE
        StateSnapshot snap = new StateSnapshot(1, "shard1", 0L, states);

        StateDelta d = new StateDelta(1, 1L,
                Collections.singletonList(new StateDelta.Entry(1, 4 /* RECOVERING */)),
                Collections.emptyList(), 2);

        // Applying same delta twice must yield same result as once
        Map<Integer, Integer> result = snap.reconstruct(Arrays.asList(d, d));
        assertEquals("idempotent: RECOVERING applied once", 4, (int) result.get(1));
    }

    @Test
    public void reconstructSkipsStaleDeltas() throws Exception {
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 5); // DOWN (captured in snapshot at seq=10)
        StateSnapshot snap = new StateSnapshot(1, "shard1", 10L, states);

        // Delta at seq=8 is before the snapshot baseline — must be ignored
        StateDelta stale = new StateDelta(1, 8L,
                Collections.singletonList(new StateDelta.Entry(1, 2 /* ACTIVE */)),
                Collections.emptyList(), 2);
        // Delta at seq=10 equals baseSeq — also stale (already captured)
        StateDelta atBase = new StateDelta(1, 10L,
                Collections.singletonList(new StateDelta.Entry(1, 2 /* ACTIVE */)),
                Collections.emptyList(), 2);
        // Delta at seq=11 is fresh
        StateDelta fresh = new StateDelta(1, 11L,
                Collections.singletonList(new StateDelta.Entry(1, 4 /* RECOVERING */)),
                Collections.emptyList(), 2);

        Map<Integer, Integer> result = snap.reconstruct(Arrays.asList(stale, atBase, fresh));
        assertEquals("only fresh delta applied → RECOVERING", 4, (int) result.get(1));
    }

    @Test
    public void perShardFieldPreserved() throws Exception {
        // Verifies per-shard identity (D1): shard name is part of the encoded snapshot
        StateSnapshot snap = new StateSnapshot(0, "shard2", 0L, Collections.emptyMap());
        StateSnapshot decoded = StateDeltaCodec.decodeStateSnapshot(
                StateDeltaCodec.encodeStateSnapshot(snap));
        assertEquals("shard2", decoded.shard);
    }

    @Test
    public void identityFieldsRoundTrip() throws Exception {
        // D4: collectionId + collectionName + explicit upToSeq survive encode/decode.
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2); // ACTIVE
        states.put(2, 1); // LEADER preserved
        StateSnapshot snap = new StateSnapshot(123L, "coll_xyz", 4, "shard9", 99L, states);
        assertEquals("upToSeq() mirrors baseSeq", 99L, snap.upToSeq());

        StateSnapshot decoded = StateDeltaCodec.decodeStateSnapshot(
                StateDeltaCodec.encodeStateSnapshot(snap));
        assertEquals(123L, decoded.collectionId);
        assertEquals("coll_xyz", decoded.collectionName);
        assertEquals(4, decoded.epoch);
        assertEquals("shard9", decoded.shard);
        assertEquals(99L, decoded.baseSeq);
        assertEquals(99L, decoded.upToSeq());
        assertEquals(1, (int) decoded.replicaStates.get(2)); // LEADER(1) preserved through new codec
        assertEquals(2, (int) decoded.replicaStates.get(1));
    }

    @Test
    public void legacyConstructorDefaultsIdentity() throws Exception {
        // The 4-arg constructor stays valid and round-trips with default identity (-1 / null).
        StateSnapshot snap = new StateSnapshot(1, "shard1", 0L, Collections.emptyMap());
        assertEquals(-1L, snap.collectionId);
        assertNull(snap.collectionName);
        StateSnapshot decoded = StateDeltaCodec.decodeStateSnapshot(
                StateDeltaCodec.encodeStateSnapshot(snap));
        assertEquals(-1L, decoded.collectionId);
        assertNull(decoded.collectionName);
    }

    @Test
    public void flatStatesListIsListInterface() throws Exception {
        // Verify the raw decoded flat states list comes back as a List (fastutil), never int[]
        Map<Integer, Integer> states = new HashMap<>();
        states.put(1, 2);
        StateSnapshot snap = new StateSnapshot(1, "shard1", 0L, states);
        byte[] bytes = StateDeltaCodec.encodeStateSnapshot(snap);

        List<?> rawTuple = (List<?>) StateDeltaCodec.decode(bytes);
        Object rawFlat = rawTuple.get(3);
        assertTrue("flat states must be a List", rawFlat instanceof List);
        assertFalse("flat states must not be int[]", rawFlat instanceof int[]);
    }
}
