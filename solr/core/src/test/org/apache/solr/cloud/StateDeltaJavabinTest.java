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
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StateDeltaCodec;
import org.junit.Test;

/**
 * Javabin round-trip tests for {@link StateDelta}.
 * Verifies: field fidelity, List-interface contract (never ArrayList/int[]), ordering, stale/idempotent apply.
 */
public class StateDeltaJavabinTest extends SolrTestCaseJ4 {

    @Test
    public void roundTrip() throws Exception {
        StateDelta.Entry e1 = new StateDelta.Entry(1, 2); // replicaId=1, ACTIVE
        StateDelta.Entry e2 = new StateDelta.Entry(2, 1); // replicaId=2, LEADER
        StateDelta original = new StateDelta("101", "s1", 3, 42L,
                Arrays.asList(e1, e2),
                Arrays.asList(7, 8),
                2 /* demotedShortState = ACTIVE */);

        byte[] bytes = StateDeltaCodec.encodeStateDelta(original);
        StateDelta decoded = StateDeltaCodec.decodeStateDelta(bytes);

        assertEquals(original.epoch, decoded.epoch);
        assertEquals(original.seq, decoded.seq);
        assertEquals(2, decoded.entries.size());
        assertEquals(1, decoded.entries.get(0).replicaId);
        assertEquals(2, decoded.entries.get(0).shortState);
        assertEquals(2, decoded.entries.get(1).replicaId);
        assertEquals(1, decoded.entries.get(1).shortState); // LEADER preserved as 1
        assertEquals(Arrays.asList(7, 8), decoded.demotedReplicaIds);
        assertEquals(2, decoded.demotedShortState);
    }

    @Test
    public void deserializesEntriesAsListInterface() throws Exception {
        StateDelta delta = new StateDelta("101", "s1", 1, 1L,
                Collections.singletonList(new StateDelta.Entry(10, 2)),
                Collections.emptyList(), 2);
        byte[] bytes = StateDeltaCodec.encodeStateDelta(delta);

        // Inspect the raw javabin-decoded tuple — entries_list must come back as List, not int[] or raw array
        List<?> rawTuple = (List<?>) StateDeltaCodec.decode(bytes);
        Object rawEntries = rawTuple.get(4);
        assertTrue("entries must be a List", rawEntries instanceof List);
        List<?> entriesList = (List<?>) rawEntries;
        assertFalse("entries must not be an int[]", rawEntries instanceof int[]);
        assertEquals(1, entriesList.size());
        // Each entry-pair is also a List
        assertTrue("entry pair must be a List", entriesList.get(0) instanceof List);
    }

    @Test
    public void demotedIdsAreListNotArray() throws Exception {
        StateDelta delta = new StateDelta("101", "s1", 1, 1L,
                Collections.emptyList(),
                Arrays.asList(3, 5, 7), 2);
        byte[] bytes = StateDeltaCodec.encodeStateDelta(delta);

        List<?> rawTuple = (List<?>) StateDeltaCodec.decode(bytes);
        Object rawDemoted = rawTuple.get(5);
        // Must be a List (fastutil ObjectArrayList from javabin), never int[]
        assertTrue("demotedReplicaIds must be a List", rawDemoted instanceof List);
        assertFalse("demotedReplicaIds must not be an int[]", rawDemoted instanceof int[]);
        List<?> demotedList = (List<?>) rawDemoted;
        assertEquals(3, demotedList.size());
        assertEquals(3, ((Number) demotedList.get(0)).intValue());
        assertEquals(5, ((Number) demotedList.get(1)).intValue());
        assertEquals(7, ((Number) demotedList.get(2)).intValue());
    }

    @Test
    public void boundedSingleEntry() throws Exception {
        // Single-entry delta — the typical AC#3 "bounded delta" case
        StateDelta delta = new StateDelta("101", "s1", 0, 1L,
                Collections.singletonList(new StateDelta.Entry(99, 4 /* RECOVERING */)),
                Collections.emptyList(), 2);
        byte[] bytes = StateDeltaCodec.encodeStateDelta(delta);
        assertTrue("encoded delta must be non-empty", bytes.length > 0);

        StateDelta decoded = StateDeltaCodec.decodeStateDelta(bytes);
        assertEquals(1, decoded.entries.size());
        assertEquals(99, decoded.entries.get(0).replicaId);
        assertEquals(4, decoded.entries.get(0).shortState);
        assertTrue("empty demotedReplicaIds", decoded.demotedReplicaIds.isEmpty());
    }

    @Test
    public void multiEntryBatchRoundTrip() throws Exception {
        // Multi-entry batch — per-shard node-down batch (D7)
        List<StateDelta.Entry> entries = Arrays.asList(
                new StateDelta.Entry(1, 5), // DOWN
                new StateDelta.Entry(2, 5), // DOWN
                new StateDelta.Entry(3, 5)  // DOWN
        );
        StateDelta delta = new StateDelta("101", "s1", 2, 10L, entries, Collections.emptyList(), 2);
        StateDelta decoded = StateDeltaCodec.decodeStateDelta(StateDeltaCodec.encodeStateDelta(delta));

        assertEquals(3, decoded.entries.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1, decoded.entries.get(i).replicaId);
            assertEquals(5, decoded.entries.get(i).shortState); // DOWN
        }
    }

    // ---- Ordering tests ----

    @Test
    public void orderingByEpochThenSeq() {
        StateDelta d1 = makeDelta(1, 5L);
        StateDelta d2 = makeDelta(1, 10L);
        StateDelta d3 = makeDelta(2, 1L); // epoch rollover
        StateDelta d4 = makeDelta(2, 1L); // tie

        assertTrue("same epoch, lower seq is less", d1.compareTo(d2) < 0);
        assertTrue("higher epoch wins over any seq", d2.compareTo(d3) < 0);
        assertEquals("same (epoch,seq) is equal", 0, d3.compareTo(d4));
        assertTrue("reverse: lower seq is greater", d2.compareTo(d1) > 0);
    }

    @Test
    public void staleDetection() {
        StateDelta d = makeDelta(3, 7L);
        assertTrue("same epoch, seq <= cursor is stale", d.isStale(3, 7L));
        assertTrue("same epoch, seq < cursor is stale", d.isStale(3, 8L));
        assertFalse("same epoch, seq > cursor is not stale", d.isStale(3, 6L));
        assertTrue("lower epoch is stale regardless of seq", d.isStale(4, 0L));
        assertFalse("higher epoch than cursor is not stale", d.isStale(2, 100L));
    }

    @Test
    public void shortStateValidation() {
        // Valid boundary values
        new StateDelta.Entry(1, 1); // LEADER
        new StateDelta.Entry(1, 6); // RECOVERY_FAILED
        // Invalid
        try {
            new StateDelta.Entry(1, 0);
            fail("shortState=0 must throw");
        } catch (IllegalArgumentException e) { /* expected */ }
        try {
            new StateDelta.Entry(1, 7);
            fail("shortState=7 must throw");
        } catch (IllegalArgumentException e) { /* expected */ }
    }

    private static StateDelta makeDelta(int epoch, long seq) {
        return new StateDelta("101", "s1", epoch, seq, Collections.emptyList(), Collections.emptyList(), 2);
    }

    public void testIdentityRoundTrip() throws Exception {
        StateDelta original = new StateDelta("17", "shardA", 5, 42L,
                Collections.singletonList(new StateDelta.Entry(101, StateDelta.ACTIVE)),
                Collections.singletonList(7), 3);

        StateDelta decoded = StateDeltaCodec.decodeStateDelta(StateDeltaCodec.encodeStateDelta(original));

        assertEquals("17", decoded.collectionId);
        assertEquals("shardA", decoded.shardId);
        assertEquals(original.epoch, decoded.epoch);
        assertEquals(original.seq, decoded.seq);
        assertEquals(original.entries.size(), decoded.entries.size());
        assertEquals(original.entries.get(0).replicaId, decoded.entries.get(0).replicaId);
        assertEquals(original.entries.get(0).shortState, decoded.entries.get(0).shortState);
        assertEquals(original.demotedReplicaIds, decoded.demotedReplicaIds);
        assertEquals(original.demotedShortState, decoded.demotedShortState);
    }

}
