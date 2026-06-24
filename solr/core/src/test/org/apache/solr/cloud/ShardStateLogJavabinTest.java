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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ShardStateLog;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StateDeltaCodec;
import org.junit.Test;

/**
 * Javabin round-trip tests for {@link ShardStateLog}.
 */
public class ShardStateLogJavabinTest extends SolrTestCaseJ4 {

    @Test
    public void lastSeqOnEmptyRing() {
        // An empty post-fold ring must still report a correct lastSeq (D2)
        ShardStateLog log = new ShardStateLog("101", "s1", 1, 50L, 50L, "overseer-1", Collections.emptyList());
        assertEquals("lastSeq must equal the provided value on empty ring", 50L, log.lastSeq);
        assertEquals("maxSeq() must return lastSeq on empty ring", 50L, log.maxSeq());
    }

    @Test
    public void maxSeqIsLastSeq() {
        // maxSeq() must always return lastSeq, not derived from entries (D2)
        StateDelta d = new StateDelta("101", "s1", 1, 30L,
                Collections.singletonList(new StateDelta.Entry(1, 2)),
                Collections.emptyList(), 2);
        ShardStateLog log = new ShardStateLog("101", "s1", 1, 0L, 99L, "overseer-1", Collections.singletonList(d));
        assertEquals("maxSeq() must be lastSeq regardless of entry seq values", 99L, log.maxSeq());
    }

    @Test
    public void roundTrip() throws Exception {
        StateDelta d1 = new StateDelta("101", "s1", 1, 1L,
                Collections.singletonList(new StateDelta.Entry(1, 2)),
                Collections.emptyList(), 2);
        StateDelta d2 = new StateDelta("101", "s1", 1, 2L,
                Collections.singletonList(new StateDelta.Entry(2, 1)), // LEADER
                Arrays.asList(1), 2); // demote replica 1
        ShardStateLog log = new ShardStateLog("101", "s1", 1, 0L, 2L, "election-42",
                Arrays.asList(d1, d2));

        byte[] bytes = StateDeltaCodec.encodeShardStateLog(log);
        ShardStateLog decoded = StateDeltaCodec.decodeShardStateLog(bytes);

        assertEquals(log.epoch, decoded.epoch);
        assertEquals(log.baseSeq, decoded.baseSeq);
        assertEquals(log.lastSeq, decoded.lastSeq);
        assertEquals(log.writerId, decoded.writerId);
        assertEquals(2, decoded.entries.size());

        StateDelta dec1 = decoded.entries.get(0);
        assertEquals(1L, dec1.seq);
        assertEquals(1, dec1.entries.get(0).replicaId);
        assertEquals(2, dec1.entries.get(0).shortState);

        StateDelta dec2 = decoded.entries.get(1);
        assertEquals(2L, dec2.seq);
        assertEquals(1, dec2.entries.get(0).shortState); // LEADER preserved as 1
        assertEquals(1, (int) dec2.demotedReplicaIds.get(0));
        assertEquals(2, dec2.demotedShortState); // ACTIVE
    }

    @Test
    public void emptyEntriesRoundTrip() throws Exception {
        ShardStateLog log = new ShardStateLog("101", "s1", 5, 100L, 200L, "writer-7", Collections.emptyList());
        ShardStateLog decoded = StateDeltaCodec.decodeShardStateLog(
                StateDeltaCodec.encodeShardStateLog(log));
        assertEquals(5, decoded.epoch);
        assertEquals(100L, decoded.baseSeq);
        assertEquals(200L, decoded.lastSeq);
        assertEquals("writer-7", decoded.writerId);
        assertTrue(decoded.entries.isEmpty());
        assertEquals(200L, decoded.maxSeq());
    }

    public void testIdentityRoundTrip() throws Exception {
        StateDelta delta = new StateDelta("22", "s2", 3, 9L,
                Collections.singletonList(new StateDelta.Entry(5, StateDelta.ACTIVE)),
                Collections.singletonList(4), 2);
        ShardStateLog original = new ShardStateLog("22", "s2", 3, 8L, 9L, "overseer-9",
                Collections.singletonList(delta));

        ShardStateLog decoded = StateDeltaCodec.decodeShardStateLog(
                StateDeltaCodec.encodeShardStateLog(original));

        assertEquals("22", decoded.collectionId);
        assertEquals("s2", decoded.shardId);
        assertEquals(original.epoch, decoded.epoch);
        assertEquals(original.baseSeq, decoded.baseSeq);
        assertEquals(original.lastSeq, decoded.lastSeq);
        assertEquals(original.writerId, decoded.writerId);
        assertEquals(1, decoded.entries.size());
        assertEquals("22", decoded.entries.get(0).collectionId);
        assertEquals("s2", decoded.entries.get(0).shardId);
    }

}
