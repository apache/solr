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

/**
 * Pure ZK path builders for the state delta plane. No ZK I/O.
 * ZK layout (all PERSISTENT, zero SEQUENTIAL):
 * <pre>
 * /collections/&lt;name&gt;/
 *   state/
 *     manifest       — { epoch, seeded, shards:[hint-only, D18] }
 *     shards/&lt;shard&gt;/
 *       deltas       — ShardStateLog (high-frequency CAS target, per-shard data-watch)
 *       snapshot     — StateSnapshot (per-shard, D1)
 * </pre>
 */
public final class StatePlanePaths {

    private StatePlanePaths() {}

    /** {@code /collections/&lt;name&gt;} */
    public static String collectionPath(String collectionName) {
        return ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName;
    }

    /** {@code &lt;collectionPath&gt;/state} — plane root. */
    public static String statePlaneRoot(String collectionPath) {
        return collectionPath + "/state";
    }

    /**
     * {@code &lt;collectionPath&gt;/state/manifest}.
     * {@code manifest.shards} is a discovery hint only; authoritative shard set comes from state.json (D18).
     */
    public static String manifest(String collectionPath) {
        return statePlaneRoot(collectionPath) + "/manifest";
    }

    /** {@code &lt;collectionPath&gt;/state/shards} */
    public static String shardsContainer(String collectionPath) {
        return statePlaneRoot(collectionPath) + "/shards";
    }

    /**
     * {@code &lt;collectionPath&gt;/state/shards/&lt;shard&gt;/deltas} — per-shard delta ring.
     * High-frequency CAS target. Readers set a per-shard data-watch here.
     */
    public static String shardDeltas(String collectionPath, String shard) {
        return shardsContainer(collectionPath) + "/" + shard + "/deltas";
    }

    /**
     * {@code &lt;collectionPath&gt;/state/shards/&lt;shard&gt;/snapshot} — per-shard snapshot (D1).
     */
    public static String shardSnapshot(String collectionPath, String shard) {
        return shardsContainer(collectionPath) + "/" + shard + "/snapshot";
    }
}
