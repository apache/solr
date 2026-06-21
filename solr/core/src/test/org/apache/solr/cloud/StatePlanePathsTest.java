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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.StatePlanePaths;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

/**
 * Tests for {@link StatePlanePaths} — pure string path builders.
 */
public class StatePlanePathsTest extends SolrTestCaseJ4 {

    private static final String COLL = "myCollection";
    private static final String COLL_PATH = ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLL;

    @Test
    public void collectionPath() {
        assertEquals(COLL_PATH, StatePlanePaths.collectionPath(COLL));
    }

    @Test
    public void statePlaneRoot() {
        assertEquals(COLL_PATH + "/state", StatePlanePaths.statePlaneRoot(COLL_PATH));
    }

    @Test
    public void manifest() {
        assertEquals(COLL_PATH + "/state/manifest", StatePlanePaths.manifest(COLL_PATH));
    }

    @Test
    public void shardsContainer() {
        assertEquals(COLL_PATH + "/state/shards", StatePlanePaths.shardsContainer(COLL_PATH));
    }

    @Test
    public void shardDeltas() {
        assertEquals(COLL_PATH + "/state/shards/shard1/deltas",
                StatePlanePaths.shardDeltas(COLL_PATH, "shard1"));
    }

    @Test
    public void shardSnapshot() {
        assertEquals(COLL_PATH + "/state/shards/shard1/snapshot",
                StatePlanePaths.shardSnapshot(COLL_PATH, "shard1"));
    }

    @Test
    public void differentShardsProduceDifferentPaths() {
        String d1 = StatePlanePaths.shardDeltas(COLL_PATH, "shard1");
        String d2 = StatePlanePaths.shardDeltas(COLL_PATH, "shard2");
        assertFalse("different shards must produce different paths", d1.equals(d2));
    }

    @Test
    public void pathsDoNotContainDoubleSlash() {
        String[] paths = {
            StatePlanePaths.statePlaneRoot(COLL_PATH),
            StatePlanePaths.manifest(COLL_PATH),
            StatePlanePaths.shardDeltas(COLL_PATH, "shard1"),
            StatePlanePaths.shardSnapshot(COLL_PATH, "shard1"),
        };
        for (String path : paths) {
            assertFalse("path must not contain double slash: " + path, path.contains("//"));
        }
    }

    @Test
    public void collectionNameWithHyphen() {
        String path = StatePlanePaths.shardDeltas(
                StatePlanePaths.collectionPath("my-coll-v2"), "shard3");
        assertTrue(path.contains("my-coll-v2"));
        assertTrue(path.contains("shard3"));
        assertTrue(path.endsWith("/deltas"));
    }
}
