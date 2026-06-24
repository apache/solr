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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StatePlanePaths;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZkStateReaderQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * READER dispatch tests, ZK-backed via {@link ZkTestServer}. Drives the real
 * {@link ZkStateReaderQueue} through a real {@link StatePlaneWriter}, asserting:
 *
 * <ul>
 *   <li>delta plane present ({@code state/manifest} seeded) → snapshot + deltas applied.
 *   <li>plane NOT seeded (no manifest) → reader no-ops, leaving live state unchanged.
 * </ul>
 *
 * <p>Short states (raw): LEADER=1, ACTIVE=2, RECOVERING=4, DOWN=5.
 */
public class StatePlaneReaderQueueTest extends SolrTestCaseJ4 {
  private static final String COLL_ID = "101";

  private static final int LEADER = 1;
  private static final int ACTIVE = 2;

  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader reader;
  private ZkStateReaderQueue queue;

  /** Always-elected fence with a fixed writerId; never fenced out (mirrors StatePlaneMigrationTest). */
  static class AlwaysElectedFence implements StatePlaneWriter.ElectionFence {
    final String id;
    AlwaysElectedFence(String id) { this.id = id; }
    public boolean stillElected() { return true; }
    public String writerId() { return id; }
    public boolean isFencedBy(String ringWriterId) { return false; }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Path zkDir = SolrTestUtil.createTempDir("zkData-sprq");
    server = new ZkTestServer(zkDir);
    server.run(true);
    zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.start();
    reader = new ZkStateReader(zkClient);
    queue = new ZkStateReaderQueue(reader);
  }

  @After
  public void tearDown() throws Exception {
    if (reader != null) reader.close();
    if (zkClient != null) zkClient.close();
    if (server != null) server.shutdown();
    super.tearDown();
  }

  // ---- builders ----

  private static DocCollection collection(String name, String shard, int[] ids, Replica.State[] states) {
    Map<String, Replica> replicaMap = new LinkedHashMap<>();
    for (int i = 0; i < ids.length; i++) {
      Map<String, Object> props = new HashMap<>();
      props.put(ZkStateReader.NODE_NAME_PROP, "127.0.0.1:898" + i + "_solr");
      props.put(ZkStateReader.STATE_PROP, states[i].toString());
      props.put(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name());
      props.put("id", ids[i]);
      String replicaName = shard + "_replica_n" + ids[i];
      Object2ObjectMap<String, Object> pm = new Object2ObjectLinkedOpenHashMap<>(props);
      replicaMap.put(replicaName, new Replica(replicaName, pm, name, -1, shard, null));
    }
    Slice slice = new Slice(shard, replicaMap, null, name, -1);
    Map<String, Slice> slices = new HashMap<>();
    slices.put(shard, slice);
    Map<String, Object> collProps = new HashMap<>();
    collProps.put("id", Integer.valueOf(COLL_ID));
    return new DocCollection(name, slices, collProps, CompositeIdRouter.DEFAULT);
  }

  private StatePlaneWriter writer(String id) {
    return new StatePlaneWriter(zkClient, new AlwaysElectedFence(id));
  }

  private DocCollection apply(DocCollection dc) throws Exception {
    return queue.getAndProcessStateUpdates(dc).get(10, TimeUnit.SECONDS);
  }

  private DocCollection apply(DocCollection dc, Set<String> targetShards) throws Exception {
    return queue.getAndProcessStateUpdates(dc, targetShards).get(10, TimeUnit.SECONDS);
  }

  /** One ACTIVE-registration slice with the given internal ids (live state arrives via the plane). */
  private static Slice buildSlice(String collName, String shard, int[] ids) {
    Map<String, Replica> replicaMap = new LinkedHashMap<>();
    for (int i = 0; i < ids.length; i++) {
      Map<String, Object> props = new HashMap<>();
      props.put(ZkStateReader.NODE_NAME_PROP, "127.0.0.1:898" + i + "_solr");
      props.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      props.put(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name());
      props.put("id", ids[i]);
      String replicaName = shard + "_replica_n" + ids[i];
      Object2ObjectMap<String, Object> pm = new Object2ObjectLinkedOpenHashMap<>(props);
      replicaMap.put(replicaName, new Replica(replicaName, pm, collName, -1, shard, null));
    }
    return new Slice(shard, replicaMap, null, collName, -1);
  }

  private static DocCollection twoShardCollection(String name, String shardA, int[] idsA, String shardB, int[] idsB) {
    Map<String, Slice> slices = new HashMap<>();
    slices.put(shardA, buildSlice(name, shardA, idsA));
    slices.put(shardB, buildSlice(name, shardB, idsB));
    Map<String, Object> collProps = new HashMap<>();
    collProps.put("id", Integer.valueOf(COLL_ID));
    return new DocCollection(name, slices, collProps, CompositeIdRouter.DEFAULT);
  }

  // ---- delta plane seeded -> snapshot + deltas applied ----

  @Test
  public void testDeltaApplyWhenManifestPresent() throws Exception {
    final String coll = "deltac";
    final int epoch = 3;

    StatePlaneWriter w = writer("e1");
    Map<Integer, Integer> seed = new HashMap<>();
    seed.put(201, ACTIVE);
    seed.put(202, ACTIVE);
    w.seedShard(coll, "s1", COLL_ID, epoch, seed);
    w.writeManifestSeeded(coll, epoch, Arrays.asList("s1"));
    // A live transition AFTER seeding: 202 becomes the leader.
    w.publish(coll, "s1", COLL_ID, Collections.singletonList(new StateDelta.Entry(202, LEADER)),
        Collections.emptyList());

    DocCollection dc = collection(coll, "s1", new int[] {201, 202},
        new Replica.State[] {Replica.State.ACTIVE, Replica.State.ACTIVE});

    DocCollection out = apply(dc);

    assertEquals("202 promoted to raw LEADER via the delta plane",
        Replica.State.LEADER, out.getReplicaById(202).getRawState());
    assertEquals("201 remains ACTIVE", Replica.State.ACTIVE, out.getReplicaById(201).getRawState());
    assertEquals(Integer.valueOf(202), out.getSlice("s1").getLeader().getInternalId());
    assertNotNull("delta cursors were created", out.getStatePlaneCursorsOrNull());
  }

  // ---- plane NOT seeded (no manifest) -> reader no-ops, leaving live state unchanged ----

  @Test
  public void testNoopWhenManifestAbsent() throws Exception {
    final String coll = "unseededc";

    // No state plane manifest for this collection. The reader switches onto live state only via the
    // delta-plane manifest, so with no manifest present apply() must be a no-op.
    assertFalse(zkClient.exists(
        StatePlanePaths.manifest(StatePlanePaths.collectionPath(coll))));

    DocCollection dc = collection(coll, "s1", new int[] {301},
        new Replica.State[] {Replica.State.DOWN});
    assertEquals(Replica.State.DOWN, dc.getReplicaById(301).getRawState());

    DocCollection out = apply(dc);

    assertEquals("manifest absent -> reader no-op; live state is unchanged",
        Replica.State.DOWN, out.getReplicaById(301).getRawState());
    assertNull("no manifest -> no delta cursors", out.getStatePlaneCursorsOrNull());
  }

  // ---- finding #2: a targeted-shard apply folds ONLY that shard, then a full apply catches up ----

  @Test
  public void testTargetedShardApplyFoldsOnlyThatShardThenFullCatchup() throws Exception {
    final String coll = "targetedc";
    final int epoch = 3;

    StatePlaneWriter w = writer("e1");
    Map<Integer, Integer> seedS1 = new HashMap<>();
    seedS1.put(201, ACTIVE);
    seedS1.put(202, ACTIVE);
    Map<Integer, Integer> seedS2 = new HashMap<>();
    seedS2.put(301, ACTIVE);
    seedS2.put(302, ACTIVE);
    w.seedShard(coll, "s1", COLL_ID, epoch, seedS1);
    w.seedShard(coll, "s2", COLL_ID, epoch, seedS2);
    w.writeManifestSeeded(coll, epoch, Arrays.asList("s1", "s2"));
    // A live LEADER transition on EACH shard.
    w.publish(coll, "s1", COLL_ID, Collections.singletonList(new StateDelta.Entry(202, LEADER)), Collections.emptyList());
    w.publish(coll, "s2", COLL_ID, Collections.singletonList(new StateDelta.Entry(302, LEADER)), Collections.emptyList());

    DocCollection dc = twoShardCollection(coll, "s1", new int[] {201, 202}, "s2", new int[] {301, 302});

    // Targeted apply of ONLY s1: s1's ring is folded, s2's ring is never read.
    DocCollection out = apply(dc, Collections.singleton("s1"));
    assertEquals("s1 leader transition applied (targeted shard folded)",
        Replica.State.LEADER, out.getReplicaById(202).getRawState());
    assertEquals("s2 NOT folded in a targeted s1 apply -> still registration ACTIVE",
        Replica.State.ACTIVE, out.getReplicaById(302).getRawState());

    // A subsequent full apply (targetShards == null) catches up s2.
    DocCollection out2 = apply(out, null);
    assertEquals("s1 leader stays (idempotent)", Replica.State.LEADER, out2.getReplicaById(202).getRawState());
    assertEquals("full apply now folds s2's leader transition",
        Replica.State.LEADER, out2.getReplicaById(302).getRawState());
  }

  @Test
  public void testUnknownTargetShardIsNoop() throws Exception {
    final String coll = "unknownshardc";
    final int epoch = 2;

    StatePlaneWriter w = writer("e1");
    Map<Integer, Integer> seed = new HashMap<>();
    seed.put(401, ACTIVE);
    seed.put(402, ACTIVE);
    w.seedShard(coll, "s1", COLL_ID, epoch, seed);
    w.writeManifestSeeded(coll, epoch, Arrays.asList("s1"));
    w.publish(coll, "s1", COLL_ID, Collections.singletonList(new StateDelta.Entry(402, LEADER)), Collections.emptyList());

    DocCollection dc = collection(coll, "s1", new int[] {401, 402},
        new Replica.State[] {Replica.State.ACTIVE, Replica.State.ACTIVE});

    // Target a shard that does not exist in the collection: no slice matches, nothing is folded.
    DocCollection out = apply(dc, Collections.singleton("sX"));
    assertEquals("unknown target shard -> s1 ring not read; leader transition not applied",
        Replica.State.ACTIVE, out.getReplicaById(402).getRawState());
  }
}
