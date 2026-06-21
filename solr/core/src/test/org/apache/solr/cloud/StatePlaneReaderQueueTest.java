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

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * READER dispatch tests, ZK-backed via {@link ZkTestServer}. Drives the real
 * {@link ZkStateReaderQueue} through a real {@link StatePlaneWriter}, asserting:
 *
 * <ul>
 *   <li>delta plane present ({@code state/manifest} seeded) → snapshot + deltas applied.
 *   <li>plane NOT seeded (no manifest) → reader no-ops, leaving state unchanged (it never reads a
 *       {@code _statupdates} full map for live state).
 * </ul>
 *
 * <p>Short states (raw): LEADER=1, ACTIVE=2, RECOVERING=4, DOWN=5.
 */
public class StatePlaneReaderQueueTest extends SolrTestCaseJ4 {

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
    collProps.put("id", -1L);
    return new DocCollection(name, slices, collProps, CompositeIdRouter.DEFAULT);
  }

  private StatePlaneWriter writer(String id) {
    return new StatePlaneWriter(zkClient, new AlwaysElectedFence(id));
  }

  /** Writes a legacy collection-wide {@code _statupdates} full-map node exactly like the old ZkStateWriter. */
  private void writeLegacyStateUpdates(String coll, Map<Integer, Integer> updates) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(updates, new FastOutputStream(baos));
    }
    String path = ZkStateReader.getCollectionStateUpdatesPath(coll);
    zkClient.makePath(path, baos.toByteArray(), CreateMode.PERSISTENT, true);
  }

  private DocCollection apply(DocCollection dc) throws Exception {
    return queue.getAndProcessStateUpdates(dc).get(10, TimeUnit.SECONDS);
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
    w.seedShard(coll, "s1", epoch, seed);
    w.writeManifestSeeded(coll, epoch, Arrays.asList("s1"));
    // A live transition AFTER seeding: 202 becomes the leader.
    w.publish(coll, "s1", Collections.singletonList(new StateDelta.Entry(202, LEADER)),
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

  // ---- plane NOT seeded (no manifest) -> reader no-ops; a stray _statupdates node is ignored ----

  @Test
  public void testNoopWhenManifestAbsent() throws Exception {
    final String coll = "fallbackc";

    // No state plane manifest for this collection. A stray legacy _statupdates node must NOT be
    // applied to live state — the reader switches onto state only via the delta-plane manifest.
    assertFalse(zkClient.exists(
        StatePlanePaths.manifest(StatePlanePaths.collectionPath(coll))));
    Map<Integer, Integer> legacy = new HashMap<>();
    legacy.put(301, ACTIVE);
    writeLegacyStateUpdates(coll, legacy);

    DocCollection dc = collection(coll, "s1", new int[] {301},
        new Replica.State[] {Replica.State.DOWN});
    assertEquals(Replica.State.DOWN, dc.getReplicaById(301).getRawState());

    DocCollection out = apply(dc);

    assertEquals("manifest absent -> reader no-op; the stray _statupdates node is never applied",
        Replica.State.DOWN, out.getReplicaById(301).getRawState());
    assertNull("no manifest -> no delta cursors", out.getStatePlaneCursorsOrNull());
  }
}
