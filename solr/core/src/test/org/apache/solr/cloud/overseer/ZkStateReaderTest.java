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
package org.apache.solr.cloud.overseer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel(
    "org.apache.solr.common.cloud.ZkStateReader=DEBUG;org.apache.solr.common.cloud.PerReplicaStatesOps=DEBUG")
public class ZkStateReaderTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long TIMEOUT = 30;

  private static class TestFixture implements Closeable {
    private final ZkTestServer server;
    private final SolrZkClient zkClient;
    private final ZkStateReader reader;
    private final ZkStateWriter writer;

    private TestFixture(
        ZkTestServer server, SolrZkClient zkClient, ZkStateReader reader, ZkStateWriter writer) {
      this.server = server;
      this.zkClient = zkClient;
      this.reader = reader;
      this.writer = writer;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(reader, zkClient);
      try {
        server.shutdown();
      } catch (InterruptedException e) {
        // ok. Shutting down anyway
      }
    }
  }

  private TestFixture fixture = null;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    fixture = setupTestFixture(getTestName(), -1);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (fixture != null) {
      fixture.close();
    }
    super.tearDown();
  }

  private static TestFixture setupTestFixture(String testPrefix, int minStateByteLenForCompression)
      throws Exception {
    Path zkDir = createTempDir(testPrefix);
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(OverseerTest.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
    ZkController.createClusterZkNodes(zkClient);

    ZkStateReader reader = new ZkStateReader(zkClient);
    reader.createClusterStateWatchersAndUpdate();

    ZkStateWriter writer =
        new ZkStateWriter(reader, new Stats(), minStateByteLenForCompression, new ZLibCompressor());

    return new TestFixture(server, zkClient, reader, writer);
  }

  public void testExternalCollectionWatchedNotWatched() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    // create new collection
    ZkWriteCommand c1 =
        new ZkWriteCommand(
            "c1",
            DocCollection.create(
                "c1",
                new HashMap<>(),
                Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                DocRouter.DEFAULT,
                0,
                PerReplicaStatesOps.getZkClientPrsSupplier(
                    fixture.zkClient, DocCollection.getCollectionPath("c1"))));

    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
    writer.writePendingUpdates();
    reader.forceUpdateCollection("c1");

    assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
    reader.registerCore("c1");
    assertFalse(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
    reader.unregisterCore("c1");
    assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
  }

  public void testCollectionStateWatcherCaching() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    DocCollection state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();
    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
    reader.waitForState(
        "c1", 1, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState != null);

    Map<String, Object> props = new HashMap<>();
    props.put("x", "y");
    props.put(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME);
    state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            props,
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    boolean found = false;
    TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      DocCollection c1 = reader.getClusterState().getCollection("c1");
      if ("y".equals(c1.getStr("x"))) {
        found = true;
        break;
      }
    }
    assertTrue("Could not find updated property in collection c1 even after 5 seconds", found);
  }

  public void testWatchedCollectionCreation() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1");

    // Initially there should be no c1 collection.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
    reader.forceUpdateCollection("c1");

    // Still no c1 collection, despite a collection path.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    // create new collection
    DocCollection state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

    // reader.forceUpdateCollection("c1");
    reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
  }

  /**
   * Verifies that znode and child versions are correct and version changes trigger cluster state
   * updates
   */
  public void testNodeVersion() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    ClusterState clusterState = reader.getClusterState();
    // create new collection
    DocCollection state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(
                ZkStateReader.CONFIGNAME_PROP,
                ConfigSetsHandler.DEFAULT_CONFIGSET_NAME,
                DocCollection.CollectionStateProps.PER_REPLICA_STATE,
                "true"),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();

    // have to register it here after the updates, otherwise the child node watch will not be
    // inserted
    reader.registerCore("c1");

    TimeOut timeOut = new TimeOut(5000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "Timeout on waiting for c1 to show up in cluster state",
        () -> reader.getClusterState().getCollectionOrNull("c1") != null);

    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());
    // no more dummy node
    assertEquals(0, ref.get().getChildNodesVersion());

    DocCollection collection = ref.get();
    PerReplicaStates prs =
        PerReplicaStatesOps.fetch(
            collection.getZNode(), fixture.zkClient, collection.getPerReplicaStates());
    PerReplicaStatesOps.addReplica("r1", Replica.State.DOWN, false, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1",
        () -> {
          DocCollection c = reader.getCollection("c1");
          return c.getPerReplicaStates() != null
              && c.getPerReplicaStates().get("r1") != null
              && c.getPerReplicaStates().get("r1").state == Replica.State.DOWN;
        });

    ref = reader.getClusterState().getCollectionRef("c1");
    assertEquals(0, ref.get().getZNodeVersion()); // no change in Znode version
    assertEquals(1, ref.get().getChildNodesVersion()); // but child version should be 1 now

    prs = ref.get().getPerReplicaStates();
    PerReplicaStatesOps.flipState("r1", Replica.State.ACTIVE, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1 marked as DOWN",
        () ->
            reader.getCollection("c1").getPerReplicaStates().get("r1").state
                == Replica.State.ACTIVE);

    ref = reader.getClusterState().getCollectionRef("c1");
    assertEquals(0, ref.get().getZNodeVersion()); // no change in Znode version
    // but child version should be 3 now (1 del + 1 add)
    assertEquals(3, ref.get().getChildNodesVersion());

    // now delete the collection
    wc = new ZkWriteCommand("c1", null);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();
    timeOut.waitFor(
        "Timeout on waiting for c1 to be removed from cluster state",
        () -> reader.getClusterState().getCollectionOrNull("c1") == null);

    reader.unregisterCore("c1");
    // re-add the same collection
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();
    // re-register, otherwise the child watch would be missing from collection deletion
    reader.registerCore("c1");

    // reader.forceUpdateCollection("c1");
    timeOut.waitFor(
        "Timeout on waiting for c1 to show up in cluster state again",
        () -> reader.getClusterState().getCollectionOrNull("c1") != null);
    ref = reader.getClusterState().getCollectionRef("c1");
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());
    assertEquals(0, ref.get().getChildNodesVersion()); // child node version is reset

    // re-add PRS
    collection = ref.get();
    prs =
        PerReplicaStatesOps.fetch(
            collection.getZNode(), fixture.zkClient, collection.getPerReplicaStates());
    PerReplicaStatesOps.addReplica("r1", Replica.State.DOWN, false, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1",
        () -> {
          DocCollection c = reader.getCollection("c1");
          return c.getPerReplicaStates() != null
              && c.getPerReplicaStates().get("r1") != null
              && c.getPerReplicaStates().get("r1").state == Replica.State.DOWN;
        });

    ref = reader.getClusterState().getCollectionRef("c1");

    // child version should be reset since the state.json node was deleted and re-created
    assertEquals(1, ref.get().getChildNodesVersion());
  }

  public void testForciblyRefreshAllClusterState() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1"); // watching c1, so it should get non lazy reference
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    reader.forciblyRefreshAllClusterStateSlow();
    // Initially there should be no c1 collection.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    // create new collection
    DocCollection state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

    reader.forciblyRefreshAllClusterStateSlow();
    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());

    // update the collection
    state =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            ref.get().getZNodeVersion(),
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(1, ref.get().getZNodeVersion());

    // delete the collection c1, add a collection c2 that is NOT watched
    ZkWriteCommand wc1 = new ZkWriteCommand("c1", null);

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
    state =
        DocCollection.create(
            "c2",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c2")));
    ZkWriteCommand wc2 = new ZkWriteCommand("c2", state);

    writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNull(ref);

    ref = reader.getClusterState().getCollectionRef("c2");
    assertNotNull(ref);
    assertTrue(
        "c2 should have been lazily loaded but is not!",
        ref.isLazilyLoaded()); // c2 should be lazily loaded as it's not watched
    assertEquals(0, ref.get().getZNodeVersion());
  }

  public void testForciblyRefreshAllClusterStateCompressed() throws Exception {
    fixture.close();
    fixture = setupTestFixture(getTestName(), 0);
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1"); // watching c1, so it should get non lazy reference
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    reader.forciblyRefreshAllClusterStateSlow();
    // Initially there should be no c1 collection.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    // create new collection
    DocCollection state =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0);
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

    reader.forciblyRefreshAllClusterStateSlow();
    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());

    // update the collection
    state =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            ref.get().getZNodeVersion());
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(1, ref.get().getZNodeVersion());

    // delete the collection c1, add a collection c2 that is NOT watched
    ZkWriteCommand wc1 = new ZkWriteCommand("c1", null);

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
    state =
        new DocCollection(
            "c2",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0);
    ZkWriteCommand wc2 = new ZkWriteCommand("c2", state);

    writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNull(ref);

    ref = reader.getClusterState().getCollectionRef("c2");
    assertNotNull(ref);
    assertTrue(
        "c2 should have been lazily loaded but is not!",
        ref.isLazilyLoaded()); // c2 should be lazily loaded as it's not watched
    assertEquals(0, ref.get().getZNodeVersion());
  }

  public void testGetCurrentCollections() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1"); // listen to c1. not yet exist
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
    reader.forceUpdateCollection("c1");
    Set<String> currentCollections = reader.getCurrentCollections();
    assertEquals(0, currentCollections.size()); // no active collections yet

    // now create both c1 (watched) and c2 (not watched)
    DocCollection state1 =
        DocCollection.create(
            "c1",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));
    ZkWriteCommand wc1 = new ZkWriteCommand("c1", state1);
    DocCollection state2 =
        DocCollection.create(
            "c2",
            new HashMap<>(),
            Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath("c1")));

    // do not listen to c2
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
    ZkWriteCommand wc2 = new ZkWriteCommand("c2", state2);

    writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
    writer.writePendingUpdates();

    reader.forceUpdateCollection("c1");
    reader.forceUpdateCollection("c2");

    // should detect both collections (c1 watched, c2 lazy loaded)
    currentCollections = reader.getCurrentCollections();
    assertEquals(2, currentCollections.size());
  }

  /**
   * Simulates race condition that might arise when state updates triggered by watch notification
   * contend with removal of collection watches.
   *
   * <p>Such race condition should no longer exist with the new code that uses a single map for both
   * "collection watches" and "latest state of watched collection"
   */
  public void testWatchRaceCondition() throws Exception {
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zkStateReaderTest"));
    CommonTestInjection.setDelay(1000);
    final AtomicBoolean stopMutatingThread = new AtomicBoolean(false);
    try {
      ZkStateWriter writer = fixture.writer;
      final ZkStateReader reader = fixture.reader;
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // start another thread to constantly updating the state
      final AtomicReference<Exception> updateException = new AtomicReference<>();
      executorService.submit(
          () -> {
            try {
              ClusterState clusterState = reader.getClusterState();
              while (!stopMutatingThread.get()) {
                DocCollection collection = clusterState.getCollectionOrNull("c1");
                int currentVersion = collection != null ? collection.getZNodeVersion() : 0;
                // create new collection
                DocCollection state =
                    DocCollection.create(
                        "c1",
                        new HashMap<>(),
                        Map.of(
                            ZkStateReader.CONFIGNAME_PROP,
                            ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                        DocRouter.DEFAULT,
                        currentVersion,
                        PerReplicaStatesOps.getZkClientPrsSupplier(
                            fixture.zkClient, DocCollection.getCollectionPath("c1")));
                ZkWriteCommand wc = new ZkWriteCommand("c1", state);
                writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
                clusterState = writer.writePendingUpdates();
                TimeUnit.MILLISECONDS.sleep(100);
              }
            } catch (Exception e) {
              updateException.set(e);
            }
            return null;
          });
      executorService.shutdown();

      reader.waitForState(
          "c1",
          10,
          TimeUnit.SECONDS,
          slices -> slices != null); // wait for the state to become available

      final CountDownLatch latch = new CountDownLatch(2);

      // remove itself on 2nd trigger
      DocCollectionWatcher dummyWatcher =
          collection -> {
            latch.countDown();
            return latch.getCount() == 0;
          };
      reader.registerDocCollectionWatcher("c1", dummyWatcher);
      assertTrue(
          "Missing expected collection updates after the wait", latch.await(10, TimeUnit.SECONDS));
      reader.removeDocCollectionWatcher("c1", dummyWatcher);

      // cluster state might not be updated right the way from the removeDocCollectionWatcher call
      // above as org.apache.solr.common.cloud.ZkStateReader.Notification might remove the watcher
      // as well and might still be in the middle of updating the cluster state.
      TimeOut timeOut = new TimeOut(2000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      timeOut.waitFor(
          "The ref is not lazily loaded after waiting",
          () -> reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());

      if (updateException.get() != null) {
        throw (updateException.get());
      }
    } finally {
      stopMutatingThread.set(true);
      CommonTestInjection.reset();
      ExecutorUtil.awaitTermination(executorService);
    }
  }

  /**
   * Ensure that collection state fetching (getCollectionLive etc.) would not throw exception when
   * the state.json is deleted in between the state.json read and PRS entries read
   */
  public void testDeletePrsCollection() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    String collectionName = "c1";
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    ClusterState clusterState = reader.getClusterState();

    String nodeName = "node1:10000_solr";
    String sliceName = "shard1";
    Slice slice = new Slice(sliceName, Map.of(), Map.of(), collectionName);

    // create new collection
    DocCollection state =
        DocCollection.create(
            collectionName,
            Map.of(sliceName, slice),
            Collections.singletonMap(DocCollection.CollectionStateProps.PER_REPLICA_STATE, true),
            DocRouter.DEFAULT,
            0,
            PerReplicaStatesOps.getZkClientPrsSupplier(
                fixture.zkClient, DocCollection.getCollectionPath(collectionName)));
    ZkWriteCommand wc = new ZkWriteCommand(collectionName, state);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();

    TimeOut timeOut = new TimeOut(5000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "Timeout on waiting for c1 to show up in cluster state",
        () -> reader.getClusterState().getCollectionOrNull(collectionName) != null);

    String collectionPath = ZkStateReader.getCollectionPath(collectionName);

    // now create the replica, take note that this has to be done after DocCollection creation with
    // empty slice, otherwise the DocCollection ctor would fetch the PRS entries and throw
    // exceptions
    String replicaBaseUrl = Utils.getBaseUrlForNodeName(nodeName, "http");

    String replicaName = "replica1";
    Replica replica =
        new Replica(
            replicaName,
            Map.of(
                ZkStateReader.CORE_NAME_PROP,
                "core1",
                ZkStateReader.STATE_PROP,
                Replica.State.ACTIVE.toString(),
                ZkStateReader.NODE_NAME_PROP,
                nodeName,
                ZkStateReader.BASE_URL_PROP,
                replicaBaseUrl,
                ZkStateReader.REPLICA_TYPE,
                Replica.Type.NRT.name()),
            collectionName,
            sliceName);

    wc =
        new ZkWriteCommand(
            collectionName, SliceMutator.updateReplica(state, slice, replica.getName(), replica));
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();

    timeOut.waitFor(
        "Timeout on waiting for replica to show up in cluster state",
        () ->
            reader.getCollectionLive(collectionName).getSlice(sliceName).getReplica(replicaName)
                != null);

    try (CommonTestInjection.BreakpointSetter breakpointSetter =
        new CommonTestInjection.BreakpointSetter()) {
      // set breakpoint such that after state.json fetch and before PRS entry fetch, we can delete
      // the state.json and PRS entries to trigger the race condition
      breakpointSetter.setImplementation(
          PerReplicaStatesOps.class.getName() + "/beforePrsFetch",
          (args) -> {
            try {
              // this is invoked after ZkStateReader.fetchCollectionState has fetched the state.json
              // but before PRS entries.
              // call delete state.json on ZK directly, very tricky to control execution order with
              // writer.enqueueUpdate
              reader.getZkClient().clean(collectionPath);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } catch (KeeperException e) {
              throw new RuntimeException(e);
            }
          });

      // set breakpoint to verify the expected PrsZkNodeNotFoundException is indeed thrown within
      // the execution flow, such exception is caught within the logic and not thrown to the
      // caller
      AtomicBoolean prsZkNodeNotFoundExceptionThrown = new AtomicBoolean(false);
      breakpointSetter.setImplementation(
          ZkStateReader.class.getName() + "/exercised",
          (args) -> {
            if (args[0] instanceof PerReplicaStatesOps.PrsZkNodeNotFoundException) {
              prsZkNodeNotFoundExceptionThrown.set(true);
            }
          });

      timeOut.waitFor(
          "Timeout waiting for collection state to become null",
          () -> {
            // this should not throw exception even if the PRS entry read is delayed artificially
            // (by previous command) and deleted after the following getCollectionLive call
            return reader.getCollectionLive(collectionName) == null;
          });

      assertTrue(prsZkNodeNotFoundExceptionThrown.get());
    }
  }
}
