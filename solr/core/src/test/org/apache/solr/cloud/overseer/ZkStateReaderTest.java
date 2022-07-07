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
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static TestFixture setupTestFixture(String testPrefix) throws Exception {
    Path zkDir = createTempDir(testPrefix);
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    SolrZkClient zkClient =
        new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
    ZkController.createClusterZkNodes(zkClient);

    ZkStateReader reader = new ZkStateReader(zkClient);
    reader.createClusterStateWatchersAndUpdate();

    ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

    return new TestFixture(server, zkClient, reader, writer);
  }

  public void testExternalCollectionWatchedNotWatched() throws Exception {
    try (TestFixture fixture = setupTestFixture("testExternalCollectionWatchedNotWatched")) {
      ZkStateWriter writer = fixture.writer;
      ZkStateReader reader = fixture.reader;
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // create new collection
      ZkWriteCommand c1 =
          new ZkWriteCommand(
              "c1",
              new DocCollection(
                  "c1",
                  new HashMap<>(),
                  Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                  DocRouter.DEFAULT,
                  0));

      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
      writer.writePendingUpdates();
      reader.forceUpdateCollection("c1");

      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.registerCore("c1");
      assertFalse(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.unregisterCore("c1");
      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
    }
  }

  public void testCollectionStateWatcherCaching() throws Exception {
    try (TestFixture fixture = setupTestFixture("testCollectionStateWatcherCaching")) {
      ZkStateWriter writer = fixture.writer;
      ZkStateReader reader = fixture.reader;

      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

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
      assertTrue(
          fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
      reader.waitForState(
          "c1", 1, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState != null);

      Map<String, Object> props = new HashMap<>();
      props.put("x", "y");
      props.put(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME);
      state = new DocCollection("c1", new HashMap<>(), props, DocRouter.DEFAULT, 0);
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
  }

  public void testWatchedCollectionCreation() throws Exception {
    try (TestFixture fixture = setupTestFixture("testWatchedCollectionCreation")) {
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
          new DocCollection(
              "c1",
              new HashMap<>(),
              Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
              DocRouter.DEFAULT,
              0);
      ZkWriteCommand wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();

      assertTrue(
          fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

      // reader.forceUpdateCollection("c1");
      reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
      ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
      assertNotNull(ref);
      assertFalse(ref.isLazilyLoaded());
    }
  }

  public void testForciblyRefreshAllClusterState() throws Exception {
    try (TestFixture fixture = setupTestFixture("testForciblyRefreshAllClusterState")) {
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

      assertTrue(
          fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

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
      assertTrue("c2 should have been lazily loaded but is not!", ref.isLazilyLoaded()); // c2 should be lazily loaded as it's not watched
      assertEquals(0, ref.get().getZNodeVersion());
    }
  }

  public void testGetCurrentCollections() throws Exception {
    try (TestFixture fixture = setupTestFixture("testGetCurrentCollections")) {
      ZkStateWriter writer = fixture.writer;
      ZkStateReader reader = fixture.reader;

      reader.registerCore("c1"); // listen to c1. not yet exist
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      reader.forceUpdateCollection("c1");
      Set<String> currentCollections = reader.getCurrentCollections();
      assertEquals(0, currentCollections.size()); // no active collections yet

      // now create both c1 (watched) and c2 (not watched)
      DocCollection state1 =
          new DocCollection(
              "c1",
              new HashMap<>(),
              Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
              DocRouter.DEFAULT,
              0);
      ZkWriteCommand wc1 = new ZkWriteCommand("c1", state1);
      DocCollection state2 =
          new DocCollection(
              "c2",
              new HashMap<>(),
              Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
              DocRouter.DEFAULT,
              0);

      // do not listen to c2
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
      ZkWriteCommand wc2 = new ZkWriteCommand("c2", state2);

      writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
      writer.writePendingUpdates();

      reader.forceUpdateCollection("c1");
      reader.forceUpdateCollection("c2");
      currentCollections =
          reader.getCurrentCollections(); // should detect both collections (c1 watched, c2 lazy
      // loaded)
      assertEquals(2, currentCollections.size());
    }
  }

  public void testWatchRaceCondition() throws Exception {
    final int RUN_COUNT = 10000;
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zkStateReaderTest"));

    try (TestFixture fixture = setupTestFixture("testWatchRaceCondition")) {
      ZkStateWriter writer = fixture.writer;
      final ZkStateReader reader = fixture.reader;
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // start another thread to constantly updating the state
      final AtomicBoolean stopMutatingThread = new AtomicBoolean(false);

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
                    new DocCollection(
                        "c1",
                        new HashMap<>(),
                        Map.of(
                            ZkStateReader.CONFIGNAME_PROP,
                            ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                        DocRouter.DEFAULT,
                        currentVersion);
                ZkWriteCommand wc = new ZkWriteCommand("c1", state);
                writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
                clusterState = writer.writePendingUpdates();
              }
            } catch (Exception e) {
              updateException.set(e);
            }
            return null;
          });
      executorService.shutdown();

      for (int i = 0; i < RUN_COUNT; i++) {
        final CountDownLatch latch = new CountDownLatch(2);

        // remove itself on 2nd trigger
        DocCollectionWatcher dummyWatcher =
            collection -> {
              latch.countDown();
              return latch.getCount() == 0;
            };
        reader.registerDocCollectionWatcher("c1", dummyWatcher);
        assertTrue(
            "Missing expected collection updates after the wait",
            latch.await(10, TimeUnit.SECONDS));
        reader.removeDocCollectionWatcher("c1", dummyWatcher);

        TimeOut timeOut = new TimeOut(1000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
        timeOut.waitFor(
            "The ref is not lazily loaded after waiting",
            () -> reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      }

      stopMutatingThread.set(true);
      if (updateException.get() != null) {
        throw (updateException.get());
      }

    } finally {
      ExecutorUtil.awaitTermination(executorService);
    }
  }
}
