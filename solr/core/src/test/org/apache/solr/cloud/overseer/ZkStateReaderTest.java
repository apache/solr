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

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.util.TimeOut;

public class ZkStateReaderTest extends SolrTestCaseJ4 {

  private static final long TIMEOUT = 30;

  public void testExternalCollectionWatchedNotWatched() throws Exception {
    Path zkDir = createTempDir("testExternalCollectionWatchedNotWatched");
    ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

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

    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testCollectionStateWatcherCaching() throws Exception {
    Path zkDir = createTempDir("testCollectionStateWatcherCaching");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());
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
      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
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
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testWatchedCollectionCreation() throws Exception {
    Path zkDir = createTempDir("testWatchedCollectionCreation");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      reader.registerCore("c1");

      // Initially there should be no c1 collection.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      reader.forceUpdateCollection("c1");

      // Still no c1 collection, despite a collection path.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

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

      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

      // reader.forceUpdateCollection("c1");
      reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
      ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
      assertNotNull(ref);
      assertFalse(ref.isLazilyLoaded());
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testForciblyRefreshAllClusterState() throws Exception {
    Path zkDir = createTempDir("testForciblyRefreshAllClusterState");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      reader.registerCore("c1");
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      reader.forciblyRefreshAllClusterStateSlow();
      // Initially there should be no c1 collection.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

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

      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

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

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
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
      assertFalse(ref.isLazilyLoaded());
      assertEquals(0, ref.get().getZNodeVersion());
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testGetCurrentCollections() throws Exception {
    Path zkDir = createTempDir("testGetCurrentCollections");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      reader.registerCore("c1"); // listen to c1. not yet exist
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
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
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
      ZkWriteCommand wc2 = new ZkWriteCommand("c2", state2);

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());
      writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
      writer.writePendingUpdates();

      reader.forceUpdateCollection("c1");
      reader.forceUpdateCollection("c2");
      currentCollections =
          reader.getCurrentCollections(); // should detect both collections (c1 watched, c2 lazy
      // loaded)
      assertEquals(2, currentCollections.size());
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testWatchRaceCondition() throws Exception {
    final int RUN_COUNT = 1000;
    Path zkDir = createTempDir("testConcurrencyWatch");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zkStateReaderTest"));

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      final ZkStateReader readerRef = reader;
      reader.createClusterStateWatchersAndUpdate();
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // start another thread to constantly updating the state
      final AtomicBoolean stopMutatingThread = new AtomicBoolean(false);
      final ZkStateWriter writer = new ZkStateWriter(reader, new Stats());
      final AtomicInteger updateCounts = new AtomicInteger(0);
      final AtomicReference<Exception> updateException = new AtomicReference<>();
      executorService.submit(
          () -> {
            try {
              ClusterState clusterState = readerRef.getClusterState();
              while (!stopMutatingThread.get()) {
                DocCollection collection = clusterState.getCollectionOrNull("c1");
                int currentVersion = collection != null ? collection.getZNodeVersion() : 0;
                System.out.println("current version " + currentVersion);
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

      DocCollectionWatcher dummyWatcher = collection -> false; // do not remove itself
      for (int i = 0; i < RUN_COUNT; i++) {
        reader.registerDocCollectionWatcher("c1", dummyWatcher);
        TimeUnit.MILLISECONDS.sleep(10);
        reader.removeDocCollectionWatcher("c1", dummyWatcher);
        assert (reader
            .getClusterState()
            .getCollectionRef("c1")
            .isLazilyLoaded()); // it should always be lazy loaded, as the collection is not watched
        // anymore
      }

      stopMutatingThread.set(true);
      if (updateException.get() != null) {
        throw (updateException.get());
      }

    } finally {
      IOUtils.close(reader, zkClient);
      executorService.awaitTermination(10, TimeUnit.SECONDS);
      server.shutdown();
    }
  }
}
