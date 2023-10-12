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

import static org.apache.solr.cloud.SolrCloudTestCase.configureCluster;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkSolrClientTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @SuppressWarnings({"try"})
  static class ZkConnection implements AutoCloseable {

    private final ZkTestServer server;
    private final SolrZkClient zkClient;

    ZkConnection() throws Exception {
      Path zkDir = createTempDir("zkData");
      server = new ZkTestServer(zkDir);
      server.run();

      zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
              .build();
    }

    public ZkTestServer getServer() {
      return server;
    }

    public SolrZkClient getClient() {
      return zkClient;
    }

    @Override
    public void close() throws IOException, InterruptedException {
      if (zkClient != null) zkClient.close();
      if (server != null) server.shutdown();
    }
  }

  @SuppressWarnings({"try"})
  public void testConnect() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      // do nothing
    }
  }

  @SuppressWarnings({"try"})
  public void testMakeRootNode() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(conn.getServer().getZkHost())
              .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        assertTrue(zkClient.exists("/solr"));
      }
    }
  }

  @SuppressWarnings({"try"})
  public void testClean() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      final SolrZkClient zkClient = conn.getClient();

      zkClient.makePath("/test/path/here");

      zkClient.makePath("/zz/path/here");

      zkClient.clean("/");

      assertFalse(zkClient.exists("/test"));
      assertFalse(zkClient.exists("/zz"));
    }
  }

  public void testReconnect() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server;
    server = new ZkTestServer(zkDir);
    server.run();
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath, false);

      int zkServerPort = server.getPort();
      // this tests disconnect state
      server.shutdown();

      Thread.sleep(80);

      Thread thread =
          new Thread() {
            @Override
            public void run() {
              try {
                zkClient.makePath("collections/collection2");
                // Assert.fail("Server should be down here");
              } catch (KeeperException | InterruptedException e) {

              }
            }
          };

      thread.start();

      // bring server back up
      server = new ZkTestServer(zkDir, zkServerPort);
      server.run(false);

      // TODO: can we do better?
      // wait for reconnect
      Thread.sleep(600);

      Thread thread2 =
          new Thread() {
            @Override
            public void run() {
              try {

                zkClient.makePath("collections/collection3");

              } catch (KeeperException e) {
                throw new RuntimeException(e);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          };

      thread2.start();

      thread.join();

      thread2.join();

      assertNotNull(zkClient.exists("/collections/collection3", null));
      assertNotNull(zkClient.exists("/collections/collection1", null));

      // simulate session expiration

      // one option
      long sessionId = zkClient.getZkSessionId();
      server.expire(sessionId);

      // another option
      // zkClient.getSolrZooKeeper().getConnection().disconnect();

      // this tests expired state

      Thread.sleep(1000); // pause for reconnect

      for (int i = 0; i < 8; i++) {
        try {
          zkClient.makePath("collections/collection4");
          break;
        } catch (KeeperException.SessionExpiredException
            | KeeperException.ConnectionLossException e) {

        }
        Thread.sleep(1000 * i);
      }

      assertNotNull(
          "Node does not exist, but it should", zkClient.exists("/collections/collection4", null));

    } finally {

      if (server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  @SuppressWarnings({"try"})
  public void testMultipleWatchesAsync() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      final SolrZkClient zkClient = conn.getClient();
      zkClient.makePath("/collections");

      final int numColls = random().nextInt(100);
      final CountDownLatch latch = new CountDownLatch(numColls);
      final CountDownLatch watchesDone = new CountDownLatch(numColls);
      final Set<String> collectionsInProgress = new HashSet<>(numColls);
      AtomicInteger maxCollectionsInProgress = new AtomicInteger();

      for (int i = 1; i <= numColls; i++) {
        String collPath = "/collections/collection" + i;
        zkClient.makePath(collPath);
        zkClient.getChildren(
            collPath,
            new Watcher() {
              @Override
              public void process(WatchedEvent event) {
                synchronized (collectionsInProgress) {
                  collectionsInProgress.add(
                      event.getPath()); // Will be something like /collections/collection##
                  maxCollectionsInProgress.set(
                      Math.max(maxCollectionsInProgress.get(), collectionsInProgress.size()));
                }
                latch.countDown();
                try {
                  latch.await(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
                synchronized (collectionsInProgress) {
                  collectionsInProgress.remove(event.getPath());
                }
                watchesDone.countDown();
              }
            });
      }

      for (int i = 1; i <= numColls; i++) {
        String shardsPath = "/collections/collection" + i + "/shards";
        zkClient.makePath(shardsPath);
      }

      assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
      assertEquals(
          "All collections should have been processed in parallel",
          numColls,
          maxCollectionsInProgress.get());

      // just as sanity check for the test:
      assertTrue(watchesDone.await(10000, TimeUnit.MILLISECONDS));
      synchronized (collectionsInProgress) {
        assertEquals(0, collectionsInProgress.size());
      }
    }
  }

  @SuppressWarnings({"try"})
  public void testWatchChildren() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      final SolrZkClient zkClient = conn.getClient();
      final AtomicInteger cnt = new AtomicInteger();
      final CountDownLatch latch = new CountDownLatch(1);

      zkClient.makePath("/collections");

      zkClient.getChildren(
          "/collections",
          new Watcher() {

            @Override
            public void process(WatchedEvent event) {
              cnt.incrementAndGet();
              // remake watch
              try {
                zkClient.getChildren("/collections", this);
                latch.countDown();
              } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          });

      zkClient.makePath("/collections/collection99/shards");
      latch.await(); // wait until watch has been re-created

      zkClient.makePath("collections/collection99/config=collection1");

      zkClient.makePath("collections/collection99/config=collection3");

      zkClient.makePath("/collections/collection97/shards");

      // pause for the watches to fire
      Thread.sleep(700);

      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }

      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }

      assertEquals(2, cnt.intValue());
    }
  }

  @SuppressWarnings({"try"})
  public void testSkipPathPartsOnMakePath() throws Exception {
    try (ZkConnection conn = new ZkConnection()) {
      final SolrZkClient zkClient = conn.getClient();

      zkClient.makePath("/test", true);

      // should work
      zkClient.makePath("/test/path/here", null, CreateMode.PERSISTENT, null, true, 1);

      zkClient.clean("/");

      // should not work
      KeeperException e =
          expectThrows(
              KeeperException.NoNodeException.class,
              "We should not be able to create this path",
              () ->
                  zkClient.makePath("/test/path/here", null, CreateMode.PERSISTENT, null, true, 1));

      zkClient.clean("/");

      expectThrows(
          KeeperException.NoNodeException.class,
          "We should not be able to create this path",
          () ->
              ZkMaintenanceUtils.ensureExists(
                  "/collection/collection/leader",
                  (byte[]) null,
                  CreateMode.PERSISTENT,
                  zkClient,
                  2));

      zkClient.makePath("/collection", true);

      expectThrows(
          KeeperException.NoNodeException.class,
          "We should not be able to create this path",
          () ->
              ZkMaintenanceUtils.ensureExists(
                  "/collections/collection/leader",
                  (byte[]) null,
                  CreateMode.PERSISTENT,
                  zkClient,
                  2));
      zkClient.makePath("/collection/collection", true);

      byte[] bytes = new byte[10];
      ZkMaintenanceUtils.ensureExists(
          "/collection/collection", bytes, CreateMode.PERSISTENT, zkClient, 2);

      byte[] returnedBytes = zkClient.getData("/collection/collection", null, null);

      assertNull("We skipped 2 path parts, so data won't be written", returnedBytes);

      zkClient.makePath("/collection/collection/leader", true);

      ZkMaintenanceUtils.ensureExists(
          "/collection/collection/leader", null, CreateMode.PERSISTENT, zkClient, 2);
    }
  }

  public void testZkBehavior() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4).withJettyConfig(jetty -> jetty.enableV2(true)).configure();
    try {
      SolrZkClient zkClient = cluster.getZkClient();
      zkClient.create("/test-node", null, CreateMode.PERSISTENT);

      Stat stat = zkClient.exists("/test-node", null);
      int cversion = stat.getCversion();
      zkClient.multi(
          op -> op.create().withMode(CreateMode.PERSISTENT).forPath("/test-node/abc", null),
          op -> op.delete().withVersion(-1).forPath("/test-node/abc"));
      stat = zkClient.exists("/test-node", null);
      assertTrue(stat.getCversion() >= cversion + 2);
    } finally {
      cluster.shutdown();
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
