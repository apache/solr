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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class ZkDistributedLockTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "lockColl";
  private final String SHARD_NAME = "lockShard";
  private final String REPLICA_NAME = "lockReplica";

  private static final String CONFIG_SET_NAME = "lockConfigSet";

  static final int TIMEOUT = 10000;

  /**
   * Tests the obtaining of a single read or write lock at a specific hierarchical level, for
   * distributed Collection API locking as well as distributed Config Set API locking.<br>
   * Tests the logic with a single thread, then tests multithreaded wait for lock acquire works.
   * Tests grouped to pay setup only once.
   *
   * <p>Higher level locking tests can be found at {@link
   * org.apache.solr.cloud.api.collections.CollectionApiLockingTest} and {@link
   * org.apache.solr.cloud.ConfigSetApiLockingTest}
   */
  @Test
  public void testSingleLocks() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        DistributedCollectionLockFactory collLockFactory =
            new ZkDistributedCollectionLockFactory(zkClient, "/lockTestCollectionRoot");

        monothreadedCollectionTests(collLockFactory);
        multithreadedCollectionTests(collLockFactory);

        DistributedConfigSetLockFactory configSetLockFactory =
            new ZkDistributedConfigSetLockFactory(zkClient, "/lockTestConfigSetRoot");

        monothreadedConfigSetTests(configSetLockFactory);
        multithreadedConfigSetTests(configSetLockFactory);
      }
    } finally {
      server.shutdown();
    }
  }

  /**
   * Lock directories must be ZooKeeper container nodes so they get removed once empty, and a lock
   * must still be obtainable after its directory was removed.
   */
  @Test
  public void testLockDirsAreContainers() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkHost())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        String collRoot = "/lockTestCollectionRoot";
        DistributedCollectionLockFactory collLockFactory =
            new ZkDistributedCollectionLockFactory(zkClient, collRoot);
        DistributedLock replicaLock =
            collLockFactory.createLock(
                true,
                CollectionParams.LockLevel.REPLICA,
                COLLECTION_NAME,
                SHARD_NAME,
                REPLICA_NAME,
                null);
        replicaLock.release();
        DistributedLock shardLock =
            collLockFactory.createLock(
                true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
        shardLock.release();
        DistributedLock collLock =
            collLockFactory.createLock(
                true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
        collLock.release();
        assertLockDirsAreContainers(server, replicaLock, collRoot);
        assertLockDirsAreContainers(server, shardLock, collRoot);
        assertLockDirsAreContainers(server, collLock, collRoot);

        String configSetRoot = "/lockTestConfigSetRoot";
        DistributedConfigSetLockFactory configSetLockFactory =
            new ZkDistributedConfigSetLockFactory(zkClient, configSetRoot);
        DistributedLock configSetLock = configSetLockFactory.createLock(true, CONFIG_SET_NAME);
        configSetLock.release();
        assertLockDirsAreContainers(server, configSetLock, configSetRoot);

        // Simulate ZooKeeper removing the empty containers
        zkClient.clean(parent(parent(collLock.getLockId())));
        DistributedLock lock =
            collLockFactory.createLock(
                true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
        assertTrue(lock.isAcquired());
        lock.release();
      }
    } finally {
      server.shutdown();
    }
  }

  /** Lock dirs left as persistent nodes by older versions get removed, live locks are kept. */
  @Test
  public void testDeleteEmptyLockDirs() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        String root = "/lockTestCollectionRoot";
        ZkDistributedCollectionLockFactory factory =
            new ZkDistributedCollectionLockFactory(zkClient, root);
        String collLockDir =
            lockDir(
                factory.createLock(
                    true,
                    CollectionParams.LockLevel.COLLECTION,
                    COLLECTION_NAME,
                    null,
                    null,
                    null));
        String replicaLockDir =
            lockDir(
                factory.createLock(
                    true,
                    CollectionParams.LockLevel.REPLICA,
                    COLLECTION_NAME,
                    SHARD_NAME,
                    REPLICA_NAME,
                    null));
        String shardLockDir =
            lockDir(
                factory.createLock(
                    true,
                    CollectionParams.LockLevel.SHARD,
                    COLLECTION_NAME,
                    SHARD_NAME,
                    null,
                    null));
        String otherCollLockDir =
            lockDir(
                factory.createLock(
                    true, CollectionParams.LockLevel.COLLECTION, "otherColl", null, null, null));
        String coll = parent(collLockDir);
        zkClient.clean(root);
        for (String dir : List.of(collLockDir, shardLockDir, replicaLockDir, otherCollLockDir)) {
          zkClient.makePath(dir, false);
        }

        DistributedLock shardLock =
            factory.createLock(
                true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);

        factory.deleteEmptyLockDirs(COLLECTION_NAME);
        assertFalse(zkClient.exists(collLockDir));
        assertFalse(zkClient.exists(replicaLockDir));
        assertTrue(zkClient.exists(shardLock.getLockId()));
        assertTrue(shardLock.isAcquired());

        shardLock.release();
        factory.deleteEmptyLockDirs(COLLECTION_NAME);
        assertFalse(zkClient.exists(coll));
        assertTrue(zkClient.exists(otherCollLockDir));
      }
    } finally {
      server.shutdown();
    }
  }

  /** A parent lock dir removed while the lock path is being created must not fail the lock. */
  @Test
  public void testParentRemovedWhileCreatingLockPath() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      String root = "/lockTestCollectionRoot";
      AtomicReference<String> shardLockDir = new AtomicReference<>();
      AtomicBoolean removed = new AtomicBoolean();
      try (SolrZkClient zkClient =
          new SolrZkClient(
              new SolrZkClient.Builder()
                  .withUrl(server.getZkAddress())
                  .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)) {
            @Override
            public String create(String path, byte[] data, CreateMode createMode)
                throws KeeperException, InterruptedException {
              if (path.equals(shardLockDir.get()) && removed.compareAndSet(false, true)) {
                delete(parent(path), -1);
              }
              return super.create(path, data, createMode);
            }
          }) {
        ZkDistributedCollectionLockFactory factory =
            new ZkDistributedCollectionLockFactory(zkClient, root);
        shardLockDir.set(
            lockDir(
                factory.createLock(
                    true,
                    CollectionParams.LockLevel.SHARD,
                    COLLECTION_NAME,
                    SHARD_NAME,
                    null,
                    null)));
        zkClient.clean(root);

        DistributedLock lock =
            factory.createLock(
                true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
        assertTrue(removed.get());
        assertTrue(lock.isAcquired());
        lock.release();
      }
    } finally {
      server.shutdown();
    }
  }

  /** Lock creation gives up once the lock dir keeps disappearing on every attempt. */
  @Test
  public void testLockCreationGivesUpAfterMaxAttempts() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      AtomicInteger lockNodeCreates = new AtomicInteger();
      try (SolrZkClient zkClient =
          new SolrZkClient(
              new SolrZkClient.Builder()
                  .withUrl(server.getZkAddress())
                  .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)) {
            @Override
            public String create(String path, byte[] data, CreateMode createMode)
                throws KeeperException, InterruptedException {
              if (createMode == CreateMode.EPHEMERAL_SEQUENTIAL
                  && lockNodeCreates.incrementAndGet()
                      <= ZkDistributedLockFactory.MAX_CREATE_ATTEMPTS) {
                throw new KeeperException.NoNodeException(path);
              }
              return super.create(path, data, createMode);
            }
          }) {
        ZkDistributedCollectionLockFactory factory =
            new ZkDistributedCollectionLockFactory(zkClient, "/lockTestCollectionRoot");
        SolrException e =
            expectThrows(
                SolrException.class,
                () ->
                    factory.createLock(
                        true,
                        CollectionParams.LockLevel.COLLECTION,
                        COLLECTION_NAME,
                        null,
                        null,
                        null));
        assertTrue(e.getCause() instanceof KeeperException.NoNodeException);
        assertEquals(ZkDistributedLockFactory.MAX_CREATE_ATTEMPTS, lockNodeCreates.get());
      }
    } finally {
      server.shutdown();
    }
  }

  /** Asserts every node between the lock and {@code root} is a container. */
  private static void assertLockDirsAreContainers(
      ZkTestServer server, DistributedLock lock, String root) {
    for (String dir = parent(lock.getLockId()); !dir.equals(root); dir = parent(dir)) {
      assertTrue(
          dir + " should be a container node",
          server.getZKDatabase().getDataTree().getContainers().contains(dir));
    }
  }

  /** Releases the lock and returns the dir it was created in. */
  private static String lockDir(DistributedLock lock) {
    lock.release();
    return parent(lock.getLockId());
  }

  private static String parent(String path) {
    return path.substring(0, path.lastIndexOf('/'));
  }

  private void monothreadedCollectionTests(DistributedCollectionLockFactory factory) {
    // Collection level locks
    DistributedLock collRL1 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertTrue("collRL1 should have been acquired", collRL1.isAcquired());

    DistributedLock collRL2 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertTrue("collRL1 should have been acquired", collRL2.isAcquired());

    DistributedLock collWL3 =
        factory.createLock(
            true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertFalse(
        "collWL3 should not have been acquired, due to collRL1 and collRL2", collWL3.isAcquired());

    assertTrue(
        "collRL2 should have been acquired, that should not have changed", collRL2.isAcquired());

    collRL1.release();
    collRL2.release();
    assertTrue(
        "collWL3 should have been acquired, collRL1 and collRL2 were released",
        collWL3.isAcquired());

    DistributedLock collRL4 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertFalse(
        "collRL4 should not have been acquired, due to collWL3 locking the collection",
        collRL4.isAcquired());

    // Collection is write locked by collWL3 and collRL4 read lock waiting behind. Now moving to
    // request shard level locks. These are totally independent of the Collection level locks so
    // should see no impact.
    DistributedLock shardWL5 =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    assertTrue(
        "shardWL5 should have been acquired, there is no lock on that shard",
        shardWL5.isAcquired());

    DistributedLock shardWL6 =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    assertFalse(
        "shardWL6 should not have been acquired, shardWL5 is locking that shard",
        shardWL6.isAcquired());

    // Get a lock on a Replica. Again this is independent of collection or shard level
    DistributedLock replicaRL7 =
        factory.createLock(
            false,
            CollectionParams.LockLevel.REPLICA,
            COLLECTION_NAME,
            SHARD_NAME,
            REPLICA_NAME,
            null);
    assertTrue("replicaRL7 should have been acquired", replicaRL7.isAcquired());

    DistributedLock replicaWL8 =
        factory.createLock(
            true,
            CollectionParams.LockLevel.REPLICA,
            COLLECTION_NAME,
            SHARD_NAME,
            REPLICA_NAME,
            null);
    assertFalse(
        "replicaWL8 should not have been acquired, replicaRL7 is read locking that replica",
        replicaWL8.isAcquired());

    replicaRL7.release();
    assertTrue(
        "replicaWL8 should have been acquired, as replicaRL7 got released",
        replicaWL8.isAcquired());

    collWL3.release();
    assertTrue("collRL4 should have been acquired given collWL3 released", collRL4.isAcquired());
    shardWL5.release();
    assertTrue(
        "shardWL6 should have been acquired, now that shardWL5 was released",
        shardWL6.isAcquired());

    replicaWL8.release();
    try {
      replicaWL8.isAcquired();
      fail("isAcquired() called after release() on a lock should have thrown exception");
    } catch (IllegalStateException ise) {
      // expected
    }

    // Releasing the collection lock used in the multithreaded phase
    collRL4.release();
  }

  private void multithreadedCollectionTests(DistributedCollectionLockFactory factory)
      throws Exception {
    // Acquiring right away a read lock
    DistributedLock readLock =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertTrue("readLock should have been acquired", readLock.isAcquired());

    // And now creating a write lock, that can't be acquired just yet, because of the read lock
    DistributedLock writeLock =
        factory.createLock(
            true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    assertFalse("writeLock should not have been acquired", writeLock.isAcquired());

    // Wait for acquisition of the write lock on another thread (and be notified via a latch)
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread(
            () -> {
              writeLock.waitUntilAcquired();
              // countDown() will not be called if waitUntilAcquired() threw any kind of exception
              latch.countDown();
            })
        .start();

    // Wait for the thread to start and to get blocked in waitUntilAcquired() (thread start could
    // have been checked more reliably using another latch, and verifying the thread is in
    // waitUntilAcquired done through that thread stacktrace, but that would be overkill compared to
    // the very slight race condition of waiting 30ms, but a race that would not cause the test to
    // fail since we're testing... that nothing happened yet).
    Thread.sleep(30);

    assertEquals(
        "we should not have been notified that writeLock was acquired", 1, latch.getCount());
    assertFalse("writeLock should not have been acquired", writeLock.isAcquired());

    readLock.release();
    assertTrue(
        "writeLock should have been acquired now that readlock was released",
        writeLock.isAcquired());

    // Wait for the Zookeeper watch to fire + the thread to be unblocked and countdown the latch
    // We'll wait up to 10 seconds here, so should be safe even if GC is extraordinarily high with a
    // pause
    int i = 0;
    while (i < 1000 && latch.getCount() != 0) {
      Thread.sleep(10);
      i++;
    }
    assertEquals("we should have been notified that writeLock was acquired", 0, latch.getCount());
  }

  private void monothreadedConfigSetTests(DistributedConfigSetLockFactory factory) {
    DistributedLock configSetRL1 = factory.createLock(false, CONFIG_SET_NAME);
    assertTrue("configSetRL1 should have been acquired", configSetRL1.isAcquired());

    DistributedLock configSetRL2 = factory.createLock(false, CONFIG_SET_NAME);
    assertTrue("configSetRL2 should have been acquired", configSetRL2.isAcquired());

    DistributedLock configSetWL1 = factory.createLock(true, CONFIG_SET_NAME);
    assertFalse(
        "configSetWL1 should not have been acquired due to configSetRL1 and configSetRL2",
        configSetWL1.isAcquired());

    configSetRL1.release();
    configSetRL2.release();
    assertTrue(
        "configSetWL1 should have been acquired, configSetRL1 and configSetRL2 were released",
        configSetWL1.isAcquired());

    DistributedLock configSetRL3 = factory.createLock(false, CONFIG_SET_NAME);
    assertFalse(
        "configSetRL3 should not have been acquired due to configSetWL1",
        configSetRL3.isAcquired());

    configSetWL1.release();
    assertTrue(
        "configSetRL3 should have been acquired, configSetWL1 was released",
        configSetRL3.isAcquired());

    configSetRL3.release();

    try {
      configSetRL3.isAcquired();
      fail("isAcquired() called after release() on a lock should have thrown exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  private void multithreadedConfigSetTests(DistributedConfigSetLockFactory factory)
      throws Exception {
    // Acquiring right away a read lock
    DistributedLock configSetRL1 = factory.createLock(false, CONFIG_SET_NAME);
    assertTrue("configSetRL1 should have been acquired", configSetRL1.isAcquired());

    // And now creating a write lock, that can't be acquired just yet, because of the read lock
    DistributedLock configSetWL1 = factory.createLock(true, CONFIG_SET_NAME);
    assertFalse(
        "configSetWL1 should not have been acquired due to configSetRL1",
        configSetWL1.isAcquired());

    // Wait for acquisition of the write lock on another thread (and be notified via a latch)
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread(
            () -> {
              configSetWL1.waitUntilAcquired();
              // countDown() will not be called if waitUntilAcquired() threw any kind of exception
              latch.countDown();
            })
        .start();

    // Wait for the thread to start and to get blocked in waitUntilAcquired() (thread start could
    // have been checked more reliably using another latch, and verifying the thread is in
    // waitUntilAcquired done through that thread stacktrace, but that would be overkill compared to
    // the very slight race condition of waiting 30ms, but a race that would not cause the test to
    // fail since we're testing... that nothing happened yet).
    Thread.sleep(30);

    assertEquals(
        "we should not have been notified that configSetWL1 was acquired", 1, latch.getCount());
    assertFalse("configSetWL1 should not have been acquired", configSetWL1.isAcquired());

    configSetRL1.release();
    assertTrue(
        "configSetWL1 should have been acquired now that configSetRL1 was released",
        configSetWL1.isAcquired());

    // Wait for the Zookeeper watch to fire + the thread to be unblocked and countdown the latch.
    // We'll wait up to 10 seconds here, so should be safe even if GC is extraordinarily high with a
    // pause
    int i = 0;
    while (i < 1000 && latch.getCount() != 0) {
      Thread.sleep(10);
      i++;
    }
    assertEquals(
        "we should have been notified that configSetWL1 was acquired", 0, latch.getCount());
  }
}
