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

import static org.apache.solr.cloud.ThreadBoundLock.TIMEOUT_MS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZkDistributedLockTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "lockColl";
  private final String SHARD_NAME = "lockShard";
  private final String REPLICA_NAME = "lockReplica";

  private static final String CONFIG_SET_NAME = "lockConfigSet";
  private static final String COLLECTION_ROOT = "/lockTestCollectionRoot";
  private static final String CONFIG_SET_ROOT = "/lockTestConfigSetRoot";

  private final List<ThreadBoundLock> boundLocks = new ArrayList<>();

  private ZkTestServer server;
  private SolrZkClient zkClient;

  @Before
  public void startZk() throws Exception {
    server = new ZkTestServer(createTempDir("zkData"));
    server.run();
    zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkHost())
            .withTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();
  }

  @After
  public void stopZk() throws Exception {
    boundLocks.forEach(ThreadBoundLock::close);
    zkClient.close();
    server.shutdown();
  }

  private ThreadBoundLock bind(DistributedLock lock) {
    ThreadBoundLock bound = new ThreadBoundLock(lock::waitUntilAcquired, lock::release);
    boundLocks.add(bound);
    return bound;
  }

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
    DistributedCollectionLockFactory collLockFactory =
        new ZkDistributedCollectionLockFactory(zkClient, COLLECTION_ROOT);

    monothreadedCollectionTests(collLockFactory);
    multithreadedCollectionTests(collLockFactory);

    DistributedConfigSetLockFactory configSetLockFactory =
        new ZkDistributedConfigSetLockFactory(zkClient, CONFIG_SET_ROOT);

    monothreadedConfigSetTests(configSetLockFactory);
    multithreadedConfigSetTests(configSetLockFactory);
  }

  /**
   * Lock directories must be ZooKeeper container nodes so they get removed once empty, and a lock
   * must still be obtainable after its directory was removed.
   */
  @Test
  public void testLockDirsAreContainers() throws Exception {
    DistributedCollectionLockFactory collLockFactory =
        new ZkDistributedCollectionLockFactory(zkClient, COLLECTION_ROOT);
    DistributedLock replicaLock =
        collLockFactory.createLock(
            true,
            CollectionParams.LockLevel.REPLICA,
            COLLECTION_NAME,
            SHARD_NAME,
            REPLICA_NAME,
            null);
    replicaLock.waitUntilAcquired();
    String replicaLockId = replicaLock.getLockId();
    replicaLock.release();
    DistributedLock shardLock =
        collLockFactory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    shardLock.waitUntilAcquired();
    String shardLockId = shardLock.getLockId();
    shardLock.release();
    DistributedLock collLock =
        collLockFactory.createLock(
            true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    collLock.waitUntilAcquired();
    String collLockId = collLock.getLockId();
    collLock.release();
    assertLockDirsAreContainers(server, replicaLockId, COLLECTION_ROOT);
    assertLockDirsAreContainers(server, shardLockId, COLLECTION_ROOT);
    assertLockDirsAreContainers(server, collLockId, COLLECTION_ROOT);

    DistributedConfigSetLockFactory configSetLockFactory =
        new ZkDistributedConfigSetLockFactory(zkClient, CONFIG_SET_ROOT);
    DistributedLock configSetLock = configSetLockFactory.createLock(true, CONFIG_SET_NAME);
    configSetLock.waitUntilAcquired();
    String configSetLockId = configSetLock.getLockId();
    configSetLock.release();
    assertLockDirsAreContainers(server, configSetLockId, CONFIG_SET_ROOT);

    // Simulate ZooKeeper removing the empty containers
    zkClient.clean(parent(parent(collLockId)));
    DistributedLock lock =
        collLockFactory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    lock.waitUntilAcquired();
    lock.release();
  }

  /** Lock dirs left as persistent nodes by older versions get removed, live locks are kept. */
  @Test
  public void testDeleteEmptyLockDirs() throws Exception {
    ZkDistributedCollectionLockFactory factory =
        new ZkDistributedCollectionLockFactory(zkClient, COLLECTION_ROOT);
    String collLockDir =
        lockDir(
            factory.createLock(
                true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null));
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
                true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null));
    String otherCollLockDir =
        lockDir(
            factory.createLock(
                true, CollectionParams.LockLevel.COLLECTION, "otherColl", null, null, null));
    String coll = parent(collLockDir);
    zkClient.clean(COLLECTION_ROOT);
    for (String dir : List.of(collLockDir, shardLockDir, replicaLockDir, otherCollLockDir)) {
      zkClient.makePath(dir, false);
    }

    DistributedLock shardLock =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    shardLock.waitUntilAcquired();

    factory.deleteEmptyLockDirs(COLLECTION_NAME);
    assertFalse(zkClient.exists(collLockDir));
    assertFalse(zkClient.exists(replicaLockDir));
    assertTrue(zkClient.exists(shardLock.getLockId()));

    shardLock.release();
    factory.deleteEmptyLockDirs(COLLECTION_NAME);
    assertFalse(zkClient.exists(coll));
    assertTrue(zkClient.exists(otherCollLockDir));
  }

  private DistributedCollectionLockFactory collectionLockFactory() {
    return new ZkDistributedCollectionLockFactory(zkClient, COLLECTION_ROOT);
  }

  private DistributedLock collectionLock(
      DistributedCollectionLockFactory factory, boolean write, String callingLockId) {
    return factory.createLock(
        write, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, callingLockId);
  }

  private void assertWriteAcquirable(DistributedCollectionLockFactory factory) throws Exception {
    ThreadBoundLock next = bind(collectionLock(factory, true, null));
    next.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    next.release();
  }

  @Test
  public void testInterruptedWaiterLeavesNoNode() throws Exception {
    DistributedCollectionLockFactory factory = collectionLockFactory();
    DistributedLock read = collectionLock(factory, false, null);
    ThreadBoundLock readThread = bind(read);
    readThread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    String readId = read.getLockId();

    ThreadBoundLock waiter = bind(collectionLock(factory, true, null));
    waiter.acquire();
    ThreadBoundLock.waitForLockNodes(zkClient, readId, 2);

    waiter.close();
    ThreadBoundLock.waitForLockNodes(zkClient, readId, 1);

    readThread.release();
  }

  @Test
  public void testReleaseWithoutAcquireIsNoOp() throws Exception {
    DistributedCollectionLockFactory factory = collectionLockFactory();
    collectionLock(factory, true, null).release();
    assertWriteAcquirable(factory);
  }

  @Test
  public void testMirrorOfReleasedLockFails() throws Exception {
    DistributedCollectionLockFactory factory = collectionLockFactory();
    DistributedLock write = collectionLock(factory, true, null);
    ThreadBoundLock writeThread = bind(write);
    writeThread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    String releasedId = write.getLockId();
    writeThread.release();

    DistributedLock mirror = collectionLock(factory, false, releasedId);
    SolrException e = expectThrows(SolrException.class, mirror::waitUntilAcquired);
    assertTrue(e.getMessage(), e.getMessage().contains("Missing mirrored lock node"));
  }

  @Test
  public void testReleaseOnOtherThreadFails() throws Exception {
    DistributedCollectionLockFactory factory = collectionLockFactory();
    DistributedLock write = collectionLock(factory, true, null);
    ThreadBoundLock holder = bind(write);
    holder.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    SolrException e = expectThrows(SolrException.class, write::release);
    assertTrue(e.getCause() instanceof IllegalMonitorStateException);

    holder.release();
    assertWriteAcquirable(factory);
  }

  @Test
  public void testRepeatWaitThenSingleReleaseFrees() throws Exception {
    DistributedCollectionLockFactory factory = collectionLockFactory();
    DistributedLock write = collectionLock(factory, true, null);
    write.waitUntilAcquired();
    write.waitUntilAcquired();
    write.release();
    assertWriteAcquirable(factory);
  }

  /**
   * Asserts every node from the lock node's parent up to and including {@code root} is a container.
   */
  private static void assertLockDirsAreContainers(ZkTestServer server, String lockId, String root) {
    for (String dir = parent(lockId); dir.startsWith(root); dir = parent(dir)) {
      assertTrue(
          dir + " should be a container node",
          server.getZKDatabase().getDataTree().getContainers().contains(dir));
    }
  }

  /** Releases the lock and returns the dir it was created in. */
  private static String lockDir(DistributedLock lock) {
    lock.waitUntilAcquired();
    String lockId = lock.getLockId();
    lock.release();
    return parent(lockId);
  }

  private static String parent(String path) {
    return path.substring(0, path.lastIndexOf('/'));
  }

  private void monothreadedCollectionTests(DistributedCollectionLockFactory factory)
      throws Exception {
    // Collection level locks
    DistributedLock collRL1 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock collRL1Thread = bind(collRL1);
    collRL1Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock collRL2 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock collRL2Thread = bind(collRL2);
    collRL2Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock collWL3 =
        factory.createLock(
            true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock collWL3Thread = bind(collWL3);
    Future<?> collWL3Acquired =
        collWL3Thread.acquireBlocked(
            "collWL3 should not have been acquired, due to collRL1 and collRL2",
            zkClient,
            COLLECTION_ROOT,
            1);

    collRL1Thread.release();
    collRL2Thread.release();
    collWL3Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock collRL4 =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock collRL4Thread = bind(collRL4);
    Future<?> collRL4Acquired =
        collRL4Thread.acquireBlocked(
            "collRL4 should not have been acquired, due to collWL3 locking the collection",
            zkClient,
            COLLECTION_ROOT,
            1);

    // Collection is write locked by collWL3 and collRL4 read lock waiting behind. Now moving to
    // request shard level locks. These are totally independent of the Collection level locks so
    // should see no impact.
    DistributedLock shardWL5 =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    ThreadBoundLock shardWL5Thread = bind(shardWL5);
    shardWL5Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock shardWL6 =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD_NAME, null, null);
    ThreadBoundLock shardWL6Thread = bind(shardWL6);
    Future<?> shardWL6Acquired =
        shardWL6Thread.acquireBlocked(
            "shardWL6 should not have been acquired, shardWL5 is locking that shard",
            zkClient,
            COLLECTION_ROOT,
            1);

    // Get a lock on a Replica. Again this is independent of collection or shard level
    DistributedLock replicaRL7 =
        factory.createLock(
            false,
            CollectionParams.LockLevel.REPLICA,
            COLLECTION_NAME,
            SHARD_NAME,
            REPLICA_NAME,
            null);
    ThreadBoundLock replicaRL7Thread = bind(replicaRL7);
    replicaRL7Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock replicaWL8 =
        factory.createLock(
            true,
            CollectionParams.LockLevel.REPLICA,
            COLLECTION_NAME,
            SHARD_NAME,
            REPLICA_NAME,
            null);
    ThreadBoundLock replicaWL8Thread = bind(replicaWL8);
    Future<?> replicaWL8Acquired =
        replicaWL8Thread.acquireBlocked(
            "replicaWL8 should not have been acquired, replicaRL7 is read locking that replica",
            zkClient,
            COLLECTION_ROOT,
            1);

    replicaRL7Thread.release();
    replicaWL8Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    collWL3Thread.release();
    collRL4Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    shardWL5Thread.release();
    shardWL6Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    replicaWL8Thread.release();

    // Releasing the collection lock used in the multithreaded phase
    collRL4Thread.release();
  }

  private void multithreadedCollectionTests(DistributedCollectionLockFactory factory)
      throws Exception {
    // Acquiring right away a read lock
    DistributedLock readLock =
        factory.createLock(
            false, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock readLockThread = bind(readLock);
    readLockThread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // And now creating a write lock, that can't be acquired just yet, because of the read lock
    DistributedLock writeLock =
        factory.createLock(
            true, CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null, null);
    ThreadBoundLock writeLockThread = bind(writeLock);
    Future<?> writeLockAcquired =
        writeLockThread.acquireBlocked(
            "writeLock should not have been acquired", zkClient, COLLECTION_ROOT, 1);

    readLockThread.release();
    writeLockAcquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    writeLockThread.release();
  }

  private void monothreadedConfigSetTests(DistributedConfigSetLockFactory factory)
      throws Exception {
    DistributedLock configSetRL1 = factory.createLock(false, CONFIG_SET_NAME);
    ThreadBoundLock configSetRL1Thread = bind(configSetRL1);
    configSetRL1Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock configSetRL2 = factory.createLock(false, CONFIG_SET_NAME);
    ThreadBoundLock configSetRL2Thread = bind(configSetRL2);
    configSetRL2Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock configSetWL1 = factory.createLock(true, CONFIG_SET_NAME);
    ThreadBoundLock configSetWL1Thread = bind(configSetWL1);
    Future<?> configSetWL1Acquired =
        configSetWL1Thread.acquireBlocked(
            "configSetWL1 should not have been acquired due to configSetRL1 and configSetRL2",
            zkClient,
            CONFIG_SET_ROOT,
            1);

    configSetRL1Thread.release();
    configSetRL2Thread.release();
    configSetWL1Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    DistributedLock configSetRL3 = factory.createLock(false, CONFIG_SET_NAME);
    ThreadBoundLock configSetRL3Thread = bind(configSetRL3);
    Future<?> configSetRL3Acquired =
        configSetRL3Thread.acquireBlocked(
            "configSetRL3 should not have been acquired due to configSetWL1",
            zkClient,
            CONFIG_SET_ROOT,
            1);

    configSetWL1Thread.release();
    configSetRL3Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    configSetRL3Thread.release();
  }

  private void multithreadedConfigSetTests(DistributedConfigSetLockFactory factory)
      throws Exception {
    // Acquiring right away a read lock
    DistributedLock configSetRL1 = factory.createLock(false, CONFIG_SET_NAME);
    ThreadBoundLock configSetRL1Thread = bind(configSetRL1);
    configSetRL1Thread.acquire().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

    // And now creating a write lock, that can't be acquired just yet, because of the read lock
    DistributedLock configSetWL1 = factory.createLock(true, CONFIG_SET_NAME);
    ThreadBoundLock configSetWL1Thread = bind(configSetWL1);
    Future<?> configSetWL1Acquired =
        configSetWL1Thread.acquireBlocked(
            "configSetWL1 should not have been acquired due to configSetRL1",
            zkClient,
            CONFIG_SET_ROOT,
            1);

    configSetRL1Thread.release();
    configSetWL1Acquired.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    configSetWL1Thread.release();
  }
}
