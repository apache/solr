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

package org.apache.solr.cloud.api.collections;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.DistributedMultiLock;
import org.apache.solr.cloud.ZkDistributedCollectionLockFactory;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Test;

public class CollectionApiLockingTest extends SolrTestCaseJ4 {
  private static final String COLLECTION_NAME = "lockColl";
  final String SHARD1_NAME = "lockShard1";
  final String SHARD2_NAME = "lockShard2";
  final String REPLICA_NAME = "lockReplica";

  static final int TIMEOUT = 10000;

  /**
   * Tests the Collection API locking. All tests run from single test method to save on setup time.
   */
  @Test
  public void monothreadedApiLockTests() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        CollectionApiLockFactory apiLockFactory =
            new CollectionApiLockFactory(
                new ZkDistributedCollectionLockFactory(zkClient, "/apiLockTestRoot"));

        monothreadedTests(apiLockFactory);
        multithreadedTests(apiLockFactory);
      }
    } finally {
      server.shutdown();
    }
  }

  private void monothreadedTests(CollectionApiLockFactory apiLockingHelper) {
    // Lock at collection level (which prevents locking + acquiring on any other level of the
    // hierarchy)
    DistributedMultiLock collLock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertTrue("Collection should have been acquired", collLock.isAcquired());
    assertEquals(
        "Lock at collection level expected to need one distributed lock",
        1,
        collLock.getCountInternalLocks());

    // Request a shard lock. Will not be acquired as long as we don't release the collection lock
    // above
    DistributedMultiLock shard1Lock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, null);
    assertFalse("Shard1 should not have been acquired", shard1Lock.isAcquired());
    assertEquals(
        "Lock at shard level expected to need two distributed locks",
        2,
        shard1Lock.getCountInternalLocks());

    // Request a lock on another shard. Will not be acquired as long as we don't release the
    // collection lock above
    DistributedMultiLock shard2Lock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD2_NAME, null);
    assertFalse("Shard2 should not have been acquired", shard2Lock.isAcquired());

    assertTrue("Collection should still be acquired", collLock.isAcquired());

    collLock.release();

    assertTrue(
        "Shard1 should have been acquired now that collection lock released",
        shard1Lock.isAcquired());
    assertTrue(
        "Shard2 should have been acquired now that collection lock released",
        shard2Lock.isAcquired());

    // Request a lock on replica of shard1
    DistributedMultiLock replicaShard1Lock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, REPLICA_NAME);
    assertFalse(
        "replicaShard1Lock should not have been acquired, shard1 is locked",
        replicaShard1Lock.isAcquired());

    // Now ask for a new lock on the collection
    collLock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);

    assertFalse(
        "Collection should not have been acquired, shard1 and shard2 locks preventing it",
        collLock.isAcquired());

    shard1Lock.release();
    assertTrue(
        "replicaShard1Lock should have been acquired, as shard1 got released",
        replicaShard1Lock.isAcquired());
    assertFalse(
        "Collection should not have been acquired, shard2 lock is preventing it",
        collLock.isAcquired());

    replicaShard1Lock.release();

    // Request a lock on replica of shard2
    DistributedMultiLock replicaShard2Lock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD2_NAME, REPLICA_NAME);
    assertFalse(
        "replicaShard2Lock should not have been acquired, shard2 is locked",
        replicaShard2Lock.isAcquired());

    shard2Lock.release();

    assertTrue(
        "Collection should have been acquired as shard2 got released and replicaShard2Locks was requested after the collection lock",
        collLock.isAcquired());
    assertFalse(
        "replicaShard2Lock should not have been acquired, collLock is locked",
        replicaShard2Lock.isAcquired());

    collLock.release();
    assertTrue(
        "replicaShard2Lock should have been acquired, the collection lock got released",
        replicaShard2Lock.isAcquired());

    // Release remaining lock to allow the multithreaded locking to succeed
    replicaShard2Lock.release();
  }

  private void multithreadedTests(CollectionApiLockFactory apiLockingHelper) throws Exception {
    // Lock on collection...
    DistributedMultiLock collLock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.COLLECTION, COLLECTION_NAME, null, null);
    assertTrue("Collection should have been acquired", collLock.isAcquired());

    // ...blocks a lock on replica from being acquired
    final DistributedMultiLock replicaShard1Lock =
        apiLockingHelper.createCollectionApiLock(
            CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, REPLICA_NAME);
    assertFalse(
        "replicaShard1Lock should not have been acquired, because collection is locked",
        replicaShard1Lock.isAcquired());

    // Wait for acquisition of the replica lock on another thread (and be notified via a latch)
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread(
            () -> {
              replicaShard1Lock.waitUntilAcquired();
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

    assertEquals("we should not have been notified that replica was acquired", 1, latch.getCount());
    assertFalse("replica lock should not have been acquired", replicaShard1Lock.isAcquired());

    collLock.release();
    assertTrue(
        "replica lock should have been acquired now that collection lock was released",
        replicaShard1Lock.isAcquired());

    // Wait for the Zookeeper watch to fire + the thread to be unblocked and countdown the latch
    // We'll wait up to 10 seconds here, so should be safe even if GC is extraordinarily high with a
    // pause
    int i = 0;
    while (i < 1000 && latch.getCount() != 0) {
      Thread.sleep(10);
      i++;
    }
    assertEquals(
        "we should have been notified that replica lock was acquired", 0, latch.getCount());
  }
}
