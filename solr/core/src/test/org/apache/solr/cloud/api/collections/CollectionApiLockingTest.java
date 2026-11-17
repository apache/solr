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

import static org.apache.solr.cloud.ThreadBoundLock.awaitAcquired;

import java.util.List;
import java.util.concurrent.Future;
import org.apache.solr.cloud.ApiLockingTestBase;
import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.DistributedMultiLock;
import org.apache.solr.cloud.ThreadBoundLock;
import org.apache.solr.cloud.ZkDistributedCollectionLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;
import org.junit.Test;

public class CollectionApiLockingTest extends ApiLockingTestBase {
  private static final String COLLECTION_NAME = "lockColl";
  final String SHARD1_NAME = "lockShard1";
  final String SHARD2_NAME = "lockShard2";
  final String REPLICA_NAME = "lockReplica";

  /**
   * Tests the Collection API locking. All tests run from single test method to save on setup time.
   */
  @Test
  public void monothreadedApiLockTests() throws Exception {
    CollectionApiLockFactory apiLockFactory =
        new CollectionApiLockFactory(new ZkDistributedCollectionLockFactory(zkClient, LOCK_ROOT));

    monothreadedTests(apiLockFactory);
    multithreadedTests(apiLockFactory);
    testCallingLockIdSubLocks(apiLockFactory);
  }

  @Test
  public void testBlockedMultiLockHoldsNoLowerLevelNode() throws Exception {
    ZkDistributedCollectionLockFactory factory =
        new ZkDistributedCollectionLockFactory(zkClient, LOCK_ROOT);
    CollectionApiLockFactory apiLockFactory = new CollectionApiLockFactory(factory);

    DistributedMultiLock collWrite =
        apiLockFactory.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE),
            COLLECTION_NAME,
            null,
            null);
    assertAcquired("Collection write should have been acquired", collWrite);

    DistributedMultiLock split =
        apiLockFactory.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.SPLITSHARD),
            COLLECTION_NAME,
            SHARD1_NAME,
            null);
    Future<?> splitAcquisition =
        bound(split)
            .acquireBlocked("Split should be blocked by collection write", zkClient, LOCK_ROOT, 1);

    DistributedLock shardWrite =
        factory.createLock(
            true, CollectionParams.LockLevel.SHARD, COLLECTION_NAME, SHARD1_NAME, null, null);
    ThreadBoundLock shardWriteThread =
        new ThreadBoundLock(shardWrite::waitUntilAcquired, shardWrite::release);
    try {
      awaitAcquired(
          "Plain shard write should acquire while the split is blocked",
          shardWriteThread.acquire());
      shardWriteThread.release();
    } finally {
      shardWriteThread.close();
    }

    release(collWrite);
    awaitAcquired("Split should acquire once the collection write is released", splitAcquisition);
    release(split);
  }

  private void abandon(DistributedMultiLock lock) throws Exception {
    bound(lock).abandon();
  }

  private void monothreadedTests(CollectionApiLockFactory apiLockingHelper) throws Exception {
    // Lock at collection level (which prevents locking + acquiring on any other level of the
    // hierarchy)
    DistributedMultiLock collLock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE),
            COLLECTION_NAME,
            null,
            null);
    assertAcquired("Collection should have been acquired", collLock);
    assertEquals(
        "Lock at collection level expected to need one distributed lock",
        1,
        collLock.getCountInternalLocks());

    // Request a shard lock. Will not be acquired as long as we don't release the collection lock
    // above
    DistributedMultiLock shard1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.SPLITSHARD),
            COLLECTION_NAME,
            SHARD1_NAME,
            null);
    Future<?> shard1Acquisition =
        bound(shard1Lock)
            .acquireBlocked("Shard1 should not have been acquired", zkClient, LOCK_ROOT, 1);
    assertEquals(
        "Lock at shard level expected to need two distributed locks",
        2,
        shard1Lock.getCountInternalLocks());

    // Request a lock on another shard. Will not be acquired as long as we don't release the
    // collection lock above
    DistributedMultiLock shard2Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.SPLITSHARD),
            COLLECTION_NAME,
            SHARD2_NAME,
            null);
    Future<?> shard2Acquisition =
        bound(shard2Lock)
            .acquireBlocked("Shard2 should not have been acquired", zkClient, LOCK_ROOT, 1);

    assertTrue("Collection lock node should still exist", zkClient.exists(collLock.getLockId()));

    release(collLock);

    awaitAcquired(
        "Shard1 should have been acquired now that collection lock released", shard1Acquisition);
    awaitAcquired(
        "Shard2 should have been acquired now that collection lock released", shard2Acquisition);

    // Request a lock on replica of shard1
    DistributedMultiLock replicaShard1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK),
            COLLECTION_NAME,
            SHARD1_NAME,
            REPLICA_NAME);
    Future<?> replicaShard1Acquisition =
        bound(replicaShard1Lock)
            .acquireBlocked(
                "replicaShard1Lock should not have been acquired, shard1 is locked",
                zkClient,
                LOCK_ROOT,
                2);

    // Now ask for a new lock on the collection
    collLock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE),
            COLLECTION_NAME,
            null,
            null);

    Future<?> collAcquisition =
        bound(collLock)
            .acquireBlocked(
                "Collection should not have been acquired, shard1 and shard2 locks preventing it",
                zkClient,
                LOCK_ROOT,
                1);

    release(shard1Lock);
    awaitAcquired(
        "replicaShard1Lock should have been acquired, as shard1 got released",
        replicaShard1Acquisition);
    assertFalse(
        "Collection should not have been acquired, shard2 lock is preventing it",
        collAcquisition.isDone());

    release(replicaShard1Lock);

    // Request a lock on replica of shard2
    DistributedMultiLock replicaShard2Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK),
            COLLECTION_NAME,
            SHARD2_NAME,
            REPLICA_NAME);
    Future<?> replicaShard2Acquisition =
        bound(replicaShard2Lock)
            .acquireBlocked(
                "replicaShard2Lock should not have been acquired, shard2 is locked",
                zkClient,
                LOCK_ROOT,
                1);

    release(shard2Lock);

    awaitAcquired(
        "Collection should have been acquired as shard2 got released and replicaShard2Locks was requested after the collection lock",
        collAcquisition);
    assertFalse(
        "replicaShard2Lock should not have been acquired, collLock is locked",
        replicaShard2Acquisition.isDone());

    release(collLock);
    awaitAcquired(
        "replicaShard2Lock should have been acquired, the collection lock got released",
        replicaShard2Acquisition);

    // Release remaining lock to allow the multithreaded locking to succeed
    release(replicaShard2Lock);
  }

  private void multithreadedTests(CollectionApiLockFactory apiLockingHelper) throws Exception {
    // Lock on collection...
    DistributedMultiLock collLock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE),
            COLLECTION_NAME,
            null,
            null);
    assertAcquired("Collection should have been acquired", collLock);

    // ...blocks a lock on replica from being acquired
    final DistributedMultiLock replicaShard1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK),
            COLLECTION_NAME,
            SHARD1_NAME,
            REPLICA_NAME);
    Future<?> replicaShard1Acquisition =
        bound(replicaShard1Lock)
            .acquireBlocked(
                "replicaShard1Lock should not have been acquired, because collection is locked",
                zkClient,
                LOCK_ROOT,
                1);

    release(collLock);
    awaitAcquired(
        "replica lock should have been acquired now that collection lock was released",
        replicaShard1Acquisition);
  }

  public void testCallingLockIdSubLocks(CollectionApiLockFactory apiLockingHelper)
      throws Exception {
    DistributedMultiLock coll1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE), "coll1", null, null);
    assertEquals("Wrong number of internalLocks", 1, coll1Lock.getCountInternalLocks());
    assertAcquired("Lock should be acquired", coll1Lock);

    // Test sub-locks at the same level
    DistributedMultiLock coll1Lock2 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.RELOAD), "coll1", null, null);
    assertEquals("Wrong number of internalLocks", 1, coll1Lock2.getCountInternalLocks());
    bound(coll1Lock2)
        .acquireBlocked(
            "Should not be able to lock coll1 without using a callingLockId",
            zkClient,
            LOCK_ROOT,
            1);
    abandon(coll1Lock2);
    coll1Lock2 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.RELOAD)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            null,
            null);
    assertEquals("Wrong number of internalLocks", 1, coll1Lock2.getCountInternalLocks());
    assertAcquired("Should  be able to lock coll1 when using a callingLockId", coll1Lock2);
    release(coll1Lock2);

    // Test locks underneath
    DistributedMultiLock shard1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.ADDREPLICA)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock coll1/shard1 when using a callingLockId on coll1", shard1Lock);
    DistributedMultiLock shard1Lock2 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.ADDREPLICA)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock2.getCountInternalLocks());
    bound(shard1Lock2)
        .acquireBlocked(
            "Should not be able to lock coll1/shard1 since our callingLockId is only coll1, not shard1, since shard1 is already locked",
            zkClient,
            LOCK_ROOT,
            1);
    abandon(shard1Lock2);
    DistributedMultiLock shard2Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.ADDREPLICA)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard2",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard2Lock.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock coll1/shard2 when using a callingLockId on coll1, since shard2 has not been locked yet",
        shard2Lock);
    release(shard2Lock);
    release(shard1Lock);

    // Test locks 2 underneath
    DistributedMultiLock replica1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            "replica1");
    assertEquals("Wrong number of internalLocks", 3, replica1Lock.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock shard1/replica1 when using a callingLockId on coll1",
        replica1Lock);
    DistributedMultiLock replica2Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            "replica1");
    assertEquals("Wrong number of internalLocks", 3, replica2Lock.getCountInternalLocks());
    bound(replica2Lock)
        .acquireBlocked(
            "Should not be able to lock coll1/shard1/replica1 since our callingLockId is only coll1, not replica1, which is already locked",
            zkClient,
            LOCK_ROOT,
            2);
    abandon(replica2Lock);
    shard1Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.ADDREPLICA)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock.getCountInternalLocks());
    bound(shard1Lock)
        .acquireBlocked(
            "Should not be able to lock coll1/shard1 since our callingLockId is only coll1, not shard1, which is locked because of a replica task",
            zkClient,
            LOCK_ROOT,
            1);
    abandon(shard1Lock);
    replica2Lock =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.MOCK_REPLICA_TASK)
                .withCallingLockId(coll1Lock.getLockId()),
            "coll1",
            "shard1",
            "replica2");
    assertEquals("Wrong number of internalLocks", 3, replica2Lock.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock shard2/replica2 when using a callingLockId on coll1, since shard2 has not been locked yet",
        replica2Lock);
    release(replica2Lock);
    release(replica1Lock);
    release(coll1Lock);

    // Test difference at a higher level
    DistributedMultiLock shard1Lock1 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.INSTALLSHARDDATA),
            "coll1",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock1.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock coll1/shard1 when not using a callingLockId since shard1 has not been locked yet",
        shard1Lock1);
    shard1Lock2 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.INSTALLSHARDDATA),
            "coll2",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock2.getCountInternalLocks());
    assertAcquired(
        "Should  be able to lock coll2/shard1 when not using a callingLockId since shard1 has not been locked yet for coll2",
        shard1Lock2);
    String badLockId = shard1Lock2.getLockId();
    assertThrows(
        "Should not be able to lock coll1/shard1 since our callingLockId is coll2",
        SolrException.class,
        () ->
            apiLockingHelper.createCollectionApiLock(
                new AdminCmdContext(CollectionParams.CollectionAction.INSTALLSHARDDATA)
                    .withCallingLockId(badLockId),
                "coll1",
                "shard1",
                null));
    DistributedMultiLock shard1Lock3 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.INSTALLSHARDDATA)
                .withCallingLockId(shard1Lock1.getLockId()),
            "coll1",
            "shard1",
            null);
    assertEquals("Wrong number of internalLocks", 2, shard1Lock3.getCountInternalLocks());
    assertAcquired(
        "Should be able to lock coll1/shard1 since our callingLockId is coll1/shard1", shard1Lock3);
    release(shard1Lock2);
    release(shard1Lock3);

    // Test difference at a higher level
    coll1Lock2 =
        apiLockingHelper.createCollectionApiLock(
            new AdminCmdContext(CollectionParams.CollectionAction.RELOAD), "coll1", null, null);
    assertEquals("Wrong number of internalLocks", 1, coll1Lock2.getCountInternalLocks());
    bound(coll1Lock2)
        .acquireBlocked(
            "Should not be able to lock coll1 since we have no callingLockId and shard1 is already locked. Cannot move up",
            zkClient,
            LOCK_ROOT,
            1);
    abandon(coll1Lock2);
    assertExceptionThrownWithMessageContaining(
        SolrException.class,
        List.of("Cannot mirror lock"),
        () ->
            apiLockingHelper.createCollectionApiLock(
                new AdminCmdContext(CollectionParams.CollectionAction.RELOAD)
                    .withCallingLockId(shard1Lock1.getLockId()),
                "coll1",
                null,
                null));
    release(shard1Lock3);

    // Test an unrelated lock
    assertThrows(
        "Should not be able to lock coll2even since callingLockId is coll1 and unrelated",
        SolrException.class,
        () ->
            apiLockingHelper.createCollectionApiLock(
                new AdminCmdContext(CollectionParams.CollectionAction.CREATE)
                    .withCallingLockId(shard1Lock1.getLockId()),
                "coll2",
                null,
                null));
    release(shard1Lock1);
  }
}
