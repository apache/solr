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

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.cloud.DistributedCollectionLockFactory;
import org.apache.solr.cloud.DistributedLock;
import org.apache.solr.cloud.DistributedMultiLock;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionParams;

/**
 * This class implements a higher level locking abstraction for the Collection API using lower level
 * read and write locks.
 */
public class CollectionApiLockFactory {

  private final DistributedCollectionLockFactory lockFactory;

  CollectionApiLockFactory(DistributedCollectionLockFactory lockFactory) {
    this.lockFactory = lockFactory;
  }

  /**
   * For the {@link org.apache.solr.common.params.CollectionParams.LockLevel} of the passed {@code
   * action}, obtains the required locks (if any) and returns.
   *
   * <p>This method obtains a write lock at the actual level and path of the action, and also
   * obtains read locks on "lower" lock levels. For example for a lock at the shard level, a write
   * lock will be requested at the corresponding shard path and a read lock on the corresponding
   * collection path (in order to prevent an operation locking at the collection level from
   * executing concurrently with an operation on one of the shards of the collection). See
   * documentation linked to SOLR-14840 regarding Collection API locking.
   *
   * @return a lock that once {@link DistributedMultiLock#isAcquired()} guarantees the corresponding
   *     Collection API command can execute safely. The returned lock <b>MUST</b> be {@link
   *     DistributedMultiLock#release()} no matter what once no longer needed as otherwise it would
   *     prevent other threads from locking.
   */
  DistributedMultiLock createCollectionApiLock(
      CollectionParams.LockLevel lockLevel, String collName, String shardId, String replicaName) {
    if (lockLevel == CollectionParams.LockLevel.NONE) {
      return new DistributedMultiLock(List.of());
    }

    if (lockLevel == CollectionParams.LockLevel.CLUSTER) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Bug. Not expecting locking at cluster level.");
    }

    if (collName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Bug. collName can't be null");
    }

    // There is a bug in the way locks are managed in the Overseer, it sometimes locks at a given
    // level (for example shard) without knowing on which shard to lock. This can happen for example
    // for SPLITSHARD using a split.key (details below) or DELETEREPLICA when specifying a count of
    // replicas to delete but no shard.
    //
    // When doing a splitshard using a split.key (and not specifying the actual shard name), when
    // the OverseerCollectionMessageHandler grabs a lock for the Collection API it does not have the
    // shard name and the lock is obtained for a null key. This means that this lock does not
    // prevent another operation specifically targeting the shard from executing concurrently (can
    // even be another split if it is specified with a shard name). The null shard lock does prevent
    // another split.key split for another shard of the same collection at the same time (because
    // trying to obtain another lock on the null key below the collection will fail). The splitshard
    // request is built in CollectionAdminRequest.splitShard(). The case of concern is when
    // SplitShard.setSplitKey() is then called (see test CollectionsAPISolrJTest.testSplitShard()).
    // CollectionOperation.SPLITSHARD_OP in CollectionsHandler verifies that when the key is
    // specified no shard name is specified. The locking happens from OverseerTaskProcessor.run()
    // calling OverseerCollectionMessageHandler.lockTask(). The ArrayList passed to
    // LockTree.Session.lock() contains the collection name (index 0) but not shard name (value null
    // at index 1) yet the action.lockLevel is SHARD. Note that there ends up being a single shard
    // name associated with the split key. It is computed in SplitShardCmd.getParentSlice().
    //
    // We deal with the issue to trigger the same (wrong) behavior for compatibility of distributed
    // Collection API vs Overseer based collection API. We make up a shard name that is highly
    // unlikely to otherwise exist and lock it instead. Another option would be to bump up the
    // locking level from SHARD to COLLECTION when no shard is specified (would be "more correct"
    // from the perspective of preventing competing Collection API commands from running
    // concurrently, but would restrict concurrency a bit more than existing solution, and if some
    // installations depend on current behavior...).
    if (lockLevel == CollectionParams.LockLevel.SHARD && shardId == null) {
      shardId = "MadeUpShardNameWhenNoShardNameWasProvided";
      // Bumping locking level to collection would be instead: lockLevel =
      // CollectionParams.LockLevel.COLLECTION;
    }

    // The first requested lock is a write one (on the target object for the action, depending on
    // lock level), then requesting read locks on "higher" levels (collection > shard > replica here
    // for the level. Note LockLevel "height" is other way around).
    boolean requestWriteLock = true;
    final CollectionParams.LockLevel[] iterationOrder = {
      CollectionParams.LockLevel.REPLICA,
      CollectionParams.LockLevel.SHARD,
      CollectionParams.LockLevel.COLLECTION
    };
    List<DistributedLock> locks = new ArrayList<>(iterationOrder.length);
    for (CollectionParams.LockLevel level : iterationOrder) {
      // This comparison is based on the LockLevel height value that classifies replica > shard >
      // collection.
      if (lockLevel.isHigherOrEqual(level)) {
        locks.add(lockFactory.createLock(requestWriteLock, level, collName, shardId, replicaName));
        requestWriteLock = false;
      }
    }

    return new DistributedMultiLock(locks);
  }
}
