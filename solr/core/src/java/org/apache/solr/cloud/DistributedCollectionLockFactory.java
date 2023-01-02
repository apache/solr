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

import org.apache.solr.cloud.api.collections.CollectionApiLockFactory;
import org.apache.solr.common.params.CollectionParams;

public interface DistributedCollectionLockFactory {
  /**
   * Create a new lock of the specified type (read or write) entering the "competition" for actually
   * getting the lock at the given level for the given path i.e. a lock at {@code collName} or a
   * lock at {@code collName/shardId} or a lock at {@code collName/shardId/replicaName}, depending
   * on the passed {@code level}.
   *
   * <p>The paths are used to define which locks compete with each other (locks of equal paths
   * compete).
   *
   * <p>Upon return from this call, the lock <b>has not been acquired</b> but the it had entered the
   * lock acquiring "competition", and the caller can decide to wait until the lock is granted by
   * calling {@link DistributedLock#waitUntilAcquired()}.<br>
   * Separating the lock creation from lock acquisition allows a more deterministic release of the
   * locks when/if they can't be acquired.
   *
   * <p>Locks at different paths are independent of each other, multiple {@link DistributedLock} are
   * therefore requested for a single operation and are packaged together and returned as an {@link
   * DistributedMultiLock}, see {@link CollectionApiLockFactory#createCollectionApiLock}.
   *
   * @param isWriteLock {@code true} if requesting a write lock, {@code false} for a read lock.
   * @param level The requested locking level. Can be one of:
   *     <ul>
   *       <li>{@link org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION}
   *       <li>{@link org.apache.solr.common.params.CollectionParams.LockLevel#SHARD}
   *       <li>{@link org.apache.solr.common.params.CollectionParams.LockLevel#REPLICA}
   *     </ul>
   *
   * @param collName the collection name, can never be {@code null} as is needed for all locks.
   * @param shardId is ignored and can be {@code null} if {@code level} is {@link
   *     org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION}
   * @param replicaName is ignored and can be {@code null} if {@code level} is {@link
   *     org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION} or {@link
   *     org.apache.solr.common.params.CollectionParams.LockLevel#SHARD}
   * @return a lock instance that must be {@link DistributedLock#release()}'ed in a {@code finally},
   *     regardless of the lock having been acquired or not.
   */
  DistributedLock createLock(
      boolean isWriteLock,
      CollectionParams.LockLevel level,
      String collName,
      String shardId,
      String replicaName);
}
