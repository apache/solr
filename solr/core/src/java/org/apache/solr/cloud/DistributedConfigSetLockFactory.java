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

public interface DistributedConfigSetLockFactory {
  /**
   * Create a new lock of the specified type (read or write) entering the "competition" for actually
   * getting the lock for the {@code configSetName}
   *
   * <p>Upon return from this call, the lock <b>has not been acquired</b> but the it had entered the
   * lock acquiring "competition", and the caller can decide to wait until the lock is granted by
   * calling {@link DistributedLock#waitUntilAcquired()}.<br>
   * Separating the lock creation from lock acquisition allows a more deterministic release of the
   * locks when/if they can't be acquired.
   *
   * <p>Locks at different paths are independent of each other, multiple {@link DistributedLock} are
   * therefore requested for a single operation and are packaged together and returned as an {@link
   * DistributedMultiLock}, see {@link
   * org.apache.solr.cloud.api.collections.CollectionApiLockFactory#createCollectionApiLock} or
   * {@link org.apache.solr.cloud.ConfigSetApiLockFactory#createConfigSetApiLock}.
   *
   * @param isWriteLock {@code true} if requesting a write lock, {@code false} for a read lock.
   * @param configSetName the config set name, can never be {@code null}.
   * @return a lock instance that must be {@link DistributedLock#release()}'ed in a {@code finally},
   *     regardless of the lock having been acquired or not.
   */
  DistributedLock createLock(boolean isWriteLock, String configSetName);
}
