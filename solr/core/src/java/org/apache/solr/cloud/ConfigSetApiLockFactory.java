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

import java.util.ArrayList;
import java.util.List;

/**
 * This class implements a higher level locking abstraction for the Config Set API using lower level
 * read and write locks.
 */
public class ConfigSetApiLockFactory {

  private final DistributedConfigSetLockFactory lockFactory;

  public ConfigSetApiLockFactory(DistributedConfigSetLockFactory lockFactory) {
    this.lockFactory = lockFactory;
  }

  /**
   * For the {@link org.apache.solr.common.params.CollectionParams.LockLevel} of the passed {@code
   * action}, obtains the required locks (if any) and returns.
   *
   * <p>This method obtains a write lock on {@code configSetName} as well as (when not {@code
   * null}), a read lock on {@code baseConfigSetName}.
   *
   * @return a lock that once {@link DistributedMultiLock#isAcquired()} guarantees the corresponding
   *     Config Set API command can execute safely. The returned lock <b>MUST</b> be {@link
   *     DistributedMultiLock#release()} no matter what once no longer needed as otherwise it would
   *     prevent other threads from locking.
   */
  public DistributedMultiLock createConfigSetApiLock(
      String configSetName, String baseConfigSetName) {
    List<DistributedLock> locks = new ArrayList<>(2);

    locks.add(lockFactory.createLock(true, configSetName));
    if (baseConfigSetName != null) {
      locks.add(lockFactory.createLock(false, baseConfigSetName));
    }

    return new DistributedMultiLock(locks);
  }
}
