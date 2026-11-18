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

import static org.apache.solr.cloud.ThreadBoundLock.awaitAcquired;

import java.util.concurrent.Future;
import org.junit.Test;

public class ConfigSetApiLockingTest extends ApiLockingTestBase {
  private static final String CONFIG_SET_NAME_1 = "configSet1";
  private static final String CONFIG_SET_NAME_2 = "configSet2";
  private static final String CONFIG_SET_NAME_3 = "configSet3";

  private static final String BASE_CONFIG_SET_NAME = "baseConfigSet";

  /**
   * Tests the Config Set API locking. All tests run from single test method to save on setup time.
   */
  @Test
  public void monothreadedApiLockTests() throws Exception {
    ConfigSetApiLockFactory apiLockFactory =
        new ConfigSetApiLockFactory(
            new ZkDistributedConfigSetLockFactory(zkClient, LOCK_ROOT, "testNode"));

    monothreadedTests(apiLockFactory);
    multithreadedTests(apiLockFactory);
  }

  private void monothreadedTests(ConfigSetApiLockFactory apiLockingHelper) throws Exception {
    // Config set lock without a base config set
    DistributedMultiLock cs1Lock = apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_1, null);
    assertAcquired("cs1Lock should have been acquired", cs1Lock);

    // This lock does have a base config set
    DistributedMultiLock cs2Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    assertAcquired("cs2Lock should have been acquired", cs2Lock);

    // This lock has the same base config set, but that shouldn't prevent acquiring it
    DistributedMultiLock cs3Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_3, BASE_CONFIG_SET_NAME);
    assertAcquired("cs3Lock should have been acquired", cs3Lock);

    // But we shouldn't be able to lock at this stage the base config set
    DistributedMultiLock csBaseLock =
        apiLockingHelper.createConfigSetApiLock(BASE_CONFIG_SET_NAME, null);
    Future<?> csBaseAcquisition =
        bound(csBaseLock)
            .acquireBlocked("csBaseLock should not have been acquired", zkClient, LOCK_ROOT, 1);

    release(cs2Lock);
    assertFalse(
        "csBaseLock should not have been acquired, cs3Lock still there",
        csBaseAcquisition.isDone());
    release(cs3Lock);
    awaitAcquired("csBaseLock should have been acquired", csBaseAcquisition);

    // Acquiring a lock with a locked base config set should not be possible
    cs2Lock = apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    Future<?> cs2Acquisition =
        bound(cs2Lock)
            .acquireBlocked(
                "cs2Lock should not have been acquired, csBaseLock still held",
                zkClient,
                LOCK_ROOT,
                2);

    release(csBaseLock);
    awaitAcquired("cs2Lock should now be acquired, csBaseLock was freed", cs2Acquisition);

    release(cs2Lock);
  }

  private void multithreadedTests(ConfigSetApiLockFactory apiLockingHelper) throws Exception {
    // This lock does have a base config set
    DistributedMultiLock cs2Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    assertAcquired("cs2Lock should have been acquired", cs2Lock);

    // But we shouldn't be able to lock at this stage the base config set
    DistributedMultiLock csBaseLock =
        apiLockingHelper.createConfigSetApiLock(BASE_CONFIG_SET_NAME, null);
    Future<?> csBaseAcquisition =
        bound(csBaseLock)
            .acquireBlocked("csBaseLock should not have been acquired", zkClient, LOCK_ROOT, 1);

    release(cs2Lock);
    awaitAcquired(
        "base config set lock should have been acquired now that other lock was released",
        csBaseAcquisition);
  }
}
