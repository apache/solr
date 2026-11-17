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

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.Test;

public class ConfigSetApiLockingTest extends SolrTestCaseJ4 {
  private static final String CONFIG_SET_NAME_1 = "configSet1";
  private static final String CONFIG_SET_NAME_2 = "configSet2";
  private static final String CONFIG_SET_NAME_3 = "configSet3";

  private static final String BASE_CONFIG_SET_NAME = "baseConfigSet";

  static final int TIMEOUT = 10000;

  private final Map<DistributedMultiLock, ThreadBoundLock> boundLocks = new IdentityHashMap<>();
  private SolrZkClient zkClient;

  /**
   * Tests the Config Set API locking. All tests run from single test method to save on setup time.
   */
  @Test
  public void monothreadedApiLockTests() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zkData"));
    try {
      server.run();
      try (SolrZkClient client =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        zkClient = client;
        ConfigSetApiLockFactory apiLockFactory =
            new ConfigSetApiLockFactory(
                new ZkDistributedConfigSetLockFactory(zkClient, "/apiLockTestRoot"));

        try {
          monothreadedTests(apiLockFactory);
          multithreadedTests(apiLockFactory);
        } finally {
          boundLocks.values().forEach(ThreadBoundLock::close);
        }
      }
    } finally {
      server.shutdown();
    }
  }

  private ThreadBoundLock bound(DistributedMultiLock lock) {
    return boundLocks.computeIfAbsent(
        lock, l -> new ThreadBoundLock(l::waitUntilAcquired, l::release));
  }

  private void assertAcquired(String message, DistributedMultiLock lock) throws Exception {
    awaitAcquired(message, bound(lock).acquire());
  }

  private void awaitAcquired(String message, Future<?> acquisition) throws Exception {
    try {
      acquisition.get(TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new AssertionError(message, e);
    }
  }

  private Future<?> assertBlocked(
      String message, DistributedMultiLock lock, DistributedMultiLock holder, int level, int count)
      throws Exception {
    Future<?> acquisition = bound(lock).acquire();
    ThreadBoundLock.waitForLockNodes(
        zkClient, DistributedMultiLock.splitLockIds(holder.getLockId()).get(level), count);
    assertFalse(message, acquisition.isDone());
    return acquisition;
  }

  private void release(DistributedMultiLock lock) throws Exception {
    bound(lock).release();
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
        assertBlocked("csBaseLock should not have been acquired", csBaseLock, cs2Lock, 1, 3);

    release(cs2Lock);
    assertFalse(
        "csBaseLock should not have been acquired, cs3Lock still there",
        csBaseAcquisition.isDone());
    release(cs3Lock);
    awaitAcquired("csBaseLock should have been acquired", csBaseAcquisition);

    // Acquiring a lock with a locked base config set should not be possible
    cs2Lock = apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    Future<?> cs2Acquisition =
        assertBlocked(
            "cs2Lock should not have been acquired, csBaseLock still held",
            cs2Lock,
            csBaseLock,
            0,
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
        assertBlocked("csBaseLock should not have been acquired", csBaseLock, cs2Lock, 1, 2);

    release(cs2Lock);
    awaitAcquired(
        "base config set lock should have been acquired now that other lock was released",
        csBaseAcquisition);
  }
}
