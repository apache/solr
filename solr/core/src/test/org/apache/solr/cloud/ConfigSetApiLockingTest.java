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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.Test;

public class ConfigSetApiLockingTest extends SolrTestCaseJ4 {
  private static final String CONFIG_SET_NAME_1 = "configSet1";
  private static final String CONFIG_SET_NAME_2 = "configSet2";
  private static final String CONFIG_SET_NAME_3 = "configSet3";

  private static final String BASE_CONFIG_SET_NAME = "baseConfigSet";

  static final int TIMEOUT = 10000;

  /**
   * Tests the Config Set API locking. All tests run from single test method to save on setup time.
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
        ConfigSetApiLockFactory apiLockFactory =
            new ConfigSetApiLockFactory(
                new ZkDistributedConfigSetLockFactory(zkClient, "/apiLockTestRoot"));

        monothreadedTests(apiLockFactory);
        multithreadedTests(apiLockFactory);
      }
    } finally {
      server.shutdown();
    }
  }

  private void monothreadedTests(ConfigSetApiLockFactory apiLockingHelper) {
    // Config set lock without a base config set
    DistributedMultiLock cs1Lock = apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_1, null);
    assertTrue("cs1Lock should have been acquired", cs1Lock.isAcquired());

    // This lock does have a base config set
    DistributedMultiLock cs2Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    assertTrue("cs2Lock should have been acquired", cs2Lock.isAcquired());

    // This lock has the same base config set, but that shouldn't prevent acquiring it
    DistributedMultiLock cs3Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_3, BASE_CONFIG_SET_NAME);
    assertTrue("cs3Lock should have been acquired", cs3Lock.isAcquired());

    // But we shouldn't be able to lock at this stage the base config set
    DistributedMultiLock csBaseLock =
        apiLockingHelper.createConfigSetApiLock(BASE_CONFIG_SET_NAME, null);
    assertFalse("csBaseLock should not have been acquired", csBaseLock.isAcquired());

    cs2Lock.release();
    assertFalse(
        "csBaseLock should not have been acquired, cs3Lock still there", csBaseLock.isAcquired());
    cs3Lock.release();
    assertTrue("csBaseLock should have been acquired", csBaseLock.isAcquired());

    // Acquiring a lock with a locked base config set should not be possible
    cs2Lock = apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    assertFalse(
        "cs2Lock should not have been acquired, csBaseLock still held", cs2Lock.isAcquired());

    csBaseLock.release();
    assertTrue("cs2Lock should now be acquired, csBaseLock was freed", cs2Lock.isAcquired());

    cs2Lock.release();
  }

  private void multithreadedTests(ConfigSetApiLockFactory apiLockingHelper) throws Exception {
    // This lock does have a base config set
    DistributedMultiLock cs2Lock =
        apiLockingHelper.createConfigSetApiLock(CONFIG_SET_NAME_2, BASE_CONFIG_SET_NAME);
    assertTrue("cs2Lock should have been acquired", cs2Lock.isAcquired());

    // But we shouldn't be able to lock at this stage the base config set
    DistributedMultiLock csBaseLock =
        apiLockingHelper.createConfigSetApiLock(BASE_CONFIG_SET_NAME, null);
    assertFalse("csBaseLock should not have been acquired", csBaseLock.isAcquired());

    // Wait for acquisition of the base config set lock on another thread (and be notified via a
    // latch)
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread(
            () -> {
              csBaseLock.waitUntilAcquired();
              // countDown() will not be called if waitUntilAcquired() threw an exception
              latch.countDown();
            })
        .start();

    // Wait for the thread to start and to get blocked in waitUntilAcquired()
    // (thread start could have been checked more reliably using another latch, and verifying the
    // thread is in waitUntilAcquired done through that thread stacktrace, but that would be
    // overkill compared to the very slight race condition of waiting 30ms, but a race that would
    // not cause the test to fail since we're testing... that nothing happened yet).
    Thread.sleep(30);

    assertEquals(
        "we should not have been notified that base config set lock was acquired",
        1,
        latch.getCount());
    assertFalse("base config set lock should not have been acquired", csBaseLock.isAcquired());

    cs2Lock.release();
    assertTrue(
        "base config set lock should have been acquired now that other lock was released",
        csBaseLock.isAcquired());

    // Wait for the Zookeeper watch to fire + the thread to be unblocked and countdown the latch
    // We'll wait up to 10 seconds here, so should be safe even if GC is extraordinarily high with a
    // pause
    int i = 0;
    while (i < 1000 && latch.getCount() != 0) {
      Thread.sleep(10);
      i++;
    }
    assertEquals(
        "we should have been notified that the base config set lock was acquired",
        0,
        latch.getCount());
  }
}
