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
import static org.apache.solr.cloud.ThreadBoundLock.awaitAcquired;

import java.util.IdentityHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.After;
import org.junit.Before;

/** Shared ZooKeeper setup and thread-bound lock helpers for the API locking tests. */
public abstract class ApiLockingTestBase extends SolrTestCaseJ4 {

  protected static final String LOCK_ROOT = "/apiLockTestRoot";

  private final IdentityHashMap<DistributedMultiLock, ThreadBoundLock> boundLocks =
      new IdentityHashMap<>();
  private ZkTestServer server;
  protected SolrZkClient zkClient;

  @Before
  public void startZk() throws Exception {
    server = new ZkTestServer(createTempDir("zkData"));
    server.run();
    zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();
  }

  @After
  public void stopZk() throws Exception {
    boundLocks.values().forEach(ThreadBoundLock::close);
    zkClient.close();
    server.shutdown();
  }

  protected ThreadBoundLock bound(DistributedMultiLock lock) {
    return ThreadBoundLock.bound(boundLocks, lock);
  }

  protected void assertAcquired(String message, DistributedMultiLock lock) throws Exception {
    awaitAcquired(message, bound(lock).acquire());
  }

  protected void release(DistributedMultiLock lock) throws Exception {
    bound(lock).release();
  }
}
