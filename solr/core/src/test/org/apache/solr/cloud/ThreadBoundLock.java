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

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;

/**
 * Runs a lock's acquire and release on one dedicated thread, since Curator locks belong to the
 * thread that acquired them.
 */
public class ThreadBoundLock implements Closeable {

  static final int RELEASE_TIMEOUT_MS = ZkDistributedLockTest.TIMEOUT;
  static final int LOCK_NODES_TIMEOUT_MS = ZkDistributedLockTest.TIMEOUT;

  private final ExecutorService executor =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("lockTest"));
  private final Runnable waitUntilAcquired;
  private final Runnable release;

  public ThreadBoundLock(Runnable waitUntilAcquired, Runnable release) {
    this.waitUntilAcquired = waitUntilAcquired;
    this.release = release;
  }

  public Future<?> acquire() {
    return executor.submit(waitUntilAcquired);
  }

  /** Only valid once {@link #acquire()} completed, since a blocked acquire occupies the thread. */
  public void release() throws Exception {
    executor.submit(release).get(RELEASE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  /** Interrupts a blocked acquire. */
  @Override
  public void close() {
    ExecutorUtil.shutdownNowAndAwaitTermination(executor);
  }

  /** Waits until the directory of {@code heldLockId} has {@code count} children. */
  public static void waitForLockNodes(SolrZkClient zkClient, String heldLockId, int count)
      throws Exception {
    String dir = heldLockId.substring(0, heldLockId.lastIndexOf('/'));
    new TimeOut(LOCK_NODES_TIMEOUT_MS, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "expected " + count + " lock nodes in " + dir,
            () -> {
              try {
                return zkClient.getChildren(dir, null).size() == count;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }
}
