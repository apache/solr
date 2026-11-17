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

import static org.junit.Assert.assertFalse;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Runs a lock's acquire and release on one dedicated thread, since Curator locks belong to the
 * thread that acquired them.
 */
public class ThreadBoundLock implements Closeable {

  public static final int TIMEOUT_MS = 10000;

  private final ExecutorService executor =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("lockTest"));
  private final Runnable waitUntilAcquired;
  private final Runnable release;
  private volatile Future<?> lastAcquire;

  public ThreadBoundLock(Runnable waitUntilAcquired, Runnable release) {
    this.waitUntilAcquired = waitUntilAcquired;
    this.release = release;
  }

  public Future<?> acquire() {
    lastAcquire = executor.submit(waitUntilAcquired);
    return lastAcquire;
  }

  /**
   * Starts an acquire and returns once it has added {@code newNodes} ephemeral nodes under {@code
   * root} without completing, failing if the acquire completes instead.
   */
  public Future<?> acquireBlocked(String message, SolrZkClient zkClient, String root, int newNodes)
      throws Exception {
    int expected = countEphemeralNodes(zkClient, root) + newNodes;
    Future<?> acquisition = acquire();
    new TimeOut(TIMEOUT_MS, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME)
        .waitFor(
            message + ": waiter node not seen in ZooKeeper",
            () -> {
              try {
                return acquisition.isDone() || countEphemeralNodes(zkClient, root) == expected;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    assertFalse(message, acquisition.isDone());
    return acquisition;
  }

  private static int countEphemeralNodes(SolrZkClient zkClient, String path)
      throws KeeperException, InterruptedException {
    int count = 0;
    try {
      for (String child : zkClient.getChildren(path, null)) {
        String childPath = path + "/" + child;
        Stat stat = zkClient.exists(childPath, null);
        if (stat == null) {
          continue;
        }
        count += stat.getEphemeralOwner() != 0 ? 1 : countEphemeralNodes(zkClient, childPath);
      }
    } catch (KeeperException.NoNodeException e) {
      return count;
    }
    return count;
  }

  public static ThreadBoundLock bound(
      Map<DistributedMultiLock, ThreadBoundLock> boundLocks, DistributedMultiLock lock) {
    return boundLocks.computeIfAbsent(
        lock, l -> new ThreadBoundLock(l::waitUntilAcquired, l::release));
  }

  public static void awaitAcquired(String message, Future<?> acquisition) throws Exception {
    try {
      acquisition.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new AssertionError(message, e);
    }
  }

  /** Interrupts a blocked acquire, then releases whatever was acquired, on the lock's thread. */
  public void abandon() throws Exception {
    lastAcquire.cancel(true);
    release();
  }

  /** Only valid once {@link #acquire()} completed, since a blocked acquire occupies the thread. */
  public void release() throws Exception {
    executor.submit(release).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    ExecutorUtil.shutdownNowAndAwaitTermination(executor);
  }

  /** Waits until the directory of {@code heldLockId} has {@code count} children. */
  public static void waitForLockNodes(SolrZkClient zkClient, String heldLockId, int count)
      throws Exception {
    String dir = heldLockId.substring(0, heldLockId.lastIndexOf('/'));
    new TimeOut(TIMEOUT_MS, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME)
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
