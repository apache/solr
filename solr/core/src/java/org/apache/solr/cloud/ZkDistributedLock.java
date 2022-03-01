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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A lock to be used across cluster nodes based on Zookeeper. Used for the distributed Collection
 * API and distributed Config Set API implementations.
 *
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper
 *     lock recipe</a>
 */
abstract class ZkDistributedLock implements DistributedLock {
  /** End of the lock node name prefix before the sequential part */
  static final char LOCK_PREFIX_SUFFIX = '_';
  /** Prefix of EPHEMERAL read lock node names */
  static final String READ_LOCK_PREFIX = "R" + LOCK_PREFIX_SUFFIX;
  /** Prefix of EPHEMERAL write lock node names */
  static final String WRITE_LOCK_PREFIX = "W" + LOCK_PREFIX_SUFFIX;

  /** Read lock. */
  static class Read extends ZkDistributedLock {
    protected Read(SolrZkClient zkClient, String lockPath)
        throws KeeperException, InterruptedException {
      super(zkClient, lockPath, READ_LOCK_PREFIX);
    }

    @Override
    boolean isBlockedByNodeType(String otherLockName) {
      // A read lock is only blocked by a lower numbered write lock
      // Lower numbered read locks are ok, they can coexist.
      return otherLockName.startsWith(WRITE_LOCK_PREFIX);
    }
  }

  /** Write lock. */
  static class Write extends ZkDistributedLock {
    protected Write(SolrZkClient zkClient, String lockPath)
        throws KeeperException, InterruptedException {
      super(zkClient, lockPath, WRITE_LOCK_PREFIX);
    }

    @Override
    boolean isBlockedByNodeType(String otherLockName) {
      // A write lock is blocked by another read or write lock with a lower sequence number
      return true;
    }
  }

  private final SolrZkClient zkClient;
  private final String lockDir;
  private final String lockNode;
  protected final long sequence;
  protected volatile boolean released = false;

  protected ZkDistributedLock(SolrZkClient zkClient, String lockDir, String lockNodePrefix)
      throws KeeperException, InterruptedException {
    this.zkClient = zkClient;
    this.lockDir = lockDir;

    // Create the SEQUENTIAL EPHEMERAL node. We enter the locking rat race here. We MUST eventually
    // call release() or we block others.
    lockNode =
        zkClient.create(
            lockDir
                + DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR
                + lockNodePrefix,
            null,
            CreateMode.EPHEMERAL_SEQUENTIAL,
            false);

    sequence = getSequenceFromNodename(lockNode);
  }

  private static class DeletedNodeWatcher implements Watcher {
    final CountDownLatch latch;
    final String nodeBeingWatched;
    String errorMessage = null; // non null means error

    DeletedNodeWatcher(String nodeBeingWatched) {
      this.latch = new CountDownLatch(1);
      this.nodeBeingWatched = nodeBeingWatched;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.None) {
        return;
      } else if (event.getType() != Event.EventType.NodeDeleted) {
        synchronized (this) {
          errorMessage =
              "Received unexpected watch event " + event.getType() + " on " + nodeBeingWatched;
        }
      }
      latch.countDown();
    }

    void await() throws InterruptedException {
      latch.await();
      synchronized (this) {
        if (errorMessage != null) {
          throw new SolrException(SERVER_ERROR, errorMessage);
        }
      }
    }
  }

  @Override
  public void waitUntilAcquired() {
    try {
      if (released) {
        throw new IllegalStateException(
            "Bug. waitUntilAcquired() should not be called after release(). " + lockNode);
      }
      String nodeToWatch = nodeToWatch();
      while (nodeToWatch != null) {
        final DeletedNodeWatcher watcher = new DeletedNodeWatcher(nodeToWatch);
        if (zkClient.exists(nodeToWatch, watcher, true) != null) {
          watcher.await();
        }
        nodeToWatch = nodeToWatch();
      }
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void release() {
    try {
      zkClient.delete(lockNode, -1, true);
      released = true;
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public boolean isAcquired() {
    try {
      if (released) {
        throw new IllegalStateException(
            "Bug. isAcquired() should not be called after release(). " + lockNode);
      }
      return nodeToWatch() == null;
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * @return Another lock node (complete path) that must go away for us to acquire the lock, or
   *     {@code null} if the lock is ours.
   */
  String nodeToWatch() throws KeeperException, InterruptedException {
    List<String> locks = zkClient.getChildren(lockDir, null, true);
    boolean foundSelf = false; // For finding bugs or ZK bad behavior
    // We deviate from the ZK recipe here: we do not sort the list of nodes, and we stop waiting on
    // the first one we find that blocks us. This is done in O(n), whereas sorting is more
    // expensive. And we iterate only once over the set of children. Might cause more wakeups than
    // needed though (multiple locks watching the same one rather than one another) but lock
    // contention expected low (how many concurrent and conflicting Collection API requests a
    // reasonable user can issue?) and the code below is simpler. Changing to a sorted "optimal"
    // approach implies only changes in this method.
    for (String lock : locks) {
      long seq = getSequenceFromNodename(lock);
      if (seq == sequence) {
        foundSelf = true;
      } else if (seq < sequence && isBlockedByNodeType(lock)) {
        // Return the full path to the node to watch
        return lockDir + DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR + lock;
      }
      // seq is bigger than sequence, can't block us. If the iteration was sorted we could avoid
      // iterating over those.
    }
    if (!foundSelf) {
      // If this basic assumption doesn't hold with Zookeeper, we're in deep trouble. And not only
      // here.
      throw new SolrException(SERVER_ERROR, "Missing lock node " + lockNode);
    }

    // Didn't return early on any other blocking lock, means we own it
    return null;
  }

  /**
   * @param otherLockName name of a competing lock node. Used to find out the type of the lock (read
   *     or write)
   * @return {@code true} if a lock node of the given type with a lower sequence number blocks us
   *     from acquiring the lock.
   */
  abstract boolean isBlockedByNodeType(String otherLockName);

  static long getSequenceFromNodename(String lockNode) {
    // Javadoc of ZooKeeper.create() specifies "The sequence number is always fixed length of 10
    // digits, 0 padded"
    // for sequential nodes
    final int SEQUENCE_LENGTH = 10;
    // Before the sequence we have our specific char
    if (lockNode.charAt(lockNode.length() - SEQUENCE_LENGTH - 1) != LOCK_PREFIX_SUFFIX) {
      throw new SolrException(SERVER_ERROR, "Unexpected lock node path " + lockNode);
    }

    return Long.parseLong(lockNode.substring(lockNode.length() - SEQUENCE_LENGTH));
  }

  @Override
  public String toString() {
    return lockNode;
  }
}
