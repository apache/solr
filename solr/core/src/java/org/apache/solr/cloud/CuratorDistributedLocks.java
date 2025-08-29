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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * A distributed lock implementation using Apache Curator and ZooKeeper.
 *
 * <p>This lock is used to coordinate actions across cluster nodes, such as in the distributed
 * Collection API and Config Set API in Solr. It leverages Curator's {@link CuratorFramework} for
 * all ZooKeeper operations.
 *
 * <p><b>Lock acquisition strategy:</b> watches the first blocking node it finds (i.e., any
 * lower-numbered write lock for read locks, or any lower-numbered lock for write locks), rather
 * than sorting children or only watching the immediate predecessor node as in the standard
 * ZooKeeper lock recipe. This approach may cause more wakeups (thundering herd effect), but is
 * simpler and well-suited to Solr's expected lock contention and operational requirements.
 *
 * <p>Read locks are only blocked by lower-numbered write locks. Write locks are blocked by any
 * lower-numbered lock. Lock nodes are created as ephemeral sequential znodes with "R_" or "W_"
 * prefixes.
 */
class CuratorDistributedLocks {

  /** End of the lock node name prefix before the sequential part */
  static final char LOCK_PREFIX_SUFFIX = '_';

  /** Prefix of EPHEMERAL read lock node names */
  static final String READ_LOCK_PREFIX = "R" + LOCK_PREFIX_SUFFIX;

  /** Prefix of EPHEMERAL write lock node names */
  static final String WRITE_LOCK_PREFIX = "W" + LOCK_PREFIX_SUFFIX;

  private CuratorDistributedLocks() {
    /* not instantiable */
  }

  abstract static class CuratorLockBase implements DistributedLock {
    protected final CuratorFramework curator;
    protected final String lockDir;
    protected final String lockNode;
    protected final long sequence;
    protected volatile boolean released = false;

    protected CuratorLockBase(CuratorFramework curator, String lockDir, String lockNodePrefix)
        throws Exception {
      this.curator = curator;
      this.lockDir = lockDir;
      this.lockNode =
          curator
              .create()
              .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
              .forPath(lockDir + "/" + lockNodePrefix, new byte[0]);
      this.sequence = getSequenceFromNodename(lockNode);
    }

    @Override
    public void waitUntilAcquired() {
      try {
        if (released) {
          throw new IllegalStateException(
              "waitUntilAcquired() called after release(): " + lockNode);
        }
        // Watch the first blocking node found; may cause more wakeups.
        String nodeToWatch = nodeToWatch();
        while (nodeToWatch != null) {
          final DeletedNodeWatcher watcher = new DeletedNodeWatcher();
          if (curator.checkExists().usingWatcher(watcher).forPath(nodeToWatch) != null) {
            watcher.await();
          }
          nodeToWatch = nodeToWatch();
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public void release() {
      try {
        curator.delete().forPath(lockNode);
        released = true;
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    public boolean isAcquired() {
      if (released) {
        throw new IllegalStateException("isAcquired() called after release(): " + lockNode);
      }
      try {
        return nodeToWatch() == null;
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    protected abstract boolean isBlockedByNodeType(String otherLockName);

    protected String nodeToWatch() throws Exception {
      List<String> locks = curator.getChildren().forPath(lockDir);
      boolean foundSelf = false;
      for (String lock : locks) {
        long seq = getSequenceFromNodename(lock);
        if (seq == sequence) {
          foundSelf = true;
        } else if (seq < sequence && isBlockedByNodeType(lock)) {
          return lockDir + "/" + lock;
        }
      }
      if (!foundSelf) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Missing lock node " + lockNode);
      }
      return null;
    }

    static long getSequenceFromNodename(String lockNode) {
      final int SEQUENCE_LENGTH = 10;
      int idx = lockNode.length() - SEQUENCE_LENGTH - 1;
      if (lockNode.charAt(idx) != '_') {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Unexpected lock node path " + lockNode);
      }
      return Long.parseLong(lockNode.substring(lockNode.length() - SEQUENCE_LENGTH));
    }

    private static class DeletedNodeWatcher implements CuratorWatcher {
      private final CountDownLatch latch = new CountDownLatch(1);
      private volatile String errorMessage = null;

      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == EventType.None) {
          return;
        } else if (event.getType() != EventType.NodeDeleted) {
          errorMessage = "Received unexpected watch event " + event.getType();
        }
        latch.countDown();
      }

      void await() throws InterruptedException {
        latch.await();
        if (errorMessage != null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, errorMessage);
        }
      }
    }
  }

  static class Write extends CuratorLockBase {
    public Write(CuratorFramework curator, String path) throws Exception {
      super(curator, path, WRITE_LOCK_PREFIX);
    }

    @Override
    protected boolean isBlockedByNodeType(String otherLockName) {
      // Write lock is blocked by any lower-numbered lock
      return true;
    }
  }

  static class Read extends CuratorLockBase {
    public Read(CuratorFramework curator, String path) throws Exception {
      super(curator, path, READ_LOCK_PREFIX);
    }

    @Override
    protected boolean isBlockedByNodeType(String otherLockName) {
      // Read lock is only blocked by lower-numbered write locks
      return otherLockName.startsWith(WRITE_LOCK_PREFIX);
    }
  }
}
