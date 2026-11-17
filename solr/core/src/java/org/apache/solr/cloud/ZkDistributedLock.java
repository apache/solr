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

import java.util.function.Supplier;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

/**
 * A lock to be used across cluster nodes based on Zookeeper. {@link Owned} wraps a Curator mutex,
 * {@link Mirrored} shares a node held by the caller.
 */
abstract class ZkDistributedLock implements DistributedLock {

  /** Marker Curator puts in the node name of a write lock. */
  private static final String CURATOR_WRITE_LOCK_NAME = "__WRIT__";

  /** A lock that competes in ZooKeeper, backed by a Curator mutex. */
  static final class Owned extends ZkDistributedLock {
    private final InterProcessMutex mutex;
    private final String lockPath;
    private final Supplier<String> lockPathSupplier;
    private volatile String lockId;
    private boolean acquired;
    private boolean released;

    Owned(InterProcessMutex mutex, String lockPath, Supplier<String> lockPathSupplier) {
      this.mutex = mutex;
      this.lockPath = lockPath;
      this.lockPathSupplier = lockPathSupplier;
    }

    @Override
    public void waitUntilAcquired() {
      if (released) {
        throw new IllegalStateException(
            "Bug. waitUntilAcquired() should not be called after release(). " + this);
      }
      if (acquired) {
        return;
      }
      try {
        mutex.acquire();
        lockId = lockPathSupplier.get();
        acquired = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SERVER_ERROR, e);
      } catch (Exception e) {
        throw new SolrException(SERVER_ERROR, e);
      }
    }

    @Override
    public void release() {
      released = true;
      if (!acquired) {
        return;
      }
      try {
        mutex.release();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SERVER_ERROR, e);
      } catch (Exception e) {
        throw new SolrException(SERVER_ERROR, e);
      }
    }

    @Override
    public String getLockId() {
      return lockId;
    }

    @Override
    public boolean isMirroringLock() {
      return false;
    }

    @Override
    public String toString() {
      return lockId != null ? lockId : lockPath;
    }
  }

  /** A lock that shares the node of another lock held by the caller. */
  static final class Mirrored extends ZkDistributedLock {
    private final SolrZkClient zkClient;
    private final String lockId;

    Mirrored(SolrZkClient zkClient, String lockDir, boolean isWriteLock, String lockIdToMirror) {
      if (!lockIdToMirror.startsWith(
          lockDir + DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR)) {
        throw new SolrException(
            SERVER_ERROR,
            "Requested lock with path: "
                + lockDir
                + " cannot mirror the callingLock with id: "
                + lockIdToMirror);
      }
      if (isWriteLock
          && !ZKPaths.getNodeFromPath(lockIdToMirror).contains(CURATOR_WRITE_LOCK_NAME)) {
        throw new SolrException(
            SERVER_ERROR, "Cannot mirror lock " + lockIdToMirror + " with a write lock");
      }
      this.zkClient = zkClient;
      this.lockId = lockIdToMirror;
    }

    @Override
    public void waitUntilAcquired() {
      try {
        if (!zkClient.exists(lockId)) {
          throw new SolrException(SERVER_ERROR, "Missing mirrored lock node " + lockId);
        }
      } catch (KeeperException e) {
        throw new SolrException(SERVER_ERROR, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SERVER_ERROR, e);
      }
    }

    @Override
    public void release() {}

    @Override
    public String getLockId() {
      return lockId;
    }

    @Override
    public boolean isMirroringLock() {
      return true;
    }

    @Override
    public String toString() {
      return lockId;
    }
  }
}
