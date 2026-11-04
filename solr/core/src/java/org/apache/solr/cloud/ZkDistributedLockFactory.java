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

import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

abstract class ZkDistributedLockFactory {

  static final int MAX_CREATE_ATTEMPTS = 3;

  private final SolrZkClient zkClient;
  private final String rootPath;

  ZkDistributedLockFactory(SolrZkClient zkClient, String rootPath) {
    this.zkClient = zkClient;
    this.rootPath = rootPath;
  }

  protected DistributedLock doCreateLock(
      boolean isWriteLock, String lockPath, String lockIdToMirror) {
    try {
      for (int attempt = 1; ; attempt++) {
        try {
          return isWriteLock
              ? new ZkDistributedLock.Write(zkClient, lockPath, lockIdToMirror)
              : new ZkDistributedLock.Read(zkClient, lockPath, lockIdToMirror);
        } catch (KeeperException.NoNodeException e) {
          if (attempt == MAX_CREATE_ATTEMPTS) {
            throw e;
          }
          // Lock dirs are containers, so ZK drops them when empty. Recreate and try again.
          makeLockPath(lockPath);
        }
      }
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  protected StringBuilder getPathPrefix() {
    StringBuilder path = new StringBuilder(100);
    path.append(rootPath).append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR);
    return path;
  }

  private void makeLockPath(String lockPath) throws KeeperException, InterruptedException {
    if (!zkClient.exists(rootPath)) {
      zkClient.makePath(rootPath, false);
    }
    StringBuilder path = new StringBuilder(rootPath);
    for (String node :
        lockPath
            .substring(rootPath.length() + 1)
            .split(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR)) {
      path.append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR).append(node);
      try {
        zkClient.create(path.toString(), null, CreateMode.CONTAINER);
      } catch (KeeperException.NodeExistsException nee) {
        // Some other thread (on this or another JVM) beat us to create the node, that's ok.
      } catch (KeeperException.NoNodeException nne) {
        // A parent got removed while we were here. The caller retries.
        return;
      }
    }
  }

  /**
   * Deletes the empty lock dirs under {@code path}, deepest first. Lock nodes and dirs still
   * holding them are left alone.
   */
  protected void deleteEmptyDirs(String path) throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(path, null);
    if (stat == null || stat.getEphemeralOwner() != 0) {
      return;
    }
    try {
      for (String child : zkClient.getChildren(path, null)) {
        deleteEmptyDirs(
            path + DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR + child);
      }
      zkClient.delete(path, -1);
    } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException e) {
      // Already gone, or a lock is in use underneath. Either way, nothing to do.
    }
  }
}
