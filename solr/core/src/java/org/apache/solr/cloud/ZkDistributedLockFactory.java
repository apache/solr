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

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

abstract class ZkDistributedLockFactory {

  private final SolrZkClient zkClient;
  private final String rootPath;

  ZkDistributedLockFactory(SolrZkClient zkClient, String rootPath) {
    this.zkClient = zkClient;
    this.rootPath = rootPath;
  }

  protected DistributedLock doCreateLock(
      boolean isWriteLock, String lockPath, String lockIdToMirror) {
    if (StrUtils.isBlank(lockIdToMirror)) {
      // Empty payload: otherwise Curator stores the local IP address in every lock node
      InterProcessReadWriteLock readWriteLock =
          new InterProcessReadWriteLock(zkClient.getCuratorFramework(), lockPath, new byte[0]);
      if (isWriteLock) {
        InterProcessReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
        return new ZkDistributedLock.Owned(writeLock, lockPath, writeLock::getLockPath);
      }
      InterProcessReadWriteLock.ReadLock readLock = readWriteLock.readLock();
      return new ZkDistributedLock.Owned(readLock, lockPath, readLock::getLockPath);
    }
    return new ZkDistributedLock.Mirrored(zkClient, lockPath, isWriteLock, lockIdToMirror);
  }

  protected StringBuilder getPathPrefix() {
    StringBuilder path = new StringBuilder(100);
    path.append(rootPath).append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR);
    return path;
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
