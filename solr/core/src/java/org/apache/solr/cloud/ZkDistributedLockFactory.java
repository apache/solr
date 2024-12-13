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

abstract class ZkDistributedLockFactory {

  private final SolrZkClient zkClient;
  private final String rootPath;

  ZkDistributedLockFactory(SolrZkClient zkClient, String rootPath) {
    this.zkClient = zkClient;
    this.rootPath = rootPath;
  }

  protected DistributedLock doCreateLock(boolean isWriteLock, String lockPath) {
    try {
      // TODO optimize by first attempting to create the ZkDistributedLock without calling
      // makeLockPath() and only call it if the lock creation fails. This will be less costly on
      // high contention (and slightly more on low contention)
      makeLockPath(lockPath);

      return isWriteLock
          ? new ZkDistributedLock.Write(zkClient, lockPath)
          : new ZkDistributedLock.Read(zkClient, lockPath);
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

  private void makeLockPath(String lockNodePath) throws KeeperException, InterruptedException {
    try {
      if (!zkClient.exists(lockNodePath, true)) {
        zkClient.makePath(lockNodePath, new byte[0], CreateMode.PERSISTENT, true);
      }
    } catch (KeeperException.NodeExistsException nee) {
      // Some other thread (on this or another JVM) beat us to create the node, that's ok.
    }
  }
}
