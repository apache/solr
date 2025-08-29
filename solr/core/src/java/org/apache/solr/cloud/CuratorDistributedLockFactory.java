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

import org.apache.curator.framework.CuratorFramework;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;

abstract class CuratorDistributedLockFactory {

  private final CuratorFramework curator;
  private final String rootPath;

  CuratorDistributedLockFactory(CuratorFramework curator, String rootPath) {
    this.curator = curator;
    this.rootPath = rootPath;
  }

  protected final DistributedLock doCreateLock(boolean isWriteLock, String lockPath) {
    try {
      makeLockPath(lockPath);
      return isWriteLock
          ? new CuratorDistributedLocks.Write(curator, lockPath)
          : new CuratorDistributedLocks.Read(curator, lockPath);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  protected StringBuilder getPathPrefix() {
    StringBuilder path = new StringBuilder(100);
    path.append(rootPath).append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR);
    return path;
  }

  private void makeLockPath(String lockNodePath) throws Exception {
    try {
      if (curator.checkExists().forPath(lockNodePath) == null) {
        curator.create().creatingParentsIfNeeded().forPath(lockNodePath, new byte[0]);
      }
    } catch (org.apache.zookeeper.KeeperException.NodeExistsException nee) {
      // Some other thread (on this or another JVM) beat us to create the node, that's ok.
    }
  }
}
