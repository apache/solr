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

import java.util.Objects;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams;

/**
 * A distributed lock implementation using Zookeeper "directory" nodes created within a collection
 * znode hierarchy, for use with the distributed Collection API implementation. The locks are
 * implemented using ephemeral nodes placed below the "directory" nodes.
 *
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper
 *     lock recipe</a>
 */
public class ZkDistributedCollectionLockFactory extends ZkDistributedLockFactory
    implements DistributedCollectionLockFactory {

  public ZkDistributedCollectionLockFactory(SolrZkClient zkClient, String rootPath) {
    super(zkClient, rootPath);
  }

  @Override
  public DistributedLock createLock(
      boolean isWriteLock,
      CollectionParams.LockLevel level,
      String collName,
      String shardId,
      String replicaName) {
    Objects.requireNonNull(collName, "collName can't be null");
    if (level != CollectionParams.LockLevel.COLLECTION) {
      Objects.requireNonNull(
          shardId, "shardId can't be null when getting lock for shard or replica");
    }
    if (level == CollectionParams.LockLevel.REPLICA) {
      Objects.requireNonNull(
          replicaName, "replicaName can't be null when getting lock for replica");
    }

    String lockPath = getLockPath(level, collName, shardId, replicaName);
    return doCreateLock(isWriteLock, lockPath);
  }

  /**
   * Returns the Zookeeper path to the lock, creating missing nodes if needed. Note that the
   * complete lock hierarchy (/locks and below) can be deleted if SolrCloud is stopped.
   *
   * <p>The tree of lock directories for a given collection {@code collName} is as follows:
   *
   * <pre>{@code
   * rootPath/
   *    collName/
   *       Locks   <-- EPHEMERAL collection level locks go here
   *       _ShardName1/
   *          Locks   <-- EPHEMERAL shard level locks go here
   *          _replicaNameS1R1   <-- EPHEMERAL replica level locks go here
   *          _replicaNameS1R2   <-- EPHEMERAL replica level locks go here
   *       _ShardName2/
   *          Locks   <-- EPHEMERAL shard level locks go here
   *          _replicaNameS2R1   <-- EPHEMERAL replica level locks go here
   *          _replicaNameS2R2   <-- EPHEMERAL replica level locks go here
   * }</pre>
   *
   * This method will create the path where the {@code EPHEMERAL} lock nodes should go. That path
   * is:
   *
   * <ul>
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#COLLECTION} - {@code
   *       /locks/collections/collName/Locks}
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#SHARD} - {@code
   *       /locks/collections/collName/_shardName/Locks}
   *   <li>For {@link org.apache.solr.common.params.CollectionParams.LockLevel#REPLICA} - {@code
   *       /locks/collections/collName/_shardName/_replicaName}. There is no {@code Locks} subnode
   *       here because replicas do not have children so no need to separate {@code EPHEMERAL} lock
   *       nodes from children nodes as is the case for shards and collections
   * </ul>
   *
   * Note the {@code _} prefixing shards and replica names is to support shards or replicas called
   * "{@code Locks}". Also note the returned path does not contain the separator ({@code "/"}) at
   * the end.
   */
  private String getLockPath(
      CollectionParams.LockLevel level, String collName, String shardId, String replicaName) {
    StringBuilder path = getPathPrefix();
    path.append(collName).append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR);

    final String LOCK_NODENAME = "Locks"; // Should not start with SUBNODE_PREFIX :)
    final String SUBNODE_PREFIX = "_";

    if (level == CollectionParams.LockLevel.COLLECTION) {
      return path.append(LOCK_NODENAME).toString();
    } else if (level == CollectionParams.LockLevel.SHARD) {
      return path.append(SUBNODE_PREFIX)
          .append(shardId)
          .append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR)
          .append(LOCK_NODENAME)
          .toString();
    } else if (level == CollectionParams.LockLevel.REPLICA) {
      return path.append(SUBNODE_PREFIX)
          .append(shardId)
          .append(DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR)
          .append(SUBNODE_PREFIX)
          .append(replicaName)
          .toString();
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Unsupported lock level " + level);
    }
  }
}
