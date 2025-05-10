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
import org.apache.curator.framework.CuratorFramework;

/**
 * A distributed lock factory for Solr config sets, using Apache Curator's {@link
 * org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock} to manage distributed locks
 * within a flat Zookeeper-backed config set znode structure.
 *
 * <p>This factory supports locking at the config set level only. The lock path hierarchy is flat:
 *
 * <pre>{@code
 * rootPath/
 *   configSet1/
 *   configSet2/
 *   ...
 * }</pre>
 *
 * <p>Each config set has its own lock directory under the root path. This is distinct from the
 * collection lock factory, which supports hierarchical locking for collections, shards, and
 * replicas.
 *
 * @see <a href="https://curator.apache.org/curator-recipes/locks.html">Curator Recipes - Locks</a>
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper
 *     lock recipe</a>
 */
public class CuratorDistributedConfigSetLockFactory extends CuratorDistributedLockFactory
    implements DistributedConfigSetLockFactory {

  public CuratorDistributedConfigSetLockFactory(CuratorFramework curator, String rootPath) {
    super(curator, rootPath);
  }

  @Override
  public DistributedLock createLock(boolean isWriteLock, String configSetName) {
    Objects.requireNonNull(configSetName, "configSetName can't be null");

    String lockPath = getLockPath(configSetName);
    return doCreateLock(isWriteLock, lockPath);
  }

  /**
   * Returns the Zookeeper path to the lock, creating missing nodes if needed. Note that the
   * complete lock hierarchy (/rootPath and below) can be deleted if SolrCloud is stopped.
   *
   * <p>The tree of lock directories is very flat, given there's no real structure to what's being
   * locked in a config set:
   *
   * <pre>{@code
   * rootPath/
   *    configSet1/ <-- EPHEMERAL config set locks go here
   *    configSet2/ <-- EPHEMERAL config set locks go here
   *    etc...
   * }</pre>
   *
   * This method will create the path where the {@code EPHEMERAL} lock nodes should go.
   *
   * <p>The returned path does not contain the separator ({@code "/"}) at the end.
   *
   * <p>This method is used in conjunction with Curator's InterProcessReadWriteLock to manage
   * distributed locks.
   */
  private String getLockPath(String configSetName) {
    return getPathPrefix().append(configSetName).toString();
  }
}
