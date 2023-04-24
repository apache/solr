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
import org.apache.solr.common.cloud.SolrZkClient;

/**
 * A distributed lock implementation using Zookeeper "directory" nodes created within a collection
 * znode hierarchy, for use with the distributed Collection API implementation. The locks are
 * implemented using ephemeral nodes placed below the "directory" nodes.
 *
 * @see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">Zookeeper
 *     lock recipe</a>
 */
public class ZkDistributedConfigSetLockFactory extends ZkDistributedLockFactory
    implements DistributedConfigSetLockFactory {

  public ZkDistributedConfigSetLockFactory(SolrZkClient zkClient, String rootPath) {
    super(zkClient, rootPath);
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
   */
  private String getLockPath(String configSetName) {
    return getPathPrefix().append(configSetName).toString();
  }
}
