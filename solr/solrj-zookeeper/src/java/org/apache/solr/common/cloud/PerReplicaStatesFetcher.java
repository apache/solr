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

package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerReplicaStatesFetcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * Fetch the latest {@link PerReplicaStates} . It fetches data after checking the {@link
   * Stat#getCversion()} of state.json. If this is not modified, the same object is returned
   */
  public static PerReplicaStates fetch(
      String path, SolrZkClient zkClient, PerReplicaStates current) {
    try {
      if (current != null) {
        Stat stat = zkClient.exists(current.path, null, true);
        if (stat == null) return new PerReplicaStates(path, 0, Collections.emptyList());
        if (current.cversion == stat.getCversion()) return current; // not modifiedZkStateReaderTest
      }
      Stat stat = new Stat();
      List<String> children = zkClient.getChildren(path, null, stat, true);
      return new PerReplicaStates(path, stat.getCversion(), Collections.unmodifiableList(children));
    } catch (KeeperException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error fetching per-replica states", e);
    } catch (InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Thread interrupted when loading per-replica states from " + path,
          e);
    }
  }

  public static DocCollection.PrsSupplier getZkClientPrsSupplier(
      SolrZkClient zkClient, String collectionPath) {
    return () -> PerReplicaStatesFetcher.fetch(collectionPath, zkClient, null);
  }
}
