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

import static java.util.Collections.emptySortedSet;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkLiveNodes implements Watcher {
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private volatile SortedSet<String> liveNodes = emptySortedSet();
  private final Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();
  private final SolrZkClient zkClient;

  public ZkLiveNodes(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }

  public SortedSet<String> getLiveNodes() {
    return liveNodes;
  }

  protected void setLiveNodes(SortedSet<String> liveNodes) {
    this.liveNodes = liveNodes;
  }

  public Set<LiveNodesListener> getLiveNodesListeners() {
    return liveNodesListeners;
  }

  public void addListener(LiveNodesListener liveNodesListener) {
    this.liveNodesListeners.add(liveNodesListener);
  }

  public void removeListener(LiveNodesListener liveNodesListener) {
    this.liveNodesListeners.remove(liveNodesListener);
  }

  /* Refresh live nodes */
  public synchronized void refresh() throws KeeperException, InterruptedException {
    SortedSet<String> newLiveNodes;
    try {
      List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, this, true);
      newLiveNodes = new TreeSet<>(nodeList);
    } catch (KeeperException.NoNodeException e) {
      newLiveNodes = emptySortedSet();
    }
    // Can't lock getUpdateLock() until we release the other, it would cause deadlock.
    SortedSet<String> oldLiveNodes;

    oldLiveNodes = getLiveNodes();
    setLiveNodes(newLiveNodes);
    if (oldLiveNodes.size() != newLiveNodes.size()) {
      if (log.isInfoEnabled()) {
        log.info(
            "Updated live nodes from ZooKeeper... ({}) -> ({})",
            oldLiveNodes.size(),
            newLiveNodes.size());
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
    }
    if (!oldLiveNodes.equals(newLiveNodes)) { // fire listeners
      SortedSet<String> finalNewLiveNodes = newLiveNodes;
      getLiveNodesListeners()
          .forEach(
              listener -> {
                if (listener.onChange(
                    new TreeSet<>(oldLiveNodes), new TreeSet<>(finalNewLiveNodes))) {
                  removeListener(listener);
                }
              });
    }
  }

  @Override
  public void process(WatchedEvent event) {
    // session events are not change events, and do not remove the watcher
    if (Event.EventType.None.equals(event.getType())) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "A live node change: [{}], has occurred - updating... (live nodes size: [{}])",
          event,
          getLiveNodes().size());
    }
    refreshAndWatch();
  }

  public void refreshAndWatch() {
    try {
      refresh();
    } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
      log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
    } catch (KeeperException e) {
      log.error("A ZK error has occurred", e);
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("Interrupted", e);
    }
  }
}
