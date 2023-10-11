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

import static java.util.Collections.emptyMap;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.solr.common.Callable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SecurityNodeWatcher implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;
  private ZkStateReader.ConfigData securityData;

  private final Callable<SolrZkClient.NodeData> callback;

  @SuppressWarnings("unchecked")
  public SecurityNodeWatcher(ZkStateReader zkStateReader, Runnable securityNodeListener) {
    this.zkStateReader = zkStateReader;
    callback =
        data -> {
          ZkStateReader.ConfigData cd = new ZkStateReader.ConfigData();
          cd.data =
              data.data == null || data.data.length == 0
                  ? emptyMap()
                  : Utils.getDeepCopy((Map) Utils.fromJSON(data.data), 4, false);
          cd.version = data.stat == null ? -1 : data.stat.getVersion();
          securityData = cd;
          if (securityNodeListener != null) securityNodeListener.run();
        };
  }

  @Override
  public void process(WatchedEvent event) {
    // session events are not change events, and do not remove the watcher
    if (Event.EventType.None.equals(event.getType())) {
      return;
    }
    try {
      synchronized (this) {
        log.debug("Updating [{}] ... ", ZkStateReader.SOLR_SECURITY_CONF_PATH);

        // remake watch

        SolrZkClient.NodeData data =
            new SolrZkClient.NodeData(new Stat(), "{}".getBytes(StandardCharsets.UTF_8));
        if (Event.EventType.NodeDeleted.equals(event.getType())) {
          // Node deleted, just recreate watch without attempting a read - SOLR-9679
          zkStateReader.getZkClient().exists(ZkStateReader.SOLR_SECURITY_CONF_PATH, this, true);
        } else {
          data =
              zkStateReader
                  .getZkClient()
                  .getNode(ZkStateReader.SOLR_SECURITY_CONF_PATH, this, true);
        }
        try {
          callback.call(data);
        } catch (Exception e) {
          log.error("Error running collections node listener", e);
        }
      }
    } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
    } catch (KeeperException e) {
      log.error("A ZK error has occurred", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("Interrupted", e);
    }
  }

  void register() throws InterruptedException, KeeperException {
    zkStateReader.getZkClient().exists(ZkStateReader.SOLR_SECURITY_CONF_PATH, this, true);
    securityData = getSecurityProps(true);
  }

  @SuppressWarnings("unchecked")
  ZkStateReader.ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ZkStateReader.ConfigData(emptyMap(), -1);
      return new ZkStateReader.ConfigData(securityData.data, securityData.version);
    }
    try {
      if (zkStateReader.getZkClient().exists(ZkStateReader.SOLR_SECURITY_CONF_PATH, true)) {
        SolrZkClient.NodeData d =
            zkStateReader.getZkClient().getNode(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, true);
        return d != null && d.data.length > 0
            ? new ZkStateReader.ConfigData(
                (Map<String, Object>) Utils.fromJSON(d.data), d.stat.getVersion())
            : null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    } catch (KeeperException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    }
    return null;
  }
}
