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
import java.nio.charset.StandardCharsets;
import org.apache.solr.common.Callable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SecurityNodeWatcher implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;

  private final Callable<Pair<byte[], Stat>> callback;

  public SecurityNodeWatcher(ZkStateReader zkStateReader, Callable<Pair<byte[], Stat>> callback) {
    this.zkStateReader = zkStateReader;
    this.callback = callback;
  }

  @Override
  public void process(WatchedEvent event) {
    // session events are not change events, and do not remove the watcher
    if (Event.EventType.None.equals(event.getType())) {
      return;
    }
    try {
      synchronized (zkStateReader) {
        log.debug("Updating [{}] ... ", ZkStateReader.SOLR_SECURITY_CONF_PATH);

        // remake watch
        final Stat stat = new Stat();
        byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
        if (Event.EventType.NodeDeleted.equals(event.getType())) {
          // Node deleted, just recreate watch without attempting a read - SOLR-9679
          zkStateReader.getZkClient().exists(ZkStateReader.SOLR_SECURITY_CONF_PATH, this, true);
        } else {
          data =
              zkStateReader
                  .getZkClient()
                  .getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, this, stat, true);
        }
        try {
          callback.call(new Pair<>(data, stat));
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
}
