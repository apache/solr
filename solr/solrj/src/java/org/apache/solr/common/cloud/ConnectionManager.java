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

import static org.apache.zookeeper.Watcher.Event.KeeperState.AuthFailed;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Expired;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionManager implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;

  private volatile boolean connected = false;

  private final ZkClientConnectionStrategy connectionStrategy;

  private final String zkServerAddress;

  private final SolrZkClient client;

  private final OnReconnect onReconnect;
  private final OnDisconnect beforeReconnect;

  private final ExecutorService executorService;

  private volatile boolean isClosed = false;
  private volatile boolean isReconnecting = false;

  // Track the likely expired state
  private static class LikelyExpiredState {
    private static LikelyExpiredState NOT_EXPIRED =
        new LikelyExpiredState(StateType.NOT_EXPIRED, 0);
    private static LikelyExpiredState EXPIRED = new LikelyExpiredState(StateType.EXPIRED, 0);

    public enum StateType {
      NOT_EXPIRED, // definitely not expired
      EXPIRED, // definitely expired
      TRACKING_TIME // not sure, tracking time of last disconnect
    }

    private StateType stateType;
    private long lastDisconnectTime;

    public LikelyExpiredState(StateType stateType, long lastDisconnectTime) {
      this.stateType = stateType;
      this.lastDisconnectTime = lastDisconnectTime;
    }

    public boolean isLikelyExpired(long timeToExpire) {
      return stateType == StateType.EXPIRED
          || (stateType == StateType.TRACKING_TIME
              && (System.nanoTime() - lastDisconnectTime
                  > TimeUnit.NANOSECONDS.convert(timeToExpire, TimeUnit.MILLISECONDS)));
    }
  }

  @FunctionalInterface
  public interface IsClosed {
    boolean isClosed();
  }

  private volatile LikelyExpiredState likelyExpiredState = LikelyExpiredState.EXPIRED;

  private IsClosed isClosedCheck;

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, ZkClientConnectionStrategy strat, OnReconnect onConnect, OnDisconnect beforeReconnect, IsClosed isClosed) {
    this(name, client, zkServerAddress, strat, onConnect, beforeReconnect, isClosed, null);
  }

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, ZkClientConnectionStrategy strat, OnReconnect onConnect, OnDisconnect onDisconnect, IsClosed isClosed, ExecutorService executorService) {
    this.name = name;
    this.client = client;
    this.connectionStrategy = strat;
    this.zkServerAddress = zkServerAddress;
    this.onReconnect = onConnect;
    this.beforeReconnect = onDisconnect;
    this.isClosedCheck = isClosed;
    this.executorService = executorService;
  }

  private synchronized void connected() {
    connected = true;
    likelyExpiredState = LikelyExpiredState.NOT_EXPIRED;
    notifyAll();
  }

  private synchronized void disconnected() {
    connected = false;
    // record the time we expired unless we are already likely expired
    if (!likelyExpiredState.isLikelyExpired(0)) {
      likelyExpiredState =
          new LikelyExpiredState(LikelyExpiredState.StateType.TRACKING_TIME, System.nanoTime());
    }
    notifyAll();
  }

  @Override
  public void process(WatchedEvent event) {
    if (executorService == null) {
      processInternal(event);
    } else {
      try {
        executorService.submit(() -> processInternal(event));
      } catch (RejectedExecutionException e) {
        // If not a graceful shutdown
        if (!isClosed()) {
          throw e;
        }
      }
    }
  }

  public void processInternal(WatchedEvent event) {
    if (event.getState() == AuthFailed
        || event.getState() == Disconnected
        || event.getState() == Expired) {
      log.warn(
          "Watcher {} name: {} got event {} path: {} type: {}",
          this,
          name,
          event,
          event.getPath(),
          event.getType());
    } else {
      if (log.isDebugEnabled()) {
        log.debug(
            "Watcher {} name: {} got event {} path: {} type: {}",
            this,
            name,
            event,
            event.getPath(),
            event.getType());
      }
    }

    if (isClosed()) {
      log.debug("Client->ZooKeeper status change trigger but we are already closed");
      return;
    }

    KeeperState state = event.getState();

    if (state == KeeperState.SyncConnected) {
      log.info("zkClient has connected");
      connected();
      connectionStrategy.connected();

      if (isReconnecting && onReconnect != null) {
        try {
          onReconnect.command();
        } catch (Exception e) {
          log.warn("Exception running onReconnect command", e);
        }
      }

      isReconnecting = false;
    } else if (state == Expired) {
      if (isClosed()) {
        return;
      }
      // we don't call disconnected here, because we know we are expired
      connected = false;
      likelyExpiredState = LikelyExpiredState.EXPIRED;
      isReconnecting = true;

      log.warn(
          "Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...");

      if (beforeReconnect != null) {
        try {
          beforeReconnect.command();
        } catch (Exception e) {
          log.warn("Exception running beforeReconnect command", e);
        }
      }

      try {
        connectionStrategy.reconnect(
            zkServerAddress,
            client.getZkClientTimeout(),
            this,
            client::updateKeeper);
        log.info("zkClient reconnect started");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // we must have been asked to stop
        throw new RuntimeException(e);
      } catch (IOException | TimeoutException e) {
        log.warn("Could not reconnect to ZK", e);
      }
    } else if (state == KeeperState.Disconnected) {
      log.warn("zkClient has disconnected");
      disconnected();
      connectionStrategy.disconnected();
    } else if (state == KeeperState.AuthFailed) {
      log.warn("zkClient received AuthFailed");
    }
  }

  public synchronized boolean isConnectedAndNotClosed() {
    return !isClosed() && connected;
  }

  public synchronized boolean isConnected() {
    return connected;
  }

  // we use a volatile rather than sync
  // to avoid possible deadlock on shutdown
  public void close() {
    this.isClosed = true;
    this.likelyExpiredState = LikelyExpiredState.EXPIRED;
  }

  private boolean isClosed() {
    return isClosed || isClosedCheck.isClosed();
  }

  public boolean isLikelyExpired() {
    return isClosed()
        || likelyExpiredState.isLikelyExpired((long) (client.getZkClientTimeout() * 0.90));
  }

  /**
   * Wait for an established zookeeper connection
   *
   * @param waitForConnection time to wait, in ms
   */
  public synchronized void waitForConnected(long waitForConnection) throws TimeoutException {
    log.info("Waiting up to {}ms for client to connect to ZooKeeper", waitForConnection);
    long expire =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(waitForConnection, TimeUnit.MILLISECONDS);
    long left = 1;
    while (!connected && left > 0) {
      if (isClosed()) {
        break;
      }
      try {
        wait(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      left = expire - System.nanoTime();
    }
    if (!connected) {
      throw new TimeoutException(
          "Could not connect to ZooKeeper "
              + zkServerAddress
              + " within "
              + waitForConnection
              + " ms");
    }
    log.info("Client is connected to ZooKeeper");
  }
}
