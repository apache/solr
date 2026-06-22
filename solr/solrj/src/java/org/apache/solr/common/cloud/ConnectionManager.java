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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.Watcher.Event.KeeperState.AuthFailed;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Expired;

public class ConnectionManager implements Watcher, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final int zkSessionTimeout;

  private volatile boolean connected = false;

  private volatile boolean hasConnected = false;

  private final String zkServerAddress;

  private final SolrZkClient client;

  private volatile SolrZooKeeper keeper;

  private volatile OnReconnect onReconnect;
  private volatile BeforeReconnect beforeReconnect;

  private volatile boolean isClosed = false;

  private final ReentrantLock keeperLock = new ReentrantLock(false);

  private volatile DisconnectListener disconnectListener;
  private volatile int lastConnectedState = 0;

  private volatile ZkCredentialsProvider zkCredentialsToAddAutomatically;
  private volatile boolean zkCredentialsToAddAutomaticallyUsed;


  private final ReentrantLock connectLock = new ReentrantLock(false);

  private final Condition connectCondition = connectLock.newCondition();

  private volatile boolean expired;
  private volatile boolean started;

  //  private Set<ZkClientConnectionStrategy.DisconnectedListener> disconnectedListeners = ConcurrentHashMap.newKeySet();
//  private Set<ZkClientConnectionStrategy.ConnectedListener> connectedListeners = ConcurrentHashMap.newKeySet();

  public void setOnReconnect(OnReconnect onReconnect) {
    this.onReconnect = onReconnect;
  }

  public void setBeforeReconnect(BeforeReconnect beforeReconnect) {
    this.beforeReconnect = beforeReconnect;
  }

  public ZooKeeper getKeeper() {
    if (!started) {
      IllegalStateException e = new IllegalStateException("You must call start on " + SolrZkClient.class.getName() + " before you can use it");
      log.error("Exception getting zk client", e);
      throw e;
    }

    return keeper;
  }

  public void setZkCredentialsToAddAutomatically(ZkCredentialsProvider zkCredentialsToAddAutomatically) {
    this.zkCredentialsToAddAutomatically = zkCredentialsToAddAutomatically;
  }

  public ZkCredentialsProvider getZkCredentialsToAddAutomatically() {
    return this.zkCredentialsToAddAutomatically;
  }


  public static abstract class IsClosed {
    public abstract boolean isClosed();
  }

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkSessionTimeout) {
    this.name = name;
    this.client = client;
    this.zkServerAddress = zkServerAddress;
    this.zkSessionTimeout = zkSessionTimeout;
  }

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkSessionTimeout, OnReconnect onConnect, BeforeReconnect beforeReconnect) {
    this.name = name;
    this.client = client;
    this.zkServerAddress = zkServerAddress;
    this.onReconnect = onConnect;
    this.beforeReconnect = beforeReconnect;
    this.zkSessionTimeout = zkSessionTimeout;
    assert ObjectReleaseTracker.getInstance().track(this);
  }

  private void connected() {

    connected = true;
    lastConnectedState = 1;
    if (log.isDebugEnabled()) log.debug("Connected, notify any wait");


    connectLock.lock();
    try {
      connectCondition.signalAll();
    } finally {
      connectLock.unlock();
    }

  }

  public void start() throws IOException {
    updateZk();
    this.started = true;
  }

  private void updateZk() throws IOException {
    SolrZooKeeper oldKeeper = null;
    keeperLock.lock();
    try {
      try {

        if (keeper != null) {
          oldKeeper = keeper;
          if (beforeReconnect != null && expired) {

            try {
              beforeReconnect.command();
            } catch (Exception e) {
              log.error("Exception running beforeReconnect command", e);
              if (e instanceof AlreadyClosedException) {
                return;
              }
            }
          }

        }
        keeper = createSolrZooKeeper(zkServerAddress, zkSessionTimeout, this);
      } finally {
        IOUtils.closeQuietly(oldKeeper);
      }
    } finally {
      keeperLock.unlock();
    }
  }

  @Override
  public void process(WatchedEvent event) {
      if (event.getState() == AuthFailed || event.getState() == Disconnected || event.getState() == Expired) {
        log.warn("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
      } else {
        if (log.isInfoEnabled()) {
          log.info("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
        }
      }

      KeeperState state = event.getState();

      if (state == KeeperState.SyncConnected) {
        expired = false;
        connected = true;
        hasConnected = true;
        if (isClosed()) return;
        if (log.isDebugEnabled()) log.debug("zkClient has connected");

        ParWork.getRootSharedExecutor().execute(this::connected);

      } else if (state == Expired) {
        connected = false;
        expired = true;

        log.warn("Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...");

        ParWork.submitIO("zkReconnect", this::reconnect);

      } else if (state == KeeperState.Disconnected) {
        log.info("zkClient has disconnected");

      } else if (state == KeeperState.AuthFailed) {
        log.warn("zkClient received AuthFailed");
      } else if (state == KeeperState.Closed) {
        log.info("zkClient session is closed");
        lastConnectedState = 0;
      }

  }

  private void reconnect() {
    ActionThrottle throttle = new ActionThrottle("ConnectionManager", 1000);
    do {
      if (isClosed() || isClosed) return;

      connected = false;

      try {
        if (disconnectListener != null) {
          disconnectListener.disconnected();
        }
      } catch (NullPointerException e) {
        // okay
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        log.warn("Exception firing disonnectListener");
      }
      lastConnectedState = 0;

      // This loop will break if a valid connection is made. If a connection is not made then it will repeat and
      // try again to create a new connection.
      throttle.minimumWaitBetweenActions();
      throttle.markAttemptingAction();
      log.info("Running reconnect strategy");
      try {
        updateZk();
        try {
          if (onReconnect != null) {
            // Run the reconnect callback inline on this dedicated zkReconnect thread rather than
            // resubmitting to zkCallbackExecutor: close() calls shutdownNow() on that executor, so a
            // close racing reconnect would reject the submission with RejectedExecutionException and
            // silently drop the critical onReconnect (retryElection) task. Running inline removes the
            // race; if we are already closed, AlreadyClosedException below ends the reconnect loop.
            try {
              onReconnect.command();
            } catch (AlreadyClosedException e) {
              throw e;
            } catch (Exception e) {
              log.error("Exception in on reconnect", e);
            }
          }
        } catch (AlreadyClosedException e) {
          throw e;
        } catch (Exception e1) {
          log.info("Could not connect due to error, trying again ..", e1);
        }

      } catch (AlreadyClosedException e) {
        log.warn("Ran into AlreadyClosedException on reconnect");
        if (isClosed) {
          return;
        }
      } catch (Exception e) {

        log.info("Could not connect due to error, trying again ..", e);
        //IOUtils.closeQuietly(keeper);
        //break;
      }

    } while (!getKeeper().getState().isAlive());

    log.info("zkClient Connected: {}", connected);
  }

  public boolean isConnected() {
    SolrZooKeeper fkeeper = keeper;
    return fkeeper != null && fkeeper.getState().isConnected() && !isClosed;
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Close called on ZK ConnectionManager");
    this.isClosed = true;

    client.zkCallbackExecutor.shutdownNow();

    SolrZooKeeper fkeeper = keeper;
    if (fkeeper != null) {
      fkeeper.close();
    }

    connectLock.lock();
    try {
      connectCondition.signalAll();
    } finally {
      connectLock.unlock();
    }

    assert ObjectReleaseTracker.getInstance().release(this);
  }

  private boolean isClosed() {
    return started && client.isClosed();
  }

  public void waitForConnected() {
    waitForConnected(Integer.getInteger("waitForZk", 30) * 1000);
  }

  public void waitForConnected(int timeoutms) {
    if (timeoutms <= 0) timeoutms = 30000;
    log.debug("Waiting for client to connect to ZooKeeper");
    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      if (isClosed) throw new AlreadyClosedException();
      IsClosed hlc = client.higherLevelIsClosed;
      if (hlc != null && hlc.isClosed()) throw new AlreadyClosedException();
      SolrZooKeeper fkeeper = keeper;
      // Also require !expired: on session expiry the old keeper handle can still briefly report
      // CONNECTED before reconnect() swaps it out. Without this gate a waiter could observe the
      // dying handle as connected and issue an op on a being-torn-down session. expired stays true
      // for the whole reconnect window (cleared only on the next SyncConnected).
      if (fkeeper != null && !expired && fkeeper.getState().isConnected()) {
        log.debug("Client is connected");
        return;
      }
      connectLock.lock();
      try {
        connectCondition.await(250, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        throw new AlreadyClosedException();
      } finally {
        connectLock.unlock();
      }
    }
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Could not connect to ZooKeeper " + zkServerAddress + " within " + timeoutms + "ms");
  }


  public interface DisconnectListener {
    void disconnected();
  }

  public void setDisconnectListener(DisconnectListener dl) {
    this.disconnectListener = dl;
  }

  protected SolrZooKeeper createSolrZooKeeper(final String serverAddress, final int zkSessionTimeout,
                                              final Watcher watcher) throws IOException {
    if (isClosed) {
      throw new AlreadyClosedException();
    }

    SolrZooKeeper result = new SolrZooKeeper(serverAddress, zkSessionTimeout, watcher, client.isFastCloseForTests());

    if (zkCredentialsToAddAutomatically != null) {
      for (ZkCredentialsProvider.ZkCredentials zkCredentials : zkCredentialsToAddAutomatically.getCredentials()) {
        result.addAuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth());
      }
    }

    return result;
  }


  public interface DisconnectedListener {
    void disconnected();
  }

  public interface ConnectedListener {
    void connected();
  }
}
