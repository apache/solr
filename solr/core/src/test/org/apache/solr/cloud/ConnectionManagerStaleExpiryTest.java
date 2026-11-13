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

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/** Exercises expiries that are queued behind other connection events. */
@SuppressForbidden(reason = "needs a log4j appender to delay a connection callback")
public class ConnectionManagerStaleExpiryTest extends SolrTestCase {
  private static final int WAIT_SECONDS = 20;

  @Test
  public void testStaleExpiryDoesNotReplaceRecoveredSession() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zk-replacement"));
    SocketProxy proxy = new SocketProxy();
    RecordingStrategy strategy = new RecordingStrategy();
    CallbackAppender appender = new CallbackAppender(proxy);
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    String loggerName = ConnectionManager.class.getName();
    LoggerConfig previousLogger = config.getLoggers().get(loggerName);
    LoggerConfig logger = new LoggerConfig(loggerName, Level.DEBUG, true);
    boolean serverStarted = false;
    boolean proxyStarted = false;
    appender.start();
    logger.addAppender(appender, Level.DEBUG, null);
    config.addLogger(loggerName, logger);
    context.updateLoggers();
    try {
      server.run();
      serverStarted = true;
      proxy.open(new URI("http://127.0.0.1:" + server.getPort()));
      proxyStarted = true;
      CountDownLatch reconnected = new CountDownLatch(1);
      CountDownLatch allowReconnectReturn = new CountDownLatch(1);
      AtomicInteger reconnectCalls = new AtomicInteger();
      AtomicBoolean reconnectHoldTimedOut = new AtomicBoolean();
      try (SolrZkClient client =
          new SolrZkClient.Builder()
              .withUrl("127.0.0.1:" + proxy.getListenPort())
              .withTimeout(20, TimeUnit.SECONDS)
              .withConnTimeOut(10, TimeUnit.SECONDS)
              .withConnStrategy(strategy)
              .withReconnectListener(
                  () -> {
                    if (reconnectCalls.incrementAndGet() == 1) {
                      reconnected.countDown();
                      try {
                        if (!allowReconnectReturn.await(60, TimeUnit.SECONDS)) {
                          reconnectHoldTimedOut.set(true);
                        }
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        reconnectHoldTimedOut.set(true);
                      }
                    }
                  })
              .build()) {
        try {
          ConnectionManager manager = client.getConnectionManager();
          long sessionA = client.getZooKeeper().getSessionId();
          appender.arm(manager.toString());
          server.expire(sessionA);
          assertTrue(
              "replacement session was not created",
              strategy.replacementCreated.await(WAIT_SECONDS, TimeUnit.SECONDS));
          assertTrue(
              "replacement connected callback was not held",
              appender.connectedHeld.await(WAIT_SECONDS, TimeUnit.SECONDS));
          ZooKeeper replacement = strategy.replacement.get();
          assertTrue(
              "replacement must have a real connected session",
              replacement.getState().isConnected());
          long sessionB = replacement.getSessionId();
          assertNotEquals(sessionA, sessionB);

          proxy.halfClose();
          assertTrue(
              "replacement disconnect was not delivered",
              strategy.replacementDisconnected.await(WAIT_SECONDS, TimeUnit.SECONDS));
          assertTrue(
              "replacement disconnect was not applied",
              appender.disconnected.await(WAIT_SECONDS, TimeUnit.SECONDS));
          assertFalse(manager.isConnected());
          server.expire(sessionB);
          // A direct client verifies the server is reachable while the replacement is expired.
          try (SolrZkClient control =
              new SolrZkClient.Builder()
                  .withUrl(server.getZkAddress())
                  .withTimeout(20, TimeUnit.SECONDS)
                  .withConnTimeOut(10, TimeUnit.SECONDS)
                  .build()) {
            assertTrue(control.getZooKeeper().getState().isConnected());
          }
          proxy.goOn();
          assertTrue(
              "replacement expiry was not delivered",
              strategy.replacementExpired.await(WAIT_SECONDS, TimeUnit.SECONDS));
          new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
              .waitFor("replacement was not terminal", () -> !replacement.getState().isAlive());

          // Keep C from connecting until the stale B callback has actually been rejected.
          assertTrue(
              "next replacement was not requested",
              strategy.thirdCreationRequested.await(WAIT_SECONDS, TimeUnit.SECONDS));
          appender.releaseConnected.countDown();
          assertTrue(
              "delayed connected callback was not rejected",
              appender.staleConnected.await(WAIT_SECONDS, TimeUnit.SECONDS));
          assertFalse("connected callback hold timed out", appender.holdTimedOut.get());
          strategy.allowThirdCreation.countDown();
          assertTrue(
              "connection manager did not recover",
              reconnected.await(WAIT_SECONDS, TimeUnit.SECONDS));
          ZooKeeper sessionC = client.getZooKeeper();
          assertNotEquals(sessionB, sessionC.getSessionId());
          assertTrue(sessionC.getState().isConnected());
          allowReconnectReturn.countDown();
          assertTrue(
              "queued replacement expiry was not processed",
              appender.replacementExpiryProcessed.await(WAIT_SECONDS, TimeUnit.SECONDS));
          assertFalse("expiry callback hold timed out", appender.expiryHoldTimedOut.get());
          assertFalse("reconnect callback hold timed out", reconnectHoldTimedOut.get());
          assertTrue(
              "stale expiry disconnected the recovered session", manager.isConnectedAndNotClosed());
          assertSame("stale expiry replaced the recovered keeper", sessionC, client.getZooKeeper());
          assertEquals("stale expiry created another keeper", 3, strategy.attempts.get());
          assertEquals("stale expiry invoked another reconnect callback", 1, reconnectCalls.get());
        } finally {
          allowReconnectReturn.countDown();
          appender.allowExpiryReturn.countDown();
          appender.releaseConnected.countDown();
          strategy.allowThirdCreation.countDown();
          proxy.goOn();
        }
      }
    } finally {
      appender.releaseConnected.countDown();
      appender.allowExpiryReturn.countDown();
      strategy.allowThirdCreation.countDown();
      config.removeLogger(loggerName);
      if (previousLogger != null) {
        config.addLogger(loggerName, previousLogger);
      }
      context.updateLoggers();
      appender.stop();
      if (proxyStarted) {
        proxy.close();
      }
      if (serverStarted) {
        server.shutdown();
      }
    }
  }

  /**
   * The reconnect for session A is still running when session B expires, so B's expiry waits in the
   * queue. A late event from the replaced session A is applied in the meantime. B's expiry must
   * still be acted on.
   */
  @Test
  public void testLateEventFromReplacedSessionDoesNotDropCurrentExpiry() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("zk-late-event"));
    RecordingStrategy strategy = new RecordingStrategy();
    strategy.allowThirdCreation.countDown();
    CountDownLatch reconnected = new CountDownLatch(1);
    CountDownLatch allowReconnectReturn = new CountDownLatch(1);
    AtomicInteger reconnectCalls = new AtomicInteger();
    try {
      server.run();
      try (SolrZkClient client =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(20, TimeUnit.SECONDS)
              .withConnTimeOut(10, TimeUnit.SECONDS)
              .withConnStrategy(strategy)
              .withReconnectListener(
                  () -> {
                    if (reconnectCalls.incrementAndGet() == 1) {
                      reconnected.countDown();
                      try {
                        allowReconnectReturn.await(60, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }
                  })
              .build()) {
        try {
          ConnectionManager manager = client.getConnectionManager();
          server.expire(client.getZooKeeper().getSessionId());
          assertTrue(
              "replacement session was not installed",
              reconnected.await(WAIT_SECONDS, TimeUnit.SECONDS));
          long sessionB = client.getZooKeeper().getSessionId();

          // B's expiry is delivered, but cannot be handled while the reconnect above is running.
          server.expire(sessionB);
          assertTrue(
              "replacement expiry was not delivered",
              strategy.replacementExpired.await(WAIT_SECONDS, TimeUnit.SECONDS));

          strategy
              .watchers
              .get(1)
              .process(
                  new WatchedEvent(
                      Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
          new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
              .waitFor("late event was not applied", () -> !manager.isConnected());

          allowReconnectReturn.countDown();
          new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
              .waitFor(
                  "the expired session was never replaced",
                  () ->
                      manager.isConnectedAndNotClosed()
                          && client.getZooKeeper().getSessionId() != sessionB);
        } finally {
          allowReconnectReturn.countDown();
        }
      }
    } finally {
      server.shutdown();
    }
  }

  private static class RecordingStrategy extends DefaultConnectionStrategy {
    private final AtomicInteger attempts = new AtomicInteger();
    private final Map<Integer, Watcher> watchers = new ConcurrentHashMap<>();
    private final AtomicReference<ZooKeeper> replacement = new AtomicReference<>();
    private final CountDownLatch replacementCreated = new CountDownLatch(1);
    private final CountDownLatch replacementDisconnected = new CountDownLatch(1);
    private final CountDownLatch replacementExpired = new CountDownLatch(1);
    private final CountDownLatch thirdCreationRequested = new CountDownLatch(1);
    private final CountDownLatch allowThirdCreation = new CountDownLatch(1);

    @Override
    protected ZooKeeper newZooKeeperInstance(String address, int timeout, Watcher watcher)
        throws IOException {
      int attempt = attempts.incrementAndGet();
      watchers.put(attempt, watcher);
      if (attempt == 3) {
        thirdCreationRequested.countDown();
        try {
          if (!allowThirdCreation.await(30, TimeUnit.SECONDS)) {
            throw new IOException("timed out waiting to create the next session");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
      }
      Watcher observingWatcher =
          event -> {
            watcher.process(event);
            if (attempt == 2 && event.getState() == Watcher.Event.KeeperState.Disconnected) {
              replacementDisconnected.countDown();
            }
            if (attempt == 2 && event.getState() == Watcher.Event.KeeperState.Expired) {
              replacementExpired.countDown();
            }
          };
      ZooKeeper keeper = super.newZooKeeperInstance(address, timeout, observingWatcher);
      if (attempt == 2) {
        replacement.set(keeper);
        replacementCreated.countDown();
      }
      return keeper;
    }
  }

  @SuppressForbidden(reason = "needs log4j2 classes to delay a logging call")
  private static class CallbackAppender extends AbstractAppender {
    private final SocketProxy proxy;
    private final AtomicBoolean paused = new AtomicBoolean();
    private final AtomicBoolean holdTimedOut = new AtomicBoolean();
    private final AtomicBoolean expiryHoldTimedOut = new AtomicBoolean();
    private final AtomicInteger expiryEvents = new AtomicInteger();
    private final AtomicReference<Thread> connectedThread = new AtomicReference<>();
    private final AtomicReference<Thread> disconnectedThread = new AtomicReference<>();
    private final AtomicReference<Thread> replacementExpiryThread = new AtomicReference<>();
    private volatile String manager;
    private final CountDownLatch connectedHeld = new CountDownLatch(1);
    private final CountDownLatch releaseConnected = new CountDownLatch(1);
    private final CountDownLatch disconnected = new CountDownLatch(1);
    private final CountDownLatch staleConnected = new CountDownLatch(1);
    private final CountDownLatch replacementExpiryProcessed = new CountDownLatch(1);
    private final CountDownLatch allowExpiryReturn = new CountDownLatch(1);

    CallbackAppender(SocketProxy proxy) {
      super("replacementCallback", null, null, true, Property.EMPTY_ARRAY);
      this.proxy = proxy;
    }

    void arm(String target) {
      manager = target;
    }

    @Override
    public void append(LogEvent event) {
      String message = event.getMessage().getFormattedMessage();
      if (manager == null) {
        return;
      }
      if (message.startsWith("Watcher " + manager) && message.contains("state:Expired")) {
        if (expiryEvents.incrementAndGet() == 2) {
          replacementExpiryThread.set(Thread.currentThread());
        }
      }
      if (message.startsWith("Watcher " + manager)
          && message.contains("state:SyncConnected")
          && connectedHeld.getCount() != 0) {
        connectedThread.set(Thread.currentThread());
        connectedHeld.countDown();
        try {
          if (!releaseConnected.await(60, TimeUnit.SECONDS)) {
            holdTimedOut.set(true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else if (connectedHeld.getCount() == 0
          && message.startsWith("Watcher " + manager)
          && message.contains("state:Disconnected")
          && paused.compareAndSet(false, true)) {
        disconnectedThread.set(Thread.currentThread());
        proxy.pause();
      } else if (Thread.currentThread() == disconnectedThread.get()
          && message.equals("zkClient has disconnected")) {
        disconnected.countDown();
      } else if (Thread.currentThread() == connectedThread.get()
          && message.startsWith("Ignoring stale connected event ")) {
        staleConnected.countDown();
      } else if (Thread.currentThread() == replacementExpiryThread.get()
          && (message.startsWith("Ignoring stale expired event ")
              || message.startsWith("Our previous ZooKeeper session was expired."))) {
        replacementExpiryProcessed.countDown();
        try {
          if (!allowExpiryReturn.await(60, TimeUnit.SECONDS)) {
            expiryHoldTimedOut.set(true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          expiryHoldTimedOut.set(true);
        }
      }
    }
  }
}
