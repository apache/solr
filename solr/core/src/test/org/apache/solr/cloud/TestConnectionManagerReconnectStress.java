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

import java.net.URI;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Drops the connection between a real {@link SolrZkClient} and ZooKeeper over and over, while
 * slowing down the processing of {@code Disconnected} events the way a stalled callback thread
 * would. {@link ConnectionManager} receives those events on an unbounded pool, so a slow {@code
 * Disconnected} can be applied after the {@code SyncConnected} that followed it. The manager must
 * still end up connected whenever ZooKeeper is. Every few disconnects the session is expired
 * instead, which must also be recovered from.
 */
@Nightly
@SuppressForbidden(reason = "needs log4j2 classes to delay a logging call")
public class TestConnectionManagerReconnectStress extends SolrTestCase {

  private static final int FLAPS = 25;
  private static final int EXPIRE_EVERY = 5;
  private static final int MAX_DISCONNECT_CALLBACK_DELAY_MS = 1500;
  private static final int SETTLE_SECONDS = 10;
  private static final int DRAIN_SECONDS = 30;

  private ZkTestServer zkServer;
  private SocketProxy proxy;
  private SlowDisconnectAppender slowDisconnects;
  private LoggerConfig loggerConfig;

  /** Delays the log call that precedes the state change made for a {@code Disconnected} event. */
  @SuppressForbidden(reason = "needs log4j2 classes to delay a logging call")
  private static class SlowDisconnectAppender extends AbstractAppender {
    final AtomicInteger delayed = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();

    SlowDisconnectAppender() {
      super("slowDisconnects", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      if (!event.getMessage().getFormattedMessage().contains("state:Disconnected")) {
        return;
      }
      delayed.incrementAndGet();
      inFlight.incrementAndGet();
      try {
        Thread.sleep(random().nextInt(MAX_DISCONNECT_CALLBACK_DELAY_MS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        inFlight.decrementAndGet();
      }
    }
  }

  @Before
  public void setUpServers() throws Exception {
    zkServer = new ZkTestServer(createTempDir("zkData"));
    zkServer.run();
    proxy = new SocketProxy();
    proxy.open(new URI("http://127.0.0.1:" + zkServer.getPort()));

    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    String loggerName = ConnectionManager.class.getName();
    loggerConfig = new LoggerConfig(loggerName, Level.WARN, true);
    slowDisconnects = new SlowDisconnectAppender();
    slowDisconnects.start();
    loggerConfig.addAppender(slowDisconnects, Level.WARN, null);
    config.addLogger(loggerName, loggerConfig);
    context.updateLoggers();
  }

  @After
  public void tearDownServers() throws Exception {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    context.getConfiguration().removeLogger(ConnectionManager.class.getName());
    context.updateLoggers();
    slowDisconnects.stop();
    proxy.close();
    zkServer.shutdown();
  }

  @Test
  public void testConnectionManagerFollowsZooKeeperAcrossDisconnectsAndExpirations()
      throws Exception {
    Semaphore reconnected = new Semaphore(0);
    try (SolrZkClient client =
        new SolrZkClient.Builder()
            .withUrl("127.0.0.1:" + proxy.getListenPort())
            .withTimeout(60, TimeUnit.SECONDS)
            .withConnTimeOut(10, TimeUnit.SECONDS)
            .withReconnectListener(reconnected::release)
            .build()) {
      ConnectionManager connectionManager = client.getConnectionManager();
      assertTrue(connectionManager.isConnectedAndNotClosed());

      int stuck = 0;
      for (int i = 0; i < FLAPS; i++) {
        if (i % EXPIRE_EVERY == EXPIRE_EVERY - 1) {
          expireSession(client, reconnected);
        } else {
          flap(client);
        }

        try {
          new TimeOut(SETTLE_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
              .waitFor(
                  "ConnectionManager did not recover", connectionManager::isConnectedAndNotClosed);
        } catch (TimeoutException e) {
          stuck++;
          // Recover so one stuck iteration doesn't hide the others; a fresh SyncConnected resets
          // it.
          flap(client);
        }
      }

      assertTrue(
          "Expected the test to delay some Disconnected callbacks",
          slowDisconnects.delayed.get() > 0);
      assertEquals(
          "ConnectionManager stayed disconnected after ZooKeeper reconnected, in "
              + stuck
              + " of "
              + FLAPS
              + " disconnects or expirations",
          0,
          stuck);
    }
  }

  /** Expires the session, then waits for {@link ConnectionManager} to establish a new one. */
  private void expireSession(SolrZkClient client, Semaphore reconnected) throws Exception {
    long previousSession = client.getZooKeeper().getSessionId();
    client.getZooKeeper().getTestable().injectSessionExpiration();
    assertTrue(
        "ConnectionManager did not re-establish the session after it expired",
        reconnected.tryAcquire(DRAIN_SECONDS, TimeUnit.SECONDS));
    assertNotEquals(previousSession, client.getZooKeeper().getSessionId());
    awaitZooKeeperClientConnected(client);
  }

  /** Drops the connection, then waits for the client to notice and reconnect. */
  private void flap(SolrZkClient client) throws Exception {
    int disconnectsSeen = slowDisconnects.delayed.get();
    // The proxy may have had no live connection to drop, so try again if nothing was noticed.
    for (int attempt = 0; slowDisconnects.delayed.get() == disconnectsSeen; attempt++) {
      assertTrue("Could not drop the ZooKeeper connection", attempt < 10);
      proxy.close();
      proxy.reopen();
      try {
        new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME)
            .waitFor(
                "ConnectionManager never saw a Disconnected event",
                () -> slowDisconnects.delayed.get() > disconnectsSeen);
      } catch (TimeoutException e) {
        awaitZooKeeperClientConnected(client);
      }
    }
    awaitZooKeeperClientConnected(client);
    awaitDelayedCallbacksDone();
  }

  private void awaitZooKeeperClientConnected(SolrZkClient client) throws Exception {
    new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "ZooKeeper client did not reconnect",
            () -> client.getZooKeeper().getState().isConnected());
  }

  private void awaitDelayedCallbacksDone() throws Exception {
    new TimeOut(DRAIN_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor("Delayed callbacks did not finish", () -> slowDisconnects.inFlight.get() == 0);
  }
}
