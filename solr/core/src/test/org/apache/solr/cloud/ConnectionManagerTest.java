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
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;
import org.junit.Test;

public class ConnectionManagerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 3000;

  @Ignore
  public void testConnectionManager() throws Exception {

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());

        ZooKeeper zk = zkClient.getZooKeeper();
        assertTrue(zk instanceof TestableZooKeeper);
        ((TestableZooKeeper) zk).testableConnloss();
        server.expire(zkClient.getZooKeeper().getSessionId());

        Thread.sleep(TIMEOUT);

        assertTrue(cm.isLikelyExpired());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  public void testLikelyExpired() throws Exception {

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      RecordingConnectionStrategy strategy = new RecordingConnectionStrategy();
      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .withConnStrategy(strategy)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();
      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        strategy.fire(KeeperState.Disconnected);
        waitFor("disconnect was not applied", () -> !cm.isConnectedAndNotClosed());
        // disconnect shouldn't immediately set likelyExpired
        assertFalse(cm.isLikelyExpired());

        // but it should after the timeout
        Thread.sleep((long) (zkClient.getZkClientTimeout() * 1.5));
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // even if we disconnect immediately again
        strategy.fire(KeeperState.Disconnected);
        assertFalse(cm.isConnectedAndNotClosed());
        assertTrue(cm.isLikelyExpired());

        // reconnect -- should no longer be likely expired
        strategy.fire(KeeperState.SyncConnected);
        waitFor("reconnect was not applied", cm::isConnectedAndNotClosed);
        assertFalse(cm.isLikelyExpired());
      } finally {
        cm.close();
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testReconnectWhenZkDisappeared() throws Exception {
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(
            new SolrNamedThreadFactory("connectionManagerTest"));

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      MockZkClientConnectionStrategy strategy = new MockZkClientConnectionStrategy();
      SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .withConnStrategy(strategy)
              .build();
      ConnectionManager cm = zkClient.getConnectionManager();

      try {
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        ZooKeeper expiredKeeper = zkClient.getZooKeeper();

        // reconnect -- should no longer be likely expired
        strategy.fire(KeeperState.Expired);
        waitFor(
            "the expired session was not replaced",
            () -> zkClient.getZooKeeper() != expiredKeeper && cm.isConnectedAndNotClosed());
        assertFalse(cm.isLikelyExpired());
        assertTrue(cm.isConnectedAndNotClosed());
        assertTrue(strategy.isExceptionThrow());
      } finally {
        cm.close();
        zkClient.close();
        executor.shutdown();
      }
    } finally {
      server.shutdown();
    }
  }

  private static void waitFor(String message, Supplier<Boolean> condition) throws Exception {
    new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME).waitFor(message, condition);
  }

  /** Records the watcher of the latest ZooKeeper instance, so tests can deliver events to it. */
  private static class RecordingConnectionStrategy extends TestConnectionStrategy {
    private volatile Watcher watcher;

    @Override
    protected ZooKeeper newZooKeeperInstance(
        String serverAddress, int zkClientTimeout, Watcher watcher) throws IOException {
      this.watcher = watcher;
      return super.newZooKeeperInstance(serverAddress, zkClientTimeout, watcher);
    }

    void fire(KeeperState state) {
      watcher.process(new WatchedEvent(EventType.None, state, null));
    }
  }

  private static class MockZkClientConnectionStrategy extends RecordingConnectionStrategy {
    volatile int called = 0;
    volatile boolean exceptionThrown = false;

    @Override
    public void reconnect(
        final String serverAddress,
        final int zkClientTimeout,
        final Watcher watcher,
        final ZkUpdate updater)
        throws IOException, InterruptedException, TimeoutException {

      if (called++ < 1) {
        exceptionThrown = true;
        throw new IOException("Testing");
      }

      super.reconnect(serverAddress, zkClientTimeout, watcher, updater);
    }

    public boolean isExceptionThrow() {
      return exceptionThrown;
    }
  }
}
