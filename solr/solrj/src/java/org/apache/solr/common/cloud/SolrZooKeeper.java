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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// we use this class to expose nasty stuff for tests
@SuppressWarnings({"try"})
public class SolrZooKeeper extends ZooKeeperAdmin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloseTracker closeTracker;

  /**
   * Test-only, client/reader connections only: when true, {@link #close()} cuts short the hardcoded
   * {@code Thread.sleep(100)} in {@code ClientCnxnSocketNIO.cleanup()} so close() does not block ~100ms in
   * {@code sendThread.join()} per connection. The graceful closeSession round-trip is preserved — the
   * SendThread is interrupted only after a short grace window (see {@link #startSendThreadInterrupter}), so
   * the close packet has time to be written before the interrupt fires and ephemerals still release
   * promptly. Always false in production. Restricted to client connections after enabling it server-wide
   * regressed an async audit-event test.
   */
  private final boolean fastClose;

  /**
   * Grace window (ms) the fast-close watchdog waits before interrupting the SendThread, leaving time for the
   * graceful closeSession packet to be written. Comfortably larger than a localhost round-trip yet far
   * smaller than the 100ms cleanup nap it short-circuits.
   */
  private static final long SEND_GRACE_MILLIS = 50;

  public SolrZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    this(connectString, sessionTimeout, watcher, false);
  }

  public SolrZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean fastClose) throws IOException {
    super(connectString, sessionTimeout, watcher);
    this.fastClose = fastClose;
    assert (closeTracker = new CloseTracker()) != null;
  }

  public ClientCnxn getConnection() {
    return cnxn;
  }

  public SocketAddress getSocketAddress() {
    return testableLocalSocketAddress();
  }

  public void closeCnxn() {
    final Runnable t = new Runnable() {
      @Override public void run() {
        AccessController.doPrivileged((PrivilegedAction<Void>) this::closeZookeeperChannel);
      }

      @SuppressForbidden(reason = "Hack for Zookeper needs access to private methods.") private Void closeZookeeperChannel() {
        final ClientCnxn cnxn = getConnection();

        try {
          final Field sendThreadFld = cnxn.getClass().getDeclaredField("sendThread");
          sendThreadFld.setAccessible(true);
          Object sendThread = sendThreadFld.get(cnxn);
          if (sendThread != null) {
            Method method = sendThread.getClass().getDeclaredMethod("testableCloseSocket");
            method.setAccessible(true);
            try {
              method.invoke(sendThread);
            } catch (InvocationTargetException e) {
              // is fine
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException("Closing Zookeeper send channel failed.", e);
        }

        return null; // Void
      }
    };

    ParWork.submit("ZkCloseConnectTestThread", t);
  }

  @Override
  public synchronized void close() {
    assert closeTracker != null ? closeTracker.close() : true;
    // Fast-close must NOT interrupt the SendThread before super.close(): super.close() is what queues the
    // graceful closeSession packet, and interrupting first makes the SendThread abort the write with
    // ClosedByInterruptException, so the session is dropped instead of closed gracefully and ephemerals
    // linger until session timeout. Instead start a watchdog that lets the close packet go out, then
    // interrupts the SendThread only after a short grace window to cut short the 100ms cleanup nap.
    Thread interrupter = fastClose ? startSendThreadInterrupter() : null;
    try {
      super.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // super.close() returned (the cleanup nap already happened or was interrupted); stop the watchdog so
      // it does not interrupt an unrelated thread if the SendThread field was reused.
      if (interrupter != null) {
        interrupter.interrupt();
      }
    }
  }

  /**
   * Starts a daemon watchdog that waits {@link #SEND_GRACE_MILLIS} for the graceful closeSession round-trip
   * to be written, then interrupts the ZK client SendThread if it is still alive so the hardcoded
   * {@code Thread.sleep(100)} in {@code ClientCnxnSocketNIO.cleanup()} returns at once (test fast-close path;
   * see {@link #fastClose}). Returns the watchdog thread, or {@code null} if the SendThread could not be
   * located.
   */
  private Thread startSendThreadInterrupter() {
    final Thread sendThread = findSendThread();
    if (sendThread == null) {
      return null;
    }
    Thread watchdog = new Thread("SolrZkFastCloseInterrupter") {
      @Override public void run() {
        try {
          sendThread.join(SEND_GRACE_MILLIS);
        } catch (InterruptedException e) {
          // close() finished before the grace window elapsed; nothing left to interrupt.
          return;
        }
        if (sendThread.isAlive()) {
          sendThread.interrupt();
        }
      }
    };
    watchdog.setDaemon(true);
    watchdog.start();
    return watchdog;
  }

  /** Reflectively locates the ZK client SendThread, or returns {@code null} if it cannot be found. */
  private Thread findSendThread() {
    try {
      return AccessController.doPrivileged((PrivilegedAction<Thread>) () -> {
        try {
          final ClientCnxn c = getConnection();
          if (c != null) {
            final Field sendThreadFld = ClientCnxn.class.getDeclaredField("sendThread");
            sendThreadFld.setAccessible(true);
            final Object sendThread = sendThreadFld.get(c);
            if (sendThread instanceof Thread) {
              return (Thread) sendThread;
            }
          }
        } catch (Exception e) {
          log.debug("fast-close: could not locate ZK SendThread; graceful close will proceed", e);
        }
        return null;
      });
    } catch (Throwable t) {
      log.debug("fast-close: error locating ZK SendThread", t);
      return null;
    }
  }

}

