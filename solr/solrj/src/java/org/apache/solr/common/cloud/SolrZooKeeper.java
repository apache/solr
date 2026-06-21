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
   * Test-only, client/reader connections only: when true, {@link #close()} interrupts the ZK SendThread
   * so the hardcoded {@code Thread.sleep(100)} in {@code ClientCnxnSocketNIO.cleanup()} returns
   * immediately. The graceful closeSession round-trip is still performed (ephemerals release promptly),
   * so this is behavior-preserving — it only avoids the wasted ~100ms nap per close. Always false in
   * production. Restricted to client connections after enabling it server-wide regressed an async
   * audit-event test.
   */
  private final boolean fastClose;

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
    if (fastClose) {
      // ZooKeeper 3.7.0's ClientCnxnSocketNIO.cleanup() runs a hardcoded Thread.sleep(100) on the
      // SendThread during shutdown, so close() blocks ~100ms in sendThread.join() per connection. That
      // sleep catches InterruptedException, so interrupting the SendThread makes it return immediately.
      // The graceful close (closeSession round-trip) below is otherwise untouched, so ephemerals still
      // release promptly. Test-only.
      interruptSendThread();
    }
    try {
      super.close();
    } catch (InterruptedException e) {

    }
  }

  /** Interrupts the ZK client SendThread so the hardcoded {@code Thread.sleep(100)} in
   *  {@code ClientCnxnSocketNIO.cleanup()} returns at once (test fast-close path; see {@link #fastClose}). */
  private void interruptSendThread() {
    try {
      AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
        try {
          final ClientCnxn c = getConnection();
          if (c != null) {
            final Field sendThreadFld = ClientCnxn.class.getDeclaredField("sendThread");
            sendThreadFld.setAccessible(true);
            final Object sendThread = sendThreadFld.get(c);
            if (sendThread instanceof Thread) {
              ((Thread) sendThread).interrupt();
            }
          }
        } catch (Exception e) {
          log.debug("fast-close: could not interrupt ZK SendThread; graceful close will proceed", e);
        }
        return null;
      });
    } catch (Throwable t) {
      log.debug("fast-close: error interrupting ZK SendThread", t);
    }
  }

}

