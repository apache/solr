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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2NullableObjectHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * All Solr ZooKeeper interactions should go through this class rather than
 * ZooKeeper. This class handles synchronous connects and reconnections.
 *
 */
public class SolrZkClient implements Closeable {
  private static final int MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW = 150;

  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 60000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final String INDENT = "  ";
  private static final Pattern COMPILE = Pattern.compile("\\n");
  private final int zkClientConnectTimeout;
  private CloseTracker closeTracker;

  private final ConnectionManager connManager;

  /**
   * Test-only: when set (via {@link #setFastCloseForTests} before {@link #start()}, by client/reader
   * paths such as {@link ZkStateReader#ZkStateReader(String, int, int)}), the underlying
   * {@link SolrZooKeeper} skips ZooKeeper's hardcoded ~100ms cleanup nap on close. The graceful
   * closeSession is still sent, so behavior is unchanged — only the wasted sleep is avoided.
   */
  private volatile boolean fastCloseForTests = false;

  private ZkCmdExecutor zkCmdExecutor;

  protected final ExecutorService zkCallbackExecutor = ParWork.getParExecutorService("coreAdminExecutor", 8, 32, 1000);

  private volatile boolean isClosed = true;

  private volatile int zkClientTimeout;
  private volatile ZkACLProvider zkACLProvider;
  private volatile String zkServerAddress;
  volatile IsClosed higherLevelIsClosed;
  private volatile boolean started;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public int getZkClientConnectTimeout() {
    return zkClientConnectTimeout;
  }

  // expert: for tests
  public SolrZkClient() {
    assert (closeTracker = new CloseTracker()) != null;
    zkClientConnectTimeout = 0;
    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, zkClientTimeout);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, DEFAULT_CLIENT_CONNECT_TIMEOUT);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout,  int zkClientConnectTimeout, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, onReconnect, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout, final OnReconnect onReconnect, BeforeReconnect beforeReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, onReconnect, beforeReconnect, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout, final OnReconnect onReconnect, BeforeReconnect beforeReconnect, ZkACLProvider zkACLProvider, IsClosed higherLevelIsClosed) {
    assert ObjectReleaseTracker.getInstance().track(this);
    log.info("Creating new zkclient instance timeout={} connectTimeout={}", zkClientTimeout, clientConnectTimeout);
    if (log.isDebugEnabled()) log.debug("Creating new {} instance {}", SolrZkClient.class.getSimpleName(), this);
    assert (closeTracker = new CloseTracker()) != null;
    this.zkServerAddress = zkServerAddress;
    this.higherLevelIsClosed = higherLevelIsClosed;

    this.zkClientTimeout = zkClientTimeout;
    this.zkClientConnectTimeout = clientConnectTimeout;

    if (zkACLProvider == null) {
      this.zkACLProvider = createZkACLProvider();
    } else {
      this.zkACLProvider = zkACLProvider;
    }

    zkCmdExecutor = new ZkCmdExecutor(this, 30, new IsClosed() {

      @Override
      public boolean isClosed() {
        try {

          return isClosed;

        } catch (NullPointerException e) {
          log.error("ZkClient is null", e);
          throw e;
        }
      }
    });

    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, zkClientTimeout, onReconnect, beforeReconnect);

    ZkCredentialsProvider zkCredentialsToAddAutomatically = createZkCredentialsToAddAutomatically();
    if (zkCredentialsToAddAutomatically != null) {
      connManager.setZkCredentialsToAddAutomatically(zkCredentialsToAddAutomatically);
    }
  }

  public SolrZkClient start() {
    if (started) {
      throw new IllegalStateException("Already started");
    }
    started = true;
    if (log.isDebugEnabled()) log.debug("Starting {} instance {}", SolrZkClient.class.getSimpleName(), this);
    try {
      connManager.start();
      // Use the session timeout as an upper bound so that a client constructed with a
      // short session timeout (e.g. tests probing an invalid address) fails fast.
      // For a valid address the connection typically arrives well within the session timeout.
      // start() does not propagate the timeout as an exception; callers that need a hard
      // guarantee should check isConnected() or rely on individual operation failures.
      int connectWait = (zkClientConnectTimeout > 0 && zkClientTimeout > 0)
          ? Math.min(zkClientConnectTimeout, zkClientTimeout)
          : Math.max(zkClientConnectTimeout, zkClientTimeout);
      try {
        connManager.waitForConnected(connectWait);
      } catch (SolrException e) {
        log.warn("Could not connect to ZooKeeper {} within {}ms; operations will fail until connected",
            zkServerAddress, connectWait);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
    isClosed = false;
    return this;
  }

  public void setOnReconnect(OnReconnect onReconnect) {
    this.connManager.setOnReconnect(onReconnect);
  }

  public ConnectionManager getConnectionManager() {
    return connManager;
  }

  /** Test-only: enable fast close (skip ZooKeeper's hardcoded cleanup nap) for this connection. Must be
   *  called before {@link #start()}. Only set for client/reader connections (graceful closeSession is
   *  still sent regardless; we restrict it to clients out of caution after server-wide use regressed an
   *  async audit-event test). */
  public SolrZkClient setFastCloseForTests(boolean fastClose) {
    this.fastCloseForTests = fastClose;
    return this;
  }

  /** Test-only: whether this connection's {@link SolrZooKeeper} should skip ZooKeeper's hardcoded
   *  cleanup nap on close (the graceful closeSession is still sent). Set via {@link #setFastCloseForTests}
   *  by client/reader paths; off in production. */
  public boolean isFastCloseForTests() {
    return fastCloseForTests;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";
  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName = System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        if (log.isDebugEnabled()) log.debug("Using ZkCredentialsProvider: {}", zkCredentialsProviderClassName);
        return (ZkCredentialsProvider)Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor", t);
      }
    }
    if (log.isDebugEnabled()) log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";
  protected ZkACLProvider createZkACLProvider() {
    String zkACLProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkACLProviderClassName)) {
      try {
        if (log.isDebugEnabled()) log.debug("Using ZkACLProvider: {}", zkACLProviderClassName);
        return (ZkACLProvider)Class.forName(zkACLProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkACLProvider does not point to a class implementing ZkACLProvider and with a non-arg constructor", t);
      }
    }
    if (log.isDebugEnabled()) log.debug("Using default ZkACLProvider");
    return new DefaultZkACLProvider();
  }

  /**
   * Returns true if client is connected
   */
  public boolean isConnected() {
    if (!started || isClosed) {
      return false;
    }
    try {
      return connManager.getKeeper().getState().isConnected() && connManager.isConnected();
    } catch (AlreadyClosedException e) {
      // The underlying keeper/connection has been closed out from under us — not connected.
      return false;
    }
  }

  public boolean isAlive() {
    try {
      return started && !isClosed && connManager.getKeeper().getState().isAlive();
    } catch (AlreadyClosedException e) {
      return true;
    }
  }

  public void delete(final String path, final int version) throws KeeperException, InterruptedException {
    delete(path, version, true, true);
  }

  public void delete(final String path, final int version, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {
    delete(path, version, retryOnConnLoss, true);
  }

  public void delete(final String path, final int version, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws InterruptedException, KeeperException {

    if (retryOnConnLoss) {
      ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
        connManager.getKeeper().delete(path, version);
        return null;
      }, retryOnSessionExpiration);
    } else {
      connManager.getKeeper().delete(path, version);
    }
  }

  public void deleteAsync(final String path, final int version, AsyncCallback.VoidCallback cb)
      throws InterruptedException, KeeperException {
    connManager.getKeeper().delete(path, version, cb, "");
  }

  public void deleteAsync(final String path, final int version)
      throws InterruptedException, KeeperException {
    connManager.getKeeper().delete(path, version, (rc, path1, ctx) -> {
      if (rc != 0) {
        KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
        log.warn("Exception deleting znode path={}", path1, e);
      }
    }, "");
  }

  /**
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a watch object will
   * only be triggered once for a given notification, users need to wrap their watcher using this method before
   * calling {@link #exists(String, Watcher, boolean)} or
   * {@link #getData(String, Watcher, Stat, boolean)}.
   */
  public Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null || watcher instanceof ProcessWatchWithExecutor) return watcher; // || watcher instanceof DoNotWrap

    return new ProcessWatchWithExecutor(watcher, this);
  }

  public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return exists(path, watcher, true);
  }

  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return exists(path, watcher, retryOnConnLoss, true);
  }

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   * <p>
   * If the watch is non-null and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that creates/delete the node or sets
   * the data on the node.
   *
   * @param path the node path
   * @param watcher explicit watcher
   * @return the stat of the node of the given path; return null if no such a
   *         node exists.
   * @throws KeeperException If the server signals an error
   * @throws InterruptedException If the server transaction is interrupted.
   * @throws IllegalArgumentException if an invalid path is specified
   */
  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().exists(path, watcher == null ? null : wrapWatcher(watcher)), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().exists(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  /**
   * Returns true if path exists
   */
  public Boolean exists(final String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      Stat existsStat = ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().exists(path, null));
      if (log.isDebugEnabled()) log.debug("exists state return is {} {}", path, existsStat);
      return existsStat != null;
    } else {
      Stat existsStat = connManager.getKeeper().exists(path, null);
      if (log.isDebugEnabled()) log.debug("exists state return is {} {}", path, existsStat);
      return existsStat != null;
    }
  }

  public Boolean exists(final String path) throws KeeperException, InterruptedException {
    return this.exists(path, true);
  }

      /**
       * Returns children of the node at the path
       */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher)));
    } else {
      return connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  public List<String> getChildren(final String path, final Watcher watcher, Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return getChildren(path, watcher, stat, retryOnConnLoss, false);
  }

  public List<String> getChildren(final String path, final Watcher watcher, Stat stat, boolean retryOnConnLoss,  boolean retrySessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher), stat),
          retrySessionExpiration);
    } else {
      return connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException, InterruptedException {
    return getData(path, watcher, stat, true);
  }

  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return getData(path, watcher, stat, retryOnConnLoss, false);
  }

      /**
       * Returns node's data
       */
  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss && zkCmdExecutor != null) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), stat), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), stat);
    }
  }

  public void getData(final String path, final Watcher watcher, AsyncCallback.DataCallback cb, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss && zkCmdExecutor != null) {
       ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
         connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), cb, null);
         return null;
       }, false);
    } else {
      connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), cb, null);
    }
  }

  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return setData(path, data, version, retryOnConnLoss, false);
  }

  public void setData(final String path, final byte data[], final int version, AsyncCallback.StatCallback cb, Object ctx)
      throws KeeperException, InterruptedException {
    connManager.getKeeper().setData(path, data, version, cb, ctx);
  }

  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, new SetData(connManager, path, data, version), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().setData(path, data, version);
    }
  }

  public void atomicUpdate(String path, Function<byte[], byte[]> editor) throws KeeperException, InterruptedException {
   atomicUpdate(path, (stat, bytes) -> editor.apply(bytes));
  }

  public void atomicUpdate(String path, BiFunction<Stat , byte[], byte[]> editor) throws KeeperException, InterruptedException {
    for (; ; ) {
      byte[] modified;
      byte[] zkData = null;
      Stat s = new Stat();
      try {
        if (exists(path)) {
          zkData = getData(path, null, s);
          modified = editor.apply(s, zkData);
          if (modified == null) {
            //no change , no need to persist
            return;
          }
          setData(path, modified, s.getVersion(), true);
          break;
        } else {
          modified = editor.apply(s,null);
          if (modified == null) {
            //no change , no need to persist
            return;
          }
          create(path, modified, CreateMode.PERSISTENT, true);
          break;
        }
      } catch (KeeperException.BadVersionException | NodeExistsException e) {
        log.error("atomic update exception", e);
      }
    }


  }


  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public void create(final String path, final byte data[], CreateMode createMode, AsyncCallback.Create2Callback cb) throws KeeperException, InterruptedException {
    List<ACL> acls = zkACLProvider.getACLsToAdd(path);
    connManager.getKeeper().create(path, data, acls, createMode, cb, "create", -1);
  }

  public String create(final String path, final byte[] data, final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    return create(path, data, createMode, retryOnConnLoss, false);
  }


  public void sync(String path, AsyncCallback.VoidCallback cb) {
    connManager.getKeeper().sync(path, cb, "");
  }

  /**
   * Returns path of created node
   */
  public String create(final String path, final byte[] data, final CreateMode createMode, boolean retryOnConnLoss, boolean retryOnSessionExp) throws KeeperException, InterruptedException {
    List<ACL> acls = zkACLProvider.getACLsToAdd(path);

    if (retryOnConnLoss) {
      // create is not idempotent: a ConnectionLoss may be returned for a create that actually
      // succeeded on the server, so the retry would throw NodeExistsException for our own node and
      // surface as a spurious hard failure. Reconcile by treating NodeExists as success only when it
      // arrives on a retry (a prior attempt was already made) of a non-sequential create — meaning
      // the node we are seeing is the one our lost-ack attempt created. A first-attempt NodeExists
      // (node truly pre-existed) still propagates. Sequential creates cannot be reconciled (the
      // assigned name is unknown after a lost ack), so they are left to fail as before.
      final boolean[] attempted = {false};
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
        boolean isRetry = attempted[0];
        attempted[0] = true;
        try {
          return connManager.getKeeper().create(path, data, acls, createMode);
        } catch (NodeExistsException e) {
          if (isRetry && !createMode.isSequential()) {
            return path;
          }
          throw e;
        }
      }, retryOnSessionExp);
    } else {
      return connManager.getKeeper().create(path, data, acls, createMode);
    }
  }

  public String create(final String path, final File file, final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException, InterruptedException, IOException {
    byte[] data = FileUtils.readFileToByteArray(file);
    return create(path, data, createMode, retryOnConnLoss);
  }

  public void makePath(String path, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, File file, boolean failOnExists, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file),
        CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file), retryOnConnLoss);
  }

  public void makePath(String path, CreateMode createMode, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, createMode, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, true, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, failOnExists, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * skipPathParts will force the call to fail if the first skipPathParts do not exist already.
   *
   * Note: retryOnConnLoss is only respected for the final node - nodes
   * before that are always retried on connection loss.
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss, int skipPathParts) throws KeeperException, InterruptedException {

    if (log.isDebugEnabled()) log.debug("makePath: {}", path);

    boolean retry = true;
    if (!path.isEmpty() && path.charAt(0) == '/') {
      path = path.substring(1);
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder(128);
    int sz = paths.length;
    for (int i = 0; i < sz; i++) {
      String pathPiece = paths[i];
      sbPath.append('/').append(pathPiece);
      if (i < skipPathParts) {
        continue;
      }
      byte[] bytes = null;
      final String currentPath = sbPath.toString();

      CreateMode mode = CreateMode.PERSISTENT;
      if (i == paths.length - 1) {
        mode = createMode;
        bytes = data;
        if (!retryOnConnLoss) retry = false;
      }

      try {
        if (retry) {
          final CreateMode finalMode = mode;
          final byte[] finalBytes = bytes;
          ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
            connManager.getKeeper().create(currentPath, finalBytes, zkACLProvider.getACLsToAdd(currentPath), finalMode);
            return null;
          });
        } else {
          connManager.getKeeper().create(currentPath, bytes, zkACLProvider.getACLsToAdd(currentPath), mode);
        }
      } catch (NoAuthException e) {
        // in auth cases, we may not have permission for an earlier part of a path, which is fine
        if (i == paths.length - 1 || !exists(currentPath, retryOnConnLoss)) {

          throw e;
        }
      } catch (NodeExistsException e) {

        if (!failOnExists && i == paths.length - 1) {
          // TODO: version ? for now, don't worry about race
          setData(currentPath, data, -1, retryOnConnLoss);
          // set new watch
          exists(currentPath, watcher, retryOnConnLoss);
          return;
        }

        // ignore unless it's the last node in the path
        if (i == paths.length - 1) {
          throw e;
        }
      }

    }
  }

  public void makePath(String zkPath, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher, retryOnConnLoss);
  }

  public void mkDirs(String path, byte[] bytes) throws KeeperException {
    Map dataMap = new Object2ObjectOpenHashMap(1);
    dataMap.put(path, bytes);
    mkdirs(dataMap);
  }

  public void mkdirs(String... paths) throws KeeperException {
    Map dataMap = new Object2NullableObjectHashMap(paths.length, Hashing.DEFAULT_LOAD_FACTOR);
    for (String path : paths) {
      dataMap.put(path, null);
    }
    mkdirs(dataMap);
  }

  public void mkdirs(Map<String,byte[]> dataMap) throws KeeperException {
    mkDirs(dataMap, Collections.emptyMap(), 0, true);
  }

  /**
   * @param overwriteExistingData when true, a node that already exists is overwritten via setData with the
   *   value supplied in {@code dataMap} (used by config upload, which intentionally re-writes data onto
   *   existing nodes). When false, existing nodes are left untouched — create-only / idempotent "mkdir -p"
   *   semantics — so seed data never clobbers pre-existing state (e.g. /aliases.json on a cluster re-init).
   */
  public void mkdirs(Map<String,byte[]> dataMap, boolean overwriteExistingData) throws KeeperException {
    mkDirs(dataMap, Collections.emptyMap(), 0, overwriteExistingData);
  }

  public void mkdirs(Map<String,byte[]> dataMap, int pathsAlreadyCreated) throws KeeperException {
    mkDirs(dataMap, Collections.emptyMap(), pathsAlreadyCreated, true);
  }

  public void mkdirs(Map<String,byte[]> dataMap, Map<String,CreateMode> createModeMap) throws KeeperException {
    mkDirs(dataMap, createModeMap, 0, true);
  }

  final static Pattern REPLACESLASH = Pattern.compile("/");

  public void mkDirs(Map<String,byte[]> dataMap, Map<String,CreateMode> createModeMap, int pathsAlreadyCreated) throws KeeperException {
    mkDirs(dataMap, createModeMap, pathsAlreadyCreated, true);
  }

  public void mkDirs(Map<String,byte[]> dataMap, Map<String,CreateMode> createModeMap, int pathsAlreadyCreated, boolean overwriteExistingData) throws KeeperException {

    Set<String> paths = dataMap.keySet();

    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String paths={}) - start", paths);
    }
    //Set<String> madePaths = new HashSet<>(paths.size() * 3);
    List<String> pathsToMake = new ArrayList<>(paths.size() * 3);

    for (String fullpath : paths) {
      if (!(!fullpath.isEmpty() && fullpath.charAt(0) == '/')) throw new IllegalArgumentException("Paths must start with /, " + fullpath);
      StringBuilder sb = new StringBuilder(128);
      if (log.isDebugEnabled()) {
        log.debug("path {}", fullpath);
      }
      String[] subpaths = fullpath.split("/");
      int cnt = 0;
      for (String subpath : subpaths) {
        if (subpath.isEmpty()) continue;
        cnt++;
        if (subpath.length() == 0) continue;
        if (log.isDebugEnabled()) {
          log.debug("subpath {}", subpath);
        }
        sb.append('/').append(REPLACESLASH.matcher(subpath).replaceAll(""));
        if (cnt > pathsAlreadyCreated) {
          String path = sb.toString();
          log.debug("path to make: {}", path);
          if (!pathsToMake.contains(path)) {
            pathsToMake.add(path);
          }
        }
      }
    }

    Set<String> nodeAlreadyExistsPaths = ConcurrentHashMap.newKeySet();

    CountDownLatch latch = new CountDownLatch(pathsToMake.size());
    // Capture the FIRST non-NODEEXISTS create failure across all the concurrent callbacks. This used
    // to be single-slot arrays (code/path/failed/nodata) reset at the top of each loop iteration while
    // prior iterations' callbacks were still running on the ZK EventThread, so a later iteration's reset
    // (or a success) could clobber a captured error and mkDirs would return success with a node never
    // created (review finding H2). One immutable, atomically-set record removes the race.
    AtomicReference<MkDirsError> firstError = new AtomicReference<>();
    for (String makePath : pathsToMake) {
      if (!(!makePath.isEmpty() && makePath.charAt(0) == '/')) makePath = "/" + makePath;

      byte[] data = dataMap.get(makePath);


      CreateMode createMode;
      if (createModeMap != null) {
        createMode = createModeMap.getOrDefault(makePath, CreateMode.PERSISTENT);
      } else {
        createMode = CreateMode.PERSISTENT;
      }

      if (log.isDebugEnabled()) log.debug("makepath {}", makePath + " data: " + (data == null ? "none" : data.length + "b"));


      connManager.getKeeper().create(makePath, data, zkACLProvider.getACLsToAdd(makePath), createMode,
          new MkDirsCallback(nodeAlreadyExistsPaths, firstError, latch), "");
    }


    boolean success;
    try {
      success = latch.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      log.error("mkDirs(String={})", paths, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    // The latch.await return was previously ignored: a timeout left some async creates unacked while
    // the method returned "successfully", so a partially-created path tree looked complete. Fail
    // explicitly so the caller does not proceed on an incomplete create batch.
    if (!success) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Timeout waiting for mkDirs operations to complete paths=" + paths);
    }

    // Surface the first real (non-NODEEXISTS) create failure so the caller does not proceed on an
    // incomplete path tree.
    MkDirsError err = firstError.get();
    if (err != null) {
      throw KeeperException.create(KeeperException.Code.get(err.code), err.path);
    }

    // A node that already existed was skipped by create() above (NODEEXISTS), so any explicit data
    // supplied for it in dataMap was never written. When overwriteExistingData is set, apply it now via
    // setData. This is required to write data onto an existing (e.g. intermediate/non-leaf) znode — see
    // ZkMaintenanceUtils' zknode.data handling, which targets a non-leaf parent node that frequently
    // already exists. Only paths with non-null data are touched, so ancestor segments (null data) are
    // left untouched. Callers seeding default cluster state (e.g. createClusterZkNodes) pass
    // overwriteExistingData=false so they never clobber pre-existing nodes such as /aliases.json.
    if (overwriteExistingData) {
      for (String existingPath : nodeAlreadyExistsPaths) {
        byte[] existingData = dataMap.get(existingPath);
        if (existingData != null) {
          try {
            setData(existingPath, existingData, true);
          } catch (KeeperException.NoNodeException e) {
            // raced away between create and setData; nothing to update
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          }
        }
      }
    }


    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String) - end");
    }
  }

  public Map<String,byte[]> getData(List<String> paths) {

    Map<String,byte[]> dataMap = Collections.synchronizedMap(new LinkedHashMap<>());
    CountDownLatch latch = new CountDownLatch(paths.size());
    // A non-NONODE rc (e.g. CONNECTIONLOSS) was previously dropped silently, so a failed read was
    // indistinguishable from an absent node (path simply missing from the returned map). Capture the
    // first real error and rethrow it after the await so callers see read failures.
    KeeperException[] firstError = new KeeperException[1];

    for (String path : paths) {
      try {
        connManager.getKeeper().getData(path, false, (rc, path1, ctx, data, stat) -> {
          try {
            if (rc != 0) {
              final KeeperException.Code keCode = KeeperException.Code.get(rc);
              if (keCode == KeeperException.Code.NONODE) {
                if (log.isDebugEnabled()) log.debug("No node found for {}", path1);
              } else {
                synchronized (firstError) {
                  if (firstError[0] == null) {
                    firstError[0] = KeeperException.create(keCode, path1);
                  }
                }
              }
            } else {
              dataMap.put(path1, data);
            }
          } finally {
            latch.countDown();
          }

        }, null);

      } catch (Exception e) {
        latch.countDown();
        throw e;
      }
    }

    boolean success;
    try {
      success = latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      log.error("getData(paths={})", paths, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    if (!success) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for operations to complete count=" + latch);
    }

    synchronized (firstError) {
      if (firstError[0] != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error reading zk paths " + paths, firstError[0]);
      }
    }

    return dataMap;
  }

  public CountDownLatch delete(Collection<String> paths, boolean wait, boolean noErrorLogOnMissing) throws KeeperException {
    if (log.isDebugEnabled()) log.debug("delete paths {} wait={}", paths, wait);
    CountDownLatch latch = new CountDownLatch(paths.size());

    KeeperException[] ke = new KeeperException[1];
    for (String path : paths) {
      if (log.isDebugEnabled()) log.debug("process path={} connManager={}", path, connManager);
      try {
        connManager.getKeeper().delete(path, -1, (rc, path1, ctx) -> {
          try {
            // MRM TODO:
            if (log.isDebugEnabled()) {
              log.debug("async delete resp rc={}, path1={}, ctx={}", rc, path1, ctx);
            }
            if (rc != 0) {
              if (rc == -101 && !noErrorLogOnMissing) {
                log.error("got zk error deleting paths {}", rc);
              }
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              if (e instanceof NoNodeException) {
                if (log.isDebugEnabled()) log.debug("Problem removing zk node {}", path1);
              } else if (e instanceof KeeperException.ConnectionLossException) {
                connManager.waitForConnected();
                try {
                  delete(paths, true, noErrorLogOnMissing);
                } catch (KeeperException keeperException) {
                  log.error("got zk error deleting paths", keeperException);
                }
              } else {
                ke[0] = e;
              }
            }
          } finally {
            latch.countDown();
          }
        }, null);
      } catch (Exception e) {
        latch.countDown();
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("done with all paths, see if wait ... wait={}", wait);
    }
    if (wait) {
      try {
        boolean success = latch.await(10, TimeUnit.SECONDS);

        if (log.isDebugEnabled()) log.debug("done waiting on latch, success={}", success);
        if (success) {
          if (ke[0] != null) {
            throw ke[0];
          }
        }
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("done with delete {} {}", paths, wait);
    }

    return latch;
  }

  // Calls setData for a list of existing paths in parallel
  private void updateExistingPaths(List<String> pathsToUpdate, Map<String,byte[]> dataMap) throws KeeperException {
    final KeeperException[] keeperExceptions = new KeeperException[1];
    pathsToUpdate.parallelStream().forEach(new PathConsumer(dataMap, keeperExceptions));
    if (keeperExceptions[0] != null) {
      throw keeperExceptions[0];
    }
  }

  public void data(String path, byte[] data) throws KeeperException {
    try {

      connManager.getKeeper().setData(path, data, -1);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
  }

  public void mkdirs(String znode, File file) throws KeeperException {
    try {
      mkDirs(znode, FileUtils.readFileToByteArray(file));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
  }

  public void mkdir(String path) throws KeeperException, InterruptedException {
    mkdir(path, null);
  }

  public void mkdir(String path, byte[] data) throws KeeperException, InterruptedException {
    mkdir(path, data, CreateMode.PERSISTENT);
  }

  public String mkdir(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("mkdir path={}", path);
    boolean retryOnConnLoss = true; // MRM TODO:
    if (retryOnConnLoss) {
      ZkCmdExecutor.retryOperation(zkCmdExecutor, new CreateZkOperation(path, data, createMode), true);
    } else {
      String createdPath;
      try {
    
        createdPath = connManager.getKeeper().create(path, data, zkACLProvider.getACLsToAdd(path), createMode);
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, path, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
      }
      return createdPath;
    }
    return null;
  }

  /**
   * Write data to ZooKeeper.
   */
  public Stat setData(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    return setData(path, data, -1, retryOnConnLoss);
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   *
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param file path to file to be uploaded
   */
  public Stat setData(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Write to ZooKeeper: {} to {}", file.getAbsolutePath(), path);
    }
    byte[] data = FileUtils.readFileToByteArray(file);
    return setData(path, data, retryOnConnLoss);
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
     return multi(ops, retryOnConnLoss, true);
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss, boolean retryOnSessionExp) throws InterruptedException, KeeperException  {
  
    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().multi(ops), retryOnSessionExp);
    } else {
      return connManager.getKeeper().multi(ops);
    }
  }

  public void printLayout(String path, int indent, int maxBytesBeforeSuppress, StringBuilder output) {
    try {
      printLayout(path, "", indent, maxBytesBeforeSuppress, output);
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  public void printLayout(String path, int indent, StringBuilder output) {
    try {
      printLayout(path, "", indent, MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW, output);
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  private static Pattern ENDS_WITH_INT_SORT = Pattern.compile(".*?(\\d+)");

  /**
   * Fills string with printout of current ZooKeeper layout.
   */
  public void printLayout(String path, String node, int indent, int maxBytesBeforeSuppress, StringBuilder output) {
    try {
      //log.info("path={} node={} indext={}", path, node, indent);

      //    if (node != null && node.length() > 0) {
      //      path = path;
      //    }

      List<String> children = null;
      if (!path.trim().equals("/")) {
        byte[] data;
        Stat dataStat = new Stat();
        try {
          dataStat = exists(path, null);
          children = getChildren(path, null, true);
          Collections.sort(children);
          children.sort(Comparator.comparingInt(s -> {
            Matcher m = ENDS_WITH_INT_SORT.matcher(s);
            if (m.matches() && m.group(1).length() < "328229213983290".length()) {
              String endingInt = m.group(1);
              return Integer.parseInt(endingInt);
            }
            return 0;
          }));
        } catch (Exception e1) {
          if (e1 instanceof NoNodeException) {
            // things change ...
            children = Collections.emptyList();
          } else {
            ParWork.propagateInterrupt(e1, true);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Problem with path='" + path + "'", e1);
          }
        }
        StringBuilder dent = new StringBuilder(128);
        for (int i = 0; i < indent; i++) {
          dent.append(INDENT);
        }
        String childrenString;
        if (children.size() > 0) {
          childrenString = "c=" + children.size() + ',';
        } else {
          childrenString = "";
        }
        output.append(dent.toString()).append(children.size() == 0 ? node : '+' + node).append(" [").append(childrenString).append("v=").append(dataStat == null ? "?" : dataStat.getVersion())
            .append(']');
        StringBuilder dataBuilder = new StringBuilder(128);
        String dataString;
        if (dataStat != null && dataStat.getDataLength() > 0) {
//          if (path.endsWith(".json")) {
//            dataString = Utils.fromJSON(data).toString();
//          } else {

        //  }
          int lines = 0;
//          if (maxBytesBeforeSuppress != MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW) {
//            lines = 0;
//          } else {
//            lines = dataString.split("\\r\\n|\\r|\\n").length;
//          }

          if (dataStat.getDataLength() < maxBytesBeforeSuppress || path.endsWith("state.json") || path.endsWith("security.json") || path.endsWith("solrconfig.xml") && Boolean
              .getBoolean("solr.tests.printsolrconfig") || path.contains("/terms/") || path.endsWith("leader")) {
            //        if (path.endsWith(".xml")) {
            //          // this is the cluster state in xml format - lets pretty print
            //          dataString = prettyPrint(path, dataString);
            //        }
            try {
              data = getData(path, null, dataStat, true);
            } catch (NoNodeException e) {
              // things change
              return;
            }
            dataString = new String(data, StandardCharsets.UTF_8);
            dataString = COMPILE.matcher(dataString).replaceAll("\n" + dent + INDENT);

            dataBuilder.append(" (").append(dataStat.getDataLength()).append("b) : ").append(lines > 1 ? "\n" + dent + INDENT : "").append(dataString.trim()).append(NEWL);
          } else {
            dataBuilder.append(" (").append(dataStat.getDataLength()).append("b) : ...supressed...").append(NEWL);
          }
        } else {
          output.append(NEWL);
        }
        output.append(dataBuilder);
        indent += 2;
      } else {
        output.append("/");
      }
      if (children == null) {
        try {
          children = getChildren(path, null, true);
        } catch (KeeperException | InterruptedException e1) {
          if (e1 instanceof NoNodeException) {
            // things change ...
            children = Collections.emptyList();
          } else {
            ParWork.propagateInterrupt(e1, true);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Problem with path='" + path + "'", e1);
          }
        }
      }
      if (children != null) {
        for (String child : children) {
          if (!child.equals("quota") && !child.equals("/zookeeper")) {
            printLayout(path.equals("/") ? "/" + child : path + "/" + child, child, indent, maxBytesBeforeSuppress, output);
          }
        }
      }
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  public void printLayout() {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/",0, sb);
    log.warn("\n\n_____________________________________________________________________\n\n\nZOOKEEPER LAYOUT:\n\n{}\n\n_____________________________________________________________________\n\n",
        sb.toString());
  }

  public void printLayoutToStream(PrintStream out) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/", 0, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, String path) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout(path, 0, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, int maxBytesBeforeSuppress) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/", 0, maxBytesBeforeSuppress, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, String path, int maxBytesBeforeSuppress) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout(path, 0, maxBytesBeforeSuppress, sb);
    out.println(sb.toString());
  }

  public void printLayoutToFile(Path file) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/",0, sb);
    try {
      Files.writeString(file, sb.toString(), StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing {} instance {}", SolrZkClient.class.getSimpleName(), this);

    isClosed = true;
    connManager.close();

    assert closeTracker != null ? closeTracker.close() : true;
    assert ObjectReleaseTracker.getInstance().release(this);
  }

  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Validates if zkHost contains a chroot. See http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
   */
  public static boolean containsChroot(String zkHost) {
    return zkHost.contains("/");
  }

  /**
   * Check to see if a Throwable is an InterruptedException, and if it is, set the thread interrupt flag
   * @param e the Throwable
   * @return the Throwable
   */
  public static Throwable checkInterrupted(Throwable e) {
    if (e instanceof InterruptedException)
      Thread.currentThread().interrupt();
    return e;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  /**
   * Gets the raw config node /zookeeper/config as returned by server. Response may look like
   * <pre>
   * server.1=localhost:2780:2783:participant;localhost:2791
   * server.2=localhost:2781:2784:participant;localhost:2792
   * server.3=localhost:2782:2785:participant;localhost:2793
   * version=400000003
   * </pre>
   * @return Multi line string representing the config. For standalone ZK this will return empty string
   */
  public String getConfig() {
    try {
      Stat stat = new Stat();

      connManager.getKeeper().sync(ZooDefs.CONFIG_NODE, null, null);
      byte[] data = connManager.getKeeper().getConfig(false, stat);
      if (data == null || data.length == 0) {
        return "";
      }
      return new String(data, StandardCharsets.UTF_8);
    } catch (KeeperException|InterruptedException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to get config from zookeeper", ex);
    }
  }

  public ZkACLProvider getZkACLProvider() {
    return zkACLProvider;
  }

  /**
   * Set the ACL on a single node in ZooKeeper. This will replace all existing ACL on that node.
   *
   * @param path path to set ACL on e.g. /solr/conf/solrconfig.xml
   * @param acls a list of {@link ACL}s to be applied
   * @param retryOnConnLoss true if the command should be retried on connection loss
   */
  public Stat setACL(String path, List<ACL> acls, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
  
      return connManager.getKeeper().setACL(path, acls, -1);
  }

  public void setHigherLevelIsClosed(IsClosed isClosed) {
    this.higherLevelIsClosed = isClosed;
  }

  public IsClosed getHigherLevelIsClosed() {
    return this.higherLevelIsClosed;
  }

  /**
   * Update all ACLs for a zk tree based on our configured {@link ZkACLProvider}.
   * @param root the root node to recursively update
   */
  public void updateACLs(final String root) throws KeeperException, InterruptedException {
    ZkMaintenanceUtils.traverseZkTree(this, root, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, path -> {
      try {
        setACL(path, zkACLProvider.getACLsToAdd(path), true);
        log.debug("Updated ACL on {}", path);
      } catch (NoNodeException e) {
        // If a node was deleted, don't bother trying to set ACLs on it.
      }
    });
  }

  // Some pass-throughs to allow less code disruption to other classes that use SolrZkClient.
  public void clean(String path) throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.clean(this, path);
  }

  public void cleanChildren(String path) throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.cleanChildren(this, path);
  }

  public void clean(String path, Predicate<String> nodeFilter) throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) log.debug("clean path {}", path);
    ZkMaintenanceUtils.clean(this, path, nodeFilter);
  }

  public void upConfig(Path confPath, String confName) throws IOException, KeeperException {
    ZkMaintenanceUtils.upConfig(this, confPath, confName);
  }

  public String listZnode(String path, Boolean recurse) throws KeeperException, InterruptedException, SolrServerException {
    return ZkMaintenanceUtils.listZnode(this, path, recurse);
  }

  public void downConfig(String confName, Path confPath) throws IOException {
    ZkMaintenanceUtils.downConfig(this, confName, confPath);
  }

  public void zkTransfer(String src, Boolean srcIsZk,
                         String dst, Boolean dstIsZk,
                         Boolean recurse) throws SolrServerException, KeeperException, InterruptedException, IOException {
    ZkMaintenanceUtils.zkTransfer(this, src, srcIsZk, dst, dstIsZk, recurse);
  }

  public void moveZnode(String src, String dst) throws SolrServerException, KeeperException, InterruptedException {
    ZkMaintenanceUtils.moveZnode(this, src, dst);
  }

  public void uploadToZK(final Path rootPath, final String zkPath,
                         final Pattern filenameExclusions) throws IOException, KeeperException {
    ZkMaintenanceUtils.uploadToZK(this, rootPath, zkPath, filenameExclusions);
  }
  public void downloadFromZK(String zkPath, Path dir) throws IOException {
    ZkMaintenanceUtils.downloadFromZK(this, zkPath, dir);
  }

  public Op createPathOp(String path) {
    return createPathOp(path, null);
  }

  public Op createPathOp(String path, byte[] data) {
    return Op.create(path, data, zkACLProvider.getACLsToAdd(path), CreateMode.PERSISTENT);
  }

  public void setAclProvider(ZkACLProvider zkACLProvider) {
    this.zkACLProvider = zkACLProvider;
  }

  public void setIsClosed(IsClosed isClosed) {
    this.higherLevelIsClosed = isClosed;
  }

  public void setDisconnectListener(ConnectionManager.DisconnectListener dl) {
    this.connManager.setDisconnectListener(dl);
  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode) throws KeeperException, InterruptedException {
    addWatch(basePath, watcher, mode, false);
  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    addWatch(basePath, watcher, mode, retryOnConnLoss, false);
  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, boolean retryOnConnLoss, boolean retryOnSessionExp) throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      ZkCmdExecutor.retryOperation(zkCmdExecutor, () ->  {
        connManager.getKeeper().addWatch(basePath, watcher == null ? null : wrapWatcher(watcher), mode);
        return null;
      }, retryOnSessionExp);
    } else {
      connManager.getKeeper().addWatch(basePath, watcher == null ? null : wrapWatcher(watcher), mode);
    }

  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, AsyncCallback.VoidCallback cb, Object ctx) {
    connManager.getKeeper().addWatch(basePath, watcher == null ? null : wrapWatcher(watcher), mode, cb, ctx);
  }

  public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local, AsyncCallback.VoidCallback cb, Object ctx) {
    // addWatch() stores wrapWatcher(watcher) (a ProcessWatchWithExecutor); ZK matches watchers by
    // equals/hashCode, which on the wrapper delegate to the inner watcher but return false for a raw
    // (non-wrapper) argument. So we must wrap symmetrically here or the stored wrapper never matches
    // and ZK returns NoWatcher -> the (recursive) watch leaks forever (review finding H1).
    connManager.getKeeper().removeWatches(path, watcher == null ? null : wrapWatcher(watcher), watcherType, local, cb, ctx);
  }

  public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local) throws KeeperException, InterruptedException {
    // Wrap symmetrically with addWatch (see the overload above) so the stored wrapper matches.
    connManager.getKeeper().removeWatches(path, watcher == null ? null : wrapWatcher(watcher), watcherType, local);
  }

  public void removeAllWatches(String path) throws KeeperException, InterruptedException {
    connManager.getKeeper().removeAllWatches(path, Watcher.WatcherType.Any, true);
  }

  public long getSessionId() {
    return connManager.getKeeper().getSessionId();
  }

  /**
   * Watcher wrapper that ensures that heavy implementations of process do not interfere with our ability
   * to react to other watches, but also ensures that two wrappers containing equal watches are considered
   * equal (and thus we won't accumulate multiple wrappers of the same watch).
   */
  private final static class ProcessWatchWithExecutor implements Watcher { // see below for why final.
    private final Watcher watcher;
    SolrZkClient solrZkClient;

    ProcessWatchWithExecutor(Watcher watcher, SolrZkClient solrZkClient) {
      this.solrZkClient = solrZkClient;
      if (watcher == null) {
        throw new IllegalArgumentException("Watcher must not be null");
      }
      this.watcher = watcher;
    }

    @Override
    public void process(final WatchedEvent event) {
      try {
        if (watcher instanceof ConnectionManager) {
          solrZkClient.zkCallbackExecutor.submit(() -> watcher.process(event));
        } else {
          if (event.getType() != Event.EventType.None) {
            solrZkClient.zkCallbackExecutor.submit(() -> watcher.process(event));
          }
        }
      } catch (Exception e) {
        log.error("Will not process zookeeper notification due to exception", e);
      }
    }

    // These overrides of hashcode/equals ensure that we don't store the same exact watch
    // multiple times in org.apache.zookeeper.ZooKeeper.ZKWatchManager.dataWatches
    // (a Map<String<Set<Watch>>). This class is marked final to avoid oddball
    // cases with sub-classes, if you need different behavior, find a new class or make
    // sure you account for the case where two diff sub-classes with different behavior
    // for process(WatchEvent) and have been created with the same watch object.
    @Override
    public int hashCode() {
      return watcher.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ProcessWatchWithExecutor) {
        return this.watcher.equals(((ProcessWatchWithExecutor) obj).watcher);
      }
      return false;
    }
  }

  private static class SetData implements ZkOperation {
    private final ConnectionManager connManager;
    private final String path;
    private final byte[] data;
    private final int version;

    public SetData(ConnectionManager connManager, String path, byte[] data, int version) {
      this.connManager = connManager;
      this.path = path;
      this.data = data;
      this.version = version;
    }

    @Override
    public Object execute() throws KeeperException, InterruptedException {
      // Read the live ZooKeeper handle per attempt: after a session expiry/reconnect the
      // SolrZooKeeper instance is swapped, and retries must target the current session
      // rather than a stale/closed handle (otherwise the write silently never lands).
      return connManager.getKeeper().setData(path, data, version);
    }
  }

  private static class MkDirsCallback implements AsyncCallback.Create2Callback {
    private final Set<String> nodeAlreadyExistsPaths;
    private final AtomicReference<MkDirsError> firstError;
    private final CountDownLatch latch;

    public MkDirsCallback(Set<String> nodeAlreadyExistsPaths, AtomicReference<MkDirsError> firstError, CountDownLatch latch) {
      this.nodeAlreadyExistsPaths = nodeAlreadyExistsPaths;
      this.firstError = firstError;
      this.latch = latch;
    }

    @Override
    public void processResult(int rc, String zkpath, Object ctx, String name, Stat stat) {
      log.debug("got result in mkdirs callback {} {} {} {} {}", rc, zkpath, ctx, name, stat);
      try {
        if (rc != 0) {
          final KeeperException.Code keCode = KeeperException.Code.get(rc);
          if (keCode == KeeperException.Code.NODEEXISTS) {
            nodeAlreadyExistsPaths.add(zkpath);
          } else {
            log.warn("create znode {} failed due to: {}", zkpath, keCode);
            // Record the first real failure atomically across all concurrent callbacks (review
            // finding H2). compareAndSet keeps the first failure and is safe against the EventThread
            // racing the caller, unlike the old single-slot arrays reset per loop iteration.
            firstError.compareAndSet(null, new MkDirsError(rc, zkpath));
          }
        } else {
          log.debug("Created znode at path: {}", zkpath);
        }
      } finally {
        if (latch != null) {
          latch.countDown();
        }
      }
    }
  }

  /** First non-NODEEXISTS failure captured during an async mkDirs batch (review finding H2). */
  private static final class MkDirsError {
    final int code;
    final String path;

    MkDirsError(int code, String path) {
      this.code = code;
      this.path = path;
    }
  }

  private class PathConsumer implements Consumer<String> {
    private final Map<String,byte[]> dataMap;
    private final KeeperException[] keeperExceptions;

    public PathConsumer(Map<String,byte[]> dataMap, KeeperException[] keeperExceptions) {
      this.dataMap = dataMap;
      this.keeperExceptions = keeperExceptions;
    }

    @Override
    public void accept(String p) {
      try {
        setData(p, dataMap.get(p), -1, true);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        log.error("Failed to set data for {}", p, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException ke) {
        log.error("Failed to set data for {}", p, ke);
        keeperExceptions[0] = ke;
      }
    }
  }

  private class CreateZkOperation implements ZkOperation {
    private final String path;
    private final byte[] data;
    private final CreateMode createMode;

    public CreateZkOperation(String path, byte[] data, CreateMode createMode) {
      this.path = path;
      this.data = data;
      this.createMode = createMode;
    }

    @Override
    public Object execute() throws KeeperException {
      String createdPath;
      try {
    
        createdPath = connManager.getKeeper().create(path, data, getZkACLProvider().getACLsToAdd(path), createMode);
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, path, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
      }
      return createdPath;
    }
  }
}
