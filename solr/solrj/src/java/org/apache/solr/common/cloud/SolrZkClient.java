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
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All Solr ZooKeeper interactions should go through this class rather than ZooKeeper. This class
 * handles synchronous connects and reconnections.
 */
public class SolrZkClient implements Closeable {

  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 30000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  CuratorFramework client;

  private final ExecutorService zkCallbackExecutor =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("zkCallback"));
  private final ExecutorService zkConnectionListenerCallbackExecutor =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(
          new SolrNamedThreadFactory("zkConnectionListenerCallback"));

  private volatile boolean isClosed = false;
  private int zkClientTimeout;
  private ACLProvider aclProvider;
  private String zkServerAddress;

  private IsClosed higherLevelIsClosed;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  // expert: for tests
  public SolrZkClient() {}

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(
        zkServerAddress,
        zkClientTimeout,
        zkClientConnectTimeout,
        null,
        null);
  }

  public SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      int zkClientConnectTimeout,
      OnReconnect onReonnect) {
    this(
        zkServerAddress,
        zkClientTimeout,
        zkClientConnectTimeout,
        null,
        onReonnect);
  }

  public SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      ZkCredentialsProvider zkCredentialsProvider,
      final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, DEFAULT_CLIENT_CONNECT_TIMEOUT, zkCredentialsProvider, onReconnect);
  }

  public SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      int clientConnectTimeout,
      ZkCredentialsProvider zkCredentialsProvider,
      final OnReconnect onReconnect) {
    this(
        zkServerAddress,
        zkClientTimeout,
        clientConnectTimeout,
        zkCredentialsProvider,
        null,
        onReconnect,
        null,
        null);
  }

  public SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      int clientConnectTimeout,
      ZkCredentialsProvider zkCredentialsProvider,
      final OnReconnect onReconnect,
      OnDisconnect onDisconnect) {
    this(
        zkServerAddress,
        zkClientTimeout,
        clientConnectTimeout,
        zkCredentialsProvider,
        null,
        onReconnect,
        onDisconnect,
        null);
  }

  public SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      int clientConnectTimeout,
      ZkCredentialsProvider zkCredentialsProvider,
      ACLProvider aclProvider,
      final OnReconnect onReconnect,
      OnDisconnect onDisconnect,
      IsClosed higherLevelIsClosed) {
    this.zkServerAddress = zkServerAddress;
    String chroot, zkHost;
    int chrootIndex = zkServerAddress.indexOf('/');
    if (chrootIndex == -1) {
      zkHost = zkServerAddress;
      chroot = null;
    } else if (chrootIndex == zkServerAddress.length() - 1) {
      zkHost = zkServerAddress.substring(0, zkServerAddress.length() - 1);
      chroot = null;
    } else {
      zkHost = zkServerAddress.substring(0, chrootIndex);
      chroot = zkServerAddress.substring(chrootIndex);
    }
    this.higherLevelIsClosed = higherLevelIsClosed;

    if (zkCredentialsProvider == null) {
      zkCredentialsProvider = createZkCredentialsToAddAutomatically();
    }
    if (aclProvider == null) {
      this.aclProvider = createACLProvider();
    } else {
      this.aclProvider = aclProvider;
    }

    this.zkClientTimeout = zkClientTimeout;

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    var clientBuilder = CuratorFrameworkFactory.builder()
        .ensembleProvider(new FixedEnsembleProvider(zkHost))
        .namespace(chroot)
        .connectionTimeoutMs(clientConnectTimeout)
        .aclProvider(this.aclProvider)
        .authorization(zkCredentialsProvider.getCredentials())
        .retryPolicy(retryPolicy);

    client = clientBuilder.build();
    if (onReconnect != null) {
      client.getConnectionStateListenable().addListener(onReconnect, zkConnectionListenerCallbackExecutor);
    }
    if (onDisconnect != null) {
      client.getConnectionStateListenable().addListener(onDisconnect, zkConnectionListenerCallbackExecutor);
    }
    client.start();
    try {
      client.blockUntilConnected();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    assert ObjectReleaseTracker.track(this);
  }

  public CuratorFramework getCuratorFramework() {
    return client;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";

  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName =
        System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        log.info("Using ZkCredentialsProvider: {}", zkCredentialsProviderClassName);
        return (ZkCredentialsProvider)
            Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn(
            "VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor",
            t);
      }
    }
    log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";

  protected ACLProvider createACLProvider() {
    String aclProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(aclProviderClassName)) {
      try {
        log.info("Using ACLProvider: {}", aclProviderClassName);
        return (ACLProvider) Class.forName(aclProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn(
            "VM param zkACLProvider does not point to a class implementingACLProvider and with a non-arg constructor",
            t);
      }
    }
    log.debug("Using default ACLProvider");
    return new DefaultACLProvider();
  }

  /** Returns true if client is connected */
  public boolean isConnected() {
    return client.getZookeeperClient().isConnected();
  }

  public void delete(final String path, final int version)
      throws InterruptedException, KeeperException {
    runWithCorrectThrows(
        "deleting znode",
        () -> client.delete().withVersion(version).forPath(path));
  }

  /**
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a
   * watch object will only be triggered once for a given notification, users need to wrap their
   * watcher using this method before calling {@link #exists(String, Watcher)} or {@link #getData(String, Watcher, Stat)}.
   */
  public Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null || watcher instanceof ProcessWatchWithExecutor) return watcher;
    return new ProcessWatchWithExecutor(watcher);
  }

  /**
   * Return the stat of the node of the given path. Return null if no such a node exists.
   *
   * <p>If the watch is non-null and the call is successful (no exception is thrown), a watch will
   * be left on the node with the given path. The watch will be triggered by a successful operation
   * that creates/delete the node or sets the data on the node.
   *
   * @param path the node path
   * @param watcher explicit watcher
   * @return the stat of the node of the given path; return null if no such a node exists.
   * @throws KeeperException If the server signals an error
   * @throws InterruptedException If the server transaction is interrupted.
   * @throws IllegalArgumentException if an invalid path is specified
   */
  public Stat exists(final String path, final Watcher watcher)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "checking exists",
        () -> client.checkExists().usingWatcher(wrapWatcher(watcher)).forPath(path));
  }

  /** Returns true if path exists */
  public Boolean exists(final String path)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "checking exists",
        () -> client.checkExists().forPath(path) != null);
  }

  /** Returns children of the node at the path */
  public List<String> getChildren(final String path, final Watcher watcher)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "getting children",
        () -> client.getChildren().usingWatcher(wrapWatcher(watcher)).forPath(path));
  }

  /** Returns children of the node at the path */
  public List<String> getChildren(
      final String path, final Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "getting children",
        () -> client.getChildren().storingStatIn(stat).usingWatcher(wrapWatcher(watcher)).forPath(path));
  }

  /** Returns node's data */
  public byte[] getData(
      final String path, final Watcher watcher, final Stat stat)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "getting data",
        () -> client.getData().storingStatIn(stat).usingWatcher(wrapWatcher(watcher)).forPath(path));
  }

  public void atomicUpdate(String path, Function<byte[], byte[]> editor)
      throws KeeperException, InterruptedException {
    atomicUpdate(path, (stat, bytes) -> editor.apply(bytes));
  }

  public void atomicUpdate(String path, BiFunction<Stat, byte[], byte[]> editor)
      throws KeeperException, InterruptedException {
    for (; ; ) {
      byte[] modified = null;
      byte[] zkData = null;
      Stat s = new Stat();
      try {
        if (exists(path)) {
          zkData = getData(path, null, s);
          modified = editor.apply(s, zkData);
          if (modified == null) {
            // no change , no need to persist
            return;
          }
          setData(path, modified, s.getVersion());
          break;
        } else {
          modified = editor.apply(s, null);
          if (modified == null) {
            // no change , no need to persist
            return;
          }
          create(path, modified, CreateMode.PERSISTENT);
          break;
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        continue;
      }
    }
  }

  /** Returns path of created node */
  public String create(
      final String path, final byte[] data, final CreateMode createMode)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "creating znode",
        () -> client.create().withMode(createMode).forPath(path, data));
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   */
  public void makePath(String path)
      throws KeeperException, InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT);
  }

  public void makePath(String path, boolean failOnExists)
      throws KeeperException, InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists);
  }

  public void makePath(String path, Path data, boolean failOnExists)
      throws IOException, KeeperException, InterruptedException {
    makePath(
        path,
        Files.readAllBytes(data),
        CreateMode.PERSISTENT,
        null,
        failOnExists
    );
  }

  public void makePath(String path, Path data)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, Files.readAllBytes(data));
  }

  public void makePath(String path, CreateMode createMode)
      throws KeeperException, InterruptedException {
    makePath(path, null, createMode);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data)
      throws KeeperException, InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null);
  }

  public void makePath(
      String zkPath, CreateMode createMode, Watcher watcher)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(
      String path, byte[] data, CreateMode createMode, Watcher watcher)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, true);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   *
   * <p>Note: if failOnExists == false then we will always overwrite the existing data with the
   * given data
   */
  public void makePath(
      String path,
      byte[] data,
      CreateMode createMode,
      Watcher watcher,
      boolean failOnExists)
      throws KeeperException, InterruptedException {
    log.debug("makePath: {}", path);
    var createBuilder = client.create();
    if (!failOnExists) {
      createBuilder.orSetData();
    }
    runWithCorrectThrows(
        "making path",
        () -> {
          createBuilder.creatingParentsIfNeeded().withMode(createMode).forPath(path, data);
          return client.checkExists().usingWatcher(wrapWatcher(watcher)).forPath(path);
        });
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   */
  public void ensureExists(final String path)
      throws KeeperException, InterruptedException {
    ensureExists(path, null);
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   * @param data the optional data to set on the znode
   */
  public void ensureExists(
      final String path,
      final byte[] data)
      throws KeeperException, InterruptedException {
    ensureExists(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   * @param data the optional data to set on the znode
   * @param createMode the mode with which to create the znode
   */
  public void ensureExists(
      final String path,
      final byte[] data,
      CreateMode createMode)
      throws KeeperException, InterruptedException {
    ensureExists(path, data, createMode, 0);
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   * @param data the optional data to set on the znode
   * @param createMode the mode with which to create the znode
   * @param skipPathParts how many path elements to skip
   */
  public void ensureExists(
      final String path,
      final byte[] data,
      CreateMode createMode,
      int skipPathParts)
      throws KeeperException, InterruptedException {
    if (exists(path)) {
      return;
    }
    try {
      if (skipPathParts > 0) {
        int endingIndex = 0;
        for (int i = 0; i < skipPathParts && endingIndex >= 0; i++) {
          endingIndex = path.indexOf('/', endingIndex + 1);
        }
        if (endingIndex == -1 || endingIndex == path.length() - 1) {
          throw new KeeperException.NoNodeException(path);
        }
        String startingPath = path.substring(endingIndex + 1);
        if (!exists(startingPath)) {
          throw new KeeperException.NoNodeException(startingPath);
        }
      }
      makePath(path, data, createMode, null, true);
    } catch (KeeperException.NodeExistsException ignored) {
      // it's okay if another beats us creating the node
    }
  }

  /** Write data to ZooKeeper. */
  public Stat setData(String path, byte[] data)
      throws KeeperException, InterruptedException {
    return setData(path, data, -1);
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   *  @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param data a filepath to read data from
   */
  public Stat setData(String path, Path data)
      throws IOException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Write to ZooKeeper: {} to {}", data.toAbsolutePath(), path);
    }
    return setData(path, Files.readAllBytes(data));
  }

  /** Returns node's state */
  public Stat setData(
      final String path, final byte[] data, final int version)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "setting data",
        () -> client.setData().withVersion(version).forPath(path, data));
  }

  @FunctionalInterface
  public interface CuratorOpBuilder {
    CuratorOp build(TransactionOp startingOp) throws Exception;

    default CuratorOp buildWithoutThrows(TransactionOp startingOp) {
      try {
        return build(startingOp);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public List<CuratorTransactionResult> multi(CuratorOpBuilder... ops)
      throws InterruptedException, KeeperException {
    return multi(Arrays.asList(ops));
  }

  public List<CuratorTransactionResult> multi(final List<CuratorOpBuilder> ops)
      throws InterruptedException, KeeperException {
    return runWithCorrectThrows(
        "executing multi-transaction",
        () -> client.transaction().forOperations(ops.stream().map(op -> op.buildWithoutThrows(client.transactionOp())).collect(Collectors.toList())));
  }

  /** Fills string with printout of current ZooKeeper layout. */
  public void printLayout(String path, int indent, StringBuilder string)
      throws KeeperException, InterruptedException {
    byte[] data = getData(path, null, null);
    List<String> children = getChildren(path, null);
    StringBuilder dent = new StringBuilder();
    dent.append(" ".repeat(Math.max(0, indent)));
    string.append(dent).append(path).append(" (").append(children.size()).append(")").append(NEWL);
    if (data != null) {
      String dataString = new String(data, StandardCharsets.UTF_8);
      if (!path.endsWith(".txt") && !path.endsWith(".xml")) {
        string
            .append(dent)
            .append("DATA:\n")
            .append(dent)
            .append("    ")
            .append(dataString.replaceAll("\n", "\n" + dent + "    "))
            .append(NEWL);
      } else {
        string.append(dent).append("DATA: ...supressed...").append(NEWL);
      }
    }

    for (String child : children) {
      if (!child.equals("quota")) {
        try {
          printLayout(path + (path.equals("/") ? "" : "/") + child, indent + 1, string);
        } catch (NoNodeException e) {
          // must have gone away
        }
      }
    }
  }

  public void printLayoutToStream(PrintStream out) throws KeeperException, InterruptedException {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    out.println(sb.toString());
  }

  public void close() {
    if (isClosed) return; // it's okay if we over close - same as solrcore
    isClosed = true;
    try {
      closeCallbackExecutor();
    } finally {
      client.close();
    }
    assert ObjectReleaseTracker.release(this);
  }

  public boolean isClosed() {
    return isClosed || (higherLevelIsClosed != null && higherLevelIsClosed.isClosed());
  }

  public SolrZooKeeper getSolrZooKeeper() {
    return null;
  }

  public long getZkSessionId() {
    if (isConnected()) {
      try {
        return client.getZookeeperClient().getZooKeeper().getSessionId();
      } catch (Exception ignored) { }
    }
    return -1;
  }

  public int getZkSessionTimeout() {
    if (isConnected()) {
      try {
        return client.getZookeeperClient().getZooKeeper().getSessionTimeout();
      } catch (Exception ignored) { }
    }
    return 0;
  }

  private void closeCallbackExecutor() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkCallbackExecutor);
    } catch (Exception e) {
      SolrException.log(log, e);
    }

    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkConnectionListenerCallbackExecutor);
    } catch (Exception e) {
      SolrException.log(log, e);
    }
  }

  /**
   * Validates if zkHost contains a chroot. See
   * http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
   */
  public static boolean containsChroot(String zkHost) {
    return zkHost.contains("/");
  }

  /**
   * Check to see if a Throwable is an InterruptedException, and if it is, set the thread interrupt
   * flag
   *
   * @param e the Throwable
   * @return the Throwable
   */
  public static Throwable checkInterrupted(Throwable e) {
    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
    return e;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getChroot() {
    return client.getNamespace();
  }

  /**
   * Gets the raw config node /zookeeper/config as returned by server. Response may look like
   *
   * <pre>
   * server.1=localhost:2780:2783:participant;localhost:2791
   * server.2=localhost:2781:2784:participant;localhost:2792
   * server.3=localhost:2782:2785:participant;localhost:2793
   * version=400000003
   * </pre>
   *
   * @return Multi line string representing the config. For standalone ZK this will return empty
   *     string
   */
  public String getConfig() {
    QuorumVerifier currentConfig = client.getCurrentConfig();
    if (currentConfig == null) {
      log.debug("Zookeeper does not have the /zookeeper/config znode, assuming old ZK version");
      return "";
    } else {
      return currentConfig.toString();
    }
  }

  public ACLProvider getZkACLProvider() {
    return aclProvider;
  }

  /**
   * @return the ACLs on a single node in ZooKeeper.
   */
  public List<ACL> getACL(String path, Stat stat)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "getting acls",
        () -> client.getACL().storingStatIn(stat).forPath(path));
  }

  /**
   * Set the ACL on a single node in ZooKeeper. This will replace all existing ACL on that node.
   *
   * @param path path to set ACL on e.g. /solr/conf/solrconfig.xml
   * @param acls a list of {@link ACL}s to be applied
   * @return the stat of the node
   */
  public Stat setACL(String path, List<ACL> acls)
      throws InterruptedException, KeeperException {
    return runWithCorrectThrows(
        "setting acls",
        () -> client.setACL().withVersion(-1).withACL(acls).forPath(path));
  }

  /**
   * Update all ACLs for a zk tree based on our configured {@link ZkACLProvider}.
   *
   * @param root the root node to recursively update
   */
  public void updateACLs(final String root) throws KeeperException, InterruptedException {
    ZkMaintenanceUtils.traverseZkTree(
        this,
        root,
        ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST,
        path -> {
          try {
            runWithCorrectThrows(
                "updating acls",
                () -> client.setACL().withACL(null).forPath(path));
          } catch (NoNodeException ignored) {
            // If a node was deleted, don't bother trying to set ACLs on it.
          }
        });
  }

  @FunctionalInterface
  private interface SupplierWithException<T> {
    T get() throws Exception;
  }

  private <T> T runWithCorrectThrows(String action, SupplierWithException<T> func) throws KeeperException, InterruptedException {
    try {
      return func.get();
    } catch (KeeperException | RuntimeException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Exception occurred while " + action, e);
    }
  }

  // Some pass-throughs to allow less code disruption to other classes that use SolrZkClient.
  public void clean(String path) throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.clean(this, path);
  }

  public void clean(String path, Predicate<String> nodeFilter)
      throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.clean(this, path, nodeFilter);
  }

  public void upConfig(Path confPath, String confName) throws IOException {
    ZkMaintenanceUtils.uploadToZK(
        this,
        confPath,
        ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
        ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  public String listZnode(String path, Boolean recurse)
      throws KeeperException, InterruptedException, SolrServerException {
    return ZkMaintenanceUtils.listZnode(this, path, recurse);
  }

  public void downConfig(String confName, Path confPath) throws IOException {
    ZkMaintenanceUtils.downloadFromZK(
        this, ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName, confPath);
  }

  public void zkTransfer(String src, Boolean srcIsZk, String dst, Boolean dstIsZk, Boolean recurse)
      throws SolrServerException, KeeperException, InterruptedException, IOException {
    ZkMaintenanceUtils.zkTransfer(this, src, srcIsZk, dst, dstIsZk, recurse);
  }

  public void moveZnode(String src, String dst)
      throws SolrServerException, KeeperException, InterruptedException {
    ZkMaintenanceUtils.moveZnode(this, src, dst);
  }

  public void uploadToZK(final Path rootPath, final String zkPath, final Pattern filenameExclusions)
      throws IOException {
    ZkMaintenanceUtils.uploadToZK(this, rootPath, zkPath, filenameExclusions);
  }

  public void downloadFromZK(String zkPath, Path dir) throws IOException {
    ZkMaintenanceUtils.downloadFromZK(this, zkPath, dir);
  }

  @FunctionalInterface
  public interface IsClosed {
    boolean isClosed();
  }

  /**
   * Watcher wrapper that ensures that heavy implementations of process do not interfere with our
   * ability to react to other watches, but also ensures that two wrappers containing equal watches
   * are considered equal (and thus we won't accumulate multiple wrappers of the same watch).
   */
  private final class ProcessWatchWithExecutor implements Watcher { // see below for why final.
    private final Watcher watcher;

    ProcessWatchWithExecutor(Watcher watcher) {
      if (watcher == null) {
        throw new IllegalArgumentException("Watcher must not be null");
      }
      this.watcher = watcher;
    }

    @Override
    public void process(final WatchedEvent event) {
      log.debug("Submitting job to respond to event {}", event);
      try {
        zkCallbackExecutor.submit(() -> watcher.process(event));
      } catch (RejectedExecutionException e) {
        // If not a graceful shutdown
        if (!isClosed()) {
          throw e;
        }
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
}
