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
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
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
import org.apache.curator.utils.ZKPaths;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All Solr ZooKeeper interactions should go through this class rather than ZooKeeper. This class
 * handles synchronous connects and reconnections.
 */
// The constructor overloads are a little awkward, it would be nice to move this to a builder
public class SolrZkClient implements Closeable {

  static final String NEWL = System.getProperty("line.separator");

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ExecutorService curatorSafeServiceExecutor;
  CuratorFramework client;

  private final ZkMetrics metrics = new ZkMetrics();

  private Compressor compressor;

  public MapWriter getMetrics() {
    return metrics::writeMap;
  }

  private final ExecutorService zkCallbackExecutor =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("zkCallback"));
  private final ExecutorService zkConnectionListenerCallbackExecutor =
      ExecutorUtil.newMDCAwareSingleThreadExecutor(
          new SolrNamedThreadFactory("zkConnectionListenerCallback"));

  private volatile boolean isClosed = false;
  private int zkClientTimeout;
  private ACLProvider aclProvider;
  private ZkCredentialsInjector zkCredentialsInjector;
  private String zkServerAddress;
  private SolrClassLoader solrClassLoader;

  private IsClosed higherLevelIsClosed;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public SolrZkClient(Builder builder) {
    this(
        builder.zkServerAddress,
        builder.zkClientTimeout,
        builder.zkClientConnectTimeout,
        builder.zkCredentialsProvider,
        builder.aclProvider,
        builder.onReconnect,
        builder.beforeReconnect,
        builder.higherLevelIsClosed,
        builder.compressor,
        builder.solrClassLoader);
  }

  private SolrZkClient(
      String zkServerAddress,
      int zkClientTimeout,
      int clientConnectTimeout,
      ZkCredentialsProvider zkCredentialsProvider,
      ACLProvider aclProvider,
      final OnReconnect onReconnect,
      BeforeReconnect beforeReconnect,
      IsClosed higherLevelIsClosed,
      Compressor compressor,
      SolrClassLoader solrClassLoader) {

    if (zkServerAddress == null) {
      // only tests should create one without server address
      return;
    }
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
      chroot = zkServerAddress.substring(chrootIndex + 1);
    }

    this.higherLevelIsClosed = higherLevelIsClosed;

    if (zkCredentialsProvider == null) {
      zkCredentialsInjector = createZkCredentialsInjector();
      zkCredentialsProvider = createZkCredentialsToAddAutomatically();
    }
    if (aclProvider == null) {
      aclProvider = createACLProvider();
    }
    if (chroot != null && aclProvider instanceof SecurityAwareZkACLProvider) {
      this.aclProvider = ((SecurityAwareZkACLProvider) aclProvider).withChroot(chroot);
    } else {
      this.aclProvider = aclProvider;
    }

    this.zkClientTimeout = zkClientTimeout;

    curatorSafeServiceExecutor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("curator-safeService"));

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    var clientBuilder =
        CuratorFrameworkFactory.builder()
            .ensembleProvider(new FixedEnsembleProvider(zkHost))
            .namespace(chroot)
            .sessionTimeoutMs(zkClientTimeout)
            .connectionTimeoutMs(clientConnectTimeout)
            .aclProvider(this.aclProvider)
            .authorization(zkCredentialsProvider.getCredentials())
            .retryPolicy(retryPolicy)
            .runSafeService(curatorSafeServiceExecutor);

    client = clientBuilder.build();
    if (onReconnect != null) {
      client
          .getConnectionStateListenable()
          .addListener(onReconnect, zkConnectionListenerCallbackExecutor);
    }
    if (beforeReconnect != null) {
      client
          .getConnectionStateListenable()
          .addListener(beforeReconnect, zkConnectionListenerCallbackExecutor);
    }
    client.start();
    try {
      if (!client.blockUntilConnected(clientConnectTimeout, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException(
            String.format(
                Locale.ROOT,
                "Timeout while waiting for Zookeeper Client to connect: %d ms",
                clientConnectTimeout));
      }
      ;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      client.close();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    assert ObjectReleaseTracker.track(this);

    if (compressor == null) {
      this.compressor = new ZLibCompressor();
    } else {
      this.compressor = compressor;
    }
  }

  public CuratorFramework getCuratorFramework() {
    return client;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";

  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName =
        System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (StrUtils.isNotNullOrEmpty(zkCredentialsProviderClassName)) {
      try {
        log.info("Using ZkCredentialsProvider: {}", zkCredentialsProviderClassName);
        ZkCredentialsProvider zkCredentialsProvider =
            solrClassLoader == null
                ? Class.forName(zkCredentialsProviderClassName)
                    .asSubclass(ZkCredentialsProvider.class)
                    .getConstructor()
                    .newInstance()
                : solrClassLoader.newInstance(
                    zkCredentialsProviderClassName, ZkCredentialsProvider.class);
        zkCredentialsProvider.setZkCredentialsInjector(zkCredentialsInjector);
        return zkCredentialsProvider;
      } catch (Exception e) {
        // just ignore - go default
        log.warn(
            "VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor",
            e);
      }
    }
    log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";

  protected ACLProvider createACLProvider() {
    String zkACLProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (StrUtils.isNotNullOrEmpty(zkACLProviderClassName)) {
      try {
        log.info("Using ZkACLProvider: {}", zkACLProviderClassName);
        ZkACLProvider zkACLProvider =
            solrClassLoader == null
                ? Class.forName(zkACLProviderClassName)
                    .asSubclass(ZkACLProvider.class)
                    .getConstructor()
                    .newInstance()
                : solrClassLoader.newInstance(zkACLProviderClassName, ZkACLProvider.class);
        zkACLProvider.setZkCredentialsInjector(zkCredentialsInjector);
        return zkACLProvider;
      } catch (Exception e) {
        // Fail-fast. If the instantiation fails better fail-fast rather than use the default unsafe
        // ZkACLProvider
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "VM param zkACLProvider does not point to a class implementing "
                + "ZkACLProvider and with a non-arg constructor",
            e);
      }
    }
    log.warn(
        "Using default ZkACLProvider. DefaultZkACLProvider is not secure, it creates 'OPEN_ACL_UNSAFE' ACLs to Zookeeper nodes");
    return new DefaultZkACLProvider();
  }

  public static final String ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME =
      "zkCredentialsInjector";

  protected ZkCredentialsInjector createZkCredentialsInjector() {
    String zkCredentialsInjectorClassName =
        System.getProperty(ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME);
    if (StrUtils.isNotNullOrEmpty(zkCredentialsInjectorClassName)) {
      try {
        log.info("Using ZkCredentialsInjector: {}", zkCredentialsInjectorClassName);
        return solrClassLoader == null
            ? Class.forName(zkCredentialsInjectorClassName)
                .asSubclass(ZkCredentialsInjector.class)
                .getConstructor()
                .newInstance()
            : solrClassLoader.newInstance(
                zkCredentialsInjectorClassName, ZkCredentialsInjector.class);
      } catch (Exception e) {
        // Fail-fast. If the instantiation fails better fail-fast rather than use the default unsafe
        // ZkCredentialsInjector
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "VM param zkCredentialsInjector does not point to a class implementing "
                + "ZkCredentialsInjector and with a non-arg constructor",
            e);
      }
    }
    log.warn(
        "Using default ZkCredentialsInjector. ZkCredentialsInjector is not secure, it creates an empty list of "
            + "credentials which leads to 'OPEN_ACL_UNSAFE' ACLs to Zookeeper nodes");
    return new DefaultZkCredentialsInjector();
  }

  /** Returns true if client is connected */
  public boolean isConnected() {
    return client.getZookeeperClient().isConnected();
  }

  public void delete(final String path, final int version, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {
    runWithCorrectThrows(
        "deleting znode", () -> client.delete().withVersion(version).forPath(path));
    metrics.deletes.increment();
  }

  /**
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a
   * watch object will only be triggered once for a given notification, users need to wrap their
   * watcher using this method before calling {@link #exists(String, org.apache.zookeeper.Watcher,
   * boolean)} or {@link #getData(String, org.apache.zookeeper.Watcher,
   * org.apache.zookeeper.data.Stat, boolean)}.
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
  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    Stat result =
        runWithCorrectThrows(
            "checking exists",
            () -> client.checkExists().usingWatcher(wrapWatcher(watcher)).forPath(path));
    metrics.existsChecks.increment();
    return result;
  }

  /** Returns true if path exists */
  public Boolean exists(final String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    Boolean result =
        runWithCorrectThrows("checking exists", () -> client.checkExists().forPath(path) != null);
    metrics.existsChecks.increment();
    return result;
  }

  /** Returns children of the node at the path */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    List<String> result =
        runWithCorrectThrows(
            "getting children",
            () -> client.getChildren().usingWatcher(wrapWatcher(watcher)).forPath(path));

    metrics.childFetches.increment();
    if (result != null) {
      metrics.cumulativeChildrenFetched.add(result.size());
    }
    return result;
  }

  /** Returns children of the node at the path */
  public List<String> getChildren(
      final String path, final Watcher watcher, Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    List<String> result =
        runWithCorrectThrows(
            "getting children",
            () ->
                client
                    .getChildren()
                    .storingStatIn(stat)
                    .usingWatcher(wrapWatcher(watcher))
                    .forPath(path));

    metrics.childFetches.increment();
    if (result != null) {
      metrics.cumulativeChildrenFetched.add(result.size());
    }
    return result;
  }

  /** Returns node's data */
  public byte[] getData(
      final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    byte[] result =
        runWithCorrectThrows(
            "getting data",
            () ->
                client
                    .getData()
                    .storingStatIn(stat)
                    .usingWatcher(wrapWatcher(watcher))
                    .forPath(path));
    if (compressor.isCompressedBytes(result)) {
      log.debug("Zookeeper data at path {} is compressed", path);
      try {
        result = compressor.decompressBytes(result);
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            String.format(
                Locale.ROOT, "Unable to decompress data at path: %s from zookeeper", path),
            e);
      }
    }
    metrics.reads.increment();
    if (result != null) {
      metrics.bytesRead.add(result.length);
    }
    return result;
  }

  public NodeData getNode(final String path, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    return new NodeData(stat, getData(path, watcher, stat, retryOnConnLoss));
  }

  /** Returns node's state */
  public Stat setData(
      final String path, final byte data[], final int version, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    Stat result =
        runWithCorrectThrows(
            "setting data", () -> client.setData().withVersion(version).forPath(path, data));
    metrics.writes.increment();
    if (data != null) {
      metrics.bytesWritten.add(data.length);
    }
    return result;
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
        if (exists(path, true)) {
          zkData = getData(path, null, s, true);
          modified = editor.apply(s, zkData);
          if (modified == null) {
            // no change , no need to persist
            return;
          }
          setData(path, modified, s.getVersion(), true);
          break;
        } else {
          modified = editor.apply(s, null);
          if (modified == null) {
            // no change , no need to persist
            return;
          }
          create(path, modified, CreateMode.PERSISTENT, true);
          break;
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        continue;
      }
    }
  }

  /** Returns path of created node */
  public String create(
      final String path, final byte[] data, final CreateMode createMode, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    String result =
        runWithCorrectThrows(
            "creating znode", () -> client.create().withMode(createMode).forPath(path, data));
    metrics.writes.increment();
    if (data != null) {
      metrics.bytesWritten.add(data.length);
    }
    return result;
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   */
  public void makePath(String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  public void makePath(String path, boolean failOnExists, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, Path data, boolean failOnExists, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(
        path,
        Files.readAllBytes(data),
        CreateMode.PERSISTENT,
        null,
        failOnExists,
        retryOnConnLoss,
        0);
  }

  public void makePath(String path, Path data, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, Files.readAllBytes(data), retryOnConnLoss);
  }

  public void makePath(String path, CreateMode createMode, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, null, createMode, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
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
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(
      String path, byte[] data, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, true, retryOnConnLoss, 0);
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
      String path,
      byte[] data,
      CreateMode createMode,
      Watcher watcher,
      boolean failOnExists,
      boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, failOnExists, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * <p>e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr, group, node exist,
   * each will be created.
   *
   * <p>skipPathParts will force the call to fail if the first skipPathParts do not exist already.
   *
   * <p>Note: retryOnConnLoss is no longer respected at all and is ignored
   *
   * <p>Note: if failOnExists == false then we will always overwrite the existing data with the
   * given data
   */
  public void makePath(
      String path,
      byte[] data,
      CreateMode createMode,
      Watcher watcher,
      boolean failOnExists,
      boolean retryOnConnLoss,
      int skipPathParts)
      throws KeeperException, InterruptedException {
    log.debug("makePath: {}", path);
    var createBuilder = client.create();
    if (!failOnExists) {
      createBuilder.orSetData();
    }
    metrics.writes.increment();
    if (data != null) {
      metrics.bytesWritten.add(data.length);
    }

    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (skipPathParts > 0) {
      String[] paths = path.split("/");
      StringBuilder sbPath = new StringBuilder();
      for (int i = 0; i < paths.length; i++) {
        if (i == skipPathParts) {
          break;
        }
        String pathPiece = paths[i];
        sbPath.append("/").append(pathPiece);
      }
      String skipPathPartsPath = sbPath.toString();
      final String finalSkipPathPartsPath;
      if (!skipPathPartsPath.startsWith("/")) {
        finalSkipPathPartsPath = "/" + skipPathPartsPath;
      } else {
        finalSkipPathPartsPath = skipPathPartsPath;
      }
      Stat exists =
          runWithCorrectThrows(
              "checking skipPathParts exists",
              () -> client.checkExists().forPath(finalSkipPathPartsPath));
      if (exists == null) {
        throw new KeeperException.NoNodeException(finalSkipPathPartsPath + " didn't already exist");
      }
    }

    final String finalPath;
    if (!path.startsWith("/")) {
      finalPath = "/" + path;
    } else {
      finalPath = path;
    }
    runWithCorrectThrows(
        "making path",
        () -> {
          createBuilder.creatingParentsIfNeeded().withMode(createMode).forPath(finalPath, data);
          return client.checkExists().usingWatcher(wrapWatcher(watcher)).forPath(finalPath);
        });
  }

  public void makePath(
      String zkPath, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher, retryOnConnLoss);
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   */
  public void ensureExists(final String path) throws KeeperException, InterruptedException {
    ensureExists(path, null);
  }

  /**
   * Create a node if it does not exist
   *
   * @param path the path at which to create the znode
   * @param data the optional data to set on the znode
   */
  public void ensureExists(final String path, final byte[] data)
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
  public void ensureExists(final String path, final byte[] data, CreateMode createMode)
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
      final String path, final byte[] data, CreateMode createMode, int skipPathParts)
      throws KeeperException, InterruptedException {
    if (exists(path, true)) {
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
        String startingPath = path.substring(0, endingIndex);
        if (!exists(startingPath, true)) {
          throw new KeeperException.NoNodeException(startingPath);
        }
      }
      makePath(path, data, createMode, null, true, true, skipPathParts);
    } catch (KeeperException.NodeExistsException ignored) {
      // it's okay if another beats us creating the node
    }
  }

  /** Write data to ZooKeeper. */
  public Stat setData(String path, byte[] data, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return setData(path, data, -1, retryOnConnLoss);
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   *
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param data a filepath to read data from
   */
  public Stat setData(String path, Path data, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Write to ZooKeeper: {} to {}", data.toAbsolutePath(), path);
    }
    return setData(path, Files.readAllBytes(data), retryOnConnLoss);
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
    List<CuratorTransactionResult> result =
        runWithCorrectThrows(
            "executing multi-transaction",
            () ->
                client
                    .transaction()
                    .forOperations(
                        ops.stream()
                            .map(op -> op.buildWithoutThrows(client.transactionOp()))
                            .collect(Collectors.toList())));

    metrics.multiOps.increment();
    if (result != null) {
      metrics.cumulativeMultiOps.add(result.size());
    }
    return result;
  }

  /** Fills string with printout of current ZooKeeper layout. */
  public void printLayout(String path, int indent, StringBuilder string)
      throws KeeperException, InterruptedException {
    byte[] data = getData(path, null, null, true);
    List<String> children = getChildren(path, null, true);
    StringBuilder dent = new StringBuilder();
    dent.append(" ".repeat(Math.max(0, indent)));
    string.append(dent).append(path).append(" (").append(children.size()).append(")").append(NEWL);
    if (data != null && data.length > 0) {
      String dataString = new String(data, StandardCharsets.UTF_8);
      if (!path.endsWith(".txt") && !path.endsWith(".xml")) {
        string
            .append(dent)
            .append("DATA:\n")
            .append(dent)
            .append("    ")
            .append(dataString.replace("\n", "\n" + dent + "    "))
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

  @Override
  public void close() {
    if (isClosed) return; // it's okay if we over close - same as solrcore
    isClosed = true;
    try {
      closeCallbackExecutor();
    } finally {
      IOUtils.closeQuietly(client);
      ExecutorUtil.shutdownNowAndAwaitTermination(curatorSafeServiceExecutor);
    }
    assert ObjectReleaseTracker.release(this);
  }

  public boolean isClosed() {
    return isClosed || (higherLevelIsClosed != null && higherLevelIsClosed.isClosed());
  }

  public ZooKeeper getZooKeeper() throws Exception {
    return client.getZookeeperClient().getZooKeeper();
  }

  public long getZkSessionId() {
    if (isConnected()) {
      try {
        return client.getZookeeperClient().getZooKeeper().getSessionId();
      } catch (Exception ignored) {
      }
    }
    return -1;
  }

  public int getZkSessionTimeout() {
    if (isConnected()) {
      try {
        return client.getZookeeperClient().getZooKeeper().getSessionTimeout();
      } catch (Exception ignored) {
      }
    }
    return 0;
  }

  private void closeCallbackExecutor() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkCallbackExecutor);
    } catch (Exception e) {
      log.error("Error shutting down zkCallbackExecutor", e);
    }

    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkConnectionListenerCallbackExecutor);
    } catch (Exception e) {
      log.error("Error shutting down zkConnManagerCallbackExecutor", e);
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
   * @return the address of the zookeeper cluster
   */
  public String getAbsolutePath(String path) {
    return ZKPaths.fixForNamespace(getChroot(), path);
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
  public List<ACL> getACL(String path, Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return runWithCorrectThrows(
        "getting acls", () -> client.getACL().storingStatIn(stat).forPath(path));
  }

  /**
   * Set the ACL on a single node in ZooKeeper. This will replace all existing ACL on that node.
   *
   * @param path path to set ACL on e.g. /solr/conf/solrconfig.xml
   * @param acls a list of {@link ACL}s to be applied
   * @return the stat of the node
   */
  public Stat setACL(String path, List<ACL> acls) throws InterruptedException, KeeperException {
    return runWithCorrectThrows(
        "setting acls", () -> client.setACL().withVersion(-1).withACL(acls).forPath(path));
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
                "updating acls", () -> client.setACL().withACL(null).forPath(path));
          } catch (NoNodeException ignored) {
            // If a node was deleted, don't bother trying to set ACLs on it.
          }
        });
  }

  @FunctionalInterface
  protected interface SupplierWithException<T> {
    T get() throws Exception;
  }

  protected <T> T runWithCorrectThrows(String action, SupplierWithException<T> func)
      throws KeeperException, InterruptedException {
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

  // all fields of this class are public because ReflectMapWriter requires them to be.
  // however the object itself is private and only this class can modify it
  public static class ZkMetrics implements ReflectMapWriter {
    @JsonProperty public final LongAdder watchesFired = new LongAdder();
    @JsonProperty public final LongAdder reads = new LongAdder();
    @JsonProperty public final LongAdder writes = new LongAdder();
    @JsonProperty public final LongAdder bytesRead = new LongAdder();
    @JsonProperty public final LongAdder bytesWritten = new LongAdder();

    @JsonProperty public final LongAdder multiOps = new LongAdder();

    @JsonProperty public final LongAdder cumulativeMultiOps = new LongAdder();

    @JsonProperty public final LongAdder childFetches = new LongAdder();

    @JsonProperty public final LongAdder cumulativeChildrenFetched = new LongAdder();

    @JsonProperty public final LongAdder existsChecks = new LongAdder();

    @JsonProperty public final LongAdder deletes = new LongAdder();

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ReflectMapWriter.super.writeMap(
          new EntryWriter() {
            @Override
            public EntryWriter put(CharSequence k, Object v) throws IOException {
              if (v instanceof LongAdder) {
                ew.put(k, ((LongAdder) v).longValue());
              } else {
                ew.put(k, v);
              }
              return this;
            }
          });
    }
  }

  public static class NodeData {

    public final Stat stat;
    public final byte[] data;

    public NodeData(Stat stat, byte[] data) {
      this.stat = stat;
      this.data = data;
    }
  }

  public static class Builder {
    public String zkServerAddress;
    public int zkClientTimeout = SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT;
    public int zkClientConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;
    public OnReconnect onReconnect;
    public BeforeReconnect beforeReconnect;
    public ZkCredentialsProvider zkCredentialsProvider;
    public ACLProvider aclProvider;
    public IsClosed higherLevelIsClosed;
    public SolrClassLoader solrClassLoader;

    public Compressor compressor;

    public Builder withUrl(String server) {
      this.zkServerAddress = server;
      return this;
    }

    /**
     * Sets the Zk client session timeout
     *
     * @param zkClientTimeout timeout value
     * @param unit time unit
     */
    public Builder withTimeout(int zkClientTimeout, TimeUnit unit) {
      this.zkClientTimeout = Math.toIntExact(unit.toMillis(zkClientTimeout));
      return this;
    }

    /**
     * Sets the Zk connection timeout
     *
     * @param zkConnectTimeout timeout value
     * @param unit time unit
     */
    public Builder withConnTimeOut(int zkConnectTimeout, TimeUnit unit) {
      this.zkClientConnectTimeout = Math.toIntExact(unit.toMillis(zkConnectTimeout));
      return this;
    }

    public Builder withReconnectListener(OnReconnect onReconnect) {
      this.onReconnect = onReconnect;
      return this;
    }

    public Builder withBeforeConnect(BeforeReconnect beforeReconnect) {
      this.beforeReconnect = beforeReconnect;
      return this;
    }

    public Builder withZkCredentialsProvider(ZkCredentialsProvider zkCredentialsProvider) {
      this.zkCredentialsProvider = zkCredentialsProvider;
      return this;
    }

    public Builder withAclProvider(ACLProvider aclProvider) {
      this.aclProvider = aclProvider;
      return this;
    }

    public Builder withClosedCheck(IsClosed higherLevelIsClosed) {
      this.higherLevelIsClosed = higherLevelIsClosed;
      return this;
    }

    public Builder withCompressor(Compressor c) {
      this.compressor = c;
      return this;
    }

    public Builder withSolrClassLoader(SolrClassLoader solrClassLoader) {
      this.solrClassLoader = solrClassLoader;
      return this;
    }

    public SolrZkClient build() {
      return new SolrZkClient(this);
    }
  }
}
