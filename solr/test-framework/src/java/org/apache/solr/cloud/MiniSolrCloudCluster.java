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

import static org.apache.solr.core.ConfigSetProperties.DEFAULT_FILENAME;

import com.codahale.metrics.MetricRegistry;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.servlet.Filter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.TracerConfigurator;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.tracing.SimplePropagator;
import org.apache.solr.util.tracing.TraceUtils;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** "Mini" SolrCloud cluster to be used for testing */
public class MiniSolrCloudCluster {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final URL PRE_GENERATED_PRIVATE_KEY_URL =
      MiniSolrCloudCluster.class.getClassLoader().getResource("cryptokeys/priv_key512_pkcs8.pem");
  private static final URL PRE_GENERATED_PUBLIC_KEY_URL =
      MiniSolrCloudCluster.class.getClassLoader().getResource("cryptokeys/pub_key512.der");

  public static final String TEST_URL_ALLOW_LIST = SolrTestCaseJ4.TEST_URL_ALLOW_LIST;

  public static final int DEFAULT_TIMEOUT = 30;

  public static final String DEFAULT_CLOUD_SOLR_XML =
      "<solr>\n"
          + "\n"
          + "  <str name=\"shareSchema\">${shareSchema:false}</str>\n"
          + "  <str name=\"allowPaths\">${solr.allowPaths:}</str>\n"
          + "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n"
          + "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n"
          + "  <str name=\"collectionsHandler\">${collectionsHandler:solr.CollectionsHandler}</str>\n"
          + "  <str name=\"allowUrls\">${"
          + TEST_URL_ALLOW_LIST
          + ":}</str>\n"
          + "\n"
          + "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n"
          + "    <str name=\"urlScheme\">${urlScheme:}</str>\n"
          + "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n"
          + "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n"
          + "  </shardHandlerFactory>\n"
          + "\n"
          + "  <solrcloud>\n"
          + "    <str name=\"host\">127.0.0.1</str>\n"
          + "    <int name=\"hostPort\">${hostPort:8983}</int>\n"
          + "    <str name=\"hostContext\">${hostContext:solr}</str>\n"
          + "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n"
          + "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n"
          + "    <int name=\"leaderVoteWait\">${leaderVoteWait:10000}</int>\n"
          + "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n"
          + "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n"
          + "    <str name=\"zkCredentialsProvider\">${zkCredentialsProvider:org.apache.solr.common.cloud.DefaultZkCredentialsProvider}</str> \n"
          + "    <str name=\"zkACLProvider\">${zkACLProvider:org.apache.solr.common.cloud.DefaultZkACLProvider}</str> \n"
          + "    <str name=\"pkiHandlerPrivateKeyPath\">${pkiHandlerPrivateKeyPath:"
          + (PRE_GENERATED_PRIVATE_KEY_URL != null
              ? PRE_GENERATED_PRIVATE_KEY_URL.toExternalForm()
              : "")
          + "}</str> \n"
          + "    <str name=\"pkiHandlerPublicKeyPath\">${pkiHandlerPublicKeyPath:"
          + (PRE_GENERATED_PUBLIC_KEY_URL != null
              ? PRE_GENERATED_PUBLIC_KEY_URL.toExternalForm()
              : "")
          + "}</str> \n"
          + "    <str name=\"distributedClusterStateUpdates\">${solr.distributedClusterStateUpdates:false}</str> \n"
          + "    <str name=\"distributedCollectionConfigSetExecution\">${solr.distributedCollectionConfigSetExecution:false}</str> \n"
          + "  </solrcloud>\n"
          +
          // NOTE: this turns off the metrics collection unless overridden by a sysprop
          "  <metrics enabled=\"${metricsEnabled:false}\">\n"
          + "    <reporter name=\"default\" class=\"org.apache.solr.metrics.reporters.SolrJmxReporter\">\n"
          + "      <str name=\"rootName\">solr_${hostPort:8983}</str>\n"
          + "    </reporter>\n"
          + "  </metrics>\n"
          + "  \n"
          + "</solr>\n";

  private final Object startupWait = new Object();
  private volatile ZkTestServer zkServer; // non-final due to injectChaos()
  private final boolean externalZkServer;
  private final List<JettySolrRunner> jettys = new CopyOnWriteArrayList<>();
  private final Path baseDir;
  private CloudSolrClient solrClient;
  private final JettyConfig jettyConfig;
  private final boolean trackJettyMetrics;

  private final AtomicInteger nodeIds = new AtomicInteger();
  private final Map<String, CloudSolrClient> solrClientByCollection = new ConcurrentHashMap<>();

  /**
   * Create a MiniSolrCloudCluster with default solr.xml
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param jettyConfig Jetty configuration
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, JettyConfig jettyConfig)
      throws Exception {
    this(numServers, baseDir, DEFAULT_CLOUD_SOLR_XML, jettyConfig, null, false);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(
      int numServers,
      String hostContext,
      Path baseDir,
      String solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters)
      throws Exception {
    this(numServers, hostContext, baseDir, solrXml, extraServlets, extraRequestFilters, null);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(
      int numServers,
      String hostContext,
      Path baseDir,
      String solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters,
      SSLConfig sslConfig)
      throws Exception {
    this(
        numServers,
        baseDir,
        solrXml,
        JettyConfig.builder()
            .setContext(hostContext)
            .withSSLConfig(sslConfig)
            .withFilters(extraRequestFilters)
            .withServlets(extraServlets)
            .build());
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(int numServers, Path baseDir, String solrXml, JettyConfig jettyConfig)
      throws Exception {
    this(numServers, baseDir, solrXml, jettyConfig, null, false);
  }

  /**
   * Create a MiniSolrCloudCluster
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use. If null, one will be created
   * @throws Exception if there was an error starting the cluster
   */
  public MiniSolrCloudCluster(
      int numServers,
      Path baseDir,
      String solrXml,
      JettyConfig jettyConfig,
      ZkTestServer zkTestServer,
      boolean formatZkServer)
      throws Exception {
    this(numServers, baseDir, solrXml, jettyConfig, zkTestServer, Optional.empty(), formatZkServer);
  }

  /**
   * Create a MiniSolrCloudCluster. Note - this constructor visibility is changed to package
   * protected so as to discourage its usage. Ideally *new* functionality should use {@linkplain
   * SolrCloudTestCase} to configure any additional parameters.
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use. If null, one will be created
   * @param securityJson A string representation of security.json file (optional).
   * @throws Exception if there was an error starting the cluster
   */
  MiniSolrCloudCluster(
      int numServers,
      Path baseDir,
      String solrXml,
      JettyConfig jettyConfig,
      ZkTestServer zkTestServer,
      Optional<String> securityJson,
      boolean formatZkServer)
      throws Exception {
    this(
        numServers,
        baseDir,
        solrXml,
        jettyConfig,
        zkTestServer,
        securityJson,
        false,
        formatZkServer);
  }

  /**
   * Create a MiniSolrCloudCluster. Note - this constructor visibility is changed to package
   * protected so as to discourage its usage. Ideally *new* functionality should use {@linkplain
   * SolrCloudTestCase} to configure any additional parameters.
   *
   * @param numServers number of Solr servers to start
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param jettyConfig Jetty configuration
   * @param zkTestServer ZkTestServer to use. If null, one will be created
   * @param securityJson A string representation of security.json file (optional).
   * @param trackJettyMetrics supply jetties with metrics registry
   * @throws Exception if there was an error starting the cluster
   */
  MiniSolrCloudCluster(
      int numServers,
      Path baseDir,
      String solrXml,
      JettyConfig jettyConfig,
      ZkTestServer zkTestServer,
      Optional<String> securityJson,
      boolean trackJettyMetrics,
      boolean formatZkServer)
      throws Exception {

    Objects.requireNonNull(securityJson);
    this.baseDir = Objects.requireNonNull(baseDir);
    this.jettyConfig = Objects.requireNonNull(jettyConfig);
    this.trackJettyMetrics = trackJettyMetrics;

    log.info("Starting cluster of {} servers in {}", numServers, baseDir);

    Files.createDirectories(baseDir);

    this.externalZkServer = zkTestServer != null;
    if (!externalZkServer) {
      Path zkDir = baseDir.resolve("zookeeper/server1/data");
      zkTestServer = new ZkTestServer(zkDir);
      try {
        zkTestServer.run(formatZkServer);
      } catch (Exception e) {
        log.error("Error starting Zk Test Server, trying again ...");
        zkTestServer.shutdown();
        zkTestServer = new ZkTestServer(zkDir);
        zkTestServer.run();
      }
    }
    this.zkServer = zkTestServer;

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      if (!zkClient.exists("/solr/solr.xml", true)) {
        zkClient.makePath("/solr/solr.xml", solrXml.getBytes(Charset.defaultCharset()), true);
        if (jettyConfig.sslConfig != null && jettyConfig.sslConfig.isSSLMode()) {
          zkClient.makePath(
              "/solr" + ZkStateReader.CLUSTER_PROPS,
              "{'urlScheme':'https'}".getBytes(StandardCharsets.UTF_8),
              true);
        }
        if (securityJson.isPresent()) { // configure Solr security
          zkClient.makePath(
              "/solr/security.json", securityJson.get().getBytes(Charset.defaultCharset()), true);
        }
      }
    }

    List<Callable<JettySolrRunner>> startups = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; ++i) {
      startups.add(() -> startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig));
    }

    final ExecutorService executorLauncher =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("jetty-launcher"));
    Collection<Future<JettySolrRunner>> futures = executorLauncher.invokeAll(startups);
    ExecutorUtil.shutdownAndAwaitTermination(executorLauncher);
    Exception startupError = checkForExceptions("Error starting up MiniSolrCloudCluster", futures);
    if (startupError != null) {
      try {
        this.shutdown();
      } catch (Throwable t) {
        startupError.addSuppressed(t);
      }
      throw startupError;
    }

    solrClient = buildSolrClient();

    if (numServers > 0) {
      waitForAllNodes(numServers, 60);
    }
  }

  private void waitForAllNodes(int numServers, int timeoutSeconds)
      throws IOException, InterruptedException, TimeoutException {
    log.info("waitForAllNodes: numServers={}", numServers);

    int numRunning;

    if (timeoutSeconds == 0) {
      timeoutSeconds = DEFAULT_TIMEOUT;
    }
    TimeOut timeout = new TimeOut(timeoutSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    synchronized (startupWait) {
      while (numServers != (numRunning = numRunningJetty(getJettySolrRunners()))) {
        if (timeout.hasTimedOut()) {
          throw new IllegalStateException(
              "giving up waiting for all jetty instances to be running. numServers="
                  + numServers
                  + " numRunning="
                  + numRunning);
        }
        startupWait.wait(500);
      }
    }
    for (JettySolrRunner runner : getJettySolrRunners()) {
      waitForNode(runner, (int) timeout.timeLeft(TimeUnit.SECONDS));
    }
  }

  private int numRunningJetty(List<JettySolrRunner> runners) {
    int numRunning = 0;
    for (JettySolrRunner jsr : runners) {
      if (jsr.isRunning()) numRunning++;
    }
    return numRunning;
  }

  public void waitForNode(JettySolrRunner jetty, int timeoutSeconds)
      throws InterruptedException, TimeoutException {
    String nodeName = jetty.getNodeName();
    if (nodeName == null) {
      throw new IllegalArgumentException("Cannot wait for Jetty with null node name");
    }
    log.info("waitForNode: {}", nodeName);

    getZkStateReader()
        .waitForLiveNodes(
            timeoutSeconds, TimeUnit.SECONDS, (o, n) -> n != null && n.contains(nodeName));
  }

  /**
   * This method wait till all Solr JVMs ( Jettys ) are running . It waits up to the timeout (in
   * seconds) for the JVMs to be up before throwing IllegalStateException. This is called
   * automatically on cluster startup and so is only needed when starting additional Jetty
   * instances.
   *
   * @param timeout number of seconds to wait before throwing an IllegalStateException
   * @throws IOException if there was an error communicating with ZooKeeper
   * @throws InterruptedException if the calling thread is interrupted during the wait operation
   * @throws TimeoutException on timeout before all nodes being ready
   */
  public void waitForAllNodes(int timeout)
      throws IOException, InterruptedException, TimeoutException {
    waitForAllNodes(jettys.size(), timeout);
  }

  private String newNodeName() {
    return "node" + nodeIds.incrementAndGet();
  }

  private Path createInstancePath(String name) throws IOException {
    Path instancePath = baseDir.resolve(name);
    Files.createDirectory(instancePath);
    return instancePath;
  }

  /**
   * @return ZooKeeper server used by the MiniCluster
   */
  public ZkTestServer getZkServer() {
    return zkServer;
  }

  /** The {@link ZkStateReader} inside {@link #getSolrClient()}. */
  public ZkStateReader getZkStateReader() {
    return ZkStateReader.from(getSolrClient());
  }

  /**
   * @return Unmodifiable list of all the currently started Solr Jettys.
   */
  public List<JettySolrRunner> getJettySolrRunners() {
    return Collections.unmodifiableList(jettys);
  }

  /**
   * @return a randomly-selected Jetty
   */
  public JettySolrRunner getRandomJetty(Random random) {
    int index = random.nextInt(jettys.size());
    return jettys.get(index);
  }

  /**
   * Start a new Solr instance
   *
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(
      String name,
      String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters)
      throws Exception {
    return startJettySolrRunner(name, hostContext, extraServlets, extraRequestFilters, null);
  }

  /**
   * Start a new Solr instance
   *
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(
      String name,
      String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters,
      SSLConfig sslConfig)
      throws Exception {
    return startJettySolrRunner(
        name,
        hostContext,
        JettyConfig.builder()
            .withServlets(extraServlets)
            .withFilters(extraRequestFilters)
            .withSSLConfig(sslConfig)
            .build());
  }

  public JettySolrRunner getJettySolrRunner(int index) {
    return jettys.get(index);
  }

  /**
   * Start a new Solr instance on a particular servlet context
   *
   * @param name the instance name
   * @param hostContext the context to run on
   * @param config a JettyConfig for the instance's {@link org.apache.solr.embedded.JettySolrRunner}
   * @return a JettySolrRunner
   */
  public JettySolrRunner startJettySolrRunner(String name, String hostContext, JettyConfig config)
      throws Exception {
    // tell solr node to look in zookeeper for solr.xml
    final Properties nodeProps = new Properties();
    nodeProps.setProperty("zkHost", zkServer.getZkAddress());

    Path runnerPath = createInstancePath(name);
    String context = getHostContextSuitableForServletContext(hostContext);
    JettyConfig newConfig = JettyConfig.builder(config).setContext(context).build();
    JettySolrRunner jetty =
        !trackJettyMetrics
            ? new JettySolrRunner(runnerPath.toString(), nodeProps, newConfig)
            : new JettySolrRunnerWithMetrics(runnerPath.toString(), nodeProps, newConfig);
    jetty.start();
    jettys.add(jetty);
    synchronized (startupWait) {
      startupWait.notifyAll();
    }
    return jetty;
  }

  /**
   * Start a new Solr instance, using the default config
   *
   * @return a JettySolrRunner
   */
  public JettySolrRunner startJettySolrRunner() throws Exception {
    return startJettySolrRunner(newNodeName(), jettyConfig.context, jettyConfig);
  }

  /**
   * Stop a Solr instance
   *
   * @param index the index of node in collection returned by {@link #getJettySolrRunners()}
   * @return the shut down node
   */
  public JettySolrRunner stopJettySolrRunner(int index) throws Exception {
    JettySolrRunner jetty = jettys.get(index);
    jetty.stop();
    jettys.remove(index);
    return jetty;
  }

  /**
   * Add a previously stopped node back to the cluster
   *
   * @param jetty a {@link JettySolrRunner} previously returned by {@link #stopJettySolrRunner(int)}
   * @return the started node
   * @throws Exception on error
   */
  public JettySolrRunner startJettySolrRunner(JettySolrRunner jetty) throws Exception {
    jetty.start(false);
    if (!jettys.contains(jetty)) jettys.add(jetty);
    return jetty;
  }

  /**
   * Stop the given Solr instance. It will be removed from the cluster's list of running instances.
   *
   * @param jetty a {@link JettySolrRunner} to be stopped
   * @return the same {@link JettySolrRunner} instance provided to this method
   * @throws Exception on error
   */
  public JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
    jetty.stop();
    jettys.remove(jetty);
    return jetty;
  }

  /**
   * Upload a config set
   *
   * @param configDir a path to the config set to upload
   * @param configName the name to give the configset
   */
  public void uploadConfigSet(Path configDir, String configName) throws IOException {
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      ZkMaintenanceUtils.uploadToZK(
          zkClient,
          configDir,
          ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + configName,
          ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
    }
  }

  /** Delete all collections (and aliases) */
  public void deleteAllCollections() throws Exception {
    try (ZkStateReader reader = new ZkStateReader(getZkClient())) {
      final CountDownLatch latch = new CountDownLatch(1);
      reader.registerCloudCollectionsListener(
          (oldCollections, newCollections) -> {
            if (newCollections != null && newCollections.size() == 0) {
              latch.countDown();
            }
          });

      reader.createClusterStateWatchersAndUpdate(); // up to date aliases & collections
      reader.aliasesManager.applyModificationAndExportToZk(aliases -> Aliases.EMPTY);
      for (String collection : reader.getClusterState().getCollectionStates().keySet()) {
        CollectionAdminRequest.deleteCollection(collection).process(solrClient);
      }

      boolean success = latch.await(60, TimeUnit.SECONDS);
      if (!success) {
        throw new IllegalStateException(
            "Still waiting to see all collections removed from clusterstate.");
      }

      for (String collection : reader.getClusterState().getCollectionStates().keySet()) {
        reader.waitForState(collection, 15, TimeUnit.SECONDS, Objects::isNull);
      }
    }

    // may be deleted, but may not be gone yet - we only wait to not see it in ZK, not for core
    // unloads
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (true) {

      if (timeout.hasTimedOut()) {
        throw new TimeoutException("Timed out waiting for all collections to be fully removed.");
      }

      boolean allContainersEmpty = true;
      for (JettySolrRunner jetty : jettys) {
        CoreContainer cc = jetty.getCoreContainer();
        if (cc != null && cc.getCores().size() != 0) {
          allContainersEmpty = false;
        }
      }
      if (allContainersEmpty) {
        break;
      }
    }
  }

  public void deleteAllConfigSets() throws Exception {

    List<String> configSetNames =
        new ConfigSetAdminRequest.List().process(solrClient).getConfigSets();

    for (String configSet : configSetNames) {
      if (configSet.equals("_default")) {
        continue;
      }
      try {
        // cleanup any property before removing the configset
        getZkClient()
            .delete(
                ZkConfigSetService.CONFIGS_ZKNODE + "/" + configSet + "/" + DEFAULT_FILENAME,
                -1,
                true);
      } catch (KeeperException.NoNodeException nne) {
      }
      new ConfigSetAdminRequest.Delete().setConfigSetName(configSet).process(solrClient);
    }
  }

  /** Shut down the cluster, including all Solr nodes and ZooKeeper */
  public void shutdown() throws Exception {
    try {

      IOUtils.closeQuietly(solrClient);

      solrClientByCollection.values().parallelStream()
          .forEach(
              c -> {
                IOUtils.closeQuietly(c);
              });
      solrClientByCollection.clear();
      ;
      List<Callable<JettySolrRunner>> shutdowns = new ArrayList<>(jettys.size());
      for (final JettySolrRunner jetty : jettys) {
        shutdowns.add(() -> stopJettySolrRunner(jetty));
      }
      jettys.clear();
      final ExecutorService executorCloser =
          ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("jetty-closer"));
      Collection<Future<JettySolrRunner>> futures = executorCloser.invokeAll(shutdowns);
      ExecutorUtil.shutdownAndAwaitTermination(executorCloser);
      Exception shutdownError =
          checkForExceptions("Error shutting down MiniSolrCloudCluster", futures);
      if (shutdownError != null) {
        throw shutdownError;
      }
    } finally {
      if (!externalZkServer) {
        zkServer.shutdown();
      }
      resetRecordingFlag();
    }
  }

  public Path getBaseDir() {
    return baseDir;
  }

  public CloudSolrClient getSolrClient() {
    return solrClient;
  }

  /**
   * Returns a SolrClient that has a defaultCollection set for it. SolrClients are cached by their
   * collectionName for reuse and are closed for you.
   *
   * @param collectionName The name of the collection to get a SolrClient for.
   * @return CloudSolrClient configured for the specific collection.
   */
  public CloudSolrClient getSolrClient(String collectionName) {
    return solrClientByCollection.computeIfAbsent(
        collectionName,
        k -> {
          CloudSolrClient solrClient =
              new CloudLegacySolrClient.Builder(
                      Collections.singletonList(zkServer.getZkAddress()), Optional.empty())
                  .withDefaultCollection(collectionName)
                  .withSocketTimeout(90000)
                  .withConnectionTimeout(15000)
                  .build();

          solrClient.connect();
          if (log.isInfoEnabled()) {
            log.info(
                "Created solrClient for collection {} with updatesToLeaders={} and parallelUpdates={}",
                collectionName,
                solrClient.isUpdatesToLeaders(),
                solrClient.isParallelUpdates());
          }
          return solrClient;
        });
  }

  public SolrZkClient getZkClient() {
    return getZkStateReader().getZkClient();
  }

  /**
   * Set data in zk without exposing caller to the ZK API, i.e. tests won't need to include
   * Zookeeper dependencies
   */
  public void zkSetData(String path, byte[] data, boolean retryOnConnLoss)
      throws InterruptedException {
    try {
      getZkClient().setData(path, data, -1, retryOnConnLoss);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.UNKNOWN, "Failed writing to Zookeeper", e);
    }
  }

  protected CloudSolrClient buildSolrClient() {
    return new CloudLegacySolrClient.Builder(
            Collections.singletonList(getZkServer().getZkAddress()), Optional.empty())
        .withSocketTimeout(90000, TimeUnit.MILLISECONDS)
        .withConnectionTimeout(15000, TimeUnit.MILLISECONDS)
        .build(); // we choose 90 because we run in some harsh envs
  }

  /**
   * creates a basic CloudSolrClient Builder that then can be customized by callers, for example by
   * specifying what collection they want to use.
   *
   * @return CloudLegacySolrClient.Builder
   */
  public CloudLegacySolrClient.Builder basicSolrClientBuilder() {
    return new CloudLegacySolrClient.Builder(
            Collections.singletonList(getZkServer().getZkAddress()), Optional.empty())
        .withSocketTimeout(90000) // we choose 90 because we run in some harsh envs
        .withConnectionTimeout(15000);
  }

  private static String getHostContextSuitableForServletContext(String ctx) {
    if (ctx == null || ctx.isEmpty()) ctx = "/solr";
    if (ctx.endsWith("/")) ctx = ctx.substring(0, ctx.length() - 1);
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    return ctx;
  }

  private Exception checkForExceptions(String message, Collection<Future<JettySolrRunner>> futures)
      throws InterruptedException {
    Exception parsed = new Exception(message);
    boolean ok = true;
    for (Future<JettySolrRunner> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        parsed.addSuppressed(e.getCause());
        ok = false;
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw e;
      }
    }
    return ok ? null : parsed;
  }

  /** Return the jetty that a particular replica resides on */
  public JettySolrRunner getReplicaJetty(Replica replica) {
    for (JettySolrRunner jetty : jettys) {
      if (jetty.isStopped()) continue;
      if (replica.getCoreUrl().startsWith(jetty.getBaseUrl().toString())) return jetty;
    }
    throw new IllegalArgumentException(
        "Cannot find Jetty for a replica with core url " + replica.getCoreUrl());
  }

  /** Make the zookeeper session on a particular jetty lose connection and expire */
  public void expireZkSession(JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      ChaosMonkey.causeConnectionLoss(jetty);
      zkServer.expire(cores.getZkController().getZkClient().getZooKeeper().getSessionId());
      if (log.isInfoEnabled()) {
        log.info("Expired zookeeper session from node {}", jetty.getBaseUrl());
      }
    }
  }

  public synchronized void injectChaos(Random random) throws Exception {

    // sometimes we restart one of the jetty nodes
    if (random.nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random.nextInt(jettys.size()));
      jetty.stop();
      log.info("============ Restarting jetty");
      jetty.start();
    }

    // sometimes we restart zookeeper
    if (random.nextBoolean()) {
      zkServer.shutdown();
      log.info("============ Restarting zookeeper");
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run(false);
    }

    // sometimes we cause a connection loss - sometimes it will hit the overseer
    if (random.nextBoolean()) {
      JettySolrRunner jetty = jettys.get(random.nextInt(jettys.size()));
      ChaosMonkey.causeConnectionLoss(jetty);
    }
  }

  public Overseer getOpenOverseer() {
    List<Overseer> overseers = new ArrayList<>();
    for (int i = 0; i < jettys.size(); i++) {
      JettySolrRunner runner = getJettySolrRunner(i);
      if (runner.getCoreContainer() != null) {
        overseers.add(runner.getCoreContainer().getZkController().getOverseer());
      }
    }

    return getOpenOverseer(overseers);
  }

  public static Overseer getOpenOverseer(List<Overseer> overseers) {
    ArrayList<Overseer> shuffledOverseers = new ArrayList<Overseer>(overseers);
    Collections.shuffle(shuffledOverseers, LuceneTestCase.random());
    for (Overseer overseer : shuffledOverseers) {
      if (!overseer.isClosed()) {
        return overseer;
      }
    }
    throw new SolrException(ErrorCode.NOT_FOUND, "No open Overseer found");
  }

  public void waitForActiveCollection(
      String collection, long wait, TimeUnit unit, int shards, int totalReplicas) {
    log.info("waitForActiveCollection: {}", collection);
    CollectionStatePredicate predicate = expectedShardsAndActiveReplicas(shards, totalReplicas);

    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      getZkStateReader()
          .waitForState(
              collection,
              wait,
              unit,
              (n, c) -> {
                state.set(c);
                liveNodesLastSeen.set(n);

                return predicate.matches(n, c);
              });
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(
          "Failed while waiting for active collection"
              + "\n"
              + e.getMessage()
              + "\nLive Nodes: "
              + Arrays.toString(liveNodesLastSeen.get().toArray())
              + "\nLast available state: "
              + state.get());
    }
  }

  public void waitForActiveCollection(String collection, int shards, int totalReplicas) {
    waitForActiveCollection(collection, 30, TimeUnit.SECONDS, shards, totalReplicas);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit) {
    log.info("waitForActiveCollection: {}", collection);
    CollectionStatePredicate predicate = expectedActive();

    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      getZkStateReader()
          .waitForState(
              collection,
              wait,
              unit,
              (n, c) -> {
                state.set(c);
                liveNodesLastSeen.set(n);

                return predicate.matches(n, c);
              });
    } catch (TimeoutException | InterruptedException e) {
      throw new RuntimeException(
          "Failed while waiting for active collection"
              + "\n"
              + e.getMessage()
              + "\nLive Nodes: "
              + Arrays.toString(liveNodesLastSeen.get().toArray())
              + "\nLast available state: "
              + state.get());
    }
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(
      int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null) return false;
      if (collectionState.getSlices().size() != expectedShards) {
        return false;
      }

      int activeReplicas = 0;
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes)) {
            activeReplicas++;
          }
        }
      }
      if (activeReplicas == expectedReplicas) {
        return true;
      }

      return false;
    };
  }

  public static CollectionStatePredicate expectedActive() {
    return (liveNodes, collectionState) -> {
      if (collectionState == null) return false;

      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (!replica.isActive(liveNodes)) {
            return false;
          }
        }
      }

      return true;
    };
  }

  public void waitForJettyToStop(JettySolrRunner runner) throws TimeoutException {
    String nodeName = runner.getNodeName();
    if (nodeName == null) {
      log.info("Cannot wait for Jetty with null node name");
      return;
    }

    log.info("waitForJettyToStop: {}", nodeName);

    try {
      getZkStateReader().waitForLiveNodes(15, TimeUnit.SECONDS, (o, n) -> !n.contains(nodeName));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "interrupted", e);
    }
  }

  public void dumpMetrics(File outputDirectory, String fileName) throws IOException {
    for (JettySolrRunner jetty : jettys) {
      jetty.outputMetrics(outputDirectory, fileName);
    }
  }

  public void dumpCoreInfo(PrintStream pw) throws IOException {
    for (JettySolrRunner jetty : jettys) {
      jetty.dumpCoresInfo(pw);
    }
  }

  /**
   * @lucene.experimental
   */
  public static final class JettySolrRunnerWithMetrics extends JettySolrRunner {
    public JettySolrRunnerWithMetrics(String solrHome, Properties nodeProps, JettyConfig config) {
      super(solrHome, nodeProps, config);
    }

    private volatile MetricRegistry metricRegistry;

    @Override
    protected HandlerWrapper injectJettyHandlers(HandlerWrapper chain) {
      metricRegistry = new MetricRegistry();
      io.dropwizard.metrics.jetty10.InstrumentedHandler metrics =
          new io.dropwizard.metrics.jetty10.InstrumentedHandler(metricRegistry);
      metrics.setHandler(chain);
      return metrics;
    }

    /**
     * @return optional subj. It may be null, if it's not yet created.
     */
    public MetricRegistry getMetricRegistry() {
      return metricRegistry;
    }
  }

  private static class Config {
    final String name;
    final Path path;

    private Config(String name, Path path) {
      this.name = name;
      this.path = path;
    }
  }

  /** Builder class for a MiniSolrCloudCluster */
  public static class Builder {

    private final int nodeCount;
    private final Path baseDir;
    private String solrXml = DEFAULT_CLOUD_SOLR_XML;
    private JettyConfig.Builder jettyConfigBuilder;
    private Optional<String> securityJson = Optional.empty();

    private List<Config> configs = new ArrayList<>();
    private Map<String, Object> clusterProperties = new HashMap<>();

    private boolean trackJettyMetrics;
    private boolean useDistributedCollectionConfigSetExecution;
    private boolean useDistributedClusterStateUpdate;
    private boolean formatZkServer = true;
    private boolean disableTraceIdGeneration = false;

    /**
     * Create a builder
     *
     * @param nodeCount the number of nodes in the cluster
     * @param baseDir a base directory for the cluster
     */
    public Builder(int nodeCount, Path baseDir) {
      this.nodeCount = nodeCount;
      this.baseDir = baseDir;

      jettyConfigBuilder = JettyConfig.builder().setContext("/solr");
      if (SolrTestCaseJ4.sslConfig != null) {
        jettyConfigBuilder =
            jettyConfigBuilder.withSSLConfig(SolrTestCaseJ4.sslConfig.buildServerSSLConfig());
      }
    }

    /** Use a JettyConfig.Builder to configure the cluster's jetty servers */
    public Builder withJettyConfig(Consumer<JettyConfig.Builder> fun) {
      fun.accept(jettyConfigBuilder);
      return this;
    }

    /** Use the provided string as solr.xml content */
    public Builder withSolrXml(String solrXml) {
      this.solrXml = solrXml;
      return this;
    }

    /** Read solr.xml from the provided path */
    public Builder withSolrXml(Path solrXml) {
      try {
        this.solrXml = new String(Files.readAllBytes(solrXml), Charset.defaultCharset());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    /**
     * Configure the specified security.json for the {@linkplain MiniSolrCloudCluster}
     *
     * @param securityJson The path specifying the security.json file
     * @return the instance of {@linkplain Builder}
     */
    public Builder withSecurityJson(Path securityJson) {
      try {
        this.securityJson =
            Optional.of(new String(Files.readAllBytes(securityJson), Charset.defaultCharset()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    /**
     * Configure the specified security.json for the {@linkplain MiniSolrCloudCluster}
     *
     * @param securityJson The string specifying the security.json configuration
     * @return the instance of {@linkplain Builder}
     */
    public Builder withSecurityJson(String securityJson) {
      this.securityJson = Optional.of(securityJson);
      return this;
    }

    /**
     * Upload a collection config before tests start
     *
     * @param configName the config name
     * @param configPath the path to the config files
     */
    public Builder addConfig(String configName, Path configPath) {
      this.configs.add(new Config(configName, configPath));
      return this;
    }

    /**
     * This method makes the MiniSolrCloudCluster use the "other" Collection API execution strategy
     * than it normally would. When some test classes call this method (and some don't) we make sure
     * that a run of multiple tests with a single seed will exercise both code lines (distributed
     * and Overseer based Collection API) so regressions can be spotted faster.
     *
     * <p>The real need is for a few tests covering reasonable use cases to call this method. If
     * you're adding a new test, you don't have to call it (but it's ok if you do).
     */
    public Builder useOtherCollectionConfigSetExecution() {
      // Switch from Overseer to distributed Collection execution and vice versa
      useDistributedCollectionConfigSetExecution = !useDistributedCollectionConfigSetExecution;
      // Reverse distributed cluster state updates as well if possible (state can't be Overseer
      // based if Collections API is distributed)
      useDistributedClusterStateUpdate =
          !useDistributedClusterStateUpdate || useDistributedCollectionConfigSetExecution;
      return this;
    }

    /**
     * Force the cluster Collection and config state API execution as well as the cluster state
     * update strategy to be either Overseer based or distributed. <b>This method can be useful when
     * debugging tests</b> failing in only one of the two modes to have all local runs exhibit the
     * issue, as well obviously for tests that are not compatible with one of the two modes.
     *
     * <p>If this method is not called, the strategy being used will be random if the configuration
     * passed to the cluster ({@code solr.xml} equivalent) contains a placeholder similar to:
     *
     * <pre>{@code
     * <solrcloud>
     *   ....
     *   <str name="distributedClusterStateUpdates">${solr.distributedClusterStateUpdates:false}</str>
     *   <str name="distributedCollectionConfigSetExecution">${solr.distributedCollectionConfigSetExecution:false}</str>
     *   ....
     * </solrcloud>
     * }</pre>
     *
     * For an example of a configuration supporting this setting, see {@link
     * MiniSolrCloudCluster#DEFAULT_CLOUD_SOLR_XML}. When a test sets a different {@code solr.xml}
     * config (using {@link #withSolrXml}), if the config does not contain the placeholder, the
     * strategy will be defined by the values assigned to {@code useDistributedClusterStateUpdates}
     * and {@code useDistributedCollectionConfigSetExecution} in {@link
     * org.apache.solr.core.CloudConfig.CloudConfigBuilder}.
     *
     * @param distributedCollectionConfigSetApi When {@code true}, Collection and Config Set API
     *     commands are executed in a distributed way by nodes. When {@code false}, they are
     *     executed by Overseer.
     * @param distributedClusterStateUpdates When {@code true}, cluster state updates are handled in
     *     a distributed way by nodes. When {@code false}, cluster state updates are handled by
     *     Overseer.
     *     <p>If {@code distributedCollectionConfigSetApi} is {@code true} then this parameter must
     *     be {@code true}.
     */
    @SuppressWarnings("InvalidParam")
    public Builder withDistributedClusterStateUpdates(
        boolean distributedCollectionConfigSetApi, boolean distributedClusterStateUpdates) {
      useDistributedCollectionConfigSetExecution = distributedCollectionConfigSetApi;
      useDistributedClusterStateUpdate = distributedClusterStateUpdates;
      return this;
    }

    /**
     * Set a cluster property
     *
     * @param propertyName the property name
     * @param propertyValue the property value
     */
    public Builder withProperty(String propertyName, String propertyValue) {
      this.clusterProperties.put(propertyName, propertyValue);
      return this;
    }

    public Builder withMetrics(boolean trackJettyMetrics) {
      this.trackJettyMetrics = trackJettyMetrics;
      return this;
    }

    public Builder formatZkServer(boolean formatZkServer) {
      this.formatZkServer = formatZkServer;
      return this;
    }

    /**
     * Configure and run the {@link MiniSolrCloudCluster}
     *
     * @throws Exception if an error occurs on startup
     */
    public MiniSolrCloudCluster configure() throws Exception {
      return SolrCloudTestCase.cluster = build();
    }

    /**
     * Configure, run and return the {@link MiniSolrCloudCluster}
     *
     * @throws Exception if an error occurs on startup
     */
    public MiniSolrCloudCluster build() throws Exception {
      // Two lines below will have an impact on how the MiniSolrCloudCluster and therefore the test
      // run if the config being
      // used in the test does have the appropriate placeholders. See for example
      // DEFAULT_CLOUD_SOLR_XML in MiniSolrCloudCluster.
      // Hard coding values here will impact such tests.
      // To hard code behavior for tests not having these placeholders - and for SolrCloud as well
      // for that matter! -
      // change the values assigned to useDistributedClusterStateUpdates and
      // useDistributedCollectionConfigSetExecution in
      // org.apache.solr.core.CloudConfig.CloudConfigBuilder. Do not forget then to revert before
      // commit!
      System.setProperty(
          "solr.distributedCollectionConfigSetExecution",
          Boolean.toString(useDistributedCollectionConfigSetExecution));
      System.setProperty(
          "solr.distributedClusterStateUpdates",
          Boolean.toString(useDistributedClusterStateUpdate));

      // eager init to prevent OTEL init races caused by test setup
      if (!disableTraceIdGeneration && TracerConfigurator.TRACE_ID_GEN_ENABLED) {
        SimplePropagator.load();
        injectRandomRecordingFlag();
      }

      JettyConfig jettyConfig = jettyConfigBuilder.build();
      MiniSolrCloudCluster cluster =
          new MiniSolrCloudCluster(
              nodeCount,
              baseDir,
              solrXml,
              jettyConfig,
              null,
              securityJson,
              trackJettyMetrics,
              formatZkServer);
      for (Config config : configs) {
        cluster.uploadConfigSet(config.path, config.name);
      }

      if (clusterProperties.size() > 0) {
        ClusterProperties props = new ClusterProperties(cluster.getZkClient());
        for (Map.Entry<String, Object> entry : clusterProperties.entrySet()) {
          props.setClusterProperty(entry.getKey(), entry.getValue());
        }
      }
      return cluster;
    }

    public Builder withDefaultClusterProperty(String key, String value) {
      @SuppressWarnings({"unchecked"})
      HashMap<String, Object> defaults =
          (HashMap<String, Object>) this.clusterProperties.get(CollectionAdminParams.DEFAULTS);
      if (defaults == null) {
        defaults = new HashMap<>();
        this.clusterProperties.put(CollectionAdminParams.DEFAULTS, defaults);
      }
      @SuppressWarnings({"unchecked"})
      HashMap<String, Object> cluster =
          (HashMap<String, Object>) defaults.get(CollectionAdminParams.CLUSTER);
      if (cluster == null) {
        cluster = new HashMap<>();
        defaults.put(CollectionAdminParams.CLUSTER, cluster);
      }
      cluster.put(key, value);
      return this;
    }

    /**
     * Disables the default/built-in simple trace ID generation/propagation.
     *
     * <p>Tracers are registered as global singletons and if for example a test needs to use a
     * MockTracer or a "real" Tracer, it needs to call this method so that the test setup doesn't
     * accidentally reset the Tracer it wants to use.
     */
    public Builder withTraceIdGenerationDisabled() {
      this.disableTraceIdGeneration = true;
      return this;
    }
  }

  /**
   * Randomizes the tracing Span::isRecording check.
   *
   * <p>This will randomize the Span::isRecording check so we have better coverage of all methods
   * that deal with span creation without having to enable otel module.
   *
   * <p>It only makes sense to call this if we are using the alwaysOn tracer, the OTEL tracer
   * already has this flag turned on and randomizing it would risk not recording trace data.
   *
   * <p>Note. Tracing is not a SolrCloud only feature. this method is placed here for convenience
   * only, any test can make use of this example for more complete coverage of the tracing
   * mechanics.
   */
  private static void injectRandomRecordingFlag() {
    try {
      boolean isRecording = LuceneTestCase.rarely();
      TraceUtils.IS_RECORDING = (ignored) -> isRecording;
    } catch (IllegalStateException e) {
      // This can happen in benchmarks or other places that aren't in a randomized test
      log.warn("Unable to inject random recording flag due to outside randomized context", e);
    }
  }

  private static void resetRecordingFlag() {
    TraceUtils.IS_RECORDING = TraceUtils.DEFAULT_IS_RECORDING;
  }
}
