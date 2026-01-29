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
import io.dropwizard.metrics.jetty12.ee10.InstrumentedEE10Handler;
import jakarta.servlet.Filter;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.apache.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.jetty.SSLConfig;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.OpenTelemetryConfigurator;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.tracing.TraceUtils;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Handler;
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
          + "  <str name=\"allowPaths\">${solr.security.allow.paths:}</str>\n"
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
          + "    <int name=\"zkClientTimeout\">${solr.zookeeper.client.timeout:30000}</int>\n"
          + "    <int name=\"leaderVoteWait\">${leaderVoteWait:10000}</int>\n"
          + "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n"
          + "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n"
          + "    <str name=\"zkCredentialsInjector\">${zkCredentialsInjector:org.apache.solr.common.cloud.DefaultZkCredentialsInjector}</str> \n"
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
  private volatile ZkTestServer zkServer;
  private final boolean externalZkServer;
  private final List<JettySolrRunner> jettys = new CopyOnWriteArrayList<>();
  private final Path baseDir;
  private CloudSolrClient solrClient;
  private final JettyConfig jettyConfig;
  private final String solrXml;
  private final boolean trackJettyMetrics;
  private final String zkHost; // ZK connection string (used in quorum mode when zkServer is null)

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
   * protected to discourage its usage. Ideally *new* functionality should use {@linkplain
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
   * protected to discourage its usage. Ideally *new* functionality should use {@linkplain
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
    this.solrXml = solrXml == null ? DEFAULT_CLOUD_SOLR_XML : solrXml;
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
    this.zkHost = null; // Not used in standard mode

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      if (!zkClient.exists("/solr/initialized")) {
        zkClient.makePath("/solr/initialized", "yes".getBytes(Charset.defaultCharset()), true);
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
      startups.add(() -> startJettySolrRunner(newNodeName(), jettyConfig, solrXml));
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

  /**
   * Create a MiniSolrCloudCluster with embedded ZooKeeper quorum mode. Each Solr node runs its own
   * embedded ZooKeeper server, and together they form a quorum.
   *
   * @param numServers number of Solr servers (must be at least 3 for quorum)
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file content
   * @param jettyConfig Jetty configuration
   * @param securityJson Optional security.json configuration
   * @param trackJettyMetrics whether to track Jetty metrics
   * @throws Exception if there was an error starting the cluster
   */
  MiniSolrCloudCluster(
      int numServers,
      Path baseDir,
      String solrXml,
      JettyConfig jettyConfig,
      Optional<String> securityJson,
      boolean trackJettyMetrics,
      boolean useEmbeddedZkQuorum)
      throws Exception {

    if (!useEmbeddedZkQuorum) {
      throw new IllegalArgumentException("This constructor is only for embedded ZK quorum mode");
    }
    if (numServers < 3) {
      throw new IllegalArgumentException(
          "ZooKeeper quorum requires at least 3 nodes, got: " + numServers);
    }

    Objects.requireNonNull(securityJson);
    this.baseDir = Objects.requireNonNull(baseDir);
    this.jettyConfig = Objects.requireNonNull(jettyConfig);
    this.solrXml = solrXml == null ? DEFAULT_CLOUD_SOLR_XML : solrXml;
    this.trackJettyMetrics = trackJettyMetrics;
    this.externalZkServer = true; // No ZkTestServer in quorum mode
    this.zkServer = null; // No single ZK server

    log.info("Starting cluster of {} servers with embedded ZK quorum in {}", numServers, baseDir);
    Files.createDirectories(baseDir);

    // Phase 1: Reserve random ports for all nodes
    int[] ports = reservePortPairs(numServers);

    // Build the zkHost string with all ZK ports (Solr port + 1000)
    StringBuilder zkHostBuilder = new StringBuilder();
    for (int i = 0; i < numServers; i++) {
      if (i > 0) {
        zkHostBuilder.append(",");
      }
      int zkPort = ports[i] + 1000;
      zkHostBuilder.append("127.0.0.1:").append(zkPort);
    }
    this.zkHost = zkHostBuilder.toString(); // Save for later use

    if (log.isInfoEnabled()) {
      log.info("Reserved ports for {} nodes: {}", numServers, java.util.Arrays.toString(ports));
      log.info("ZK connection string: {}", this.zkHost);
    }

    // Set system properties for embedded ZK quorum mode
    System.setProperty("solr.zookeeper.server.enabled", "true");
    System.setProperty("solr.security.manager.enabled", "false");
    System.setProperty("solr.node.roles", "data:on,overseer:allowed,zookeeper_quorum:on");
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    System.setProperty("solr.zookeeper.client.timeout", "300000"); // 5 minutes

    // Phase 2: Start all nodes in parallel
    List<Callable<JettySolrRunner>> startups = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; i++) {
      final int solrPort = ports[i];
      final String nodeName = newNodeName();
      startups.add(
          () -> {
            Path runnerPath = createInstancePath(nodeName);
            Files.write(runnerPath.resolve("solr.xml"), solrXml.getBytes(StandardCharsets.UTF_8));

            Properties nodeProps = new Properties();
            nodeProps.setProperty("zkHost", this.zkHost);
            nodeProps.setProperty("hostPort", String.valueOf(solrPort));

            JettyConfig newConfig = JettyConfig.builder(jettyConfig).setPort(solrPort).build();

            JettySolrRunner jetty =
                !trackJettyMetrics
                    ? new JettySolrRunner(runnerPath.toString(), nodeProps, newConfig)
                    : new JettySolrRunnerWithMetrics(runnerPath.toString(), nodeProps, newConfig);

            int zkPort = solrPort + 1000;
            log.info("Starting {} on port {} with ZK on port {}", nodeName, solrPort, zkPort);
            jetty.start();
            log.info("Node {} started successfully", nodeName);

            jettys.add(jetty);
            synchronized (startupWait) {
              startupWait.notifyAll();
            }
            return jetty;
          });
    }

    final ExecutorService executorLauncher =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("jetty-launcher"));
    Collection<Future<JettySolrRunner>> futures = executorLauncher.invokeAll(startups);
    ExecutorUtil.shutdownAndAwaitTermination(executorLauncher);
    Exception startupError =
        checkForExceptions(
            "Error starting up MiniSolrCloudCluster with embedded ZK quorum", futures);
    if (startupError != null) {
      try {
        this.shutdown();
      } catch (Throwable t) {
        startupError.addSuppressed(t);
      }
      throw startupError;
    }

    log.info("All {} nodes started, waiting for quorum formation...", numServers);
    Thread.sleep(10000); // Wait for ZK quorum to fully form

    // Initialize ZK paths and security (if provided)
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(this.zkHost)
            .withTimeout(60000, TimeUnit.MILLISECONDS)
            .build()) {
      if (!zkClient.exists("/solr")) {
        zkClient.makePath("/solr", true);
      }

      if (jettyConfig.sslConfig != null && jettyConfig.sslConfig.isSSLMode()) {
        zkClient.makePath(
            "/solr" + ZkStateReader.CLUSTER_PROPS,
            "{'urlScheme':'https'}".getBytes(StandardCharsets.UTF_8),
            true);
      }
      if (securityJson.isPresent()) {
        zkClient.makePath(
            "/solr/security.json", securityJson.get().getBytes(Charset.defaultCharset()), true);
      }
    }

    solrClient = buildSolrClientForQuorum(this.zkHost);

    if (numServers > 0) {
      waitForAllNodes(numServers, 60);
    }

    log.info("Embedded ZK quorum cluster started successfully with {} nodes", numServers);
  }

  /**
   * Reserves port pairs for embedded ZK quorum mode. For each node, we need both a Solr port and a
   * ZK port (Solr port + 1000). This method ensures both ports in each pair are available before
   * returning.
   *
   * <p>The method keeps all ServerSockets open during the search to prevent race conditions where
   * another process might grab a port between our check and actual usage.
   *
   * @param numPairs the number of port pairs to reserve
   * @return array of Solr ports (ZK ports are Solr port + 1000)
   * @throws IOException if unable to find enough available port pairs
   */
  private int[] reservePortPairs(int numPairs) throws IOException {
    List<ServerSocket> solrSockets = new ArrayList<>();
    List<ServerSocket> zkSockets = new ArrayList<>();
    int[] ports = new int[numPairs];

    try {
      int pairsFound = 0;
      int maxAttempts = numPairs * 100; // Reasonable limit to avoid infinite loops
      int attempts = 0;

      while (pairsFound < numPairs && attempts < maxAttempts) {
        attempts++;
        ServerSocket solrSocket = null;
        ServerSocket zkSocket = null;

        try {
          // Try to get a random available port for Solr
          solrSocket = new ServerSocket(0);
          int solrPort = solrSocket.getLocalPort();
          int zkPort = solrPort + 1000;

          // Check if ZK port would exceed the valid port range (0-65535)
          if (zkPort > 65535) {
            solrSocket.close();
            continue; // Skip this port and try again
          }

          // Verify the corresponding ZK port is also available
          zkSocket = new ServerSocket(zkPort);

          // Both ports are available - keep the sockets and record the port
          solrSockets.add(solrSocket);
          zkSockets.add(zkSocket);
          ports[pairsFound] = solrPort;
          pairsFound++;

          if (log.isDebugEnabled()) {
            log.debug(
                "Reserved port pair {}/{}: Solr={}, ZK={}", pairsFound, numPairs, solrPort, zkPort);
          }

        } catch (IOException | IllegalArgumentException e) {
          // ZK port was not available or invalid, close sockets and try again
          if (solrSocket != null) {
            try {
              solrSocket.close();
            } catch (IOException ignored) {
            }
          }
          if (zkSocket != null) {
            try {
              zkSocket.close();
            } catch (IOException ignored) {
            }
          }
        }
      }

      if (pairsFound < numPairs) {
        throw new IOException(
            "Unable to find " + numPairs + " available port pairs after " + attempts + " attempts");
      }
      return ports;

    } finally {
      // Close all sockets now that we've recorded the ports
      // The ports will remain available for immediate reuse
      for (ServerSocket socket : solrSockets) {
        try {
          socket.close();
        } catch (IOException e) {
          log.warn("Error closing Solr socket", e);
        }
      }
      for (ServerSocket socket : zkSockets) {
        try {
          socket.close();
        } catch (IOException e) {
          log.warn("Error closing ZK socket", e);
        }
      }
    }
  }

  /**
   * Get the ZK connection string. Works for both standard mode (using zkServer) and quorum mode
   * (using zkHost field).
   *
   * @return ZK connection string
   */
  private String getZkAddress() {
    if (zkHost != null) {
      return zkHost; // Quorum mode
    }
    return zkServer.getZkAddress(); // Standard mode
  }

  private CloudSolrClient buildSolrClientForQuorum(String zkHost) {
    return new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
        .withSocketTimeout(90000, TimeUnit.MILLISECONDS)
        .withConnectionTimeout(15000, TimeUnit.MILLISECONDS)
        .build();
  }

  private void waitForAllNodes(int numServers, int timeoutSeconds)
      throws InterruptedException, TimeoutException {
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
   * Wait for the expected number of live nodes in the cluster.
   *
   * @param expectedCount expected number of live nodes
   * @param timeoutSeconds timeout in seconds
   * @throws InterruptedException if interrupted while waiting
   * @throws TimeoutException if the expected count is not reached within the timeout
   */
  public void waitForLiveNodes(int expectedCount, int timeoutSeconds)
      throws InterruptedException, TimeoutException {
    TimeOut timeout = new TimeOut(timeoutSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      long runningNodes = jettys.stream().filter(JettySolrRunner::isRunning).count();
      if (runningNodes == expectedCount) {
        log.info("Verified {} live nodes", runningNodes);
        return;
      }
      Thread.sleep(200);
    }
    // Final check after timeout
    long actualCount = jettys.stream().filter(JettySolrRunner::isRunning).count();
    throw new TimeoutException(
        "Live node count mismatch: expected " + expectedCount + " but got " + actualCount);
  }

  /**
   * Wait for the document count in a collection to reach the expected value.
   *
   * @param collectionName name of the collection to check
   * @param expectedCount expected number of documents
   * @param description description for logging
   * @param timeoutValue timeout value in seconds
   * @param timeoutUnit timeout unit
   * @throws InterruptedException if interrupted while waiting
   * @throws TimeoutException if the expected count is not reached within the timeout
   */
  public void waitForDocCount(
      String collectionName,
      long expectedCount,
      String description,
      int timeoutValue,
      TimeUnit timeoutUnit)
      throws InterruptedException, TimeoutException {
    TimeOut timeout = new TimeOut(timeoutValue, timeoutUnit, TimeSource.NANO_TIME);
    SolrClient client = getSolrClient(collectionName);
    while (!timeout.hasTimedOut()) {
      try {
        QueryResponse response = client.query(new SolrQuery("*:*").setRows(0));
        long actualCount = response.getResults().getNumFound();
        if (actualCount == expectedCount) {
          log.info("Verified {}: {} documents", description, actualCount);
          return;
        }
        Thread.sleep(100);
      } catch (Exception e) {
        // Cluster might be temporarily unavailable during recovery
        Thread.sleep(500);
      }
    }
    // Final check after timeout
    try {
      QueryResponse response = client.query(new SolrQuery("*:*").setRows(0));
      long actualCount = response.getResults().getNumFound();
      throw new TimeoutException(
          "Document count mismatch for: "
              + description
              + ". Expected "
              + expectedCount
              + " but got "
              + actualCount);
    } catch (Exception e) {
      throw new TimeoutException(
          "Document count check failed for: "
              + description
              + ". Expected "
              + expectedCount
              + " but query failed: "
              + e.getMessage());
    }
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
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(
      String name,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class<? extends Filter>, String> extraRequestFilters,
      SSLConfig sslConfig)
      throws Exception {
    return startJettySolrRunner(
        name,
        JettyConfig.builder()
            .withServlets(extraServlets)
            .withFilters(extraRequestFilters)
            .withSSLConfig(sslConfig)
            .build(),
        null);
  }

  public JettySolrRunner getJettySolrRunner(int index) {
    return jettys.get(index);
  }

  /**
   * Start a new Solr instance on a particular servlet context
   *
   * @param name the instance name
   * @param config a JettyConfig for the instance's {@link org.apache.solr.embedded.JettySolrRunner}
   * @param solrXml the string content of the solr.xml file to use, or null to just use the
   *     cluster's default
   * @return a JettySolrRunner
   */
  public JettySolrRunner startJettySolrRunner(String name, JettyConfig config, String solrXml)
      throws Exception {
    final Properties nodeProps = new Properties();
    nodeProps.setProperty("zkHost", getZkAddress());

    Path runnerPath = createInstancePath(name);
    if (solrXml == null) {
      solrXml = this.solrXml;
    }
    Files.write(runnerPath.resolve("solr.xml"), solrXml.getBytes(StandardCharsets.UTF_8));
    JettyConfig newConfig = JettyConfig.builder(config).build();
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
    return startJettySolrRunner(newNodeName(), jettyConfig, null);
  }

  /**
   * Add a previously stopped node back to the cluster on a different port
   *
   * @param jetty a {@link JettySolrRunner} previously returned by {@link #stopJettySolrRunner(int)}
   * @return the started node
   * @throws Exception on error
   */
  public JettySolrRunner startJettySolrRunner(JettySolrRunner jetty) throws Exception {
    return startJettySolrRunner(jetty, false);
  }

  /**
   * Add a previously stopped node back to the cluster
   *
   * @param jetty a {@link JettySolrRunner} previously returned by {@link #stopJettySolrRunner(int)}
   * @param reusePort the port previously used by jetty
   * @return the started node
   * @throws Exception on error
   */
  public JettySolrRunner startJettySolrRunner(JettySolrRunner jetty, boolean reusePort)
      throws Exception {
    jetty.start(reusePort);
    if (!jettys.contains(jetty)) jettys.add(jetty);
    return jetty;
  }

  /**
   * Stop a Solr instance
   *
   * @param index the index of node in collection returned by {@link #getJettySolrRunners()}
   * @return the now shut down node
   */
  public JettySolrRunner stopJettySolrRunner(int index) throws Exception {
    JettySolrRunner jetty = jettys.get(index);
    jetty.stop();
    jettys.remove(index);
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
            .withUrl(getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      new ZkConfigSetService(zkClient).uploadConfig(configName, configDir);
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

      reader.createClusterStateWatchersAndUpdate(); // up-to-date aliases & collections
      reader.aliasesManager.applyModificationAndExportToZk(aliases -> Aliases.EMPTY);
      for (String collection : reader.getClusterState().getCollectionNames()) {
        CollectionAdminRequest.deleteCollection(collection).process(solrClient);
      }

      boolean success = latch.await(60, TimeUnit.SECONDS);
      if (!success) {
        throw new IllegalStateException(
            "Still waiting to see all collections removed from clusterstate.");
      }

      for (String collection : reader.getClusterState().getCollectionNames()) {
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
                ZkConfigSetService.CONFIGS_ZKNODE + "/" + configSet + "/" + DEFAULT_FILENAME, -1);
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
      // Only shut down zkServer if it exists (not null) and we created it (!externalZkServer)
      // In quorum mode, zkServer is null and each node's embedded ZK is shut down with the node
      if (!externalZkServer && zkServer != null) {
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
                      Collections.singletonList(getZkAddress()), Optional.empty())
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
  public void zkSetData(String path, byte[] data) throws InterruptedException {
    try {
      getZkClient().setData(path, data, -1);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.UNKNOWN, "Failed writing to Zookeeper", e);
    }
  }

  protected CloudSolrClient buildSolrClient() {
    return new CloudLegacySolrClient.Builder(
            Collections.singletonList(getZkAddress()), Optional.empty())
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
            Collections.singletonList(getZkAddress()), Optional.empty())
        .withSocketTimeout(90000) // we choose 90 because we run in some harsh envs
        .withConnectionTimeout(15000);
  }

  private Exception checkForExceptions(String message, Collection<Future<JettySolrRunner>> futures)
      throws InterruptedException {
    Exception parsed = new Exception(message);
    boolean ok = true;
    for (Future<JettySolrRunner> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        log.error(message, e);
        parsed.addSuppressed(e.getCause());
        ok = false;
      } catch (InterruptedException e) {
        log.error(message, e);
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
    ChaosMonkey.expireSession(jetty, zkServer);
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
      return activeReplicas == expectedReplicas;
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

  public void dumpMetrics(Path outputDirectory, String fileName) throws IOException {
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
    protected Handler.Wrapper injectJettyHandlers(Handler.Wrapper chain) {
      metricRegistry = new MetricRegistry();
      InstrumentedEE10Handler metrics = new InstrumentedEE10Handler(metricRegistry);
      metrics.setHandler(chain);
      return metrics;
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
    private boolean overseerEnabled =
        EnvUtils.getPropertyAsBool("solr.cloud.overseer.enabled", true);
    private boolean formatZkServer = true;
    private boolean disableTraceIdGeneration = false;
    private boolean useEmbeddedZkQuorum = false;

    /**
     * Create a builder
     *
     * @param nodeCount the number of nodes in the cluster
     * @param baseDir a base directory for the cluster
     */
    public Builder(int nodeCount, Path baseDir) {
      this.nodeCount = nodeCount;
      this.baseDir = baseDir;

      jettyConfigBuilder = JettyConfig.builder();
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
     * Force the cluster Collection and config state API execution as well as the cluster state
     * update strategy to be either Overseer based or distributed. <b>This method can be useful when
     * debugging tests</b> failing in only one of the two modes to have all local runs exhibit the
     * issue, as well obviously for tests that are not compatible with one of the two modes.
     * Alternatively, a system property can be used in lieu of this method.
     *
     * <p>If this method is not called nor set via system property, the strategy being used will
     * default to Overseer mode (overseerEnabled=true). However, note {@link SolrCloudTestCase}
     * (above this) randomly chooses the mode.
     *
     * <p>For tests that need to explicitly test distributed vs Overseer behavior, use this method
     * to control which mode is used. The cluster property 'overseerEnabled' will be set
     * accordingly.
     *
     * @param overseerEnabled When {@code false}, Collection and Config Set API commands are
     *     executed in a distributed way by nodes. When {@code true}, they are executed by Overseer.
     */
    public Builder withOverseer(boolean overseerEnabled) {
      this.overseerEnabled = overseerEnabled;
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
     * Configure cluster to use embedded ZooKeeper quorum mode where each Solr node runs its own
     * ZooKeeper server.
     *
     * <p>When enabled, instead of using a separate {@link ZkTestServer}, each Solr node will run an
     * embedded ZooKeeper server, and together they form a quorum. This tests the embedded ZK quorum
     * functionality.
     *
     * <p>Requires at least 3 nodes for a valid quorum.
     *
     * @return this Builder
     */
    public Builder withEmbeddedZkQuorum() {
      if (nodeCount < 3) {
        throw new IllegalArgumentException(
            "ZooKeeper quorum requires at least 3 nodes, got: " + nodeCount);
      }
      this.useEmbeddedZkQuorum = true;
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
      System.setProperty("solr.cloud.overseer.enabled", Boolean.toString(overseerEnabled));

      if (!disableTraceIdGeneration && OpenTelemetryConfigurator.TRACE_ID_GEN_ENABLED) {
        OpenTelemetryConfigurator.initializeOpenTelemetrySdk(null, null);
        injectRandomRecordingFlag();
      }

      JettyConfig jettyConfig = jettyConfigBuilder.build();
      MiniSolrCloudCluster cluster;

      if (useEmbeddedZkQuorum) {
        // Use embedded ZK quorum mode constructor
        cluster =
            new MiniSolrCloudCluster(
                nodeCount,
                baseDir,
                solrXml,
                jettyConfig,
                securityJson,
                trackJettyMetrics,
                true); // useEmbeddedZkQuorum = true
      } else {
        // Use standard constructor with ZkTestServer
        cluster =
            new MiniSolrCloudCluster(
                nodeCount,
                baseDir,
                solrXml,
                jettyConfig,
                null,
                securityJson,
                trackJettyMetrics,
                formatZkServer);
      }

      for (Config config : configs) {
        cluster.uploadConfigSet(config.path, config.name);
      }

      // TODO process BEFORE nodes start up!  Some props are only read on node start.
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
