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
package org.apache.solr.core;

import static org.apache.solr.common.cloud.ZkStateReader.HTTPS;
import static org.apache.solr.common.cloud.ZkStateReader.HTTPS_PORT_PROP;

import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.servlet.CoreContainerProvider;
import org.apache.solr.util.AddressUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link CoreContainer} to hold ZooKeeper / SolrCloud info, especially {@link
 * ZkController}. Mainly it does some ZK initialization, and ensures a loading core registers in ZK.
 * Even when in standalone mode, perhaps surprisingly, an instance of this class exists. If {@link
 * #getZkController()} returns null then we're in standalone mode.
 */
public class ZkContainer {
  // NOTE DWS: It's debatable if this in-between class is needed instead of folding it all into
  // ZkController. ZKC is huge though.

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_WHITELIST_PROPERTY = "zookeeper.4lw.commands.whitelist";
  public static final String ZK_MAX_CNXNS_PROPERTY = "zookeeper.maxCnxns";
  // Per ZooKeeper, "0" means no limit for max client connections.
  public static final String ZK_MAX_CNXNS_DEFAULT = "0";

  protected ZkController zkController;

  private volatile ZooKeeperServerEmbedded zkServerEmbedded;

  private ExecutorService coreZkRegister =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("coreZkRegister"));

  private SolrMetricProducer metricProducer;

  public ZkContainer() {}

  public void initZooKeeper(final CoreContainer cc, CloudConfig config) {
    // zkServerEnabled is set whenever in solrCloud mode ('-c') but no explicit zkHost/ZK_HOST is
    // provided.
    final boolean zkServerEnabled =
        EnvUtils.getPropertyAsBool("solr.zookeeper.server.enabled", false);
    boolean zkQuorumNode = false;
    if (NodeRoles.MODE_ON.equals(cc.nodeRoles.getRoleMode(NodeRoles.Role.ZOOKEEPER_QUORUM))) {
      zkQuorumNode = true;
      log.info("Starting node in ZooKeeper Quorum role.");
    }

    if (zkServerEnabled && config == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Cannot start Solr in cloud mode - no cloud config provided");
    }

    if (config == null) {
      log.info("Solr is running in standalone mode");
      return;
    }

    final boolean runAsQuorum = config.getZkHost() != null && zkQuorumNode;

    String zookeeperHost = config.getZkHost();
    final var solrHome = cc.getSolrHome();
    // runAsQuorum takes precedence: the zookeeper_quorum node role alone is sufficient to start
    // embedded ZK in quorum mode — no need to also set solr.zookeeper.server.enabled=true.
    // zkServerEnabled is only used for legacy standalone embedded ZK (bin/solr -c without ZK_HOST).
    if (runAsQuorum) {
      // ZooKeeperServerEmbedded being used under the covers.
      // The zookeeper_quorum node role is sufficient — no extra sysprop needed.
      // The data directory defaults to zoo_home under solrHome but can be overridden via
      // solr.zookeeper.server.datadir (or ZK_SERVER_DATA_DIR env var). When running multiple
      // quorum nodes on the same machine sharing a solrHome, set this property to a distinct
      // path per node to avoid directory collisions.
      final int zkPort = config.getSolrHostPort() + 1000;
      final var zkHomeDir =
          Path.of(
              EnvUtils.getProperty(
                  "solr.zookeeper.server.datadir", solrHome.resolve("zoo_home").toString()));
      final var zkDataDir = zkHomeDir.resolve("data");

      // Populate a zoo.cfg
      final String zooCfgTemplate =
          """
              tickTime=2000
              initLimit=10
              syncLimit=5
              dataDir=@@DATA_DIR@@
              4lw.commands.whitelist=mntr,conf,ruok
              admin.enableServer=false
              clientPort=@@ZK_CLIENT_PORT@@
              """;
      String zooCfgContents =
          zooCfgTemplate
              .replace("@@DATA_DIR@@", zkDataDir.toString())
              .replace("@@ZK_CLIENT_PORT@@", String.valueOf(zkPort));
      final String[] zkHosts = config.getZkHost().split(",");
      int myId = -1;
      // TODO: myId detection uses exact string matching between config.getHost() and the host
      // portion of zkHost entries. This fails when zkHost uses "localhost" but Solr is
      // configured with "127.0.0.1" (or vice versa). Consider resolving hostnames/IPs before
      // matching, or matching by port alone (which is unique per node in a single-machine setup).
      final String targetConnStringSection = config.getHost() + ":" + zkPort;
      if (log.isInfoEnabled()) {
        log.info(
            "Trying to match {} against zkHostString {} to determine myid",
            targetConnStringSection,
            config.getZkHost());
      }
      for (int i = 0; i < zkHosts.length; i++) {
        final String host = zkHosts[i];
        if (targetConnStringSection.equals(zkHosts[i])) {
          myId = (i + 1);
        }
        final var hostComponents = host.split(":");
        if (hostComponents.length < 2) {
          throw new IllegalStateException(
              "Invalid zkHost entry (expected 'host:port'): '" + host + "'");
        }
        final var zkServer = hostComponents[0];
        final int zkClientPort;
        try {
          zkClientPort = Integer.parseInt(hostComponents[1]);
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "Invalid port in zkHost entry '" + host + "': " + hostComponents[1], e);
        }
        final var zkQuorumPort = zkClientPort + 1;
        final var zkLeaderPort = zkClientPort + 2;
        final String configEntry =
            "server." + (i + 1) + "=" + zkServer + ":" + zkQuorumPort + ":" + zkLeaderPort + "\n";
        zooCfgContents = zooCfgContents + configEntry;
      }

      if (myId == -1) {
        throw new IllegalStateException(
            "Unable to determine ZK 'myid' for target " + targetConnStringSection);
      }

      try {
        Files.createDirectories(zkHomeDir);
        Files.createDirectories(zkDataDir);
        Files.writeString(zkDataDir.resolve("myid"), String.valueOf(myId));
        Properties zkProps = new Properties();
        zkProps.load(new StringReader(zooCfgContents));
        startZooKeeperServerEmbedded(zkPort, zkHomeDir, zkProps);
      } catch (Exception e) {
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR,
            "IOException bootstrapping zk quorum instance",
            e);
      }
    } else if (zkServerEnabled) {
      try {
        final Path zkDataHome =
            Path.of(
                EnvUtils.getProperty(
                    "solr.zookeeper.server.datadir", solrHome.resolve("zoo_data").toString()));
        final Path zkConfHome =
            Path.of(EnvUtils.getProperty("solr.zookeeper.server.confdir", solrHome.toString()));

        Path zooCfgPath = zkConfHome.resolve("zoo.cfg");
        if (!Files.exists(zooCfgPath)) {
          log.info("Zookeeper configuration not found in {}, using built-in default", zkConfHome);
          String solrInstallDir = System.getProperty(CoreContainerProvider.SOLR_INSTALL_DIR);
          if (solrInstallDir == null) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Could not find default zoo.cfg file due to missing "
                    + CoreContainerProvider.SOLR_INSTALL_DIR);
          }
          zooCfgPath = Path.of(solrInstallDir).resolve("server").resolve("solr").resolve("zoo.cfg");
        }

        final Properties zkProps = new Properties();
        try (Reader reader = Files.newBufferedReader(zooCfgPath)) {
          zkProps.load(reader);
        }
        if (zkProps.getProperty("dataDir") == null) {
          zkProps.setProperty("dataDir", zkDataHome.toString());
        }
        final String clientPortAddress =
            EnvUtils.getProperty("solr.zookeeper.embedded.host", "127.0.0.1");
        zkProps.setProperty("clientPortAddress", clientPortAddress);
        if (zkProps.getProperty("clientPort") == null) {
          zkProps.setProperty("clientPort", Integer.toString(config.getSolrHostPort() + 1000));
        }
        if (System.getProperty(ZK_WHITELIST_PROPERTY) == null) {
          System.setProperty(ZK_WHITELIST_PROPERTY, "ruok, mntr, conf");
        }

        Files.createDirectories(zkDataHome);
        final int zkPort = Integer.parseInt(zkProps.getProperty("clientPort"));
        startZooKeeperServerEmbedded(zkPort, zkDataHome, zkProps);

        if (zookeeperHost == null) {
          // We cannot advertise 0.0.0.0, so choose the best host to advertise
          // (the same that the Solr Node defaults to)
          final String hostName =
              "0.0.0.0".equals(clientPortAddress)
                  ? AddressUtils.getHostToAdvertise()
                  : clientPortAddress;
          zookeeperHost = hostName + ":" + zkPort;
        }
      } catch (Exception e) {
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Exception starting embedded zookeeper server",
            e);
      }
    }

    int zkClientConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;

    if (zookeeperHost != null) {

      // we are ZooKeeper enabled
      try {
        // If this is an ensemble, allow for a long connect time for other servers to come up
        if (runAsQuorum) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000; // 1 day for embedded quorum
          log.info("Zookeeper client={} (quorum mode)  Waiting for a quorum.", zookeeperHost);
        } else {
          log.info("Zookeeper client={}", zookeeperHost);
        }
        boolean createRoot = EnvUtils.getPropertyAsBool("solr.zookeeper.chroot.create", false);

        if (!ZkController.checkChrootPath(zookeeperHost, createRoot)) {
          throw new ZooKeeperException(
              SolrException.ErrorCode.SERVER_ERROR,
              "A chroot was specified in ZkHost but the znode doesn't exist. " + zookeeperHost);
        }

        ZkController zkController =
            new ZkController(cc, zookeeperHost, zkClientConnectTimeout, config);

        if (zkServerEnabled || runAsQuorum) {
          if (StrUtils.isNotNullOrEmpty(System.getProperty(HTTPS_PORT_PROP))) {
            // Embedded ZK and probably running with SSL
            new ClusterProperties(zkController.getZkClient())
                .setClusterProperty(ZkStateReader.URL_SCHEME, HTTPS);
          }
        }

        this.zkController = zkController;

        metricProducer =
            new SolrMetricProducer() {
              SolrMetricsContext ctx;

              @Override
              public void initializeMetrics(
                  SolrMetricsContext parentContext, Attributes attributes) {
                ctx = parentContext.getChildContext(this);

                var metricsListener = zkController.getZkClient().getMetrics();

                ctx.observableLongCounter(
                    "solr_zk_ops",
                    "Total number of ZooKeeper operations",
                    measurement -> {
                      measurement.record(
                          metricsListener.getReads(),
                          attributes.toBuilder().put(OPERATION_ATTR, "read").build());
                      measurement.record(
                          metricsListener.getDeletes(),
                          attributes.toBuilder().put(OPERATION_ATTR, "delete").build());
                      measurement.record(
                          metricsListener.getWrites(),
                          attributes.toBuilder().put(OPERATION_ATTR, "write").build());
                      measurement.record(
                          metricsListener.getMultiOps(),
                          attributes.toBuilder().put(OPERATION_ATTR, "multi").build());
                      measurement.record(
                          metricsListener.getExistsChecks(),
                          attributes.toBuilder().put(OPERATION_ATTR, "exists").build());
                    });

                ctx.observableLongCounter(
                    "solr_zk_read",
                    "Total bytes read from ZooKeeper",
                    measurement -> {
                      measurement.record(metricsListener.getBytesRead(), attributes);
                    },
                    OtelUnit.BYTES);

                ctx.observableLongCounter(
                    "solr_zk_watches_fired",
                    "Total number of ZooKeeper watches fired",
                    measurement -> {
                      measurement.record(metricsListener.getWatchesFired(), attributes);
                    });

                ctx.observableLongCounter(
                    "solr_zk_written",
                    "Total bytes written to ZooKeeper",
                    measurement -> {
                      measurement.record(metricsListener.getBytesWritten(), attributes);
                    },
                    OtelUnit.BYTES);

                ctx.observableLongCounter(
                    "solr_zk_cumulative_multi_ops_total",
                    "Total cumulative multi-operations count",
                    measurement -> {
                      measurement.record(metricsListener.getCumulativeMultiOps(), attributes);
                    });

                ctx.observableLongCounter(
                    "solr_zk_child_fetches",
                    "Total number of ZooKeeper child node fetches",
                    measurement -> {
                      measurement.record(metricsListener.getChildFetches(), attributes);
                    });

                ctx.observableLongCounter(
                    "solr_zk_cumulative_children_fetched",
                    "Total cumulative children fetched count",
                    measurement -> {
                      measurement.record(
                          metricsListener.getCumulativeChildrenFetched(), attributes);
                    });
              }

              @Override
              public SolrMetricsContext getSolrMetricsContext() {
                return ctx;
              }
            };
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (TimeoutException e) {
        log.error("Could not connect to ZooKeeper", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (IOException | KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
    }
  }

  private void startZooKeeperServerEmbedded(int port, Path zkHomeDir, Properties p)
      throws Exception {
    ensureZkMaxCnxnsConfigured();
    zkServerEmbedded =
        ZooKeeperServerEmbedded.builder().baseDir(zkHomeDir).configuration(p).build();
    zkServerEmbedded.start();
    log.info("Started embedded ZooKeeper server on port {}", port);
  }

  public static void ensureZkMaxCnxnsConfigured() {
    System.getProperties().putIfAbsent(ZK_MAX_CNXNS_PROPERTY, ZK_MAX_CNXNS_DEFAULT);
  }

  public static volatile Predicate<CoreDescriptor> testing_beforeRegisterInZk;

  public void registerInZk(final SolrCore core, boolean background, boolean skipRecovery) {
    if (zkController == null) {
      return;
    }

    CoreDescriptor cd = core.getCoreDescriptor(); // save this here - the core may not have it later
    Runnable r =
        () -> {
          MDCLoggingContext.setCore(core);
          try {
            try {
              if (testing_beforeRegisterInZk != null) {
                boolean didTrigger = testing_beforeRegisterInZk.test(cd);
                if (log.isDebugEnabled()) {
                  log.debug("{} pre-zk hook", (didTrigger ? "Ran" : "Skipped"));
                }
              }
              if (!core.getCoreContainer().isShutDown()) {
                zkController.register(core.getName(), cd, skipRecovery);
              }
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("Interrupted", e);
            } catch (KeeperException e) {
              log.error("KeeperException registering core {}", core.getName(), e);
            } catch (IllegalStateException ignore) {

            } catch (Exception e) {
              log.error("Exception registering core {}", core.getName(), e);
              try {
                zkController.publish(cd, Replica.State.DOWN);
              } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                log.error("Interrupted", e1);
              } catch (Exception e1) {
                log.error("Exception publishing down state for core {}", core.getName(), e1);
              }
            }
          } finally {
            MDCLoggingContext.clear();
          }
        };

    if (background) {
      coreZkRegister.execute(r);
    } else {
      r.run();
    }
  }

  public ZkController getZkController() {
    return zkController;
  }

  public void close() {

    try {
      ExecutorUtil.shutdownAndAwaitTermination(coreZkRegister);
    } finally {
      try {
        if (zkController != null) {
          zkController.close();
        }
      } finally {
        if (zkServerEmbedded != null) {
          try {
            zkServerEmbedded.close();
            // ZooKeeperServerEmbedded.close() is asynchronous: ZK's QuorumCnxManager
            // WorkerSender/WorkerReceiver threads may still be running. Wait up to 5s.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
              boolean quorumThreadsRunning =
                  Thread.getAllStackTraces().keySet().stream()
                      .anyMatch(
                          t ->
                              t.getName().startsWith("WorkerSender")
                                  || t.getName().startsWith("WorkerReceiver"));
              if (!quorumThreadsRunning) break;
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
              }
            }
            log.info("Closed embedded ZooKeeper server");
          } catch (Exception e) {
            log.error("Error closing embedded ZooKeeper server", e);
          }
        }
      }
      IOUtils.closeQuietly(metricProducer);
    }
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return coreZkRegister;
  }

  public SolrMetricProducer getZkMetricsProducer() {
    return this.metricProducer;
  }
}
