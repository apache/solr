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
import java.io.FileReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.SolrZkServer;
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

  protected ZkController zkController;

  // zkServer (and SolrZkServer) wrap a ZooKeeperServerMain if standalone mode, but in quorum we
  // just use ZooKeeperServerEmbedded
  // directly!  Why?  Can we use ZooKeeperServerEmbedded in one node directly instead?
  private SolrZkServer zkServer;
  private ZooKeeperServerEmbedded zkServerEmbedded;

  private ExecutorService coreZkRegister =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("coreZkRegister"));

  private SolrMetricProducer metricProducer;

  private List<AutoCloseable> toClose;

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
    if (zkServerEnabled) {
      if (!runAsQuorum) {
        // Old school ZooKeeperServerMain being used under the covers.
        zkServer =
            SolrZkServer.createAndStart(config.getZkHost(), solrHome, config.getSolrHostPort());

        // set client from server config if not already set
        if (zookeeperHost == null) {
          zookeeperHost = zkServer.getClientString();
        }
      } else {
        // ZooKeeperServerEmbedded being used under the covers.
        // Figure out where to put zoo-data
        final var zkHomeDir = solrHome.resolve("zoo_home");
        final var zkDataDir = zkHomeDir.resolve("data");

        // Populate a zoo.cfg
        final String zooCfgTemplate =
            ""
                + "tickTime=2000\n"
                + "initLimit=10\n"
                + "syncLimit=5\n"
                + "dataDir=@@DATA_DIR@@\n"
                + "4lw.commands.whitelist=mntr,conf,ruok\n"
                + "admin.enableServer=false\n"
                + "clientPort=@@ZK_CLIENT_PORT@@\n";

        final int zkPort = config.getSolrHostPort() + 1000;
        String zooCfgContents =
            zooCfgTemplate
                .replace("@@DATA_DIR@@", zkDataDir.toString())
                .replace("@@ZK_CLIENT_PORT@@", String.valueOf(zkPort));
        final String[] zkHosts = config.getZkHost().split(",");
        int myId = -1;
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
          final var zkServer = hostComponents[0];
          final var zkClientPort = Integer.valueOf(hostComponents[1]);
          final var zkQuorumPort = zkClientPort - 4000;
          final var zkLeaderPort = zkClientPort - 3000;
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
          Files.writeString(zkHomeDir.resolve("zoo.cfg"), zooCfgContents);
          Files.createDirectories(zkDataDir);
          Files.writeString(zkDataDir.resolve("myid"), String.valueOf(myId));
          // Run ZKSE
          startZooKeeperServerEmbedded(zkPort, zkHomeDir.toString());
        } catch (Exception e) {
          throw new ZooKeeperException(
              SolrException.ErrorCode.SERVER_ERROR,
              "IOException bootstrapping zk quorum instance",
              e);
        }
      }
    }

    int zkClientConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;

    if (zookeeperHost != null) {

      // we are ZooKeeper enabled
      try {
        // If this is an ensemble, allow for a long connect time for other servers to come up
        if (zkServerEnabled && zkServer != null && zkServer.getServers().size() > 1) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000; // 1 day for embedded ensemble
          log.info("Zookeeper client={}  Waiting for a quorum.", zookeeperHost);
        } else if (zkServerEnabled && runAsQuorum) {
          // Quorum mode also needs long timeout for other nodes to start
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

        if (zkServerEnabled) {
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
                final List<AutoCloseable> observables = new ArrayList<>();
                ctx = parentContext.getChildContext(this);

                var metricsListener = zkController.getZkClient().getMetrics();

                observables.add(
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
                        }));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_read",
                        "Total bytes read from ZooKeeper",
                        measurement -> {
                          measurement.record(metricsListener.getBytesRead(), attributes);
                        },
                        OtelUnit.BYTES));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_watches_fired",
                        "Total number of ZooKeeper watches fired",
                        measurement -> {
                          measurement.record(metricsListener.getWatchesFired(), attributes);
                        }));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_written",
                        "Total bytes written to ZooKeeper",
                        measurement -> {
                          measurement.record(metricsListener.getBytesWritten(), attributes);
                        },
                        OtelUnit.BYTES));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_cumulative_multi_ops_total",
                        "Total cumulative multi-operations count",
                        measurement -> {
                          measurement.record(metricsListener.getCumulativeMultiOps(), attributes);
                        }));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_child_fetches",
                        "Total number of ZooKeeper child node fetches",
                        measurement -> {
                          measurement.record(metricsListener.getChildFetches(), attributes);
                        }));

                observables.add(
                    ctx.observableLongCounter(
                        "solr_zk_cumulative_children_fetched",
                        "Total cumulative children fetched count",
                        measurement -> {
                          measurement.record(
                              metricsListener.getCumulativeChildrenFetched(), attributes);
                        }));
                toClose = Collections.unmodifiableList(observables);
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

  private void startZooKeeperServerEmbedded(int port, String zkHomeDir) throws Exception {
    Properties p = new Properties();
    try (FileReader fr = new FileReader(zkHomeDir + "/zoo.cfg", StandardCharsets.UTF_8)) {
      p.load(fr);
    }
    p.setProperty("clientPort", String.valueOf(port));

    zkServerEmbedded =
        ZooKeeperServerEmbedded.builder().baseDir(Path.of(zkHomeDir)).configuration(p).build();
    zkServerEmbedded.start();
    log.info("Started embedded ZooKeeper server in quorum mode on port {}", port);
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
        try {
          if (zkServer != null) {
            zkServer.stop();
          }
        } finally {
          if (zkServerEmbedded != null) {
            try {
              zkServerEmbedded.close();
              log.info("Closed embedded ZooKeeper server in quorum mode");
            } catch (Exception e) {
              log.error("Error closing embedded ZooKeeper server", e);
            }
          }
        }
      }
      IOUtils.closeQuietly(toClose);
    }
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return coreZkRegister;
  }

  public SolrMetricProducer getZkMetricsProducer() {
    return this.metricProducer;
  }
}
