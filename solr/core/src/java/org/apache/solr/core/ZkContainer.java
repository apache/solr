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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
  private SolrZkServer zkServer;

  private ExecutorService coreZkRegister =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("coreZkRegister"));

  private SolrMetricProducer metricProducer;

  private List<AutoCloseable> toClose;

  public ZkContainer() {}

  public void initZooKeeper(final CoreContainer cc, CloudConfig config) {
    boolean zkRun = EnvUtils.getPropertyAsBool("solr.zookeeper.server.enabled", false);

    if (zkRun && config == null)
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Cannot start Solr in cloud mode - no cloud config provided");

    if (config == null) return; // not in zk mode

    String zookeeperHost = config.getZkHost();

    // zookeeper in quorum mode currently causes a failure when trying to
    // register log4j mbeans.  See SOLR-2369
    // TODO: remove after updating to an slf4j based zookeeper
    System.setProperty("zookeeper.jmx.log4j.disable", "true");

    Path solrHome = cc.getSolrHome();
    if (zkRun) {
      String zkDataHome =
          EnvUtils.getProperty(
              "solr.zookeeper.server.datadir", solrHome.resolve("zoo_data").toString());
      String zkConfHome =
          EnvUtils.getProperty("solr.zookeeper.server.confdir", solrHome.toString());
      zkServer =
          new SolrZkServer(
              zkRun,
              stripChroot(config.getZkHost()),
              Path.of(zkDataHome),
              zkConfHome,
              config.getSolrHostPort());
      zkServer.parseConfig();
      zkServer.start();

      // set client from server config if not already set
      if (zookeeperHost == null) {
        zookeeperHost = zkServer.getClientString();
      }
    }

    int zkClientConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;

    if (zookeeperHost != null) {

      // we are ZooKeeper enabled
      try {
        // If this is an ensemble, allow for a long connect time for other servers to come up
        if (zkRun && zkServer.getServers().size() > 1) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000; // 1 day for embedded ensemble
          log.info("Zookeeper client={}  Waiting for a quorum.", zookeeperHost);
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

        if (zkRun) {
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

  private String stripChroot(String zkRun) {
    if (zkRun == null || zkRun.trim().isEmpty() || zkRun.lastIndexOf('/') < 0) return zkRun;
    return zkRun.substring(0, zkRun.lastIndexOf('/'));
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
        if (zkServer != null) {
          zkServer.stop();
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
