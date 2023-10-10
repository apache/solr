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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.SolrZkServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
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

  // see ZkController.zkRunOnly
  private boolean zkRunOnly = Boolean.getBoolean("zkRunOnly"); // expert

  private SolrMetricProducer metricProducer;

  public ZkContainer() {}

  public void initZooKeeper(final CoreContainer cc, CloudConfig config) {
    String zkRun = System.getProperty("zkRun");

    if (zkRun != null && config == null)
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Cannot start Solr in cloud mode - no cloud config provided");

    if (config == null) return; // not in zk mode

    String zookeeperHost = config.getZkHost();

    // zookeeper in quorum mode currently causes a failure when trying to
    // register log4j mbeans.  See SOLR-2369
    // TODO: remove after updating to an slf4j based zookeeper
    System.setProperty("zookeeper.jmx.log4j.disable", "true");

    String solrHome = cc.getSolrHome();
    if (zkRun != null) {
      String zkDataHome =
          System.getProperty("zkServerDataDir", Paths.get(solrHome).resolve("zoo_data").toString());
      String zkConfHome = System.getProperty("zkServerConfDir", solrHome);
      zkServer =
          new SolrZkServer(
              stripChroot(zkRun),
              stripChroot(config.getZkHost()),
              new File(zkDataHome),
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
        if (zkRun != null && zkServer.getServers().size() > 1) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000; // 1 day for embedded ensemble
          log.info("Zookeeper client={}  Waiting for a quorum.", zookeeperHost);
        } else {
          log.info("Zookeeper client={}", zookeeperHost);
        }
        boolean createRoot = Boolean.getBoolean("createZkChroot");

        // We may have already loaded NodeConfig from zookeeper with same connect string, so no need
        // to recheck chroot
        boolean alreadyUsedChroot =
            (cc.getConfig().isFromZookeeper()
                && zookeeperHost.equals(cc.getConfig().getDefaultZkHost()));
        if (!alreadyUsedChroot
            && !ZkController.checkChrootPath(zookeeperHost, zkRunOnly || createRoot)) {
          throw new ZooKeeperException(
              SolrException.ErrorCode.SERVER_ERROR,
              "A chroot was specified in ZkHost but the znode doesn't exist. " + zookeeperHost);
        }

        Supplier<List<CoreDescriptor>> descriptorsSupplier =
            () ->
                cc.getCores().stream()
                    .map(SolrCore::getCoreDescriptor)
                    .collect(Collectors.toList());

        ZkController zkController =
            new ZkController(
                cc, zookeeperHost, zkClientConnectTimeout, config, descriptorsSupplier);

        if (zkRun != null) {
          if (StrUtils.isNotNullOrEmpty(System.getProperty(HTTPS_PORT_PROP))) {
            // Embedded ZK and probably running with SSL
            new ClusterProperties(zkController.getZkClient())
                .setClusterProperty(ZkStateReader.URL_SCHEME, HTTPS);
          }
        }

        this.zkController = zkController;
        MetricsMap metricsMap = new MetricsMap(zkController.getZkClient().getMetrics());
        metricProducer =
            new SolrMetricProducer() {
              SolrMetricsContext ctx;

              @Override
              public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
                ctx = parentContext.getChildContext(this);
                ctx.gauge(
                    metricsMap, true, scope, null, SolrInfoBean.Category.CONTAINER.toString());
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
    if (zkRun == null || zkRun.trim().length() == 0 || zkRun.lastIndexOf('/') < 0) return zkRun;
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
            } catch (AlreadyClosedException ignore) {

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
      if (zkController != null) {
        zkController.close();
      }
    } finally {
      try {
        if (zkServer != null) {
          zkServer.stop();
        }
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(coreZkRegister);
      }
    }
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return coreZkRegister;
  }

  public SolrMetricProducer getZkMetricsProducer() {
    return this.metricProducer;
  }
}
