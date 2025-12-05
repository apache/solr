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
package org.apache.solr.update;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.Attributes;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.SolrHttpConstants;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateShardHandler implements SolrInfoBean {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * A downside to configuring an upper bound will be big update reorders (when that upper bound is hit)
   * and then undetected shard inconsistency as a result.
   * Therefore this thread pool is left unbounded. See SOLR-8205
   */
  private ExecutorService updateExecutor =
      new ExecutorUtil.MDCAwareThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          60L,
          TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          new SolrNamedThreadFactory("updateExecutor"),
          // the Runnable added to this executor handles all exceptions so we disable stack trace
          // collection as an optimization
          // see SOLR-11880 for more details
          false);

  private ExecutorService recoveryExecutor;

  private final HttpJettySolrClient updateOnlyClient;

  private final HttpJettySolrClient recoveryOnlyClient;

  private final InstrumentedHttpListenerFactory trackHttpSolrMetrics;

  private SolrMetricsContext solrMetricsContext;

  private int socketTimeout = SolrHttpConstants.DEFAULT_SO_TIMEOUT;
  private int connectionTimeout = SolrHttpConstants.DEFAULT_CONNECT_TIMEOUT;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {

    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null) {
      clientParams.set(SolrHttpConstants.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
      clientParams.set(
          SolrHttpConstants.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
      // following is done only for logging complete configuration.
      // The maxConnections and maxConnectionsPerHost have already been specified on the connection
      // manager
      clientParams.set(SolrHttpConstants.PROP_MAX_CONNECTIONS, cfg.getMaxUpdateConnections());
      clientParams.set(
          SolrHttpConstants.PROP_MAX_CONNECTIONS_PER_HOST, cfg.getMaxUpdateConnectionsPerHost());
      socketTimeout = cfg.getDistributedSocketTimeout();
      connectionTimeout = cfg.getDistributedConnectionTimeout();
    }
    log.debug("Created default UpdateShardHandler HTTP client with params: {}", clientParams);

    trackHttpSolrMetrics = new InstrumentedHttpListenerFactory(getNameStrategy(cfg));

    Set<String> urlParamNames =
        Set.of(
            DistributedUpdateProcessor.DISTRIB_FROM,
            DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    var updateOnlyClientBuilder = new HttpJettySolrClient.Builder();
    var recoveryOnlyClientBuilder = new HttpJettySolrClient.Builder();
    if (cfg != null) {
      updateOnlyClientBuilder
          .addListenerFactory(trackHttpSolrMetrics)
          .withConnectionTimeout(cfg.getDistributedConnectionTimeout(), TimeUnit.MILLISECONDS)
          .withIdleTimeout(cfg.getDistributedSocketTimeout(), TimeUnit.MILLISECONDS)
          .withMaxConnectionsPerHost(cfg.getMaxUpdateConnectionsPerHost());
      recoveryOnlyClientBuilder
          .addListenerFactory(trackHttpSolrMetrics)
          .withConnectionTimeout(cfg.getDistributedConnectionTimeout(), TimeUnit.MILLISECONDS)
          .withIdleTimeout(cfg.getDistributedSocketTimeout(), TimeUnit.MILLISECONDS)
          .withRequestTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
          .withMaxConnectionsPerHost(cfg.getMaxUpdateConnectionsPerHost());
    }

    updateOnlyClientBuilder.withTheseParamNamesInTheUrl(urlParamNames);
    updateOnlyClient = updateOnlyClientBuilder.build();

    recoveryOnlyClient = recoveryOnlyClientBuilder.build();

    ThreadFactory recoveryThreadFactory = new SolrNamedThreadFactory("recoveryExecutor");
    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
      if (log.isDebugEnabled()) {
        log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
      }
      recoveryExecutor =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              cfg.getMaxRecoveryThreads(), recoveryThreadFactory);
    } else {
      log.debug("Creating recoveryExecutor with unbounded pool");
      recoveryExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(recoveryThreadFactory);
    }
  }

  private InstrumentedHttpListenerFactory.NameStrategy getNameStrategy(
      UpdateShardHandlerConfig cfg) {
    InstrumentedHttpListenerFactory.NameStrategy nameStrategy =
        InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(
            UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);

    if (cfg != null) {
      nameStrategy =
          InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(
              cfg.getMetricNameStrategy());
      if (nameStrategy == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: "
                + cfg.getMetricNameStrategy()
                + " found. Must be one of: "
                + InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return nameStrategy;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    solrMetricsContext = parentContext.getChildContext(this);
    trackHttpSolrMetrics.initializeMetrics(solrMetricsContext, Attributes.empty());
    updateExecutor =
        solrMetricsContext.instrumentedExecutorService(
            updateExecutor, "solr_core_executor", "updateOnlyExecutor", getCategory());
    recoveryExecutor =
        solrMetricsContext.instrumentedExecutorService(
            recoveryExecutor, "solr_core_executor", "recoveryExecutor", getCategory());
  }

  @Override
  public String getDescription() {
    return "Metrics tracked by UpdateShardHandler related to distributed updates and recovery";
  }

  @Override
  public Category getCategory() {
    return Category.UPDATE;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  // don't introduce a bug, this client is for sending updates only!
  public HttpJettySolrClient getUpdateOnlyHttpClient() {
    return updateOnlyClient;
  }

  // don't introduce a bug, this client is for recovery ops only!
  public HttpJettySolrClient getRecoveryOnlyHttpClient() {
    return recoveryOnlyClient;
  }

  /**
   * This method returns an executor that is meant for non search related tasks.
   *
   * @return an executor for update side related activities.
   */
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }

  /**
   * @return executor for recovery operations
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  @Override
  public void close() {
    try {
      // do not interrupt, do not interrupt
      ExecutorUtil.shutdownAndAwaitTermination(updateExecutor);
      ExecutorUtil.shutdownAndAwaitTermination(recoveryExecutor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        SolrInfoBean.super.close();
      } catch (Exception e) {
        // do nothing
      }
      IOUtils.closeQuietly(updateOnlyClient);
      IOUtils.closeQuietly(recoveryOnlyClient);
    }
  }

  @VisibleForTesting
  public int getSocketTimeout() {
    return socketTimeout;
  }

  @VisibleForTesting
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setSecurityBuilder(HttpClientBuilderPlugin builder) {
    builder.setup(updateOnlyClient);
    builder.setup(recoveryOnlyClient);
  }
}
