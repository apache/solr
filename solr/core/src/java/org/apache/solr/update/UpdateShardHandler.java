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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

public class UpdateShardHandler implements SolrInfoBean {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloseTracker closeTracker;

  private final Http2SolrClient sharedHttpClient;

  private final Http2SolrClient solrCmdDistributorClient;

  private final Http2SolrClient searchOnlyClient;

  private final Http2SolrClient recoveryOnlyClient;

  private ExecutorService recoveryExecutor;

 // private final InstrumentedHttpRequestExecutor httpRequestExecutor;

  //private final InstrumentedHttpListenerFactory updateHttpListenerFactory;


  //private final Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;

  private int socketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
  private int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    assert ObjectReleaseTracker.getInstance().track(this);
    assert (closeTracker = new CloseTracker()) != null;
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null ) {
      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
      // following is done only for logging complete configuration.
      // The maxConnections and maxConnectionsPerHost have already been specified on the connection manager
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, cfg.getMaxUpdateConnections());
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, cfg.getMaxUpdateConnectionsPerHost());
      socketTimeout = cfg.getDistributedSocketTimeout();
      connectionTimeout = cfg.getDistributedConnectionTimeout();
    }
    if (log.isDebugEnabled()) {
      log.debug("Created default UpdateShardHandler HTTP client with params: {}", clientParams);
    }

   // httpRequestExecutor = new InstrumentedHttpRequestExecutor(getMetricNameStrategy(cfg));

    Http2SolrClient.Builder sharedClientBuilder = new Http2SolrClient.Builder();
    if (cfg != null) {
      sharedClientBuilder
          .connectionTimeout(cfg.getDistributedConnectionTimeout())
          .idleTimeout(cfg.getDistributedSocketTimeout());
    }
    sharedHttpClient = sharedClientBuilder.name("Update").markInternalRequest().strictEventOrdering(false).maxOutstandingAsyncRequests(-1).build();
    sharedHttpClient.enableCloseLock();
   // updateOnlyClient.addListenerFactory(updateHttpListenerFactory);
    Set<String> queryParams = new HashSet<>(2);
    queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
    queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    sharedHttpClient.setQueryParams(queryParams);

    solrCmdDistributorClient = sharedClientBuilder.name("SolrCmdDistributor").markInternalRequest().strictEventOrdering(false).maxOutstandingAsyncRequests(500).build();
    solrCmdDistributorClient.enableCloseLock();
    // updateOnlyClient.addListenerFactory(updateHttpListenerFactory);
    queryParams = new HashSet<>(2);
    queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
    queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    solrCmdDistributorClient.setQueryParams(queryParams);


    Http2SolrClient.Builder recoveryOnlyClientBuilder = new Http2SolrClient.Builder();
    // recoveryOnlyClient is used ONLY for recovery CONTROL requests (commit-on-leader, fetch
    // versions, leader lookup) -- the bulk index transfer goes through ReplicationHandler/IndexFetcher.
    // The idle timeout therefore bounds how long such a control request may block with no response.
    // It was 30s, which means a recovery whose leader DIES mid-request (crash / partition / a node
    // that stops behind a still-open proxy, leaving a half-open connection) hangs ~30s before the
    // recovery retry loop can re-resolve the new leader. There is no leader-change-driven
    // cancelRecovery on followers in this fork, so this timeout is the ONLY thing that unblocks such
    // a stuck recovery -- 30s is far too slow for failover and outlives test waits
    // (TestCloudConsistency: leader returns within ~1s but waitForActiveCollection only allows 10s,
    // so the stale 30s commit never got a chance to retry against the restarted leader). Bound it to
    // 10s (configurable); the retry loop then re-resolves the live leader and converges quickly.
    int recoveryIdleTimeout = Integer.getInteger("solr.recovery.idleTimeout", 10000);
    recoveryOnlyClientBuilder = recoveryOnlyClientBuilder.connectionTimeout(5000).idleTimeout(recoveryIdleTimeout);


    recoveryOnlyClient = recoveryOnlyClientBuilder.name("Recover").markInternalRequest().maxOutstandingAsyncRequests(Integer.getInteger("solr.maxRecoveryHttpRequestsOut", 30)).build();
    recoveryOnlyClient.enableCloseLock();

    Http2SolrClient.Builder searchOnlyClientBuilder = new Http2SolrClient.Builder();
    searchOnlyClientBuilder.connectionTimeout(5000).maxOutstandingAsyncRequests(-1).idleTimeout(60000);


    searchOnlyClient = searchOnlyClientBuilder.name("search").markInternalRequest().build();
    searchOnlyClient.enableCloseLock();


//    ThreadFactory recoveryThreadFactory = new SolrNamedThreadFactory("recoveryExecutor");
//    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
//      if (log.isDebugEnabled()) {
//        log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
//      }
//      recoveryExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(cfg.getMaxRecoveryThreads(), recoveryThreadFactory);
//    } else {

      recoveryExecutor = ParWork.getExecutorService("recoveryExecutor", SysStats.PROC_COUNT, true);
 //   }
  }

  private static HttpClientMetricNameStrategy getMetricNameStrategy(UpdateShardHandlerConfig cfg) {
    HttpClientMetricNameStrategy metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
    if (cfg != null)  {
      metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (metricNameStrategy == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return metricNameStrategy;
  }

//  private HttpListenerFactory.NameStrategy getNameStrategy(UpdateShardHandlerConfig cfg) {
//    HttpListenerFactory.NameStrategy nameStrategy =
//        HttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
//
//    if (cfg != null)  {
//      nameStrategy = HttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
//      if (nameStrategy == null) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
//            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
//      }
//    }
//    return nameStrategy;
//  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, getCategory().name());
    //.initializeMetrics(solrMetricsContext, expandedScope);
    recoveryExecutor = MetricUtils.instrumentedExecutorService(recoveryExecutor, this, solrMetricsContext.getMetricRegistry(),
            SolrMetricManager.mkName("recoveryExecutor", expandedScope, "threadPool"));
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

  public Http2SolrClient getTheSharedHttpClient() {
    return sharedHttpClient;
  }

  public Http2SolrClient getSolrCmdDistributorClient() {
    return solrCmdDistributorClient;
  }

  public Http2SolrClient getRecoveryOnlyClient() {
    return recoveryOnlyClient;
  }


  public Http2SolrClient getSearchOnlyClient() {
    return searchOnlyClient;
  }

  /**
   * 
   * @return executor for recovery operations
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  public void close() {
    assert closeTracker != null ? closeTracker.close() : true;
    if (sharedHttpClient != null) sharedHttpClient.disableCloseLock();
    if (recoveryOnlyClient != null) recoveryOnlyClient.disableCloseLock();
    if (solrCmdDistributorClient != null) solrCmdDistributorClient.disableCloseLock();
    if (searchOnlyClient != null) searchOnlyClient.disableCloseLock();


    try (ParWork closer = new ParWork(this, false)) {
      closer.collect(recoveryOnlyClient);
      closer.collect(searchOnlyClient);
      closer.collect(sharedHttpClient);
      closer.collect(solrCmdDistributorClient);
    }
    try {
      SolrInfoBean.super.close();
    } catch (IOException e) {
      log.warn("Error closing", e);
    }
    assert ObjectReleaseTracker.getInstance().release(this);
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
    builder.setup(sharedHttpClient);
    builder.setup(recoveryOnlyClient);
    builder.setup(solrCmdDistributorClient);
    builder.setup(recoveryOnlyClient);
    builder.setup(searchOnlyClient);
  }
}
