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
package org.apache.solr.handler.component;

import com.codahale.metrics.Counter;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeLimitingHttpShardHandlerFactory extends HttpShardHandlerFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private long slowNodeTimeout;
  private boolean dryRun;
  private boolean initialized;

  private static final String TIMEOUT_CONFIG_KEY =
      "slowNodeTimeout"; // config key for timeout in millisec
  private static final String LATENCY_DROP_RATIO_THRESHOLD_CONFIG_KEY = "latencyDropRatioThreshold";
  private static final String MAX_SLOW_RESPONSE_PERCENTAGE_CONFIG_KEY = "maxSlowResponsePercentage";
  private static final String MIN_SHARD_COUNT_PER_REQUEST_CONFIG_KEY = "minShardCountPerRequest";
  private static final String SLOW_LATENCY_THRESHOLD_CONFIG_KEY = "slowLatencyThreshold";
  private static final String SLOW_NODE_TTL_CONFIG_KEY = "slowNodeTtl";
  private static final String DRY_RUN_CONFIG_KEY = "dryRun";

  private SlowNodeDetector slowNodeDetector;
  private SolrMetricsContext solrMetricsContext;
  Counter cancelledSlowNodeRequests;
  Counter cancelledDryRunSlowNodeRequests;
  private ExecutorService executorService;

  /**
   * Get {@link ShardHandler} that times out on slow shards.
   *
   * <p>Take note the returned ShardHandler is expected to handle ShardRequests spawned by a single
   * top level query.
   *
   * <p>Each ShardRequest are expected to be submitted to all the relevant cores in relatively close
   * timeframes (ie most requests are submitted before the first response comes back) Otherwise, it
   * might not detect slow nodes nor timeout slow requests as expected.
   */
  @Override
  public ShardHandler getShardHandler() {
    if (!initialized) {
      throw new RuntimeException(
          TimeLimitingHttpShardHandlerFactory.class.getSimpleName()
              + " is not initialized, run init() first or check if there are any exceptions during init()");
    }
    return new TimeLimitingHttpShardHandler(
        this,
        slowNodeTimeout,
        dryRun,
        slowNodeDetector,
        (timedOutTasks) -> {
          Counter counter = dryRun ? cancelledDryRunSlowNodeRequests : cancelledSlowNodeRequests;
          counter.inc(timedOutTasks.size());
        },
        executorService);
  }

  /** For test */
  void setSlowNodeDetector(SlowNodeDetector slowNodeDetector) {
    this.slowNodeDetector = slowNodeDetector;
  }

  @Override
  public void init(PluginInfo info) {
    super.init(info);
    NamedList<?> args = info.initArgs;

    // Actor params
    Object dryRunObject = args.get(DRY_RUN_CONFIG_KEY);
    if (dryRunObject != null) {
      dryRun = Boolean.parseBoolean(dryRunObject.toString());
    }

    Object slowNodeTimeoutObj = args.get(TIMEOUT_CONFIG_KEY);
    if (slowNodeTimeoutObj == null) {
      throw new IllegalArgumentException(
          "Missing required parameter: "
              + TIMEOUT_CONFIG_KEY
              + " for "
              + TimeLimitingHttpShardHandlerFactory.class.getSimpleName()
              + " in solr config");
    }
    slowNodeTimeout = Long.parseLong(slowNodeTimeoutObj.toString());

    if (log.isInfoEnabled()) {
      log.info(
          "Initializing Actors in {} with {} {}, {} {}",
          TimeLimitingHttpShardHandlerFactory.class.getSimpleName(),
          DRY_RUN_CONFIG_KEY,
          dryRun,
          TIMEOUT_CONFIG_KEY,
          slowNodeTimeoutObj);
    }

    // Detector params and build detector here
    SlowNodeDetector.Builder builder = new SlowNodeDetector.Builder();
    if (args.get(LATENCY_DROP_RATIO_THRESHOLD_CONFIG_KEY) != null) {
      builder.withLatencyDropRatioThreshold(
          Double.parseDouble(args.get(LATENCY_DROP_RATIO_THRESHOLD_CONFIG_KEY).toString()));
    }
    if (args.get(MAX_SLOW_RESPONSE_PERCENTAGE_CONFIG_KEY) != null) {
      builder.withMaxSlowResponsePercentage(
          Integer.parseInt(args.get(MAX_SLOW_RESPONSE_PERCENTAGE_CONFIG_KEY).toString()));
    }
    if (args.get(MIN_SHARD_COUNT_PER_REQUEST_CONFIG_KEY) != null) {
      builder.withMinShardCountPerRequest(
          Integer.parseInt(args.get(MIN_SHARD_COUNT_PER_REQUEST_CONFIG_KEY).toString()));
    }
    if (args.get(SLOW_LATENCY_THRESHOLD_CONFIG_KEY) != null) {
      builder.withSlowLatencyThreshold(
          Integer.parseInt(args.get(SLOW_LATENCY_THRESHOLD_CONFIG_KEY).toString()));
    }
    if (args.get(SLOW_NODE_TTL_CONFIG_KEY) != null) {
      builder.withSlowNodeTtl(Long.parseLong(args.get(SLOW_NODE_TTL_CONFIG_KEY).toString()));
    }

    if (log.isInfoEnabled()) {
      log.info("Building SlowNodeDetector with {}", builder);
    }

    slowNodeDetector = builder.build();
    executorService =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            new SolrNamedThreadFactory("TimeLimitingShardHandler"));

    initialized = true;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    cancelledSlowNodeRequests =
        solrMetricsContext.counter("cancelledSlowNodeRequests", expandedScope);
    cancelledDryRunSlowNodeRequests =
        solrMetricsContext.counter("cancelledDryRunSlowNodeRequests", expandedScope);

    if (slowNodeDetector != null) {
      slowNodeDetector.initializeMetrics(solrMetricsContext, expandedScope);
    }
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void close() {
    super.close();
    if (executorService != null) {
      ExecutorUtil.shutdownNowAndAwaitTermination(executorService);
    }
  }
}
