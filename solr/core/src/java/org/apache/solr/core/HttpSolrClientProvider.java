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

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider of the default SolrClient implementation.
 *
 * @lucene.internal
 */
class HttpSolrClientProvider implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String METRIC_SCOPE_NAME = "defaultHttpSolrClientProvider";

  private final Http2SolrClient httpSolrClient;

  private final InstrumentedHttpListenerFactory trackHttpSolrMetrics;

  private SolrMetricsContext solrMetricsContext;

  private int socketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

  private int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;

  public HttpSolrClientProvider(UpdateShardHandlerConfig cfg) {
    Http2SolrClient.Builder httpClientBuilder = new Http2SolrClient.Builder();
    trackHttpSolrMetrics = new InstrumentedHttpListenerFactory(getNameStrategy(cfg));

    if (cfg != null) {
      httpClientBuilder.withMaxConnectionsPerHost(cfg.getMaxUpdateConnectionsPerHost());
      socketTimeout = Math.max(cfg.getDistributedConnectionTimeout(), connectionTimeout);
      connectionTimeout = Math.max(cfg.getDistributedSocketTimeout(), socketTimeout);
    }

    httpClientBuilder
        .withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
        .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS)
        .withListenerFactory(List.of(trackHttpSolrMetrics));

    httpSolrClient = httpClientBuilder.build();
  }

  private HttpClientMetricNameStrategy getMetricNameStrategy(UpdateShardHandlerConfig cfg) {
    HttpClientMetricNameStrategy metricNameStrategy =
        KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
    if (cfg != null) {
      metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (metricNameStrategy == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: "
                + cfg.getMetricNameStrategy()
                + " found. Must be one of: "
                + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return metricNameStrategy;
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
                + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return nameStrategy;
  }

  // Return a default http client for all-purpose usage.
  public SolrClient getSolrClient() {
    return httpSolrClient;
  }

  public void setSecurityBuilder(HttpClientBuilderPlugin builder) {
    builder.setup(httpSolrClient);
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.HTTP.name());
    trackHttpSolrMetrics.initializeMetrics(solrMetricsContext, expandedScope);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(httpSolrClient);
  }
}
