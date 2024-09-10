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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider of the default SolrClient implementation.
 *
 * @lucene.internal
 */
class HttpSolrClientProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String METRIC_SCOPE_NAME = "defaultHttpSolrClientProvider";

  private final Http2SolrClient httpSolrClient;

  private final InstrumentedHttpListenerFactory trackHttpSolrMetrics;

  HttpSolrClientProvider(UpdateShardHandlerConfig cfg) {
    trackHttpSolrMetrics = new InstrumentedHttpListenerFactory(getNameStrategy(cfg));
    Http2SolrClient.Builder httpClientBuilder =
        new Http2SolrClient.Builder().withListenerFactory(List.of(trackHttpSolrMetrics));

    if (cfg != null) {
      httpClientBuilder
          .withConnectionTimeout(cfg.getDistributedConnectionTimeout(), TimeUnit.MILLISECONDS)
          .withIdleTimeout(cfg.getDistributedSocketTimeout(), TimeUnit.MILLISECONDS)
          .withMaxConnectionsPerHost(cfg.getMaxUpdateConnectionsPerHost());
    }
    httpSolrClient = httpClientBuilder.build();
  }

  private InstrumentedHttpListenerFactory.NameStrategy getNameStrategy(
      UpdateShardHandlerConfig cfg) {
    String metricNameStrategy =
        cfg != null && cfg.getMetricNameStrategy() != null
            ? cfg.getMetricNameStrategy()
            : UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY;
    return InstrumentedHttpListenerFactory.getNameStrategy(metricNameStrategy);
  }

  Http2SolrClient getSolrClient() {
    return httpSolrClient;
  }

  void setSecurityBuilder(HttpClientBuilderPlugin builder) {
    builder.setup(httpSolrClient);
  }

  void initializeMetrics(SolrMetricsContext parentContext) {
    var solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope =
        SolrMetricManager.mkName(METRIC_SCOPE_NAME, SolrInfoBean.Category.HTTP.name());
    trackHttpSolrMetrics.initializeMetrics(solrMetricsContext, expandedScope);
  }

  void close() {
    try {
      trackHttpSolrMetrics.close();
    } catch (Exception e) {
      log.error("Error closing SolrMetricProducer", e);
    }
    IOUtils.closeQuietly(httpSolrClient);
  }
}
