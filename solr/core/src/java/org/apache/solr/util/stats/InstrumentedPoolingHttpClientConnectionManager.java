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

package org.apache.solr.util.stats;

import io.opentelemetry.api.common.Attributes;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * Instrumented version of PoolingHttpClientConnectionManager that exposes connection pool metrics via OpenTelemetry
 */
 public class InstrumentedPoolingHttpClientConnectionManager
    extends PoolingHttpClientConnectionManager implements SolrMetricProducer {

  private SolrMetricsContext solrMetricsContext;

  public InstrumentedPoolingHttpClientConnectionManager(
      Registry<ConnectionSocketFactory> socketFactoryRegistry) {
    super(socketFactoryRegistry);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(
      SolrMetricsContext parentContext, Attributes attributes, String scope) {
    this.solrMetricsContext = parentContext.getChildContext(this);

    var baseAttributes = attributes.toBuilder()
      .put(CATEGORY_ATTR, SolrInfoBean.Category.HTTP.toString())
      .build();

    solrMetricsContext.observableLongGauge(
            "solr_http_connection_pool_available_connections",
            "The current number of available connections in the pool.",
            (observableLongMeasurement ->
                    observableLongMeasurement.record(getTotalStats().getAvailable(), baseAttributes)));
    // this acquires a lock on the connection pool; remove if contention sucks
    solrMetricsContext.observableLongGauge(
            "solr_http_connection_pool_leased_connections",
            "The current number of leased connections from the pool.",
            (observableLongMeasurement ->
                    observableLongMeasurement.record(getTotalStats().getLeased(), baseAttributes)));
    solrMetricsContext.observableLongGauge(
            "solr_http_connection_pool_max_connections",
            "The maximum number of total connections in the pool.",
            (observableLongMeasurement ->
                    observableLongMeasurement.record(getTotalStats().getMax(), baseAttributes)));
    solrMetricsContext.observableLongGauge(
            "solr_http_connection_pool_pending_requests",
            "The number of requests waiting for a connection from the pool.",
            (observableLongMeasurement ->
                    observableLongMeasurement.record(getTotalStats().getPending(), baseAttributes)));
  }

  @Override
  public void close() {
    super.close();
    try {
      SolrMetricProducer.super.close();
    } catch (Exception e) {
      throw new RuntimeException("Exception closing.", e);
    }
  }
}
