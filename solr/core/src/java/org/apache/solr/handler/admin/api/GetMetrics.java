/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin.api;

import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import org.apache.solr.client.api.endpoint.MetricsApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.AdminHandlersProxy;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.metrics.MetricsUtil;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.otel.FilterablePrometheusMetricReader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.PrometheusResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API implementation to fetch metrics gathered by Solr.
 *
 * <p>This API is analogous to the v1 /admin/metrics endpoint.
 */
public class GetMetrics extends AdminAPIBase implements MetricsApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrMetricManager metricManager;
  private final boolean enabled;

  @Inject
  public GetMetrics(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.metricManager = coreContainer.getMetricManager();
    this.enabled = coreContainer.getConfig().getMetricsConfig().isEnabled();
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.METRICS_READ_PERM)
  public StreamingOutput getMetrics() {

    validateRequest();

    if (proxyToNodes()) {
      return null;
    }

    SolrParams params = solrQueryRequest.getParams();

    Set<String> metricNames = MetricsUtil.readParamsAsSet(params, MetricsUtil.METRIC_NAME_PARAM);
    SortedMap<String, Set<String>> labelFilters = MetricsUtil.labelFilters(params);

    return doGetMetrics(metricNames, labelFilters);
  }

  private void validateRequest() {

    if (!enabled) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE, "Metrics collection is disabled");
    }

    if (metricManager == null) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE, "SolrMetricManager instance not initialized");
    }

    SolrParams params = solrQueryRequest.getParams();
    String format = params.get(CommonParams.WT);

    if (format == null) {
      solrQueryRequest.setParams(
          SolrParams.wrapDefaults(params, SolrParams.of("wt", "prometheus")));
    } else if (!MetricsUtil.PROMETHEUS_METRICS_WT.equals(format)
        && !MetricsUtil.OPEN_METRICS_WT.equals(format)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Only Prometheus and OpenMetrics metric formats supported. Unsupported format requested: "
              + format);
    }
  }

  private boolean proxyToNodes() {
    try {
      if (coreContainer != null
          && AdminHandlersProxy.maybeProxyToNodes(
              solrQueryRequest, solrQueryResponse, coreContainer)) {
        return true; // Request was proxied to other node
      }
    } catch (Exception e) {
      log.warn("Exception proxying to other node", e);
    }
    return false;
  }

  private StreamingOutput doGetMetrics(
      Set<String> metricNames, SortedMap<String, Set<String>> labelFilters) {

    List<MetricSnapshot> snapshots = new ArrayList<>();

    if ((metricNames == null || metricNames.isEmpty()) && labelFilters.isEmpty()) {
      snapshots.addAll(
          metricManager.getPrometheusMetricReaders().values().stream()
              .flatMap(r -> r.collect().stream())
              .toList());
    } else {
      for (FilterablePrometheusMetricReader reader :
          metricManager.getPrometheusMetricReaders().values()) {
        MetricSnapshots filteredSnapshots = reader.collect(metricNames, labelFilters);
        filteredSnapshots.forEach(snapshots::add);
      }
    }

    return writeMetricSnapshots(MetricsUtil.mergeSnapshots(snapshots));
  }

  private StreamingOutput writeMetricSnapshots(MetricSnapshots snapshots) {
    return new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        PrometheusResponseWriter writer = new PrometheusResponseWriter();
        writer.writeMetricSnapshots(output, solrQueryRequest, snapshots);
        output.flush();
      }
    };
  }
}
