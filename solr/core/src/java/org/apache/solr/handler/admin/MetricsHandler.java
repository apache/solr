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

package org.apache.solr.handler.admin;

import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.GetMetrics;
import org.apache.solr.metrics.MetricsUtil;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.otel.FilterablePrometheusMetricReader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request handler to return metrics */
public class MetricsHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrMetricManager metricManager;

  public static final String COMPACT_PARAM = "compact";
  public static final String PREFIX_PARAM = "prefix";
  public static final String REGEX_PARAM = "regex";
  public static final String PROPERTY_PARAM = "property";
  public static final String REGISTRY_PARAM = "registry";
  public static final String GROUP_PARAM = "group";
  public static final String KEY_PARAM = "key";
  public static final String EXPR_PARAM = "expr";
  public static final String TYPE_PARAM = "type";

  public static final String ALL = "all";

  private static final Pattern KEY_SPLIT_REGEX =
      Pattern.compile("(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":"));
  private final CoreContainer cc;
  private final Map<String, String> injectedSysProps = CommonTestInjection.injectAdditionalProps();
  private final boolean enabled;

  public MetricsHandler(CoreContainer coreContainer) {
    this.metricManager = coreContainer.getMetricManager();
    this.cc = coreContainer;
    this.enabled = coreContainer.getConfig().getMetricsConfig().isEnabled();
  }

  public MetricsHandler(SolrMetricManager metricManager) {
    this.metricManager = metricManager;
    this.cc = null;
    this.enabled = true;
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_READ_PERM;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (metricManager == null) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE, "SolrMetricManager instance not initialized");
    }
    log.info("MetricsHandler response writer: {}", req.getResponseWriter().getClass().getName());

    SolrParams params = req.getParams();
    String format = params.get(CommonParams.WT);

    if (format == null) {
      req.setParams(SolrParams.wrapDefaults(params, SolrParams.of("wt", "prometheus")));
    } else if (!MetricsUtil.PROMETHEUS_METRICS_WT.equals(format)
        && !MetricsUtil.OPEN_METRICS_WT.equals(format)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Only Prometheus and OpenMetrics metric formats supported. Unsupported format requested: "
              + format);
    }

    if (cc != null && AdminHandlersProxy.maybeProxyToNodes(req, rsp, cc)) {
      return; // Request was proxied to other node
    }
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
    try {
      handleRequest(req.getParams(), (k, v) -> rsp.add(k, v));
    } finally {
      SolrRequestInfo.clearRequestInfo();
    }
  }

  private void handleRequest(SolrParams params, BiConsumer<String, Object> consumer) {
    if (!enabled) {
      consumer.accept("error", "metrics collection is disabled");
      return;
    }

    Set<String> metricNames = MetricsUtil.readParamsAsSet(params, MetricsUtil.METRIC_NAME_PARAM);
    SortedMap<String, Set<String>> labelFilters = MetricsUtil.labelFilters(params);

    if (metricNames.isEmpty() && labelFilters.isEmpty()) {
      consumer.accept(
          "metrics",
          MetricsUtil.mergeSnapshots(
              metricManager.getPrometheusMetricReaders().values().stream()
                  .flatMap(r -> r.collect().stream())
                  .toList()));
      return;
    }

    List<MetricSnapshot> allSnapshots = new ArrayList<>();
    for (FilterablePrometheusMetricReader reader :
        metricManager.getPrometheusMetricReaders().values()) {
      MetricSnapshots filteredSnapshots = reader.collect(metricNames, labelFilters);
      filteredSnapshots.forEach(allSnapshots::add);
    }

    // Merge all filtered snapshots and return the merged result
    MetricSnapshots mergedSnapshots = MetricsUtil.mergeSnapshots(allSnapshots);
    consumer.accept("metrics", mergedSnapshots);
  }

  @Override
  public String getDescription() {
    return "A handler to return all the metrics gathered by Solr";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(GetMetrics.class);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
