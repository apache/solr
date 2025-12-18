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

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.otel.FilterablePrometheusMetricReader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

/** Request handler to return metrics */
public class MetricsHandler extends RequestHandlerBase implements PermissionNameProvider {
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

  // Prometheus filtering parameters
  public static final String CATEGORY_PARAM = "category";
  public static final String CORE_PARAM = "core";
  public static final String COLLECTION_PARAM = "collection";
  public static final String SHARD_PARAM = "shard";
  public static final String REPLICA_TYPE_PARAM = "replica_type";
  public static final String METRIC_NAME_PARAM = "name";
  private static final Set<String> labelFilterKeys =
      Set.of(CATEGORY_PARAM, CORE_PARAM, COLLECTION_PARAM, SHARD_PARAM, REPLICA_TYPE_PARAM);

  public static final String PROMETHEUS_METRICS_WT = "prometheus";
  public static final String OPEN_METRICS_WT = "openmetrics";

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

    SolrParams params = req.getParams();
    String format = params.get(CommonParams.WT);

    if (format == null) {
      req.setParams(SolrParams.wrapDefaults(params, SolrParams.of("wt", "prometheus")));
    } else if (!PROMETHEUS_METRICS_WT.equals(format) && !OPEN_METRICS_WT.equals(format)) {
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

    Set<String> metricNames = readParamsAsSet(params, METRIC_NAME_PARAM);
    SortedMap<String, Set<String>> labelFilters = labelFilters(params);

    if (metricNames.isEmpty() && labelFilters.isEmpty()) {
      consumer.accept(
          "metrics",
          mergeSnapshots(
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
    MetricSnapshots mergedSnapshots = mergeSnapshots(allSnapshots);
    consumer.accept("metrics", mergedSnapshots);
  }

  private SortedMap<String, Set<String>> labelFilters(SolrParams params) {
    SortedMap<String, Set<String>> labelFilters = new TreeMap<>();
    labelFilterKeys.forEach(
        (paramName) -> {
          Set<String> filterValues = readParamsAsSet(params, paramName);
          if (!filterValues.isEmpty()) {
            labelFilters.put(paramName, filterValues);
          }
        });

    return labelFilters;
  }

  private Set<String> readParamsAsSet(SolrParams params, String paramName) {
    String[] paramValues = params.getParams(paramName);
    if (paramValues == null || paramValues.length == 0) {
      return Set.of();
    }

    List<String> paramSet = new ArrayList<>();
    for (String param : paramValues) {
      paramSet.addAll(StrUtils.splitSmart(param, ','));
    }
    return Set.copyOf(paramSet);
  }

  /**
   * Merge a collection of individual {@link MetricSnapshot} instances into one {@link
   * MetricSnapshots}. This is necessary because we create a {@link
   * io.opentelemetry.sdk.metrics.SdkMeterProvider} per Solr core resulting in duplicate metric
   * names across cores which is an illegal format if under the same prometheus grouping.
   */
  private MetricSnapshots mergeSnapshots(List<MetricSnapshot> snapshots) {
    Map<String, CounterSnapshot.Builder> counterSnapshotMap = new HashMap<>();
    Map<String, GaugeSnapshot.Builder> gaugeSnapshotMap = new HashMap<>();
    Map<String, HistogramSnapshot.Builder> histogramSnapshotMap = new HashMap<>();
    InfoSnapshot otelInfoSnapshots = null;

    for (MetricSnapshot snapshot : snapshots) {
      String metricName = snapshot.getMetadata().getPrometheusName();

      switch (snapshot) {
        case CounterSnapshot counterSnapshot -> {
          CounterSnapshot.Builder builder =
              counterSnapshotMap.computeIfAbsent(
                  metricName,
                  k -> {
                    var base =
                        CounterSnapshot.builder()
                            .name(counterSnapshot.getMetadata().getName())
                            .help(counterSnapshot.getMetadata().getHelp());
                    return counterSnapshot.getMetadata().hasUnit()
                        ? base.unit(counterSnapshot.getMetadata().getUnit())
                        : base;
                  });
          counterSnapshot.getDataPoints().forEach(builder::dataPoint);
        }
        case GaugeSnapshot gaugeSnapshot -> {
          GaugeSnapshot.Builder builder =
              gaugeSnapshotMap.computeIfAbsent(
                  metricName,
                  k -> {
                    var base =
                        GaugeSnapshot.builder()
                            .name(gaugeSnapshot.getMetadata().getName())
                            .help(gaugeSnapshot.getMetadata().getHelp());
                    return gaugeSnapshot.getMetadata().hasUnit()
                        ? base.unit(gaugeSnapshot.getMetadata().getUnit())
                        : base;
                  });
          gaugeSnapshot.getDataPoints().forEach(builder::dataPoint);
        }
        case HistogramSnapshot histogramSnapshot -> {
          HistogramSnapshot.Builder builder =
              histogramSnapshotMap.computeIfAbsent(
                  metricName,
                  k -> {
                    var base =
                        HistogramSnapshot.builder()
                            .name(histogramSnapshot.getMetadata().getName())
                            .help(histogramSnapshot.getMetadata().getHelp());
                    return histogramSnapshot.getMetadata().hasUnit()
                        ? base.unit(histogramSnapshot.getMetadata().getUnit())
                        : base;
                  });
          histogramSnapshot.getDataPoints().forEach(builder::dataPoint);
        }
        case InfoSnapshot infoSnapshot -> {
          // InfoSnapshot is a special case in that each SdkMeterProvider will create a duplicate
          // metric called target_info containing OTEL SDK metadata. Only one of these need to be
          // kept
          if (otelInfoSnapshots == null)
            otelInfoSnapshots =
                new InfoSnapshot(infoSnapshot.getMetadata(), infoSnapshot.getDataPoints());
        }
        default -> {
          // Handle unexpected snapshot types gracefully
        }
      }
    }

    MetricSnapshots.Builder snapshotsBuilder = MetricSnapshots.builder();
    counterSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    gaugeSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    histogramSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    if (otelInfoSnapshots != null) snapshotsBuilder.metricSnapshot(otelInfoSnapshots);
    return snapshotsBuilder.build();
  }

  @Override
  public String getDescription() {
    return "A handler to return all the metrics gathered by Solr";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}
