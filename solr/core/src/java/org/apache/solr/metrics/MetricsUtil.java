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
package org.apache.solr.metrics;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;

/** Utility methods for Metrics */
public class MetricsUtil {

  public static final String PROMETHEUS_METRICS_WT = "prometheus";
  public static final String OPEN_METRICS_WT = "openmetrics";

  public static final String NODE_PARAM = "node";

  public static final String CATEGORY_PARAM = "category";
  public static final String CORE_PARAM = "core";
  public static final String COLLECTION_PARAM = "collection";
  public static final String SHARD_PARAM = "shard";
  public static final String REPLICA_TYPE_PARAM = "replica_type";
  public static final String METRIC_NAME_PARAM = "name";
  private static final Set<String> labelFilterKeys =
      Set.of(CATEGORY_PARAM, CORE_PARAM, COLLECTION_PARAM, SHARD_PARAM, REPLICA_TYPE_PARAM);

  /**
   * Merge a collection of individual {@link MetricSnapshot} instances into one {@link
   * MetricSnapshots}. This is necessary because we create a {@link
   * io.opentelemetry.sdk.metrics.SdkMeterProvider} per Solr core resulting in duplicate metric
   * names across cores which is an illegal format if under the same prometheus grouping.
   */
  public static MetricSnapshots mergeSnapshots(List<MetricSnapshot> snapshots) {
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

  /** Gather label filters */
  public static SortedMap<String, Set<String>> labelFilters(SolrParams params) {
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

  /** Add label filters to the filters map */
  public static void addLabelFilters(String value, Map<String, Set<String>> filters) {
    labelFilterKeys.forEach(
        (paramName) -> {
          Set<String> filterValues = paramValueAsSet(value);
          if (!filterValues.isEmpty()) {
            filters.put(paramName, filterValues);
          }
        });
  }

  /** Split the coma-separated param values into a set */
  public static Set<String> paramValueAsSet(String paramValue) {
    String[] values = paramValue.split(",");
    List<String> valuesSet = new ArrayList<>();
    for (String value : values) {
      valuesSet.add(value);
    }
    return Set.copyOf(valuesSet);
  }

  /**
   * Read Solr parameters as a Set.
   *
   * <p>Could probably be moved to a more generic utility class, but only used in MetricsHandler and
   * GetMetrics resource.
   */
  public static Set<String> readParamsAsSet(SolrParams params, String paramName) {
    String[] paramValues = params.getParams(paramName);
    if (paramValues == null || paramValues.length == 0) {
      return Set.of();
    }

    Set<String> paramSet = new HashSet<>();
    for (String param : paramValues) {
      if (param != null && param.length() > 0) paramSet.addAll(StrUtils.splitSmart(param, ','));
    }
    return paramSet;
  }
}
