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
package org.apache.solr.metrics.prometheus;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for all {@link SolrPrometheusRegistry} holding {@link MetricSnapshot}s. Can export
 * {@link Meter} to {@link MetricSnapshot} to be outputted for {@link
 * org.apache.solr.response.PrometheusResponseWriter}
 */
public abstract class SolrPrometheusRegistry {
  private final Map<String, List<CounterSnapshot.CounterDataPointSnapshot>> metricCounters;
  private final Map<String, List<GaugeSnapshot.GaugeDataPointSnapshot>> metricGauges;

  public SolrPrometheusRegistry() {
    this.metricCounters = new HashMap<>();
    this.metricGauges = new HashMap<>();
  }

  /**
   * Export {@link Meter} to {@link CounterSnapshot}. Registers new Prometheus Metric and {@link
   * io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot} if metric does
   * not exist
   *
   * @param prometheusMetricName name of prometheus metric
   * @param dropwizardMetric the {@link Meter} to be exported
   * @param labels label names and values to register with {@link CounterSnapshot}
   */
  public void exportMeter(
      String prometheusMetricName, Meter dropwizardMetric, Map<String, String> labels) {
    CounterSnapshot.CounterDataPointSnapshot dataPointBuilder =
        CounterSnapshot.CounterDataPointSnapshot.builder()
            .value(dropwizardMetric.getCount())
            .labels(Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values())))
            .build();
    if (!metricCounters.containsKey(prometheusMetricName)) {
      metricCounters.put(prometheusMetricName, new ArrayList<>());
    }
    metricCounters.get(prometheusMetricName).add(dataPointBuilder);
  }

  /**
   * Export {@link com.codahale.metrics.Counter} to {@link CounterSnapshot}. Registers new
   * Prometheus Metric and {@link
   * io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot} if metric does
   * not exist
   *
   * @param prometheusMetricName name of prometheus metric
   * @param dropwizardMetric the {@link com.codahale.metrics.Counter} to be exported
   * @param labels label names and values to record with {@link CounterSnapshot}
   */
  public void exportCounter(
      String prometheusMetricName,
      com.codahale.metrics.Counter dropwizardMetric,
      Map<String, String> labels) {
    CounterSnapshot.CounterDataPointSnapshot dataPointBuilder =
        CounterSnapshot.CounterDataPointSnapshot.builder()
            .value(dropwizardMetric.getCount())
            .labels(Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values())))
            .build();
    if (!metricCounters.containsKey(prometheusMetricName)) {
      metricCounters.put(prometheusMetricName, new ArrayList<>());
    }
    metricCounters.get(prometheusMetricName).add(dataPointBuilder);
  }

  /**
   * Export {@link Timer} to {@link GaugeSnapshot}. Registers new Prometheus Metric and {@link
   * io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot} if metric does not
   * exist
   *
   * @param prometheusMetricName name of prometheus metric
   * @param dropwizardMetric the {@link Timer} to be exported
   * @param labels label names and values to record with {@link GaugeSnapshot}
   */
  public void exportTimer(
      String prometheusMetricName, Timer dropwizardMetric, Map<String, String> labels) {
    GaugeSnapshot.GaugeDataPointSnapshot dataPointBuilder =
        GaugeSnapshot.GaugeDataPointSnapshot.builder()
            .value(dropwizardMetric.getCount())
            .labels(Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values())))
            .build();
    if (!metricGauges.containsKey(prometheusMetricName)) {
      metricGauges.put(prometheusMetricName, new ArrayList<>());
    }
    metricGauges.get(prometheusMetricName).add(dataPointBuilder);
  }

  /**
   * Export {@link com.codahale.metrics.Gauge} to {@link GaugeSnapshot}. Registers new Prometheus
   * Metric and {@link io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot}
   * if metric does not exist
   *
   * @param prometheusMetricName name of prometheus metric
   * @param dropwizardMetricRaw the {@link com.codahale.metrics.Gauge} to be exported
   * @param labelsMap label names and values to record with {@link Gauge}
   */
  public void exportGauge(
      String prometheusMetricName,
      com.codahale.metrics.Gauge<?> dropwizardMetricRaw,
      Map<String, String> labelsMap) {
    Object dropwizardMetric = (dropwizardMetricRaw).getValue();
    if (!metricGauges.containsKey(prometheusMetricName)) {
      metricGauges.put(prometheusMetricName, new ArrayList<>());
    }
    if (dropwizardMetric instanceof Number) {
      GaugeSnapshot.GaugeDataPointSnapshot dataPointBuilder =
          GaugeSnapshot.GaugeDataPointSnapshot.builder()
              .value(((Number) dropwizardMetric).doubleValue())
              .labels(
                  Labels.of(
                      new ArrayList<>(labelsMap.keySet()), new ArrayList<>(labelsMap.values())))
              .build();
      metricGauges.get(prometheusMetricName).add(dataPointBuilder);
    } else if (dropwizardMetric instanceof HashMap) {
      HashMap<?, ?> itemsMap = (HashMap<?, ?>) dropwizardMetric;
      for (Object item : itemsMap.keySet()) {
        if (itemsMap.get(item) instanceof Number) {
          List<String> labelKeys = new ArrayList<>(labelsMap.keySet());
          labelKeys.add("item");
          List<String> labelValues = new ArrayList<>(labelsMap.values());
          labelValues.add((String) item);
          GaugeSnapshot.GaugeDataPointSnapshot dataPointBuilder =
              GaugeSnapshot.GaugeDataPointSnapshot.builder()
                  .value(((Number) itemsMap.get(item)).doubleValue())
                  .labels(Labels.of(labelKeys, labelValues))
                  .build();
          metricGauges.get(prometheusMetricName).add(dataPointBuilder);
        }
      }
    }
  }

  public MetricSnapshots collect() {
    ArrayList<MetricSnapshot> snapshots = new ArrayList<>();
    for (String names : metricCounters.keySet()) {
      snapshots.add(new CounterSnapshot(new MetricMetadata(names), metricCounters.get(names)));
    }
    for (String names : metricGauges.keySet()) {
      snapshots.add(new GaugeSnapshot(new MetricMetadata(names), metricGauges.get(names)));
    }
    return new MetricSnapshots(snapshots);
  }
}
