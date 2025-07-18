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
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.Exemplars;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.Quantiles;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.util.stats.MetricUtils;

/**
 * Base class for all {@link SolrPrometheusFormatter} holding {@link MetricSnapshot}s. Can export
 * {@link com.codahale.metrics.Metric} to {@link MetricSnapshot} to be outputted for {@link
 * org.apache.solr.response.PrometheusResponseWriter}
 */
public abstract class SolrPrometheusFormatter {
  protected final Map<String, List<CounterSnapshot.CounterDataPointSnapshot>> metricCounters;
  protected final Map<String, List<GaugeSnapshot.GaugeDataPointSnapshot>> metricGauges;
  protected final Map<String, List<SummarySnapshot.SummaryDataPointSnapshot>> metricSummaries;

  public SolrPrometheusFormatter() {
    this.metricCounters = new HashMap<>();
    this.metricGauges = new HashMap<>();
    this.metricSummaries = new HashMap<>();
  }

  /**
   * Export {@link Metric} to {@link io.prometheus.metrics.model.snapshots.MetricSnapshot} and
   * register the Snapshot
   *
   * @param dropwizardMetric the {@link Metric} to be exported
   * @param metricName Dropwizard metric name
   */
  public abstract void exportDropwizardMetric(Metric dropwizardMetric, String metricName);

  /**
   * Categorize {@link Metric} based on the metric name
   *
   * @param dropwizardMetric the {@link Metric} to be exported
   * @param metricName Dropwizard metric name
   */
  public abstract SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName);

  /**
   * Export {@link Meter} to {@link
   * io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot} and collect
   * datapoint
   *
   * @param metricName name of metric after export
   * @param dropwizardMetric the {@link Meter} to be exported
   * @param labels label names and values to record
   */
  public void exportMeter(String metricName, Meter dropwizardMetric, Labels labels) {
    CounterSnapshot.CounterDataPointSnapshot dataPoint =
        createCounterDatapoint((double) dropwizardMetric.getCount(), labels);
    collectCounterDatapoint(metricName, dataPoint);
  }

  /**
   * Export {@link com.codahale.metrics.Counter} to {@link
   * io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot} and collect
   * datapoint
   *
   * @param metricName name of prometheus metric
   * @param dropwizardMetric the {@link com.codahale.metrics.Counter} to be exported
   * @param labels label names and values to record
   */
  public void exportCounter(
      String metricName, com.codahale.metrics.Counter dropwizardMetric, Labels labels) {
    CounterSnapshot.CounterDataPointSnapshot dataPoint =
        createCounterDatapoint((double) dropwizardMetric.getCount(), labels);
    collectCounterDatapoint(metricName, dataPoint);
  }

  /**
   * Export {@link Timer} ands its quantile data to {@link
   * io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot} and collect
   * datapoint
   *
   * @param metricName name of prometheus metric
   * @param dropwizardMetric the {@link Timer} to be exported
   * @param labels label names and values to record
   */
  public void exportTimer(String metricName, Timer dropwizardMetric, Labels labels) {
    Snapshot snapshot = dropwizardMetric.getSnapshot();

    long count = snapshot.size();
    double sum =
        Arrays.stream(snapshot.getValues())
            .asDoubleStream()
            .map(num -> MetricUtils.nsToMs(num))
            .sum();

    Quantiles quantiles =
        Quantiles.of(
            List.of(
                new Quantile(0.50, MetricUtils.nsToMs(snapshot.getMedian())),
                new Quantile(0.75, MetricUtils.nsToMs(snapshot.get75thPercentile())),
                new Quantile(0.99, MetricUtils.nsToMs(snapshot.get99thPercentile())),
                new Quantile(0.999, MetricUtils.nsToMs(snapshot.get999thPercentile()))));

    var summary =
        new SummarySnapshot.SummaryDataPointSnapshot(
            count, sum, quantiles, labels, Exemplars.EMPTY, 0L);
    collectSummaryDatapoint(metricName, summary);
  }

  /**
   * Export {@link Timer} ands its Count to {@link
   * io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot} and collect
   * datapoint
   *
   * @param metricName name of prometheus metric
   * @param dropwizardMetric the {@link Timer} to be exported
   * @param labels label names and values to record
   */
  public void exportTimerCount(String metricName, Timer dropwizardMetric, Labels labels) {
    CounterSnapshot.CounterDataPointSnapshot dataPoint =
        createCounterDatapoint((double) dropwizardMetric.getCount(), labels);
    collectCounterDatapoint(metricName, dataPoint);
  }

  /**
   * Export {@link com.codahale.metrics.Gauge} to {@link
   * io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot} and collect to
   * datapoint. Unlike other Dropwizard metric types, Gauges can have more complex types. In the
   * case of a hashmap, collect each as an individual metric and have its key appended as a label to
   * the metric called "item"
   *
   * @param metricName name of prometheus metric
   * @param dropwizardMetricRaw the {@link com.codahale.metrics.Gauge} to be exported
   * @param labels label names and values to record
   */
  public void exportGauge(
      String metricName, com.codahale.metrics.Gauge<?> dropwizardMetricRaw, Labels labels) {
    Object dropwizardMetric = (dropwizardMetricRaw).getValue();
    if (dropwizardMetric instanceof Number) {
      GaugeSnapshot.GaugeDataPointSnapshot dataPoint =
          createGaugeDatapoint(((Number) dropwizardMetric).doubleValue(), labels);
      collectGaugeDatapoint(metricName, dataPoint);
    } else if (dropwizardMetric instanceof HashMap<?, ?> itemsMap) {
      for (Object item : itemsMap.keySet()) {
        if (itemsMap.get(item) instanceof Number) {
          GaugeSnapshot.GaugeDataPointSnapshot dataPoint =
              createGaugeDatapoint(
                  ((Number) itemsMap.get(item)).doubleValue(),
                  labels.merge(Labels.of("item", (String) item)));
          collectGaugeDatapoint(metricName, dataPoint);
        }
      }
    }
  }

  /**
   * Create a {@link io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot}
   * with labels
   *
   * @param value metric value
   * @param labels set of name/values labels
   */
  public CounterSnapshot.CounterDataPointSnapshot createCounterDatapoint(
      double value, Labels labels) {
    return CounterSnapshot.CounterDataPointSnapshot.builder().value(value).labels(labels).build();
  }

  /**
   * Create a {@link io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot}
   * with labels
   *
   * @param value metric value
   * @param labels set of name/values labels
   */
  public GaugeSnapshot.GaugeDataPointSnapshot createGaugeDatapoint(double value, Labels labels) {
    return GaugeSnapshot.GaugeDataPointSnapshot.builder().value(value).labels(labels).build();
  }

  /**
   * Collects {@link io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot}
   * and appends to existing metric or create new metric if name does not exist
   *
   * @param metricName Name of metric
   * @param dataPoint Counter datapoint to be collected
   */
  public void collectCounterDatapoint(
      String metricName, CounterSnapshot.CounterDataPointSnapshot dataPoint) {
    if (!metricCounters.containsKey(metricName)) {
      metricCounters.put(metricName, new ArrayList<>());
    }
    metricCounters.get(metricName).add(dataPoint);
  }

  /**
   * Collects {@link io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot} and
   * appends to existing metric or create new metric if name does not exist
   *
   * @param metricName Name of metric
   * @param dataPoint Gauge datapoint to be collected
   */
  public void collectGaugeDatapoint(
      String metricName, GaugeSnapshot.GaugeDataPointSnapshot dataPoint) {
    if (!metricGauges.containsKey(metricName)) {
      metricGauges.put(metricName, new ArrayList<>());
    }
    metricGauges.get(metricName).add(dataPoint);
  }

  /**
   * Collects {@link io.prometheus.metrics.model.snapshots.SummarySnapshot.SummaryDataPointSnapshot}
   * and appends to existing metric or create new metric if name does not exist
   *
   * @param metricName Name of metric
   * @param dataPoint Gauge datapoint to be collected
   */
  public void collectSummaryDatapoint(
      String metricName, SummarySnapshot.SummaryDataPointSnapshot dataPoint) {
    metricSummaries.computeIfAbsent(metricName, k -> new ArrayList<>()).add(dataPoint);
  }

  /**
   * Returns an immutable {@link MetricSnapshots} from the {@link
   * io.prometheus.metrics.model.snapshots.DataPointSnapshot}s collected from the registry
   */
  public MetricSnapshots collect() {
    ArrayList<MetricSnapshot> snapshots = new ArrayList<>();

    metricCounters
        .entrySet()
        .forEach(
            entry ->
                snapshots.add(
                    new CounterSnapshot(new MetricMetadata(entry.getKey()), entry.getValue())));
    metricGauges
        .entrySet()
        .forEach(
            entry ->
                snapshots.add(
                    new GaugeSnapshot(new MetricMetadata(entry.getKey()), entry.getValue())));
    metricSummaries
        .entrySet()
        .forEach(
            entry ->
                snapshots.add(
                    new SummarySnapshot(new MetricMetadata(entry.getKey()), entry.getValue())));

    return new MetricSnapshots(snapshots);
  }
}
