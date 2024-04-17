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
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class SolrPrometheusRegistry {
  PrometheusRegistry prometheusRegistry;
  String registryName;
  private final Map<String, Counter> metricCounters;
  private final Map<String, Gauge> metricGauges;

  public SolrPrometheusRegistry(PrometheusRegistry prometheusRegistry) {
    this.prometheusRegistry = prometheusRegistry;
    this.metricCounters = new HashMap<>();
    this.metricGauges = new HashMap<>();
  }

  public PrometheusRegistry getPrometheusRegistry() {
    return prometheusRegistry;
  }

  public String getRegistryName() {
    return registryName;
  }

  public void exportMeter(
      Meter dropwizardMetric, String prometheusMetricName, Map<String, String> labelsMap) {
    if (!metricCounters.containsKey(prometheusMetricName)) {
      ArrayList<String> labels = new ArrayList<>(labelsMap.keySet());
      registerCounter(prometheusMetricName, labels.toArray(String[]::new));
    }
    ArrayList<String> labelValues = new ArrayList<>(labelsMap.values());
    getMetricCounter(prometheusMetricName)
        .labelValues(labelValues.toArray(String[]::new))
        .inc(dropwizardMetric.getCount());
  }

  public void exportCounter(
      com.codahale.metrics.Counter dropwizardMetric,
      String prometheusMetricName,
      Map<String, String> labelsMap) {
    if (!metricCounters.containsKey(prometheusMetricName)) {
      ArrayList<String> labels = new ArrayList<>(labelsMap.keySet());
      registerCounter(prometheusMetricName, labels.toArray(String[]::new));
    }
    ArrayList<String> labelValues = new ArrayList<>(labelsMap.values());
    getMetricCounter(prometheusMetricName)
        .labelValues(labelValues.toArray(String[]::new))
        .inc(dropwizardMetric.getCount());
  }

  public void exportTimer(
      Timer dropwizardMetric, String prometheusMetricName, Map<String, String> labelsMap) {
    if (!metricGauges.containsKey(prometheusMetricName)) {
      ArrayList<String> labels = new ArrayList<>(labelsMap.keySet());
      registerGauge(prometheusMetricName, labels.toArray(String[]::new));
    }
    ArrayList<String> labelValues = new ArrayList<>(labelsMap.values());
    getMetricGauge(prometheusMetricName)
        .labelValues(labelValues.toArray(String[]::new))
        .set(dropwizardMetric.getMeanRate());
  }

  public void exportGauge(
      com.codahale.metrics.Gauge<?> dropwizardMetricRaw,
      String prometheusMetricName,
      Map<String, String> labelsMap) {
    Object dropwizardMetric = (dropwizardMetricRaw).getValue();
    if (!metricGauges.containsKey(prometheusMetricName)) {
      ArrayList<String> labels = new ArrayList<>(labelsMap.keySet());
      if (dropwizardMetric instanceof HashMap) {
        labels.add("item");
      }
      registerGauge(prometheusMetricName, labels.toArray(String[]::new));
    }
    ArrayList<String> labelValues = new ArrayList<>(labelsMap.values());
    String[] labels = labelValues.toArray(String[]::new);
    if (dropwizardMetric instanceof Number) {
      getMetricGauge(prometheusMetricName)
          .labelValues(labels)
          .set(((Number) dropwizardMetric).doubleValue());
    } else if (dropwizardMetric instanceof HashMap) {
      HashMap<?, ?> itemsMap = (HashMap<?, ?>) dropwizardMetric;
      for (Object item : itemsMap.keySet()) {
        if (itemsMap.get(item) instanceof Number) {
          String[] newLabels = new String[labels.length + 1];
          System.arraycopy(labels, 0, newLabels, 0, labels.length);
          newLabels[labels.length] = (String) item;
          getMetricGauge(prometheusMetricName)
              .labelValues(newLabels)
              .set(((Number) itemsMap.get(item)).doubleValue());
        }
      }
    }
  }

  private Counter getMetricCounter(String metricName) {
    return metricCounters.get(metricName);
  }

  private Gauge getMetricGauge(String metricName) {
    return metricGauges.get(metricName);
  }

  private void registerCounter(String metricName, String... labelNames) {
    Counter counter =
        io.prometheus.metrics.core.metrics.Counter.builder()
            .name(metricName)
            .labelNames(labelNames)
            .register(prometheusRegistry);
    metricCounters.put(metricName, counter);
  }

  private void registerGauge(String metricName, String... labelNames) {
    Gauge gauge =
        io.prometheus.metrics.core.metrics.Gauge.builder()
            .name(metricName)
            .labelNames(labelNames)
            .register(prometheusRegistry);
    metricGauges.put(metricName, gauge);
  }
}
