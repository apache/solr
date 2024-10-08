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

import com.codahale.metrics.Metric;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class is a wrapper to categorize and export {@link com.codahale.metrics.Metric} to {@link
 * io.prometheus.metrics.model.snapshots.DataPointSnapshot} and register to a {@link
 * SolrPrometheusExporter}. {@link com.codahale.metrics.MetricRegistry} does not support tags unlike
 * prometheus. Metrics registered to the registry need to be parsed out from the metric name to be
 * exported to {@link io.prometheus.metrics.model.snapshots.DataPointSnapshot}
 */
public abstract class SolrMetric {
  public Metric dropwizardMetric;
  public String metricName;
  public Map<String, String> labels = new HashMap<>();

  public SolrMetric() {}

  public SolrMetric(Metric dropwizardMetric, String metricName) {
    this.dropwizardMetric = dropwizardMetric;
    this.metricName = metricName;
  }

  /*
   * Parse labels from the Dropwizard Metric name to be exported
   */
  public abstract SolrMetric parseLabels();

  /*
   * Export metric to Prometheus with labels
   */
  public abstract void toPrometheus(SolrPrometheusExporter exporter);

  public Labels getLabels() {
    return Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values()));
  }
}
