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
package org.apache.solr.metrics.prometheus.jvm;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/* Dropwizard metrics of name gc.* */
public class SolrJvmGcMetrics extends SolrJvmMetric {
  public static final String JVM_GC = "solr_metrics_jvm_gc";
  public static final String JVM_GC_SECONDS = "solr_metrics_jvm_gc_seconds";

  public SolrJvmGcMetrics(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("item", parsedMetric[1]);
    return this;
  }

  /*
   * Metric examples being exported
   * gc.G1-Old-Generation.time
   * gc.G1-Young-Generation.count
   */
  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Gauge) {
      if (metricName.endsWith(".count")) {
        exporter.exportGauge(JVM_GC, (Gauge<?>) dropwizardMetric, getLabels());
      } else if (metricName.endsWith(".time")) {
        exporter.exportGauge(JVM_GC_SECONDS, (Gauge<?>) dropwizardMetric, getLabels());
      }
    }
  }
}
