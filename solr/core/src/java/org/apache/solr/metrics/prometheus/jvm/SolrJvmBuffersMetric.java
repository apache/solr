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
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/* Dropwizard metrics of name buffers.* */
public class SolrJvmBuffersMetric extends SolrJvmMetric {
  public static final String JVM_BUFFERS = "solr_metrics_jvm_buffers";
  public static final String JVM_BUFFERS_BYTES = "solr_metrics_jvm_buffers_bytes";

  public SolrJvmBuffersMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  @Override
  public SolrJvmMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("pool", parsedMetric[1]);
    labels.put("item", parsedMetric[2]);
    return this;
  }

  /*
   * Metric examples being exported
   * buffers.direct.MemoryUsed
   * buffers.mapped.Count
   */
  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Gauge) {
      String[] parsedMetric = metricName.split("\\.");
      String metricType = parsedMetric[parsedMetric.length - 1];
      if (metricType.equals("Count")) {
        exporter.exportGauge(JVM_BUFFERS, (Gauge<?>) dropwizardMetric, getLabels());
      } else if (metricType.equals(("MemoryUsed")) || metricType.equals(("TotalCapacity"))) {
        exporter.exportGauge(JVM_BUFFERS_BYTES, (Gauge<?>) dropwizardMetric, getLabels());
      }
    }
  }
}
