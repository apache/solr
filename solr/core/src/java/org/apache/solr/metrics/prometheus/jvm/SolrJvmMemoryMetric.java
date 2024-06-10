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

/* Dropwizard metrics of name gc.* and memory.* */
public class SolrJvmMemoryMetric extends SolrJvmMetric {
  public static String JVM_MEMORY_POOL_BYTES = "solr_metrics_jvm_memory_pools_bytes";
  public static String JVM_MEMORY = "solr_metrics_jvm_heap";

  public SolrJvmMemoryMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("item", parsedMetric[parsedMetric.length - 1]);
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    String[] parsedMetric = metricName.split("\\.");
    if (dropwizardMetric instanceof Gauge) {
      String metricType = parsedMetric[1];
      switch (metricType) {
        case "heap":
        case "non-heap":
        case "total":
          labels.put("memory", parsedMetric[1]);
          exporter.exportGauge(JVM_MEMORY, (Gauge<?>) dropwizardMetric, getLabels());
          break;
        case "pools":
          labels.put("space", parsedMetric[2]);
          exporter.exportGauge(JVM_MEMORY_POOL_BYTES, (Gauge<?>) dropwizardMetric, getLabels());
          break;
      }
    }
  }
}
