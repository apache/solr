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

/* Dropwizard metrics of name os.* and threads.* */
public class SolrJvmOsMetric extends SolrJvmMetric {
  public static final String JVM_OS_THREADS = "solr_metrics_jvm_threads";
  public static final String JVM_OS = "solr_metrics_os";

  public SolrJvmOsMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  /*
   * Metric examples being exported
   * os.availableProcessors
   * threads.peak.count
   */

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    if (parsedMetric[0].equals("threads")) {
      labels.put("item", parsedMetric[1]);
    } else {
      labels.put("item", parsedMetric[parsedMetric.length - 1]);
    }
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (metricName.startsWith("threads.")) {
      exporter.exportGauge(JVM_OS_THREADS, (Gauge<?>) dropwizardMetric, getLabels());
    } else {
      exporter.exportGauge(JVM_OS, (Gauge<?>) dropwizardMetric, getLabels());
    }
  }
}
