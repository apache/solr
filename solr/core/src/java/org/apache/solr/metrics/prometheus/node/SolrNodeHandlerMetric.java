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
package org.apache.solr.metrics.prometheus.node;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/* Dropwizard metrics of name ADMIN.* and UPDATE.* */
public class SolrNodeHandlerMetric extends SolrNodeMetric {
  public static final String NODE_REQUESTS = "solr_metrics_node_requests";
  public static final String NODE_SECONDS_TOTAL = "solr_metrics_node_requests_time";
  public static final String NODE_CONNECTIONS = "solr_metrics_node_connections";

  public SolrNodeHandlerMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("category", parsedMetric[0]);
    labels.put("handler", parsedMetric[1]);
    labels.put("type", parsedMetric[2]);
    return this;
  }

  /*
   * Metric examples being exported
   * ADMIN./admin/collections.requests
   * UPDATE.updateShardHandler.maxConnections
   */
  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Meter) {
      exporter.exportMeter(NODE_REQUESTS, (Meter) dropwizardMetric, getLabels());
    } else if (dropwizardMetric instanceof Counter) {
      if (metricName.endsWith(".requests")) {
        exporter.exportCounter(NODE_REQUESTS, (Counter) dropwizardMetric, getLabels());
      } else if (metricName.endsWith(".totalTime")) {
        // Do not need type label for total time
        labels.remove("type");
        exporter.exportCounter(NODE_SECONDS_TOTAL, (Counter) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Gauge) {
      if (metricName.endsWith("Connections")) {
        exporter.exportGauge(NODE_CONNECTIONS, (Gauge<?>) dropwizardMetric, getLabels());
      }
    }
  }
}
