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
package org.apache.solr.metrics.prometheus.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/** Dropwizard metrics of name ADMIN/QUERY/UPDATE/REPLICATION.* */
public class SolrCoreHandlerMetric extends SolrCoreMetric {
  public static final String CORE_REQUESTS_TOTAL = "solr_metrics_core_requests";
  public static final String CORE_REQUESTS_UPDATE_HANDLER = "solr_metrics_core_update_handler";
  public static final String CORE_REQUESTS_TOTAL_TIME = "solr_metrics_core_requests_time";
  public static final String CORE_REQUEST_TIMES = "solr_metrics_core_average_request_time";

  public SolrCoreHandlerMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, coreName, metricName, cloudMode);
  }

  /*
   * Metric examples being exported
   *
   * QUERY./select.totalTime
   * UPDATE./update.requests
   * UPDATE./update.totalTime
   */

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("category", parsedMetric[0]);
    labels.put("handler", parsedMetric[1]);
    labels.put("type", parsedMetric[2]);
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Meter) {
      exporter.exportMeter(CORE_REQUESTS_TOTAL, (Meter) dropwizardMetric, getLabels());
    } else if (dropwizardMetric instanceof Counter) {
      if (metricName.endsWith("requests")) {
        exporter.exportCounter(CORE_REQUESTS_TOTAL, (Counter) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("totalTime")) {
        // Do not need type label for total time
        labels.remove("type");
        exporter.exportCounter(CORE_REQUESTS_TOTAL_TIME, (Counter) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Gauge) {
      if (!metricName.endsWith("handlerStart")) {
        exporter.exportGauge(
            CORE_REQUESTS_UPDATE_HANDLER, (Gauge<?>) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Timer) {
      // Do not need type label for request times
      labels.remove("type");
      exporter.exportTimer(CORE_REQUEST_TIMES, (Timer) dropwizardMetric, getLabels());
    }
  }
}
