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
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreRegistry;

public class SolrCoreHandlerMetric extends SolrCoreMetric {
  public static final String CORE_REQUESTS_TOTAL = "solr_metrics_core_requests";
  public static final String CORE_REQUESTS_UPDATE_HANDLER = "solr_metrics_core_update_handler";
  public static final String CORE_REQUESTS_TOTAL_TIME = "solr_metrics_core_requests_time";
  public static final String CORE_REQUEST_TIMES = "solr_metrics_core_average_request_time";

  public SolrCoreHandlerMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, coreName, metricName, cloudMode);
  }

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    String category = parsedMetric[0];
    String handler = parsedMetric[1];
    String type = parsedMetric[2];
    labels.put("category", category);
    labels.put("type", type);
    labels.put("handler", handler);
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusCoreRegistry solrPrometheusCoreRegistry) {
    if (dropwizardMetric instanceof Meter) {
      solrPrometheusCoreRegistry.exportMeter(CORE_REQUESTS_TOTAL, (Meter) dropwizardMetric, labels);
    } else if (dropwizardMetric instanceof Counter) {
      if (metricName.endsWith("requests")) {
        solrPrometheusCoreRegistry.exportCounter(
            CORE_REQUESTS_TOTAL, (Counter) dropwizardMetric, labels);
      } else if (metricName.endsWith("totalTime")) {
        // Do not need type label for total time
        labels.remove("type");
        solrPrometheusCoreRegistry.exportCounter(
            CORE_REQUESTS_TOTAL_TIME, (Counter) dropwizardMetric, labels);
      }
    } else if (dropwizardMetric instanceof Gauge) {
      if (!metricName.endsWith("handlerStart")) {
        solrPrometheusCoreRegistry.exportGauge(
            CORE_REQUESTS_UPDATE_HANDLER, (Gauge<?>) dropwizardMetric, labels);
      }
    } else if (dropwizardMetric instanceof Timer) {
      // Do not need type label for request times
      labels.remove("type");
      solrPrometheusCoreRegistry.exportTimer(CORE_REQUEST_TIMES, (Timer) dropwizardMetric, labels);
    }
  }
}
