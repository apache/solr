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
package org.apache.solr.metrics.prometheus.jetty;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/* Dropwizard metrics of name *.xx-responses and *-requests */
public class SolrJettyReqRespMetric extends SolrJettyMetric {
  public static final String JETTY_RESPONSES_TOTAL = "solr_metrics_jetty_response";
  public static final String JETTY_REQUESTS_TOTAL = "solr_metrics_jetty_requests";

  public SolrJettyReqRespMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    String label = parsedMetric[parsedMetric.length - 1].split("-")[0];
    if (metricName.endsWith("xx-responses")) {
      labels.put("status", label);
    } else {
      labels.put("method", label);
    }
    return this;
  }

  /*
   * Metric examples being exported
   * org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses
   * org.eclipse.jetty.server.handler.DefaultHandler.get-requests
   */
  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Meter) {
      if (metricName.endsWith("xx-responses")) {
        exporter.exportMeter(JETTY_RESPONSES_TOTAL, (Meter) dropwizardMetric, getLabels());
      }
    } else if (metricName.endsWith("-requests")) {
      if (dropwizardMetric instanceof Counter) {
        exporter.exportCounter(JETTY_REQUESTS_TOTAL, (Counter) dropwizardMetric, getLabels());
      } else if (dropwizardMetric instanceof Timer) {
        exporter.exportTimerCount(JETTY_REQUESTS_TOTAL, (Timer) dropwizardMetric, getLabels());
      }
    }
  }
}
