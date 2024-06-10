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

import static org.apache.solr.metrics.prometheus.core.SolrCoreCacheMetric.CORE_CACHE_SEARCHER_METRICS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/** Dropwizard metrics of name SEARCHER.* */
public class SolrCoreSearcherMetric extends SolrCoreMetric {
  public static final String CORE_SEARCHER_METRICS = "solr_metrics_core_searcher_documents";
  public static final String CORE_SEARCHER_TIMES = "solr_metrics_core_average_searcher_warmup_time";

  public SolrCoreSearcherMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, coreName, metricName, cloudMode);
  }

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    if (!(dropwizardMetric instanceof Counter)) {
      labels.put("type", parsedMetric[2]);
    }
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusExporter exporter) {
    if (dropwizardMetric instanceof Gauge) {
      if (metricName.endsWith("liveDocsCache")) {
        exporter.exportGauge(CORE_CACHE_SEARCHER_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      } else {
        exporter.exportGauge(CORE_SEARCHER_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Timer) {
      exporter.exportTimer(CORE_SEARCHER_TIMES, (Timer) dropwizardMetric, getLabels());
    }
  }
}
