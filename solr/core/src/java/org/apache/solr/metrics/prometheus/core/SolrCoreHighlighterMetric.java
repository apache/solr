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
import com.codahale.metrics.Metric;
import java.util.Map;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;

/** Dropwizard metrics of name HIGHLIGHTER.* */
public class SolrCoreHighlighterMetric extends SolrCoreMetric {
  public static final String CORE_HIGHLIGHER_METRICS = "solr_metrics_core_highlighter_requests";

  public SolrCoreHighlighterMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, coreName, metricName, cloudMode);
  }

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    String type = parsedMetric[1];
    String item = parsedMetric[2];
    labels.putAll(Map.of("type", type, "item", item));
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusCoreExporter solrPrometheusCoreExporter) {
    if (dropwizardMetric instanceof Counter) {
      solrPrometheusCoreExporter.exportCounter(
          CORE_HIGHLIGHER_METRICS, (Counter) dropwizardMetric, getLabels());
    }
  }
}
