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

import com.codahale.metrics.Metric;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrNoOpMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/**
 * This class maintains a {@link io.prometheus.metrics.model.snapshots.MetricSnapshot}s exported
 * from solr.jetty {@link com.codahale.metrics.MetricRegistry}
 */
public class SolrPrometheusJettyExporter extends SolrPrometheusExporter {
  public SolrPrometheusJettyExporter() {
    super();
  }

  @Override
  public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {
    SolrMetric solrJettyMetric = categorizeMetric(dropwizardMetric, metricName);
    solrJettyMetric.parseLabels().toPrometheus(this);
  }

  @Override
  public SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName) {
    if (metricName.endsWith("xx-responses") || metricName.endsWith("-requests")) {
      return new SolrJettyReqRespMetric(dropwizardMetric, metricName);
    } else if (metricName.endsWith(".dispatches")) {
      return new SolrJettyDispatchesMetric(dropwizardMetric, metricName);
    } else {
      return new SolrNoOpMetric();
    }
  }
}
