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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusFormatter;

/* Dropwizard metrics of name CONTAINER.* */
public class SolrNodeContainerMetric extends SolrNodeMetric {
  public static final String NODE_CORES = "solr_metrics_node_cores";
  public static final String NODE_CORE_FS_BYTES = "solr_metrics_node_core_root_fs_bytes";

  public SolrNodeContainerMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  /*
   * Metric examples being exported
   * CONTAINER.fs.coreRoot.totalSpace
   * CONTAINER.cores.loaded
   */

  @Override
  public SolrMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    labels.put("category", parsedMetric[0]);
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusFormatter formatter) {
    String[] parsedMetric = metricName.split("\\.");
    if (metricName.startsWith("CONTAINER.cores.")) {
      labels.put("item", parsedMetric[2]);
      formatter.exportGauge(NODE_CORES, (Gauge<?>) dropwizardMetric, getLabels());
    } else if (metricName.startsWith("CONTAINER.fs.coreRoot.")) {
      labels.put("item", parsedMetric[3]);
      formatter.exportGauge(NODE_CORE_FS_BYTES, (Gauge<?>) dropwizardMetric, getLabels());
    }
  }
}
