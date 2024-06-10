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

import static org.apache.solr.metrics.prometheus.node.SolrNodeMetric.NODE_THREAD_POOL;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.base.Enums;
import io.prometheus.metrics.model.snapshots.Labels;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrNoOpMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;

/**
 * This class maintains a {@link io.prometheus.metrics.model.snapshots.MetricSnapshot}s exported
 * from solr.node {@link com.codahale.metrics.MetricRegistry}
 */
public class SolrPrometheusNodeExporter extends SolrPrometheusExporter
    implements PrometheusNodeExporterInfo {
  public SolrPrometheusNodeExporter() {
    super();
  }

  @Override
  public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {
    if (metricName.contains(".threadPool.")) {
      exportThreadPoolMetric(dropwizardMetric, metricName);
      return;
    }

    SolrMetric solrNodeMetric = categorizeMetric(dropwizardMetric, metricName);
    solrNodeMetric.parseLabels().toPrometheus(this);
  }

  @Override
  public SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName) {
    String metricCategory = metricName.split("\\.", 2)[0];
    if (!Enums.getIfPresent(PrometheusNodeExporterInfo.NodeCategory.class, metricCategory)
        .isPresent()) {
      return new SolrNoOpMetric();
    }
    switch (NodeCategory.valueOf(metricCategory)) {
      case ADMIN:
      case UPDATE:
        return new SolrNodeHandlerMetric(dropwizardMetric, metricName);
      case CONTAINER:
        return new SolrNodeContainerMetric(dropwizardMetric, metricName);
      default:
        return new SolrNoOpMetric();
    }
  }

  private void exportThreadPoolMetric(Metric dropwizardMetric, String metricName) {
    Labels labels;
    String[] parsedMetric = metricName.split("\\.");
    if (parsedMetric.length >= 5) {
      labels =
          Labels.of(
              "category",
              parsedMetric[0],
              "handler",
              parsedMetric[1],
              "executer",
              parsedMetric[3],
              "task",
              parsedMetric[parsedMetric.length - 1]);
    } else {
      labels =
          Labels.of(
              "category",
              parsedMetric[0],
              "executer",
              parsedMetric[2],
              "task",
              parsedMetric[parsedMetric.length - 1]);
    }
    if (dropwizardMetric instanceof Counter) {
      exportCounter(NODE_THREAD_POOL, (Counter) dropwizardMetric, labels);
    } else if (dropwizardMetric instanceof Meter) {
      exportMeter(NODE_THREAD_POOL, (Meter) dropwizardMetric, labels);
    }
  }
}
