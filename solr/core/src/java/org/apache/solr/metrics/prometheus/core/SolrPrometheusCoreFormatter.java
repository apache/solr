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

import com.codahale.metrics.Metric;
import com.google.common.base.Enums;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrNoOpMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusFormatter;

/**
 * This class maintains a {@link io.prometheus.metrics.model.snapshots.MetricSnapshot}s exported
 * from solr.core {@link com.codahale.metrics.MetricRegistry}
 */
public class SolrPrometheusCoreFormatter extends SolrPrometheusFormatter
    implements PrometheusCoreFormatterInfo {

  public SolrPrometheusCoreFormatter() {
    super();
  }

  @Override
  public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {
    SolrMetric solrCoreMetric = categorizeMetric(dropwizardMetric, metricName);
    solrCoreMetric.parseLabels().toPrometheus(this);
  }

  @Override
  public SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName) {
    String metricCategory = metricName.split("\\.", 3)[1];
    if (!Enums.getIfPresent(PrometheusCoreFormatterInfo.CoreCategory.class, metricCategory)
        .isPresent()) {
      return new SolrNoOpMetric();
    }
    switch (CoreCategory.valueOf(metricCategory)) {
      case ADMIN:
      case QUERY:
      case UPDATE:
      case REPLICATION:
        return new SolrCoreHandlerMetric(dropwizardMetric, metricName);
      case TLOG:
        return new SolrCoreTlogMetric(dropwizardMetric, metricName);
      case CACHE:
        return new SolrCoreCacheMetric(dropwizardMetric, metricName);
      case SEARCHER:
        return new SolrCoreSearcherMetric(dropwizardMetric, metricName);
      case HIGHLIGHTER:
        return new SolrCoreHighlighterMetric(dropwizardMetric, metricName);
      case INDEX:
        return new SolrCoreIndexMetric(dropwizardMetric, metricName);
      case CORE:
      default:
        return new SolrNoOpMetric();
    }
  }
}
