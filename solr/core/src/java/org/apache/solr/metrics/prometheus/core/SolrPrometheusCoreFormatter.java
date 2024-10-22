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
  public final String coreName;
  public final boolean cloudMode;

  public SolrPrometheusCoreFormatter(String coreName, boolean cloudMode) {
    super();
    this.coreName = coreName;
    this.cloudMode = cloudMode;
  }

  @Override
  public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {
    SolrMetric solrCoreMetric = categorizeMetric(dropwizardMetric, metricName);
    solrCoreMetric.parseLabels().toPrometheus(this);
  }

  @Override
  public SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName) {
    String metricCategory = metricName.split("\\.", 2)[0];
    if (!Enums.getIfPresent(PrometheusCoreFormatterInfo.CoreCategory.class, metricCategory)
        .isPresent()) {
      return new SolrNoOpMetric();
    }
    switch (CoreCategory.valueOf(metricCategory)) {
      case ADMIN:
      case QUERY:
      case UPDATE:
      case REPLICATION:
        return new SolrCoreHandlerMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case TLOG:
        return new SolrCoreTlogMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case CACHE:
        return new SolrCoreCacheMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case SEARCHER:
        return new SolrCoreSearcherMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case HIGHLIGHTER:
        return new SolrCoreHighlighterMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case INDEX:
        return new SolrCoreIndexMetric(dropwizardMetric, coreName, metricName, cloudMode);
      case CORE:
      default:
        return new SolrNoOpMetric();
    }
  }
}
