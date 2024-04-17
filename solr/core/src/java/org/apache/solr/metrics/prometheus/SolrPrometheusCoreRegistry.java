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
package org.apache.solr.metrics.prometheus;

import com.codahale.metrics.Metric;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.solr.metrics.prometheus.core.SolrCoreCacheMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreHandlerMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreHighlighterMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreIndexMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreNoOpMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreSearcherMetric;
import org.apache.solr.metrics.prometheus.core.SolrCoreTlogMetric;

public class SolrPrometheusCoreRegistry extends SolrPrometheusRegistry {
  public final String coreName;
  public final boolean cloudMode;
  public static final String ADMIN = "ADMIN";
  public static final String QUERY = "QUERY";
  public static final String UPDATE = "UPDATE";
  public static final String REPLICATION = "REPLICATION";
  public static final String TLOG = "TLOG";
  public static final String CACHE = "CACHE";
  public static final String SEARCHER = "SEARCHER";
  public static final String HIGHLIGHTER = "HIGHLIGHTER";
  public static final String INDEX = "INDEX";
  public static final String CORE = "CORE";

  public SolrPrometheusCoreRegistry(
      PrometheusRegistry prometheusRegistry, String coreName, boolean cloudMode) {
    super(prometheusRegistry);
    this.coreName = coreName;
    this.cloudMode = cloudMode;
  }

  public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {
    SolrCoreMetric solrCoreMetric = categorizeCoreMetric(dropwizardMetric, metricName);
    solrCoreMetric.parseLabels().toPrometheus(this);
  }

  private SolrCoreMetric categorizeCoreMetric(Metric dropwizardMetric, String metricName) {
    String metricCategory = metricName.split("\\.")[0];
    switch (metricCategory) {
      case ADMIN:
      case QUERY:
      case UPDATE:
      case REPLICATION:
        {
          return new SolrCoreHandlerMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case TLOG:
        {
          return new SolrCoreTlogMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case CACHE:
        {
          return new SolrCoreCacheMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case SEARCHER:
        {
          return new SolrCoreSearcherMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case HIGHLIGHTER:
        {
          return new SolrCoreHighlighterMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case INDEX:
        {
          return new SolrCoreIndexMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
      case CORE:
      default:
        {
          return new SolrCoreNoOpMetric(dropwizardMetric, coreName, metricName, cloudMode);
        }
    }
  }
}
