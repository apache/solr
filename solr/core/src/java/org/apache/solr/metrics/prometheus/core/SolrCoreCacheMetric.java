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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.ArrayList;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;

/** Dropwizard metrics of name CACHE.* */
public class SolrCoreCacheMetric extends SolrCoreMetric {
  public static final String CORE_CACHE_SEARCHER_METRICS = "solr_metrics_core_cache";

  public SolrCoreCacheMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, coreName, metricName, cloudMode);
  }

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    if (dropwizardMetric instanceof Gauge) {
      String cacheType = parsedMetric[2];
      labels.put("cacheType", cacheType);
    }
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusCoreExporter solrPrometheusCoreRegistry) {
    if (dropwizardMetric instanceof Gauge) {
      solrPrometheusCoreRegistry.exportGauge(
          CORE_CACHE_SEARCHER_METRICS,
          (Gauge<?>) dropwizardMetric,
          Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values())));
    }
  }
}
