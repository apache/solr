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
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreRegistry;

/**
 * Base class is a wrapper to categorize and export {@link com.codahale.metrics.Metric} to {@link
 * io.prometheus.metrics.model.snapshots.DataPointSnapshot} and register to a {@link
 * SolrPrometheusCoreRegistry}. {@link com.codahale.metrics.MetricRegistry} does not support tags
 * unlike prometheus. Metrics registered to the registry need to be parsed out from the metric name
 * to be exported to {@link io.prometheus.metrics.model.snapshots.DataPointSnapshot}
 */
public abstract class SolrCoreMetric {
  public Metric dropwizardMetric;
  public String coreName;
  public String metricName;
  public Map<String, String> labels = new HashMap<>();

  public SolrCoreMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    this.dropwizardMetric = dropwizardMetric;
    this.coreName = coreName;
    this.metricName = metricName;
    labels.put("core", coreName);
    if (cloudMode) {
      String[] coreNameParsed = coreName.split("_");
      labels.put("collection", coreNameParsed[1]);
      labels.put("shard", coreNameParsed[2]);
      labels.put("replica", coreNameParsed[3] + "_" + coreNameParsed[4]);
    }
  }

  public abstract SolrCoreMetric parseLabels();

  public abstract void toPrometheus(SolrPrometheusCoreRegistry solrPrometheusCoreRegistry);
}
