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

import static org.apache.solr.metrics.prometheus.PrometheusCoreExporterInfo.CLOUD_CORE_PATTERN;

import com.codahale.metrics.Metric;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;

/**
 * Base class is a wrapper to categorize and export {@link com.codahale.metrics.Metric} to {@link
 * io.prometheus.metrics.model.snapshots.DataPointSnapshot} and register to a {@link
 * SolrPrometheusCoreExporter}. {@link com.codahale.metrics.MetricRegistry} does not support tags
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
      appendCloudModeLabels();
    }
  }

  public Labels getLabels() {
    return Labels.of(new ArrayList<>(labels.keySet()), new ArrayList<>(labels.values()));
  }

  public abstract SolrCoreMetric parseLabels();

  public abstract void toPrometheus(SolrPrometheusCoreExporter exporter);

  private void appendCloudModeLabels() {
    Matcher m = CLOUD_CORE_PATTERN.matcher(coreName);
    if (m.find()) {
      labels.put("collection", m.group(1));
      labels.put("shard", m.group(2));
      labels.put("replica", m.group(3));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Core name does not match pattern for parsing " + coreName);
    }
  }
}
