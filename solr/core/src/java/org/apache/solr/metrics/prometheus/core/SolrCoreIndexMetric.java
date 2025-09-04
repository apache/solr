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

import com.codahale.metrics.*;
import org.apache.solr.metrics.prometheus.SolrPrometheusFormatter;

/** Dropwizard metrics of name INDEX.* */
public class SolrCoreIndexMetric extends SolrCoreMetric {
  public static final String CORE_INDEX_METRICS = "solr_metrics_core_index_size_bytes";

  public static final String CORE_INDEX_FLUSH_METRICS = "solr_metrics_core_index_flush";
  public static final String CORE_INDEX_MERGE_ERRORS_METRICS = "solr_metrics_core_index_merge_errors";

  public static final String CORE_INDEX_MERGE_METRICS = "solr_metrics_core_index_merges";

  // these are only for major merges with more details enabled
  public static final String CORE_INDEX_MERGE_DOCS_METRICS = "solr_metrics_core_index_merged_docs";
  public static final String CORE_INDEX_MERGE_DELETED_DOCS_METRICS = "solr_metrics_core_index_merged_deleted_docs";

  public static final String CORE_INDEX_MERGING_METRICS = "solr_metrics_core_index_merges_running";
  public static final String CORE_INDEX_MERGING_DOCS_METRICS = "solr_metrics_core_index_merging_docs";
  public static final String CORE_INDEX_MERGING_SEGMENTS_METRICS = "solr_metrics_core_index_merging_segments";

  public SolrCoreIndexMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
  }

  /*
   * Metric examples being exported
   * INDEX.sizeInBytes
   *
   * INDEX.merge.errors
   * INDEX.merge.minor
   * INDEX.merge.minor.running
   * INDEX.merge.minor.running.docs
   * INDEX.merge.minor.running.segments
   * INDEX.merge.major
   * INDEX.merge.major.docs
   * INDEX.merge.major.deletedDocs
   * INDEX.merge.major.running
   * INDEX.merge.major.running.docs
   * INDEX.merge.major.running.segments
   */

  @Override
  public SolrCoreMetric parseLabels() {
    String[] parsedMetric = metricName.split("\\.");
    if (parsedMetric.length < 3) {
      return this;
    }
    if (parsedMetric[1].equals("merge")) {
      labels.put("type", parsedMetric[2]); // minor / major
      if (parsedMetric.length > 3) {
        labels.put("running", "true");
      } else {
        labels.put("running", "false");
      }
    }
    return this;
  }

  @Override
  public void toPrometheus(SolrPrometheusFormatter formatter) {
    if (dropwizardMetric instanceof Gauge) {
      if (metricName.endsWith("sizeInBytes")) {
        formatter.exportGauge(CORE_INDEX_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("running")) {
        formatter.exportGauge(CORE_INDEX_MERGING_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("segments")) {
        formatter.exportGauge(CORE_INDEX_MERGING_SEGMENTS_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("docs")) {
        formatter.exportGauge(CORE_INDEX_MERGING_DOCS_METRICS, (Gauge<?>) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Timer) {
      if (metricName.endsWith("minor") || metricName.endsWith("major")) {
        formatter.exportTimer(CORE_INDEX_MERGE_METRICS, (Timer) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Counter) {
      if (metricName.endsWith("errors")) {
        formatter.exportCounter(CORE_INDEX_MERGE_ERRORS_METRICS, (Counter) dropwizardMetric, getLabels());
      }
    } else if (dropwizardMetric instanceof Meter) {
      if (metricName.endsWith("flush")) {
        formatter.exportMeter(CORE_INDEX_FLUSH_METRICS, (Meter) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("docs")) {
        formatter.exportMeter(CORE_INDEX_MERGE_DOCS_METRICS, (Meter) dropwizardMetric, getLabels());
      } else if (metricName.endsWith("deletedDocs")) {
        formatter.exportMeter(CORE_INDEX_MERGE_DELETED_DOCS_METRICS, (Meter) dropwizardMetric, getLabels());
      }
    }
  }
}
