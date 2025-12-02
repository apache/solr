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
package org.apache.solr.util;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

public final class SolrMetricTestUtils {

  private static final int MAX_ITERATIONS = 100;
  private static final SolrInfoBean.Category CATEGORIES[] = SolrInfoBean.Category.values();

  // Cache name constants
  public static final String QUERY_RESULT_CACHE = "queryResultCache";
  public static final String FILTER_CACHE = "filterCache";
  public static final String DOCUMENT_CACHE = "documentCache";
  public static final String PER_SEG_FILTER_CACHE = "perSegFilter";

  public static String getRandomScope(Random random) {
    return getRandomScope(random, random.nextBoolean());
  }

  public static String getRandomScope(Random random, boolean shouldDefineScope) {
    return shouldDefineScope
        ? TestUtil.randomSimpleString(random, 5, 10)
        : null; // must be simple string for JMX publishing
  }

  public static SolrInfoBean.Category getRandomCategory(Random random) {
    return CATEGORIES[TestUtil.nextInt(random, 0, CATEGORIES.length - 1)];
  }

  public static Map<String, Long> getRandomPrometheusMetricsWithReplacements(
      Random random, Map<String, Long> existing) {
    HashMap<String, Long> metrics = new HashMap<>();
    List<String> existingKeys = List.copyOf(existing.keySet());

    int numMetrics = TestUtil.nextInt(random, 1, MAX_ITERATIONS);
    for (int i = 0; i < numMetrics; ++i) {
      boolean shouldReplaceMetric = !existing.isEmpty() && random.nextBoolean();
      String name =
          shouldReplaceMetric
              ? existingKeys.get(TestUtil.nextInt(random, 0, existingKeys.size() - 1))
              : TestUtil.randomSimpleString(random, 5, 10)
                  + SUFFIX; // must be simple string for JMX publishing

      Long incrementValue = TestUtil.nextLong(random, 1L, 1000L);
      metrics.put(name, incrementValue);
    }

    return metrics;
  }

  public static final String SUFFIX = "_testing";

  /**
   * Looks up the first {@link MetricSnapshot} named {@code metricName}, and returns the first
   * {@link DataPointSnapshot} having exactly these {@code labels}. Null if not found. The result is
   * typically casted to something useful.
   */
  public static DataPointSnapshot getDataPointSnapshot(
      PrometheusMetricReader reader, String metricName, Labels labels) {
    MetricSnapshots metricSnapshots = reader.collect();
    return metricSnapshots.stream()
        .filter(ms -> ms.getMetadata().getPrometheusName().equals(metricName))
        .findFirst()
        .flatMap(
            ms ->
                ms.getDataPoints().stream().filter(dp -> dp.getLabels().equals(labels)).findFirst())
        .orElse(null);
  }

  public static Labels.Builder newCloudLabelsBuilder(SolrCore core) {
    return Labels.builder()
        .label("collection", core.getCoreDescriptor().getCloudDescriptor().getCollectionName())
        .label("shard", core.getCoreDescriptor().getCloudDescriptor().getShardId())
        .label("core", core.getName())
        .label(
            "replica_type",
            core.getCoreDescriptor().getCloudDescriptor().getReplicaType().toString())
        .label("otel_scope_name", "org.apache.solr");
  }

  public static Labels.Builder newStandaloneLabelsBuilder(SolrCore core) {
    return newStandaloneLabelsBuilder(core.getName());
  }

  public static Labels.Builder newStandaloneLabelsBuilder(String coreName) {
    return Labels.builder().label("core", coreName).label("otel_scope_name", "org.apache.solr");
  }

  public static PrometheusMetricReader getPrometheusMetricReader(SolrCore core) {
    return getPrometheusMetricReader(
        core.getCoreContainer(), core.getCoreMetricManager().getRegistryName());
  }

  public static PrometheusMetricReader getPrometheusMetricReader(
      CoreContainer container, String registryName) {
    return container.getMetricManager().getPrometheusMetricReader(registryName);
  }

  public static <S extends MetricSnapshot> S getMetricSnapshot(
      Class<S> snapshotClass, MetricSnapshots metrics, String name) {
    return metrics.stream()
        .filter(m -> m.getMetadata().getPrometheusName().equals(name))
        .map(snapshotClass::cast)
        .findFirst()
        .get();
  }

  private static <T> T getDatapoint(
      SolrCore core, String metricName, Labels labels, Class<T> snapshotType) {
    var reader = getPrometheusMetricReader(core);
    return getDataPoint(reader, metricName, labels, snapshotType);
  }

  private static <T> T getDataPoint(
      PrometheusMetricReader reader, String metricName, Labels labels, Class<T> snapshotType) {
    return snapshotType.cast(SolrMetricTestUtils.getDataPointSnapshot(reader, metricName, labels));
  }

  public static GaugeSnapshot.GaugeDataPointSnapshot getGaugeDatapoint(
      SolrCore core, String metricName, Labels labels) {
    return getDatapoint(core, metricName, labels, GaugeSnapshot.GaugeDataPointSnapshot.class);
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCounterDatapoint(
      SolrCore core, String metricName, Labels labels) {
    return getDatapoint(core, metricName, labels, CounterSnapshot.CounterDataPointSnapshot.class);
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCounterDatapoint(
      PrometheusMetricReader reader, String metricName, Labels labels) {
    return getDataPoint(reader, metricName, labels, CounterSnapshot.CounterDataPointSnapshot.class);
  }

  public static GaugeSnapshot.GaugeDataPointSnapshot getGaugeDatapoint(
      PrometheusMetricReader reader, String metricName, Labels labels) {
    return getDataPoint(reader, metricName, labels, GaugeSnapshot.GaugeDataPointSnapshot.class);
  }

  public static HistogramSnapshot.HistogramDataPointSnapshot getHistogramDatapoint(
      PrometheusMetricReader reader, String metricName, Labels labels) {
    return getDataPoint(
        reader, metricName, labels, HistogramSnapshot.HistogramDataPointSnapshot.class);
  }

  public static HistogramSnapshot.HistogramDataPointSnapshot getHistogramDatapoint(
      SolrCore core, String metricName, Labels labels) {
    return getDatapoint(
        core, metricName, labels, HistogramSnapshot.HistogramDataPointSnapshot.class);
  }

  public static CounterSnapshot.CounterDataPointSnapshot newStandaloneSelectRequestsDatapoint(
      SolrCore core) {
    return SolrMetricTestUtils.getCounterDatapoint(
        core,
        "solr_core_requests",
        SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
            .label("handler", "/select")
            .label("category", "QUERY")
            .label("internal", "false")
            .build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot newCloudSelectRequestsDatapoint(
      SolrCore core) {
    return SolrMetricTestUtils.getCounterDatapoint(
        core,
        "solr_core_requests",
        SolrMetricTestUtils.newCloudLabelsBuilder(core)
            .label("handler", "/select")
            .label("category", "QUERY")
            .label("internal", "false")
            .build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot newStandaloneUpdateRequestsDatapoint(
      SolrCore core) {
    return SolrMetricTestUtils.getCounterDatapoint(
        core,
        "solr_core_requests",
        SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
            .label("handler", "/update")
            .label("category", "UPDATE")
            .build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot newCloudUpdateRequestsDatapoint(
      SolrCore core) {
    return SolrMetricTestUtils.getCounterDatapoint(
        core,
        "solr_core_requests",
        SolrMetricTestUtils.newCloudLabelsBuilder(core)
            .label("handler", "/update")
            .label("category", "UPDATE")
            .build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCacheSearcherOps(
      SolrCore core, String cacheName, String operation) {
    return SolrMetricTestUtils.getCounterDatapoint(
        core,
        "solr_core_indexsearcher_cache_ops",
        SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
            .label("category", "CACHE")
            .label("ops", operation)
            .label("name", cacheName)
            .build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCacheSearcherLookups(
      SolrCore core, String cacheName, String result) {
    var builder =
        SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
            .label("category", "CACHE")
            .label("name", cacheName)
            .label("result", result);
    return SolrMetricTestUtils.getCounterDatapoint(
        core, "solr_core_indexsearcher_cache_lookups", builder.build());
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCacheSearcherOpsHits(
      SolrCore core, String cacheName) {
    return SolrMetricTestUtils.getCacheSearcherLookups(core, cacheName, "hit");
  }

  public static double getCacheSearcherTotalLookups(SolrCore core, String cacheName) {
    // Calculate lookup total as hits + misses
    var hitDatapoint = SolrMetricTestUtils.getCacheSearcherOpsHits(core, cacheName);
    var missDatapoint = SolrMetricTestUtils.getCacheSearcherLookups(core, cacheName, "miss");

    return hitDatapoint.getValue() + missDatapoint.getValue();
  }

  public static CounterSnapshot.CounterDataPointSnapshot getCacheSearcherOpsInserts(
      SolrCore core, String cacheName) {
    return SolrMetricTestUtils.getCacheSearcherOps(core, cacheName, "inserts");
  }

  public static class TestSolrMetricProducer implements SolrMetricProducer {
    SolrMetricsContext solrMetricsContext;
    private final Map<String, io.opentelemetry.api.metrics.LongCounter> counters = new HashMap<>();
    private final String coreName;
    private final Map<String, Long> metrics;

    public TestSolrMetricProducer(String coreName, Map<String, Long> metrics) {
      this.coreName = coreName;
      this.metrics = metrics;
    }

    @Override
    public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
      this.solrMetricsContext = parentContext;
      for (Map.Entry<String, Long> entry : metrics.entrySet()) {
        String metricName = entry.getKey();
        Long incrementValue = entry.getValue();
        var counter = solrMetricsContext.longCounter(metricName, "testing");
        counters.put(metricName, counter);
        counter.add(incrementValue, Attributes.of(AttributeKey.stringKey("core"), coreName));
      }
    }

    @Override
    public SolrMetricsContext getSolrMetricsContext() {
      return solrMetricsContext;
    }

    public Map<String, io.opentelemetry.api.metrics.LongCounter> getCounters() {
      return counters;
    }
  }
}
