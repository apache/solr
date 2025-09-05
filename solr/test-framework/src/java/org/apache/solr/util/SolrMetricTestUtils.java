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

import com.codahale.metrics.Counter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

public final class SolrMetricTestUtils {

  private static final int MAX_ITERATIONS = 100;
  private static final SolrInfoBean.Category CATEGORIES[] = SolrInfoBean.Category.values();

  public static String getRandomScope(Random random) {
    return getRandomScope(random, random.nextBoolean());
  }

  public static String getRandomScope(Random random, boolean shouldDefineScope) {
    return shouldDefineScope
        ? TestUtil.randomSimpleString(random, 5, 10)
        : null; // must be simple string for JMX publishing
  }

  public static SolrInfoBean.Category getRandomCategory(Random random) {
    return getRandomCategory(random, random.nextBoolean());
  }

  public static SolrInfoBean.Category getRandomCategory(
      Random random, boolean shouldDefineCategory) {
    return shouldDefineCategory
        ? CATEGORIES[TestUtil.nextInt(random, 0, CATEGORIES.length - 1)]
        : null;
  }

  public static Map<String, Counter> getRandomMetrics(Random random) {
    return getRandomMetrics(random, random.nextBoolean());
  }

  public static Map<String, Counter> getRandomMetrics(Random random, boolean shouldDefineMetrics) {
    return shouldDefineMetrics ? getRandomMetricsWithReplacements(random, new HashMap<>()) : null;
  }

  /**
   * Generate random OpenTelemetry metric names for testing Prometheus metrics. Returns a map of
   * metric names to their expected increment values.
   */
  public static Map<String, Long> getRandomPrometheusMetrics(Random random) {
    return random.nextBoolean()
        ? getRandomPrometheusMetricsWithReplacements(random, new HashMap<>())
        : null;
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

  // NOCOMMIT: This will get replaced by getRandomPrometheusMetricsWithReplacements
  public static Map<String, Counter> getRandomMetricsWithReplacements(
      Random random, Map<String, Counter> existing) {
    HashMap<String, Counter> metrics = new HashMap<>();
    ArrayList<String> existingKeys = new ArrayList<>(existing.keySet());

    int numMetrics = TestUtil.nextInt(random, 1, MAX_ITERATIONS);
    for (int i = 0; i < numMetrics; ++i) {
      boolean shouldReplaceMetric = !existing.isEmpty() && random.nextBoolean();
      String name =
          shouldReplaceMetric
              ? existingKeys.get(TestUtil.nextInt(random, 0, existingKeys.size() - 1))
              : TestUtil.randomSimpleString(random, 5, 10)
                  + SUFFIX; // must be simple string for JMX publishing

      Counter counter = new Counter();
      counter.inc(random.nextLong());
      metrics.put(name, counter);
    }

    return metrics;
  }

  public static SolrMetricProducer getProducerOf(
      SolrInfoBean.Category category, String scope, Map<String, Counter> metrics) {
    return new SolrMetricProducer() {
      SolrMetricsContext solrMetricsContext;

      @Override
      public void initializeMetrics(
          SolrMetricsContext parentContext, Attributes attributes, String scope) {
        this.solrMetricsContext = parentContext.getChildContext(this);
        if (category == null) {
          throw new IllegalArgumentException("null category");
        }
        if (metrics == null || metrics.isEmpty()) {
          return;
        }
        for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
          solrMetricsContext.counter(entry.getKey(), category.toString(), scope);
        }
      }

      @Override
      public SolrMetricsContext getSolrMetricsContext() {
        return solrMetricsContext;
      }

      @Override
      public String toString() {
        return "SolrMetricProducer.of{"
            + "\ncategory="
            + category
            + "\nscope="
            + scope
            + "\nmetrics="
            + metrics
            + "\n}";
      }
    };
  }

  public static DataPointSnapshot getDataPointSnapshot(
      PrometheusMetricReader reader, String metricName, Labels labels) {
    return reader.collect().stream()
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
            "replica",
            Utils.parseMetricsReplicaName(
                core.getCoreDescriptor().getCollectionName(), core.getName()))
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

  private static <T> T getDatapoint(
      SolrCore core, String metricName, Labels labels, Class<T> snapshotType) {
    var reader = getPrometheusMetricReader(core);
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
    public void initializeMetrics(
        SolrMetricsContext parentContext, Attributes attributes, String scope) {
      this.solrMetricsContext = parentContext.getChildContext(this);
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
