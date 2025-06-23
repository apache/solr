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

package org.apache.solr.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AtomicDouble;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.junit.Test;

public class SolrMetricManagerTest extends SolrTestCaseJ4 {
  final String METER_PROVIDER_NAME = "test_provider_name";

  // NOCOMMIT: We might not be supported core swapping in 10. Maybe remove this test
  @Test
  public void testSwapRegistries() {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics1 = SolrMetricTestUtils.getRandomMetrics(r, true);
    Map<String, Counter> metrics2 = SolrMetricTestUtils.getRandomMetrics(r, true);
    String fromName = "from-" + TestUtil.randomSimpleString(r, 1, 10);
    String toName = "to-" + TestUtil.randomSimpleString(r, 1, 10);
    // register test metrics
    for (Map.Entry<String, Counter> entry : metrics1.entrySet()) {
      metricManager.registerMetric(
          null, fromName, entry.getValue(), false, entry.getKey(), "metrics1");
    }
    for (Map.Entry<String, Counter> entry : metrics2.entrySet()) {
      metricManager.registerMetric(
          null, toName, entry.getValue(), false, entry.getKey(), "metrics2");
    }
    assertEquals(metrics1.size(), metricManager.registry(fromName).getMetrics().size());
    assertEquals(metrics2.size(), metricManager.registry(toName).getMetrics().size());

    // swap
    metricManager.swapRegistries(fromName, toName);
    // check metrics
    Map<String, Metric> fromMetrics = metricManager.registry(fromName).getMetrics();
    assertEquals(metrics2.size(), fromMetrics.size());
    for (Map.Entry<String, Counter> entry : metrics2.entrySet()) {
      Object value = fromMetrics.get(SolrMetricManager.mkName(entry.getKey(), "metrics2"));
      assertNotNull(value);
      assertEquals(entry.getValue(), value);
    }
    Map<String, Metric> toMetrics = metricManager.registry(toName).getMetrics();
    assertEquals(metrics1.size(), toMetrics.size());
    for (Map.Entry<String, Counter> entry : metrics1.entrySet()) {
      Object value = toMetrics.get(SolrMetricManager.mkName(entry.getKey(), "metrics1"));
      assertNotNull(value);
      assertEquals(entry.getValue(), value);
    }
  }

  // NOCOMMIT: Migration of this to OTEL isn't possible. You can't register instruments to a
  // meterprovider that the provider itself didn't create
  @Test
  public void testRegisterAll() throws Exception {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(r, true);
    MetricRegistry mr = new MetricRegistry();
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      mr.register(entry.getKey(), entry.getValue());
    }

    String registryName = TestUtil.randomSimpleString(r, 1, 10);
    assertEquals(0, metricManager.registry(registryName).getMetrics().size());
    // There is nothing registered so we should be error-free on the first pass
    metricManager.registerAll(registryName, mr, SolrMetricManager.ResolutionStrategy.ERROR);
    // this should simply skip existing names
    metricManager.registerAll(registryName, mr, SolrMetricManager.ResolutionStrategy.IGNORE);
    // this should re-register everything, and no errors
    metricManager.registerAll(registryName, mr, SolrMetricManager.ResolutionStrategy.REPLACE);
    // this should produce error
    expectThrows(
        IllegalArgumentException.class,
        () ->
            metricManager.registerAll(
                registryName, mr, SolrMetricManager.ResolutionStrategy.ERROR));
  }

  // NOCOMMIT: Migration of this to OTEL isn't possible. You can only delete the whole
  // sdkMeterProvider and all it's recorded metrics
  @Test
  public void testClearMetrics() {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(r, true);
    String registryName = TestUtil.randomSimpleString(r, 1, 10);

    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(
          null, registryName, entry.getValue(), false, entry.getKey(), "foo", "bar");
    }
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(
          null, registryName, entry.getValue(), false, entry.getKey(), "foo", "baz");
    }
    for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
      metricManager.registerMetric(
          null, registryName, entry.getValue(), false, entry.getKey(), "foo");
    }

    assertEquals(metrics.size() * 3, metricManager.registry(registryName).getMetrics().size());

    // clear all metrics with prefix "foo.bar."
    Set<String> removed = metricManager.clearMetrics(registryName, "foo", "bar.");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo.bar."));
    }
    removed = metricManager.clearMetrics(registryName, "foo", "baz.");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo.baz."));
    }
    // perhaps surprisingly, this works too - see PrefixFilter docs
    removed = metricManager.clearMetrics(registryName, "fo");
    assertEquals(metrics.size(), removed.size());
    for (String s : removed) {
      assertTrue(s.startsWith("foo."));
    }
  }

  @Test
  public void testSimpleMetrics() {
    Random r = random();

    SolrMetricManager metricManager = new SolrMetricManager();

    String registryName = TestUtil.randomSimpleString(r, 1, 10);

    metricManager.counter(null, registryName, "simple_counter", "foo", "bar");
    metricManager.timer(null, registryName, "simple_timer", "foo", "bar");
    metricManager.meter(null, registryName, "simple_meter", "foo", "bar");
    metricManager.histogram(null, registryName, "simple_histogram", "foo", "bar");
    Map<String, Metric> metrics = metricManager.registry(registryName).getMetrics();
    assertEquals(4, metrics.size());
    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      assertTrue(entry.getKey().startsWith("foo.bar.simple_"));
    }
  }

  @Test
  public void testRegistryName() {
    Random r = random();

    String name = TestUtil.randomSimpleString(r, 1, 10);

    String result = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, name, "collection1");
    assertEquals("solr.core." + name + ".collection1", result);
    // try it with already prefixed name - group will be ignored
    result = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, result);
    assertEquals("solr.core." + name + ".collection1", result);
    // try it with already prefixed name but with additional segments
    result =
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, result, "shard1", "replica1");
    assertEquals("solr.core." + name + ".collection1.shard1.replica1", result);
  }

  @Test
  public void testReporters() throws Exception {

    try (SolrResourceLoader loader = new SolrResourceLoader(createTempDir())) {
      SolrMetricManager metricManager = new SolrMetricManager();

      PluginInfo[] plugins =
          new PluginInfo[] {
            createPluginInfo("universal_foo", null, null),
            createPluginInfo("multigroup_foo", "jvm, node, core", null),
            createPluginInfo("multiregistry_foo", null, "solr.node, solr.core.collection1"),
            createPluginInfo("specific_foo", null, "solr.core.collection1"),
            createPluginInfo("node_foo", "node", null),
            createPluginInfo("core_foo", "core", null)
          };
      String tag = "xyz";
      metricManager.loadReporters(plugins, loader, null, null, tag, SolrInfoBean.Group.node);
      Map<String, SolrMetricReporter> reporters =
          metricManager.getReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));

      assertEquals(4, reporters.size());
      assertTrue(reporters.containsKey("universal_foo@" + tag));
      assertTrue(reporters.containsKey("multigroup_foo@" + tag));
      assertTrue(reporters.containsKey("node_foo@" + tag));
      assertTrue(reporters.containsKey("multiregistry_foo@" + tag));

      metricManager.loadReporters(
          plugins, loader, null, null, tag, SolrInfoBean.Group.core, "collection1");
      reporters =
          metricManager.getReporters(
              SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, "collection1"));

      assertEquals(5, reporters.size());
      assertTrue(reporters.containsKey("universal_foo@" + tag));
      assertTrue(reporters.containsKey("multigroup_foo@" + tag));
      assertTrue(reporters.containsKey("specific_foo@" + tag));
      assertTrue(reporters.containsKey("core_foo@" + tag));
      assertTrue(reporters.containsKey("multiregistry_foo@" + tag));

      metricManager.loadReporters(plugins, loader, null, null, tag, SolrInfoBean.Group.jvm);
      reporters =
          metricManager.getReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));

      assertEquals(2, reporters.size());
      assertTrue(reporters.containsKey("universal_foo@" + tag));
      assertTrue(reporters.containsKey("multigroup_foo@" + tag));

      metricManager.removeRegistry("solr.jvm");
      reporters =
          metricManager.getReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));

      assertEquals(0, reporters.size());

      metricManager.removeRegistry("solr.node");
      reporters =
          metricManager.getReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));

      assertEquals(0, reporters.size());

      metricManager.removeRegistry("solr.core.collection1");
      reporters =
          metricManager.getReporters(
              SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, "collection1"));

      assertEquals(0, reporters.size());
    }
  }

  @Test
  public void testDefaultCloudReporterPeriodUnchanged() {
    assertEquals(60, SolrMetricManager.DEFAULT_CLOUD_REPORTER_PERIOD);
  }

  private PluginInfo createPluginInfo(String name, String group, String registry) {
    Map<String, String> attrs = new HashMap<>();
    attrs.put("name", name);
    attrs.put("class", MockMetricReporter.class.getName());
    if (group != null) {
      attrs.put("group", group);
    }
    if (registry != null) {
      attrs.put("registry", registry);
    }
    NamedList<String> initArgs = new NamedList<>();
    initArgs.add("configurable", "true");
    return new PluginInfo("SolrMetricReporter", attrs, initArgs, null);
  }

  @Test
  public void testLongCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongCounter counter =
        metricManager.longCounter(METER_PROVIDER_NAME, "my_counter", "desc", null);
    counter.add(5);
    counter.add(3);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();
    CounterSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("my_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long counter metric not found"));
    assertEquals(8, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testLongUpDownCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongUpDownCounter c =
        metricManager.longUpDownCounter(METER_PROVIDER_NAME, "long_updown_counter", "desc", null);
    c.add(10);
    c.add(-4);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("long_updown_counter"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long up down counter metric not found"));
    assertEquals(6, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleUpDownCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleUpDownCounter counter =
        metricManager.doubleUpDownCounter(
            METER_PROVIDER_NAME, "double_updown_counter", "desc", null);
    counter.add(10.0);
    counter.add(-5.5);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("double_updown_counter"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double up down counter metric not found"));
    assertEquals(4.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleCounter counter =
        metricManager.doubleCounter(METER_PROVIDER_NAME, "double_counter", "desc", null);
    counter.add(10.0);
    counter.add(5.5);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    CounterSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("double_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double counter metric not found"));
    assertEquals(15.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleHistogram() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleHistogram histogram =
        metricManager.doubleHistogram(METER_PROVIDER_NAME, "double_histogram", "desc", null);
    histogram.record(1.1);
    histogram.record(2.2);
    histogram.record(3.3);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    HistogramSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("double_histogram"))
            .map(HistogramSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double histogram metric not found"));

    assertEquals(3, actual.getDataPoints().getFirst().getCount());
    assertEquals(6.6, actual.getDataPoints().getFirst().getSum(), 0.0);
  }

  @Test
  public void testLongHistogram() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongHistogram histogram =
        metricManager.longHistogram(METER_PROVIDER_NAME, "long_histogram", "desc", null);
    histogram.record(1);
    histogram.record(2);
    histogram.record(3);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    HistogramSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("long_histogram"))
            .map(HistogramSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long histogram metric not found"));

    assertEquals(3, actual.getDataPoints().getFirst().getCount());
    assertEquals(6.0, actual.getDataPoints().getFirst().getSum(), 0.0);
  }

  @Test
  public void testDoubleGauge() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleGauge gauge =
        metricManager.doubleGauge(METER_PROVIDER_NAME, "double_gauge", "desc", null);
    gauge.set(10.0);
    gauge.set(5.5);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("double_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double gauge metric not found"));
    assertEquals(5.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testLongGauge() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongGauge gauge = metricManager.longGauge(METER_PROVIDER_NAME, "long_gauge", "desc", null);
    gauge.set(10);
    gauge.set(5);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("long_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long gauge metric not found"));

    assertEquals(5.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableLongCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongAdder val = new LongAdder();
    metricManager.observableLongCounter(
        METER_PROVIDER_NAME, "obs_long_counter", "desc", m -> m.record(val.longValue()), null);
    val.add(10);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    CounterSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long counter metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(20);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long counter metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(30.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleAdder val = new DoubleAdder();
    metricManager.observableDoubleCounter(
        METER_PROVIDER_NAME, "obs_double_counter", "desc", m -> m.record(val.doubleValue()), null);
    val.add(10.0);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    CounterSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double gauge metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(0.1);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double gauge metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(10.1, actual.getDataPoints().getFirst().getValue(), 1e-6);
  }

  @Test
  public void testObservableLongGauge() {
    SolrMetricManager metricManager = new SolrMetricManager();
    AtomicLong val = new AtomicLong();
    metricManager.observableLongGauge(
        METER_PROVIDER_NAME, "obs_long_gauge", "desc", m -> m.record(val.get()), null);
    val.set(10L);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long gauge metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.set(20L);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long gauge metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(20.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleGauge() {
    SolrMetricManager metricManager = new SolrMetricManager();
    AtomicDouble val = new AtomicDouble();
    metricManager.observableDoubleGauge(
        METER_PROVIDER_NAME, "obs_double_gauge", "desc", m -> m.record(val.get()), null);
    val.set(10.0);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double gauge metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.set(10.1);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double gauge metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(10.1, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableLongUpDownCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongAdder val = new LongAdder();
    metricManager.observableLongUpDownCounter(
        METER_PROVIDER_NAME, "obs_long_updown_gauge", "desc", m -> m.record(val.longValue()), null);
    val.add(10L);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_updown_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long up down metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(-20L);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_long_updown_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Long up down metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(-10.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleUpDownCounter() {
    SolrMetricManager metricManager = new SolrMetricManager();
    DoubleAdder val = new DoubleAdder();
    metricManager.observableDoubleUpDownCounter(
        METER_PROVIDER_NAME,
        "obs_double_updown_gauge",
        "desc",
        m -> m.record(val.doubleValue()),
        null);
    val.add(10.0);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();

    GaugeSnapshot actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_updown_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double up down gauge metric not found"));

    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(-20.1);
    metrics = reader.collect();
    actual =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("obs_double_updown_gauge"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Double up down gauge metric not found"));

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(-10.1, actual.getDataPoints().getFirst().getValue(), 1e-6);
  }

  @Test
  public void testCloseMeterProviders() {
    SolrMetricManager metricManager = new SolrMetricManager();
    LongCounter counter =
        metricManager.longCounter(METER_PROVIDER_NAME, "my_counter", "desc", null);
    counter.add(5);

    PrometheusMetricReader reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
    MetricSnapshots metrics = reader.collect();
    CounterSnapshot data =
        metrics.stream()
            .filter(m -> m.getMetadata().getPrometheusName().equals("my_counter"))
            .map(CounterSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("LongCounter metric not found"));
    assertEquals(5, data.getDataPoints().getFirst().getValue(), 0.0);

    metricManager.closeMeterProvider(METER_PROVIDER_NAME);

    assertNull(metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME));
  }
}
