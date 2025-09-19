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
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
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
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricManagerTest extends SolrTestCaseJ4 {
  final String METER_PROVIDER_NAME = "test_provider_name";
  private SolrMetricManager metricManager;
  private PrometheusMetricReader reader;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.metricManager = new SolrMetricManager();
    // Initialize a metric reader for tests
    metricManager.meterProvider(METER_PROVIDER_NAME);
    this.reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
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
  public void testDefaultCloudReporterPeriodUnchanged() {
    assertEquals(60, SolrMetricManager.DEFAULT_CLOUD_REPORTER_PERIOD);
  }

  private PluginInfo createPluginInfo(String name, String group, String registry) {
    Map<String, String> attrs = new HashMap<>();
    attrs.put("name", name);
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
    LongCounter counter =
        metricManager.longCounter(METER_PROVIDER_NAME, "my_counter", "desc", null);
    counter.add(5);
    counter.add(3);
    CounterSnapshot actual = snapshot("my_counter", CounterSnapshot.class);
    assertEquals(8.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testLongUpDownCounter() {
    LongUpDownCounter counter =
        metricManager.longUpDownCounter(METER_PROVIDER_NAME, "long_updown_counter", "desc", null);
    counter.add(10);
    counter.add(-4);
    GaugeSnapshot actual = snapshot("long_updown_counter", GaugeSnapshot.class);
    assertEquals(6.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleUpDownCounter() {
    DoubleUpDownCounter counter =
        metricManager.doubleUpDownCounter(
            METER_PROVIDER_NAME, "double_updown_counter", "desc", null);
    counter.add(10.0);
    counter.add(-5.5);

    GaugeSnapshot actual = snapshot("double_updown_counter", GaugeSnapshot.class);
    assertEquals(4.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleCounter() {
    DoubleCounter counter =
        metricManager.doubleCounter(METER_PROVIDER_NAME, "double_counter", "desc", null);
    counter.add(10.0);
    counter.add(5.5);

    CounterSnapshot actual = snapshot("double_counter", CounterSnapshot.class);
    assertEquals(15.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testDoubleHistogram() {
    DoubleHistogram histogram =
        metricManager.doubleHistogram(METER_PROVIDER_NAME, "double_histogram", "desc", null);
    histogram.record(1.1);
    histogram.record(2.2);
    histogram.record(3.3);

    HistogramSnapshot actual = snapshot("double_histogram", HistogramSnapshot.class);
    assertEquals(3, actual.getDataPoints().getFirst().getCount());
    assertEquals(6.6, actual.getDataPoints().getFirst().getSum(), 0.0);
  }

  @Test
  public void testLongHistogram() {
    LongHistogram histogram =
        metricManager.longHistogram(METER_PROVIDER_NAME, "long_histogram", "desc", null);
    histogram.record(1);
    histogram.record(2);
    histogram.record(3);

    HistogramSnapshot actual = snapshot("long_histogram", HistogramSnapshot.class);
    assertEquals(3, actual.getDataPoints().getFirst().getCount());
    assertEquals(6.0, actual.getDataPoints().getFirst().getSum(), 0.0);
  }

  @Test
  public void testDoubleGauge() {
    DoubleGauge gauge =
        metricManager.doubleGauge(METER_PROVIDER_NAME, "double_gauge", "desc", null);
    gauge.set(10.0);
    gauge.set(5.5);

    GaugeSnapshot actual = snapshot("double_gauge", GaugeSnapshot.class);
    assertEquals(5.5, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testLongGauge() {
    LongGauge gauge = metricManager.longGauge(METER_PROVIDER_NAME, "long_gauge", "desc", null);
    gauge.set(10);
    gauge.set(5);

    GaugeSnapshot actual = snapshot("long_gauge", GaugeSnapshot.class);
    assertEquals(5.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableLongCounter() {
    LongAdder val = new LongAdder();
    metricManager.observableLongCounter(
        METER_PROVIDER_NAME, "obs_long_counter", "desc", m -> m.record(val.longValue()), null);
    val.add(10);

    CounterSnapshot actual = snapshot("obs_long_counter", CounterSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(20);
    actual = snapshot("obs_long_counter", CounterSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(30.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleCounter() {
    DoubleAdder val = new DoubleAdder();
    metricManager.observableDoubleCounter(
        METER_PROVIDER_NAME, "obs_double_counter", "desc", m -> m.record(val.doubleValue()), null);
    val.add(10.0);

    CounterSnapshot actual = snapshot("obs_double_counter", CounterSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(0.1);
    actual = snapshot("obs_double_counter", CounterSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(10.1, actual.getDataPoints().getFirst().getValue(), 1e-6);
  }

  @Test
  public void testObservableLongGauge() {
    AtomicLong val = new AtomicLong();
    metricManager.observableLongGauge(
        METER_PROVIDER_NAME, "obs_long_gauge", "desc", m -> m.record(val.get()), null);
    val.set(10L);

    GaugeSnapshot actual = snapshot("obs_long_gauge", GaugeSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.set(20L);
    actual = snapshot("obs_long_gauge", GaugeSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(20.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleGauge() {
    AtomicDouble val = new AtomicDouble();
    metricManager.observableDoubleGauge(
        METER_PROVIDER_NAME, "obs_double_gauge", "desc", m -> m.record(val.get()), null);
    val.set(10.0);

    GaugeSnapshot actual = snapshot("obs_double_gauge", GaugeSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.set(10.1);
    actual = snapshot("obs_double_gauge", GaugeSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(10.1, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableLongUpDownCounter() {
    LongAdder val = new LongAdder();
    metricManager.observableLongUpDownCounter(
        METER_PROVIDER_NAME, "obs_long_updown_gauge", "desc", m -> m.record(val.longValue()), null);
    val.add(10L);

    GaugeSnapshot actual = snapshot("obs_long_updown_gauge", GaugeSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(-20L);
    actual = snapshot("obs_long_updown_gauge", GaugeSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(-10.0, actual.getDataPoints().getFirst().getValue(), 0.0);
  }

  @Test
  public void testObservableDoubleUpDownCounter() {
    DoubleAdder val = new DoubleAdder();
    metricManager.observableDoubleUpDownCounter(
        METER_PROVIDER_NAME,
        "obs_double_updown_gauge",
        "desc",
        m -> m.record(val.doubleValue()),
        null);
    val.add(10.0);
    GaugeSnapshot actual = snapshot("obs_double_updown_gauge", GaugeSnapshot.class);
    assertEquals(10.0, actual.getDataPoints().getFirst().getValue(), 0.0);

    val.add(-20.1);
    actual = snapshot("obs_double_updown_gauge", GaugeSnapshot.class);

    // Observable metrics value changes anytime metricReader collects() to trigger callback
    assertEquals(-10.1, actual.getDataPoints().getFirst().getValue(), 1e-6);
  }

  @Test
  public void testCloseMeterProviders() {
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

    metricManager.removeRegistry(METER_PROVIDER_NAME);

    assertNull(metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME));
  }

  // Helper to grab any snapshot by name and type
  private <T extends MetricSnapshot> T snapshot(String name, Class<T> cls) {
    return reader.collect().stream()
        .filter(m -> m.getMetadata().getPrometheusName().equals(name))
        .filter(cls::isInstance)
        .map(cls::cast)
        .findFirst()
        .orElseThrow(() -> new AssertionError("MetricSnapshot not found: " + name));
  }
}
