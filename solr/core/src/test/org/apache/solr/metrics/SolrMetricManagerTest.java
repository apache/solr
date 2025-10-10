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
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.RetryUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricManagerTest extends SolrTestCaseJ4 {
  final String METER_PROVIDER_NAME = "test_provider_name";
  private SolrMetricManager metricManager;
  private PrometheusMetricReader reader;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.metricManager = new SolrMetricManager(InMemoryMetricExporter.create());
    // Initialize a metric reader for tests
    metricManager.meterProvider(METER_PROVIDER_NAME);
    this.reader = metricManager.getPrometheusMetricReader(METER_PROVIDER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    metricManager.closeAllRegistries();
    super.tearDown();
  }

  @Test
  public void testDefaultCloudReporterPeriodUnchanged() {
    assertEquals(60, SolrMetricManager.DEFAULT_CLOUD_REPORTER_PERIOD);
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
  public void testMetricExporter() throws Exception {
    LongCounter counter =
        metricManager.longCounter(METER_PROVIDER_NAME, "my_counter", "desc", null);
    counter.add(5);
    counter.add(3);
    InMemoryMetricExporter exporter = (InMemoryMetricExporter) metricManager.getMetricExporter();

    RetryUtil.retryUntil(
        "my_counter metric not found from exporter within timeout",
        50,
        100L,
        TimeUnit.MILLISECONDS,
        () -> {
          Collection<MetricData> metrics = exporter.getFinishedMetricItems();
          return metrics.stream()
              .filter(m -> "my_counter".equals(m.getName()))
              .findFirst()
              .map(
                  actual ->
                      actual.getLongSumData().getPoints().stream()
                          .anyMatch(p -> p.getValue() == 8L))
              .orElse(false);
        });
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
