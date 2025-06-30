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

import static org.apache.solr.metrics.prometheus.core.SolrCoreMetric.CLOUD_CORE_PATTERN;
import static org.apache.solr.metrics.prometheus.core.SolrCoreMetric.STANDALONE_CORE_PATTERN;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.SettableGauge;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.prometheus.SolrMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusFormatter;
import org.junit.Test;

public class SolrPrometheusFormatterTest extends SolrTestCaseJ4 {
  @Test
  public void testExportMeter() {
    TestSolrPrometheusFormatter testFormatter = new TestSolrPrometheusFormatter();
    String expectedMetricName = "test_metric";
    Meter metric = new Meter();
    metric.mark(123);
    Labels expectedLabels = Labels.of("test", "test-value");

    testFormatter.exportMeter(expectedMetricName, metric, expectedLabels);
    assertTrue(testFormatter.getMetricCounters().containsKey(expectedMetricName));

    CounterSnapshot.CounterDataPointSnapshot actual =
        testFormatter.getMetricCounters().get("test_metric").get(0);
    assertEquals(123.0, actual.getValue(), 0);
    assertEquals(expectedLabels, actual.getLabels());
  }

  @Test
  public void testExportCounter() {
    TestSolrPrometheusFormatter testFormatter = new TestSolrPrometheusFormatter();
    String expectedMetricName = "test_metric";
    Counter metric = new Counter();
    metric.inc(123);
    Labels expectedLabels = Labels.of("test", "test-value");

    testFormatter.exportCounter(expectedMetricName, metric, expectedLabels);
    assertTrue(testFormatter.getMetricCounters().containsKey(expectedMetricName));

    CounterSnapshot.CounterDataPointSnapshot actual =
        testFormatter.getMetricCounters().get("test_metric").get(0);
    assertEquals(123.0, actual.getValue(), 0);
    assertEquals(expectedLabels, actual.getLabels());
  }

  @Test
  public void testExportTimer() throws InterruptedException {
    TestSolrPrometheusFormatter testFormatter = new TestSolrPrometheusFormatter();
    String expectedMetricName = "test_metric";
    Timer metric = new Timer();
    Timer.Context context = metric.time();
    TimeUnit.SECONDS.sleep(5);
    context.stop();

    Labels expectedLabels = Labels.of("test", "test-value");
    testFormatter.exportTimer(expectedMetricName, metric, expectedLabels);
    assertTrue(testFormatter.getMetricGauges().containsKey(expectedMetricName));

    GaugeSnapshot.GaugeDataPointSnapshot actual =
        testFormatter.getMetricGauges().get("test_metric").get(0);
    assertEquals(5000000000L, actual.getValue(), 500000000L);
    assertEquals(expectedLabels, actual.getLabels());
  }

  @Test
  public void testExportGaugeNumber() throws InterruptedException {
    TestSolrPrometheusFormatter testFormatter = new TestSolrPrometheusFormatter();
    String expectedMetricName = "test_metric";
    Gauge<Number> metric =
        new SettableGauge<>() {
          @Override
          public void setValue(Number value) {}

          @Override
          public Number getValue() {
            return 123.0;
          }
        };

    Labels expectedLabels = Labels.of("test", "test-value");
    testFormatter.exportGauge(expectedMetricName, metric, expectedLabels);
    assertTrue(testFormatter.getMetricGauges().containsKey(expectedMetricName));

    GaugeSnapshot.GaugeDataPointSnapshot actual =
        testFormatter.getMetricGauges().get("test_metric").get(0);
    assertEquals(123.0, actual.getValue(), 0);
    assertEquals(expectedLabels, actual.getLabels());
  }

  @Test
  public void testExportGaugeMap() throws InterruptedException {
    TestSolrPrometheusFormatter testFormatter = new TestSolrPrometheusFormatter();
    String expectedMetricName = "test_metric";
    Gauge<Map<String, Number>> metric =
        new SettableGauge<>() {
          @Override
          public void setValue(Map<String, Number> value) {}

          @Override
          public Map<String, Number> getValue() {
            final Map<String, Number> expected = new HashMap<>();
            expected.put("test-item", 123.0);
            return expected;
          }
        };

    Labels labels = Labels.of("test", "test-value");
    testFormatter.exportGauge(expectedMetricName, metric, labels);
    assertTrue(testFormatter.getMetricGauges().containsKey(expectedMetricName));

    GaugeSnapshot.GaugeDataPointSnapshot actual =
        testFormatter.getMetricGauges().get("test_metric").get(0);
    assertEquals(123.0, actual.getValue(), 0);
    Labels expectedLabels = Labels.of("test", "test-value", "item", "test-item");
    assertEquals(expectedLabels, actual.getLabels());
  }

  @Test
  public void testCloudCorePattern() {
    String metricName = "core_test-core_shard2_replica_t123.TEST./foobar/endpoint";
    Matcher m = CLOUD_CORE_PATTERN.matcher(metricName);
    assertTrue(m.find());
    assertEquals("core_test-core_shard2_replica_t123", m.group("core"));
    assertEquals("test-core", m.group("collection"));
    assertEquals("shard2", m.group("shard"));
    assertEquals("replica_t123", m.group("replica"));

    metricName = "core_foo_bar_shard24_replica_p8.QUERY.random.metric-name";
    m = CLOUD_CORE_PATTERN.matcher(metricName);
    assertTrue(m.matches());
    assertEquals("core_foo_bar_shard24_replica_p8", m.group("core"));
    assertEquals("foo_bar", m.group("collection"));
    assertEquals("shard24", m.group("shard"));
    assertEquals("replica_p8", m.group("replica"));
  }

  @Test
  public void testBadCloudCorePattern() {
    String badMetricName = "core_solrtest_shard100_replica_xyz23.TEST./foobar/endpoint";
    Matcher m = CLOUD_CORE_PATTERN.matcher(badMetricName);
    assertFalse(m.matches());

    badMetricName = "core_solrtest_shards100_replica_x23.QUERY.random.metric-name";
    m = CLOUD_CORE_PATTERN.matcher(badMetricName);
    assertFalse(m.matches());
  }

  @Test
  public void testStandaloneCorePattern() {
    String metricName = "core_test-core.TEST./foobar/endpoint";
    Matcher m = STANDALONE_CORE_PATTERN.matcher(metricName);
    assertTrue(m.find());
    assertEquals("test-core", m.group(1));
  }

  static class TestSolrPrometheusFormatter extends SolrPrometheusFormatter {
    @Override
    public void exportDropwizardMetric(Metric dropwizardMetric, String metricName) {}

    @Override
    public SolrMetric categorizeMetric(Metric dropwizardMetric, String metricName) {
      return null;
    }

    public Map<String, List<CounterSnapshot.CounterDataPointSnapshot>> getMetricCounters() {
      return metricCounters;
    }

    public Map<String, List<GaugeSnapshot.GaugeDataPointSnapshot>> getMetricGauges() {
      return metricGauges;
    }
  }
}
