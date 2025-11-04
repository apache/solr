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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.MeterProvider;
import java.io.InputStream;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.junit.Test;

/** */
public class MetricsConfigTest extends SolrTestCaseJ4 {
  private final String REGISTRY_NAME = "testRegistry";

  @Test
  public void testDefaults() {
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr =
        new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());
    mgr.meterProvider(REGISTRY_NAME);
    assertFalse(mgr.getPrometheusMetricReaders().isEmpty());
    assertNotNull(mgr.getPrometheusMetricReader(SolrMetricManager.enforcePrefix(REGISTRY_NAME)));
  }

  @Test
  public void testDisabledMetrics() {
    System.setProperty("metricsEnabled", "false");
    NodeConfig cfg = loadNodeConfig("solr-metricsconfig.xml");
    SolrMetricManager mgr =
        new SolrMetricManager(cfg.getSolrResourceLoader(), cfg.getMetricsConfig());

    MeterProvider meterProvider = mgr.meterProvider(REGISTRY_NAME);
    MeterProvider noopMeterProvider = OpenTelemetry.noop().getMeterProvider();

    assertEquals(
        "Returned MeterProvider should be NOOP",
        noopMeterProvider.getClass(),
        meterProvider.getClass());

    // Test that metric instruments are NOOP
    LongCounter longCounter =
        mgr.longCounter(REGISTRY_NAME, "test_counter", "A test counter", null);
    DoubleCounter doubleCounter =
        mgr.doubleCounter(REGISTRY_NAME, "test_double_counter", "A test double counter", null);
    LongHistogram longHistogram =
        mgr.longHistogram(REGISTRY_NAME, "test_histogram", "A test histogram", null);
    DoubleHistogram doubleHistogram =
        mgr.doubleHistogram(
            REGISTRY_NAME, "test_double_histogram", "A test double histogram", null);
    LongGauge longGauge = mgr.longGauge(REGISTRY_NAME, "test_gauge", "A test gauge", null);
    DoubleGauge doubleGauge =
        mgr.doubleGauge(REGISTRY_NAME, "test_double_gauge", "A test double gauge", null);

    try {
      longCounter.add(1);
      doubleCounter.add(1.0);
      longHistogram.record(1);
      doubleHistogram.record(1.0);
      longGauge.set(1L);
      doubleGauge.set(1.0);
    } catch (Exception e) {
      fail("NOOP metric instruments should not throw exceptions when metrics are disabled");
    }

    assertTrue(
        "No Prometheus metric readers should be created when metrics are disabled",
        mgr.getPrometheusMetricReaders().isEmpty());
    assertNull("MetricExporter should be null when metrics disabled", mgr.getMetricExporter());
  }

  private NodeConfig loadNodeConfig(String config) {
    InputStream is = MetricsConfigTest.class.getResourceAsStream("/solr/" + config);
    return SolrXmlConfig.fromInputStream(TEST_PATH(), is, new Properties()); // TODO pass in props
  }
}
