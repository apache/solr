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

import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.metrics.otel.FilterablePrometheusMetricReader;
import org.apache.solr.metrics.otel.OtelUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricsContextTest extends SolrTestCase {
  private static final String REGISTRY = "test_context_registry";
  private SolrMetricManager metricManager;
  private FilterablePrometheusMetricReader reader;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    metricManager = new SolrMetricManager(InMemoryMetricExporter.create());
    metricManager.meterProvider(REGISTRY);
    reader = metricManager.getPrometheusMetricReader(REGISTRY);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    metricManager.closeAllRegistries();
    super.tearDown();
  }

  /** Callbacks registered through a context must stop firing once the context is closed. */
  @Test
  public void testCloseUnregistersObservableCallbacks() {
    SolrMetricsContext ctx = new SolrMetricsContext(metricManager, REGISTRY);
    AtomicInteger invocations = new AtomicInteger();

    ctx.observableLongGauge("long_gauge", "d", m -> invocations.incrementAndGet());
    ctx.observableLongGauge(
        "long_gauge_unit", "d", m -> invocations.incrementAndGet(), OtelUnit.BYTES);
    ctx.observableDoubleGauge("double_gauge", "d", m -> invocations.incrementAndGet());
    ctx.observableDoubleGauge(
        "double_gauge_unit", "d", m -> invocations.incrementAndGet(), OtelUnit.BYTES);
    ctx.observableLongCounter("long_counter", "d", m -> invocations.incrementAndGet());
    ctx.observableLongCounter(
        "long_counter_unit", "d", m -> invocations.incrementAndGet(), OtelUnit.BYTES);
    ctx.observableDoubleCounter("double_counter", "d", m -> invocations.incrementAndGet());
    ctx.observableDoubleCounter(
        "double_counter_unit", "d", m -> invocations.incrementAndGet(), OtelUnit.BYTES);

    reader.collect();
    assertEquals("all 8 callbacks should fire while the context is open", 8, invocations.get());

    ctx.close();
    invocations.set(0);
    reader.collect();
    assertEquals("no callback should fire after the context is closed", 0, invocations.get());
  }
}
