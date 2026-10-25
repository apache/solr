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
package org.apache.solr.core;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTracerConfigurator extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setUpProperties() {
    System.setProperty("otel.service.name", "something");
    System.setProperty("solr.otelDefaultConfigurator", "configuratorClassDoesNotExistTest");
  }

  @Before
  public void resetOtel() {
    OpenTelemetryConfigurator.resetForTest();
  }

  /** A {@code <tracerConfig>} in solr.xml takes precedence and supplies the OpenTelemetry. */
  @Test
  public void customConfiguratorTest() {
    SolrResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"));
    NodeConfig cfg =
        new NodeConfig.NodeConfigBuilder("testNode", TEST_PATH())
            .setTracerConfig(
                new PluginInfo("tracerConfig", Map.of("class", Custom.class.getName())))
            .build();
    OpenTelemetryConfigurator.initializeOpenTelemetrySdk(cfg, loader);
    // not SimplePropagator, which is what the other code paths would have installed
    assertSame(
        W3CTraceContextPropagator.getInstance(),
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator());
  }

  public static class Custom extends OpenTelemetryConfigurator {
    @Override
    protected OpenTelemetry createOpenTelemetry() {
      return OpenTelemetry.propagating(
          ContextPropagators.create(W3CTraceContextPropagator.getInstance()));
    }
  }

  @Test
  public void configuratorClassDoesNotExistTest() {
    assertTrue(OpenTelemetryConfigurator.shouldAutoConfigOTEL());
    SolrResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"));
    OpenTelemetryConfigurator.initializeOpenTelemetrySdk(null, loader);
    assertEquals(
        "Expecting noop otel after failure to auto-init",
        TracerProvider.noop().get(null),
        TraceUtils.getGlobalTracer());
  }

  @Test
  public void otelDisabledByProperty() {
    System.setProperty("otel.sdk.disabled", "true");
    assertFalse(OpenTelemetryConfigurator.shouldAutoConfigOTEL());
  }
}
