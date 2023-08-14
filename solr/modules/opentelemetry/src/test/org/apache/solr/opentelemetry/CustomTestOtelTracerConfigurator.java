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
package org.apache.solr.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.solr.common.util.NamedList;

public class CustomTestOtelTracerConfigurator extends OtelTracerConfigurator {

  private static final InMemorySpanExporter exporter = InMemorySpanExporter.create();
  private static volatile boolean isRegistered = false;
  private static OpenTelemetrySdk otelSdk = null;

  @Override
  public synchronized Tracer getTracer() {
    if (!isRegistered) {
      initTracer();
    }
    return super.getTracer();
  }

  @Override
  public void init(NamedList<?> args) {
    // prevent parent from init otel
  }

  private synchronized void initTracer() {
    if (isRegistered) {
      return;
    }
    isRegistered = true;

    setDefaultIfNotConfigured("OTEL_TRACES_EXPORTER", "none");
    prepareConfiguration();

    otelSdk =
        AutoConfiguredOpenTelemetrySdk.builder()
            .setResultAsGlobal()
            .addTracerProviderCustomizer(
                (builder, props) -> builder.addSpanProcessor(SimpleSpanProcessor.create(exporter)))
            .build()
            .getOpenTelemetrySdk();
  }

  public static InMemorySpanExporter getInMemorySpanExporter() {
    return exporter;
  }

  public static boolean isRegistered() {
    return isRegistered;
  }

  public static synchronized void resetForTest() {
    if (isRegistered) {
      isRegistered = false;
      if (otelSdk != null) {
        otelSdk.close();
      }
      GlobalOpenTelemetry.resetForTest();
    }
  }
}
