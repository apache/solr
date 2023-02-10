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

import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delegate shim that forwards all calls to the actual {@link
 * io.opentelemetry.opentracingshim.OpenTracingShim}, and in addition calls {@link
 * SdkTracerProvider#close()} to really close the OTEL SDK tracer when the OT shim is closed.
 *
 * <p>TODO: This can be removed once we migrate Solr instrumentation from OpenTracing to
 * OpenTelemetry
 */
public class ClosableTracerShim implements Tracer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Tracer shim;
  private final SdkTracerProvider sdkTracerProvider;

  public ClosableTracerShim(Tracer shim, SdkTracerProvider sdkTracerProvider) {
    this.shim = shim;
    this.sdkTracerProvider = sdkTracerProvider;
  }

  @Override
  public ScopeManager scopeManager() {
    return shim.scopeManager();
  }

  @Override
  public Span activeSpan() {
    return shim.activeSpan();
  }

  @Override
  public Scope activateSpan(Span span) {
    return shim.activateSpan(span);
  }

  @Override
  public SpanBuilder buildSpan(String operationName) {
    return shim.buildSpan(operationName);
  }

  @Override
  public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
    shim.inject(spanContext, format, carrier);
  }

  @Override
  public <C> SpanContext extract(Format<C> format, C carrier) {
    return shim.extract(format, carrier);
  }

  @Override
  public void close() {
    shim.close();
    log.debug("Closing wrapped OTEL tracer instance.");
    sdkTracerProvider.forceFlush();
    sdkTracerProvider.close();
  }
}
