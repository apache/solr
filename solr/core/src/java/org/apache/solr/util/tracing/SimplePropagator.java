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
package org.apache.solr.util.tracing;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.logging.MDCLoggingContext;

/**
 * Simple Http Header Propagator. When enabled, this will only propagate the trace id from the
 * client to all internal requests. It is also in charge of generating a trace id if none exists.
 *
 * <p>Note: this is very similar in impl to
 * io.opentelemetry.extension.incubator.propagation.PassThroughPropagator. we should consider
 * replacing/upgrading once that becomes generally available
 */
public class SimplePropagator implements TextMapPropagator {

  private static final String TRACE_HOST_NAME =
      System.getProperty("solr.traceHostName", System.getProperty("host"));
  private static final TextMapPropagator INSTANCE = new SimplePropagator();
  private static final ContextKey<String> TRACE_ID_KEY = ContextKey.named("trace_id");

  static final String TRACE_ID = System.getProperty("solr.traceIdHeader", "X-Trace-Id");
  private static final List<String> FIELDS = List.of(TRACE_ID);

  private static final AtomicLong traceCounter = new AtomicLong(0);

  public static TextMapPropagator getInstance() {
    return INSTANCE;
  }

  private SimplePropagator() {}

  @Override
  public Collection<String> fields() {
    return FIELDS;
  }

  @Override
  public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
    if (setter == null) {
      return;
    }
    String traceId = context.get(TRACE_ID_KEY);
    if (traceId != null) {
      setter.set(carrier, TRACE_ID, traceId);
    }
  }

  @Override
  public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
    String traceId = getter.get(carrier, TRACE_ID);
    if (traceId == null) {
      traceId = newTraceId();
    }

    MDCLoggingContext.setTracerId(traceId);
    return context.with(TRACE_ID_KEY, traceId);
  }

  private static String newTraceId() {
    return TRACE_HOST_NAME + "-" + traceCounter.incrementAndGet();
  }

  @Override
  public String toString() {
    return "SimplePropagator";
  }
}
