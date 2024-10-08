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

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.tag.Tag;
import io.opentracing.util.GlobalTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePropagator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TRACE_HOST_NAME =
      System.getProperty("solr.traceHostName", System.getProperty("host"));

  static final String TRACE_ID = System.getProperty("solr.traceIdHeader", "X-Trace-Id");

  private static final AtomicLong traceCounter = new AtomicLong(0);

  public static void load() {
    GlobalTracer.registerIfAbsent(
        () -> {
          log.info("Always-on trace id generation enabled.");
          return new SimplePropagatorTracer();
        });
  }

  private static String newTraceId() {
    return TRACE_HOST_NAME + "-" + traceCounter.incrementAndGet();
  }

  /**
   * Tracer that only aims to do simple header propagation, tailored to how Solr works.
   *
   * <p>Heavily inspired from JaegerTracer, NoopTracer
   */
  static class SimplePropagatorTracer implements Tracer {

    private final ScopeManager scopeManager = new ThreadLocalScopeManager();

    @Override
    public ScopeManager scopeManager() {
      return scopeManager;
    }

    @Override
    public Span activeSpan() {
      return scopeManager.activeSpan();
    }

    @Override
    public Scope activateSpan(Span span) {
      return scopeManager.activate(span);
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
      return new SimplePropagatorSpanBuilder(scopeManager);
    }

    @Override
    public void close() {}

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
      if (!format.equals(Format.Builtin.HTTP_HEADERS)) {
        // unsupported via the Solr injectors
        return;
      }
      String traceId = spanContext.toTraceId();
      if (traceId != null && !traceId.isEmpty()) {
        TextMap tm = (TextMap) carrier;
        tm.put(TRACE_ID, traceId);
      }
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C carrier) {
      if (!format.equals(Format.Builtin.HTTP_HEADERS)) {
        // unsupported via the Solr injectors
        return NoopSpan.INSTANCE.context();
      }

      String traceId = null;
      TextMap tm = (TextMap) carrier;
      Iterator<Entry<String, String>> it = tm.iterator();
      while (it.hasNext()) {
        var e = it.next();
        // 'equalsIgnoreCase' because header name might be lowercase
        if (e.getKey().equalsIgnoreCase(TRACE_ID)) {
          traceId = e.getValue();
          break;
        }
      }
      if (traceId == null) {
        traceId = newTraceId();
      }
      return new SimplePropagatorSpan(traceId);
    }

    @Override
    public String toString() {
      return "SimplePropagator";
    }
  }

  private static final class SimplePropagatorSpanBuilder implements SpanBuilder {

    private final ScopeManager scopeManager;
    // storing parent to support the parent being injected via the asChildOf method
    private SpanContext parent;

    public SimplePropagatorSpanBuilder(ScopeManager scopeManager) {
      this.scopeManager = scopeManager;
    }

    @Override
    public Span start() {
      if (parent != null) {
        if (parent instanceof SimplePropagatorSpan) {
          return (SimplePropagatorSpan) parent;
        } else {
          return NoopSpan.INSTANCE;
        }
      }
      var activeSpan = scopeManager.activeSpan();
      if (activeSpan != null) {
        return activeSpan;
      } else {
        return new SimplePropagatorSpan(newTraceId());
      }
    }

    @Override
    public SpanBuilder addReference(String referenceType, SpanContext reference) {
      return this;
    }

    @Override
    public SpanBuilder asChildOf(SpanContext parent) {
      this.parent = parent;
      return this;
    }

    @Override
    public SpanBuilder asChildOf(Span parent) {
      return this;
    }

    @Override
    public SpanBuilder ignoreActiveSpan() {
      return this;
    }

    @Override
    public SpanBuilder withStartTimestamp(long arg0) {
      return this;
    }

    @Override
    public SpanBuilder withTag(String arg0, String arg1) {
      return this;
    }

    @Override
    public SpanBuilder withTag(String arg0, boolean arg1) {
      return this;
    }

    @Override
    public SpanBuilder withTag(String arg0, Number arg1) {
      return this;
    }

    @Override
    public <T> SpanBuilder withTag(Tag<T> arg0, T arg1) {
      return this;
    }
  }

  private static final class SimplePropagatorSpan implements Span, SpanContext, NoopSpan {

    private final String traceId;

    private SimplePropagatorSpan(String traceId) {
      this.traceId = traceId;
    }

    @Override
    public String toTraceId() {
      return traceId;
    }

    @Override
    public String toSpanId() {
      return "";
    }

    @Override
    public Iterable<Map.Entry<String, String>> baggageItems() {
      return Collections.emptyList();
    }

    @Override
    public SpanContext context() {
      return this;
    }

    @Override
    public void finish() {}

    @Override
    public void finish(long arg0) {}

    @Override
    public String getBaggageItem(String arg0) {
      return null;
    }

    @Override
    public Span log(Map<String, ?> arg0) {
      return this;
    }

    @Override
    public Span log(String arg0) {
      return this;
    }

    @Override
    public Span log(long arg0, Map<String, ?> arg1) {
      return this;
    }

    @Override
    public Span log(long arg0, String arg1) {
      return this;
    }

    @Override
    public Span setBaggageItem(String arg0, String arg1) {
      return this;
    }

    @Override
    public Span setOperationName(String arg0) {
      return this;
    }

    @Override
    public Span setTag(String arg0, String arg1) {
      return this;
    }

    @Override
    public Span setTag(String arg0, boolean arg1) {
      return this;
    }

    @Override
    public Span setTag(String arg0, Number arg1) {
      return this;
    }

    @Override
    public <T> Span setTag(Tag<T> arg0, T arg1) {
      return this;
    }
  }
}
