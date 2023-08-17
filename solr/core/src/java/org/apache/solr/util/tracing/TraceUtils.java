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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.function.Consumer;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.request.SolrQueryRequest;

/** Utilities for distributed tracing. */
public class TraceUtils {

  private static final String REQ_ATTR_TRACING_SPAN = Span.class.getName();
  private static final String REQ_ATTR_TRACING_TRACER = Tracer.class.getName();
  private static final Tracer NOOP_TRACER = TracerProvider.noop().get(null);

  public static final String DEFAULT_SPAN_NAME = "http.request";
  public static final String WRITE_QUERY_RESPONSE_SPAN_NAME = "writeQueryResponse";

  public static final AttributeKey<String> TAG_DB = AttributeKey.stringKey("db.instance");
  public static final AttributeKey<String> TAG_DB_TYPE = AttributeKey.stringKey("db.type");
  public static final AttributeKey<String> TAG_USER = AttributeKey.stringKey("db.user");
  public static final AttributeKey<Long> TAG_HTTP_STATUS = AttributeKey.longKey("http.status");
  public static final AttributeKey<String> TAG_HTTP_METHOD = AttributeKey.stringKey("http.method");
  public static final AttributeKey<String> TAG_HTTP_URL = AttributeKey.stringKey("http.url");
  public static final AttributeKey<String> TAG_HTTP_PARAMS = AttributeKey.stringKey("http.params");
  public static final AttributeKey<String> TAG_RESPONSE_WRITER = AttributeKey.stringKey("responseWriter");
  public static final AttributeKey<String> TAG_CONTENT_TYPE = AttributeKey.stringKey("contentType");

  public static final String TAG_DB_TYPE_SOLR = "solr";

  public static Tracer noop() {
    return NOOP_TRACER;
  }

  public static TextMapPropagator getTextMapPropagator() {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  public static void setCoreOrColName(SolrQueryRequest req, String coreOrColl) {
    setCoreOrColName(req.getSpan(), coreOrColl);
  }

  public static void setCoreOrColName(Span span, String coreOrColName) {
    if (coreOrColName != null) {
      span.setAttribute(TAG_DB, coreOrColName);
    }
  }

  public static void setUser(Span span, String user) {
    span.setAttribute(TAG_USER, user);
  }

  public static void setHttpStatus(Span span, int httpStatus) {
    span.setAttribute(TAG_HTTP_STATUS, httpStatus);
  }

  public static void ifNotNoop(Span span, Consumer<Span> consumer) {
    if (span.isRecording()) {
      consumer.accept(span);
    }
  }

  public static void setSpan(HttpServletRequest req, Span span) {
    req.setAttribute(REQ_ATTR_TRACING_SPAN, span);
  }

  public static Span getSpan(HttpServletRequest req) {
    return (Span) req.getAttribute(REQ_ATTR_TRACING_SPAN);
  }

  public static void setTracer(HttpServletRequest req, Tracer t) {
    req.setAttribute(REQ_ATTR_TRACING_TRACER, t);
  }

  public static Tracer getTracer(HttpServletRequest req) {
    return (Tracer) req.getAttribute(REQ_ATTR_TRACING_TRACER);
  }

  public static Context extractContext(HttpServletRequest req) {
    TextMapPropagator textMapPropagator = getTextMapPropagator();
    return textMapPropagator.extract(Context.current(), req, new HttpServletRequestGetter());
  }

  public static void injectContextIntoRequest(SolrRequest<?> req) {
    TextMapPropagator textMapPropagator = getTextMapPropagator();
    textMapPropagator.inject(Context.current(), req, new SolrRequestSetter());
  }

  public static Span startHttpRequestSpan(HttpServletRequest request, Context context) {
    Tracer tracer = getTracer(request);
    SpanBuilder spanBuilder =
        tracer
            .spanBuilder(DEFAULT_SPAN_NAME) // will be changed later
            .setParent(context)
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(TAG_HTTP_METHOD, request.getMethod())
            .setAttribute(TAG_HTTP_URL, request.getRequestURL().toString());
    if (request.getQueryString() != null) {
      spanBuilder.setAttribute(TAG_HTTP_PARAMS, request.getQueryString());
    }
    spanBuilder.setAttribute(TAG_DB_TYPE, TAG_DB_TYPE_SOLR);
    return spanBuilder.startSpan();
  }
}
