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
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.eclipse.jetty.client.api.Request;

/** Utilities for distributed tracing. */
public class TraceUtils {

  private static final String REQ_ATTR_TRACING_SPAN = Span.class.getName();
  private static final String REQ_ATTR_TRACING_TRACER = Tracer.class.getName();

  public static final String DEFAULT_SPAN_NAME = "http.request";
  public static final String WRITE_QUERY_RESPONSE_SPAN_NAME = "writeQueryResponse";

  public static final AttributeKey<String> TAG_DB = AttributeKey.stringKey("db.instance");
  public static final AttributeKey<String> TAG_DB_TYPE = AttributeKey.stringKey("db.type");
  public static final AttributeKey<String> TAG_USER = AttributeKey.stringKey("db.user");
  public static final AttributeKey<Long> TAG_HTTP_STATUS =
      AttributeKey.longKey("http.response.status_code");
  public static final AttributeKey<String> TAG_HTTP_METHOD =
      AttributeKey.stringKey("http.request.method");
  public static final AttributeKey<String> TAG_HTTP_URL = AttributeKey.stringKey("http.url");
  public static final AttributeKey<String> TAG_HTTP_PARAMS = AttributeKey.stringKey("http.params");
  public static final AttributeKey<String> TAG_RESPONSE_WRITER =
      AttributeKey.stringKey("responseWriter");
  public static final AttributeKey<String> TAG_CONTENT_TYPE = AttributeKey.stringKey("contentType");
  public static final AttributeKey<List<String>> TAG_OPS = AttributeKey.stringArrayKey("ops");
  public static final AttributeKey<String> TAG_CLASS = AttributeKey.stringKey("class");

  @Deprecated
  private static final AttributeKey<String> TAG_HTTP_METHOD_DEP =
      AttributeKey.stringKey("http.method");

  @Deprecated
  private static final AttributeKey<Long> TAG_HTTP_STATUS_DEP =
      AttributeKey.longKey("http.status_code");

  public static final String TAG_DB_TYPE_SOLR = "solr";

  public static final Predicate<Span> DEFAULT_IS_RECORDING = Span::isRecording;

  /**
   * this should only be changed in the context of testing, otherwise it would risk not recording
   * trace data.
   */
  public static Predicate<Span> IS_RECORDING = DEFAULT_IS_RECORDING;

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer("solr");
  }

  public static TextMapPropagator getTextMapPropagator() {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  public static void setDbInstance(SolrQueryRequest req, String coreOrColl) {
    if (req != null) {
      setDbInstance(req.getSpan(), coreOrColl);
    }
  }

  public static void setDbInstance(Span span, String coreOrColName) {
    if (coreOrColName != null) {
      span.setAttribute(TAG_DB, coreOrColName);
    }
  }

  public static void setUser(Span span, String user) {
    span.setAttribute(TAG_USER, user);
  }

  public static void setHttpStatus(Span span, int httpStatus) {
    span.setAttribute(TAG_HTTP_STATUS, httpStatus);
    span.setAttribute(TAG_HTTP_STATUS_DEP, httpStatus);
  }

  public static void ifNotNoop(Span span, Consumer<Span> consumer) {
    if (IS_RECORDING.test(span)) {
      consumer.accept(span);
    }
  }

  /**
   * Sometimes the tests will use a recoding noop span to verify the complete code path so we need
   * to distinguish this case and only perform a specific operation (like updating the MDC context)
   * only in case the generated trace id is valid
   *
   * @param span current span
   * @param consumer consumer to be called
   */
  public static void ifValidTraceId(Span span, Consumer<Span> consumer) {
    if (TraceId.isValid(span.getSpanContext().getTraceId())) {
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

  private static final TextMapSetter<Request> REQUEST_INJECTOR =
      (req, k, v) -> req.headers(httpFields -> httpFields.put(k, v));

  public static void injectTraceContext(Request req) {
    TextMapPropagator textMapPropagator = getTextMapPropagator();
    textMapPropagator.inject(Context.current(), req, REQUEST_INJECTOR);
  }

  private static final TextMapSetter<HttpRequest> HTTP_REQUEST_INJECTOR =
      (req, k, v) -> req.setHeader(k, v);

  public static void injectTraceContext(HttpRequest req) {
    TextMapPropagator textMapPropagator = getTextMapPropagator();
    textMapPropagator.inject(Context.current(), req, HTTP_REQUEST_INJECTOR);
  }

  public static Span startHttpRequestSpan(HttpServletRequest request, Context context) {
    Tracer tracer = getTracer(request);
    SpanBuilder spanBuilder =
        tracer
            .spanBuilder(DEFAULT_SPAN_NAME) // will be changed later
            .setParent(context)
            .setSpanKind(SpanKind.SERVER)
            .setAttribute(TAG_HTTP_METHOD, request.getMethod())
            .setAttribute(TAG_HTTP_METHOD_DEP, request.getMethod())
            .setAttribute(TAG_HTTP_URL, request.getRequestURL().toString());
    if (request.getQueryString() != null) {
      spanBuilder.setAttribute(TAG_HTTP_PARAMS, request.getQueryString());
    }
    spanBuilder.setAttribute(TAG_DB_TYPE, TAG_DB_TYPE_SOLR);
    return spanBuilder.startSpan();
  }

  public static void setOperations(SolrQueryRequest req, String clazz, List<String> ops) {
    if (!ops.isEmpty()) {
      req.getSpan().setAttribute(TAG_OPS, ops);
      req.getSpan().setAttribute(TAG_CLASS, clazz);
    }
  }
}
