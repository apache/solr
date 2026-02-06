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

package org.apache.solr.servlet;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.tracing.TraceUtils;

/**
 * Filter for distributed tracing -- creating a span for this request.
 */
public class TracingFilter extends HttpFilter {

  @Override
  public void init(FilterConfig config) throws ServletException {
    super.init(config);
  }

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    Context context = TraceUtils.extractContext(req);
    Span span = TraceUtils.startHttpRequestSpan(req, context);

    final Thread currentThread = Thread.currentThread();
    final String oldThreadName = currentThread.getName();
    try (var scope = context.with(span).makeCurrent()) {
      assert scope != null; // prevent javac warning about scope being unused
      TraceUtils.setSpan(req, span);
      TraceUtils.ifValidTraceId(
          span, s -> MDCLoggingContext.setTracerId(s.getSpanContext().getTraceId()));
      String traceId = MDCLoggingContext.getTraceId();
      if (traceId != null) {
        currentThread.setName(oldThreadName + "-" + traceId);
      }
      chain.doFilter(req, res);
    } finally {
      currentThread.setName(oldThreadName);
      TraceUtils.setHttpStatus(span, res.getStatus());
      span.end();
    }
  }
}
