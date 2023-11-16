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

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopTracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.apache.http.HttpHeaders;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.tracing.HttpServletCarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Various Util methods for interaction on servlet level, i.e. HttpServletRequest */
public abstract class ServletUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static String CLOSE_STREAM_MSG =
      "Attempted close of http request or response stream - in general you should not do this, "
          + "you may spoil connection reuse and possibly disrupt a client. If you must close without actually needing to close, "
          + "use a CloseShield*Stream. Closing or flushing the response stream commits the response and prevents us from modifying it. "
          + "Closing the request stream prevents us from guaranteeing ourselves that streams are fully read for proper connection reuse."
          + "Let the container manage the lifecycle of these streams when possible.";

  private ServletUtils() {
    /* only static methods in this class */
  }

  /**
   * Use this to get the full path after context path "/solr", which is a combination of servletPath
   * and pathInfo.
   *
   * @param request the HttpServletRequest object
   * @return String with path starting with a "/", or empty string if no path
   */
  public static String getPathAfterContext(HttpServletRequest request) {
    return request.getServletPath() + (request.getPathInfo() != null ? request.getPathInfo() : "");
  }

  /**
   * Wrap the request's input stream with a close shield. If this is a retry, we will assume that
   * the stream has already been wrapped and do nothing.
   *
   * <p>Only the container should ever actually close the servlet output stream. This method
   * possibly should be turned into a servlet filter
   *
   * @param request The request to wrap.
   * @param retry If this is an original request or a retry.
   * @return A request object with an {@link InputStream} that will ignore calls to close.
   */
  public static HttpServletRequest closeShield(HttpServletRequest request, boolean retry) {
    if (!retry) {
      return new HttpServletRequestWrapper(request) {

        @Override
        public ServletInputStream getInputStream() throws IOException {

          return new ServletInputStreamWrapper(super.getInputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full
              // understanding can take place
              assert !Thread.currentThread()
                      .getStackTrace()[2]
                      .getClassName()
                      .matches("org\\.apache\\.(?:solr|lucene).*")
                  : CLOSE_STREAM_MSG;
              this.stream = ClosedServletInputStream.CLOSED_SERVLET_INPUT_STREAM;
            }
          };
        }
      };
    } else {
      return request;
    }
  }

  /**
   * Wrap the response's output stream with a close shield. If this is a retry, we will assume that
   * the stream has already been wrapped and do nothing.
   *
   * <p>Only the container should ever actually close the servlet request stream.
   *
   * @param response The response to wrap.
   * @param retry If this response corresponds to an original request or a retry.
   * @return A response object with an {@link OutputStream} that will ignore calls to close.
   */
  public static HttpServletResponse closeShield(HttpServletResponse response, boolean retry) {
    if (!retry) {
      return new HttpServletResponseWrapper(response) {

        @Override
        public ServletOutputStream getOutputStream() throws IOException {

          return new ServletOutputStreamWrapper(super.getOutputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full
              // understanding can take place
              assert !Thread.currentThread()
                      .getStackTrace()[2]
                      .getClassName()
                      .matches("org\\.apache\\.(?:solr|lucene).*")
                  : CLOSE_STREAM_MSG;
              stream = ClosedServletOutputStream.CLOSED_SERVLET_OUTPUT_STREAM;
            }
          };
        }
      };
    } else {
      return response;
    }
  }

  static boolean excludedPath(
      List<Pattern> excludePatterns,
      HttpServletRequest request,
      HttpServletResponse response,
      FilterChain chain)
      throws IOException, ServletException {
    String requestPath = getPathAfterContext(request);
    // No need to even create the HttpSolrCall object if this path is excluded.
    if (excludePatterns != null) {
      for (Pattern p : excludePatterns) {
        Matcher matcher = p.matcher(requestPath);
        if (matcher.lookingAt()) {
          if (chain != null) {
            chain.doFilter(request, response);
          }
          return true;
        }
      }
    }
    return false;
  }

  static boolean excludedPath(
      List<Pattern> excludePatterns, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    return excludedPath(excludePatterns, request, response, null);
  }

  static void configExcludes(PathExcluder excluder, String patternConfig) {
    if (patternConfig != null) {
      String[] excludeArray = patternConfig.split(",");
      List<Pattern> patterns = new ArrayList<>();
      excluder.setExcludePatterns(patterns);
      for (String element : excludeArray) {
        patterns.add(Pattern.compile(element));
      }
    }
  }

  /**
   * Enforces rate limiting for a request. Should be converted to a servlet filter at some point.
   * Currently, this is tightly coupled with request tracing which is not ideal either.
   *
   * @param request The request to limit
   * @param response The associated response
   * @param limitedExecution code that will be traced
   */
  static void rateLimitRequest(
      RateLimitManager rateLimitManager,
      HttpServletRequest request,
      HttpServletResponse response,
      Runnable limitedExecution)
      throws ServletException, IOException {
    boolean accepted = false;
    try {
      accepted = rateLimitManager.handleRequest(request);
      if (!accepted) {
        response.sendError(ErrorCode.TOO_MANY_REQUESTS.code, RateLimitManager.ERROR_MESSAGE);
        return;
      }
      // todo: this shouldn't be required, tracing and rate limiting should be independently
      // composable
      traceHttpRequestExecution2(request, response, limitedExecution);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e.getMessage());
    } finally {
      if (accepted) {
        rateLimitManager.decrementActiveRequests(request);
      }
    }
  }

  /**
   * Sets up tracing for an HTTP request. Perhaps should be converted to a servlet filter at some
   * point.
   *
   * @param request The request to limit
   * @param response The associated response
   * @param tracedExecution the executed code
   */
  private static void traceHttpRequestExecution2(
      HttpServletRequest request, HttpServletResponse response, Runnable tracedExecution)
      throws ServletException, IOException {
    Tracer tracer = getTracer(request);
    Span span = buildSpan(tracer, request);
    final Thread currentThread = Thread.currentThread();
    final String oldThreadName = currentThread.getName();
    request.setAttribute(SolrDispatchFilter.ATTR_TRACING_SPAN, span);
    try (var scope = tracer.scopeManager().activate(span)) {
      assert scope != null; // prevent javac warning about scope being unused
      MDCLoggingContext.setTracerId(span.context().toTraceId()); // handles empty string
      String traceId = MDCLoggingContext.getTraceId();
      if (traceId != null) {
        currentThread.setName(oldThreadName + "-" + traceId);
      }
      tracedExecution.run();
    } catch (ExceptionWhileTracing e) {
      if (e.e instanceof SolrAuthenticationException) {
        // done, the response and status code have already been sent
        return;
      }
      if (e.e instanceof ServletException) {
        throw (ServletException) e.e;
      }
      if (e.e instanceof IOException) {
        throw (IOException) e.e;
      }
      if (e.e instanceof RuntimeException) {
        throw (RuntimeException) e.e;
      } else {
        throw new RuntimeException(e.e);
      }
    } finally {
      currentThread.setName(oldThreadName);
      span.setTag(Tags.HTTP_STATUS, response.getStatus());
      span.finish();
    }
  }

  private static Tracer getTracer(HttpServletRequest req) {
    return (Tracer) req.getAttribute(SolrDispatchFilter.ATTR_TRACING_TRACER);
  }

  protected static Span buildSpan(Tracer tracer, HttpServletRequest request) {
    if (tracer instanceof NoopTracer) {
      return NoopSpan.INSTANCE;
    }
    Tracer.SpanBuilder spanBuilder =
        tracer
            .buildSpan("http.request") // will be changed later
            .asChildOf(tracer.extract(Format.Builtin.HTTP_HEADERS, new HttpServletCarrier(request)))
            .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_SERVER)
            .withTag(Tags.HTTP_METHOD, request.getMethod())
            .withTag(Tags.HTTP_URL, request.getRequestURL().toString());
    if (request.getQueryString() != null) {
      spanBuilder.withTag("http.params", request.getQueryString());
    }
    spanBuilder.withTag(Tags.DB_TYPE, "solr");
    return spanBuilder.start();
  }

  // we make sure we read the full client request so that the client does
  // not hit a connection reset and we can reuse the
  // connection - see SOLR-8453 and SOLR-8683
  static void consumeInputFully(HttpServletRequest req, HttpServletResponse response) {
    try {
      ServletInputStream is = req.getInputStream();
      //noinspection StatementWithEmptyBody
      while (!is.isFinished() && is.read() != -1) {}
    } catch (IOException e) {
      if (req.getHeader(HttpHeaders.EXPECT) != null && response.isCommitted()) {
        log.debug("No input stream to consume from client");
      } else {
        log.info("Could not consume full client request", e);
      }
    }
  }

  public static class ClosedServletInputStream extends ServletInputStream {

    public static final ClosedServletInputStream CLOSED_SERVLET_INPUT_STREAM =
        new ClosedServletInputStream();

    @Override
    public int read() {
      return -1;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setReadListener(ReadListener arg0) {}
  }

  public static class ClosedServletOutputStream extends ServletOutputStream {

    public static final ClosedServletOutputStream CLOSED_SERVLET_OUTPUT_STREAM =
        new ClosedServletOutputStream();

    @Override
    public void write(final int b) throws IOException {
      throw new IOException("write(" + b + ") failed: stream is closed");
    }

    @Override
    public void flush() throws IOException {
      throw new IOException("flush() failed: stream is closed");
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {
      throw new RuntimeException("setWriteListener() failed: stream is closed");
    }
  }
}
