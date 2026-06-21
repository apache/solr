/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.JettyRequestCarrier;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.FormRequestContent;
import org.eclipse.jetty.client.util.InputStreamRequestContent;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forwards (proxies) a request from the node that received it to the node that actually hosts the
 * target core/replica, reusing the (HTTP/2) Jetty {@link HttpClient} that {@link SolrDispatchFilter}
 * already manages.
 *
 * <p>Unlike the previous fully-streaming proxy (custom {@code ReadListener}/{@code WriteListener}
 * state machine) this implementation streams the <em>request</em> body directly via
 * {@link InputStreamRequestContent} and relays the <em>response</em> as it arrives via a simple
 * content listener that performs flow-controlled (blocking) writes to the servlet output stream.
 * The response is NOT buffered in memory, so large/unbounded streaming responses (e.g. /stream,
 * /export) are relayed incrementally. This avoids both the memory cost of buffering and the
 * write-listener races that surfaced as {@code EofException: Closed} (and undiagnosable empty-body
 * 500s) when a downstream connection was torn down mid-write under load.
 */
public class SolrRequestForwarder {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrRequestForwarder() {}

  /**
   * Proxy {@code req} to {@code coreUrl} using the supplied (shared) Jetty client. Switches the
   * servlet to async mode and completes it when the proxied exchange finishes.
   *
   * @param req         the inbound servlet request
   * @param response    the inbound servlet response to relay the proxied result into
   * @param coreUrl     fully-qualified base URL of the target core (already chosen by the caller)
   * @param httpClient  the shared Jetty HTTP client (HTTP/2) owned by {@link SolrDispatchFilter}
   * @param timeoutMs   per-request timeout in milliseconds
   */
  public static void forward(HttpServletRequest req, HttpServletResponse response, String coreUrl,
                             HttpClient httpClient, long timeoutMs, SolrParams parsedParams) throws IOException {
    if (req == null) {
      return;
    }

    // Single-hop guard: a request that already carries X-Forwarded-For has been proxied once
    // already. We never proxy a proxied request again (prevents proxy loops / multi-hop fan-out).
    String fhost = req.getHeader(HttpHeader.X_FORWARDED_FOR.toString());
    if (fhost != null) {
      log.warn("Already proxied, return 404");
      SolrCall.sendError(404, "No SolrCore found to service request.", response);
      return;
    }

    if (!req.isAsyncStarted()) {
      AsyncContext asyncContext = req.startAsync();
      asyncContext.setTimeout(0);
    }
    final AsyncContext async = req.getAsyncContext();

    log.info("proxy to:{}?{}", coreUrl, req.getQueryString());

    // For form-urlencoded POSTs the servlet container may have already consumed the request body to
    // parse it into request parameters, leaving getInputStream() drained. Forwarding that drained
    // stream would send an EMPTY body to the remote handler. Detect that case and rebuild the body
    // from the parsed parameter map. getParameterMap() merges query-string and body params, so for
    // the rebuilt-body case we proxy to the URL WITHOUT its query string to avoid duplicating params.
    String contentType = req.getContentType();
    boolean isFormPost = "POST".equalsIgnoreCase(req.getMethod()) && contentType != null
        && contentType.toLowerCase(Locale.ENGLISH).startsWith("application/x-www-form-urlencoded");
    String urlStr = isFormPost ? coreUrl
        : coreUrl + '?' + (req.getQueryString() != null ? req.getQueryString() : "");

    // A path-addressed request that resolved to MULTIPLE collections (e.g. a multi-collection alias such
    // as /solr/<alias>/select) carries no "collection" query param, so a single-core forward target would
    // only search its own collection. When the caller supplied a resolved multi-collection list in
    // parsedParams, carry it on the forwarded URL so the target node fans the distributed search out across
    // all of the listed collections. Only applied when the query string does not already pin a collection.
    if (!isFormPost && parsedParams != null) {
      String forwardColl = parsedParams.get("collection");
      String qs = req.getQueryString();
      if (forwardColl != null && !forwardColl.isEmpty() && (qs == null || !qs.contains("collection="))) {
        urlStr = urlStr + (urlStr.endsWith("?") ? "" : "&") + "collection=" + URLEncoder.encode(forwardColl, "UTF-8");
      }
    }

    final Request proxyRequest;
    try {
      proxyRequest = httpClient.newRequest(new URL(urlStr).toURI())
          .method(req.getMethod())
          .version(HttpVersion.fromString(req.getProtocol()))
          .timeout(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (IllegalArgumentException | URISyntaxException e) {
      log.error("Error parsing URI for proxying {}", urlStr, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    SolrCall.copyRequestHeaders(req, proxyRequest);
    SolrCall.addProxyHeaders(req, proxyRequest);

    // Propagate the active OpenTracing span onto the proxied request so the target node's span is
    // recorded as a child of this (proxying) node's span. copyRequestHeaders only carries the
    // ORIGINAL client headers (which, for a SolrJ client that started no span, contain no trace
    // context), so without this the leader's /update span starts a brand-new trace -> two root
    // spans for a single update (TestDistributedTracing). Mirrors the inject HttpShardHandler /
    // SolrCmdDistributor perform for distributed sub-requests.
    Tracer tracer = GlobalTracer.getTracer();
    Span span = tracer != null ? tracer.activeSpan() : null;
    if (span != null) {
      tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new JettyRequestCarrier(proxyRequest));
    }

    if (isFormPost) {
      // The form body was already parsed (and the input stream drained) by HttpSolrCall's
      // constructor, so req.getParameterMap()/getInputStream() can no longer recover it. Rebuild the
      // body from the already-parsed params (which merge query-string + body params) and proxy to the
      // bare coreUrl (no query string) to avoid duplicating the query-string params.
      proxyRequest.headers(h -> {
        h.remove(HttpHeader.CONTENT_LENGTH);
        h.remove(HttpHeader.CONTENT_TYPE);
      });
      Fields fields = new Fields(true);
      if (parsedParams != null) {
        java.util.Iterator<String> names = parsedParams.getParameterNamesIterator();
        while (names.hasNext()) {
          String name = names.next();
          String[] vals = parsedParams.getParams(name);
          if (vals != null) {
            for (String v : vals) {
              if (v != null) {
                fields.add(name, v);
              }
            }
          }
        }
      }
      proxyRequest.body(new FormRequestContent(fields));
    } else if (SolrCall.hasContent(req)) {
      // InputStreamRequestContent streams the (blocking) servlet input stream directly, without the
      // fragile non-blocking ReadListener bridge.
      proxyRequest.body(new InputStreamRequestContent(req.getInputStream()));
    }

    proxyRequest.send(new Response.Listener.Adapter() {
      @Override
      public void onHeaders(Response proxyResponse) {
        response.setStatus(proxyResponse.getStatus());
        copyResponseHeaders(proxyResponse, response);
      }

      @Override
      public void onContent(Response proxyResponse, ByteBuffer content, Callback callback) {
        // Relay each chunk straight to the inbound client. We are in async mode WITHOUT a
        // WriteListener, so the servlet output stream is in blocking mode and a direct write here is
        // legal; Jetty's callback gives us flow control (no more content is delivered until we
        // succeed), so nothing is buffered beyond the current chunk.
        try {
          OutputStream os = response.getOutputStream();
          if (content.hasArray()) {
            os.write(content.array(), content.arrayOffset() + content.position(), content.remaining());
          } else {
            byte[] buf = new byte[content.remaining()];
            content.get(buf);
            os.write(buf);
          }
          os.flush();
          callback.succeeded();
        } catch (Throwable x) {
          callback.failed(x);
          proxyResponse.abort(x);
        }
      }

      @Override
      public void onComplete(Result result) {
        try {
          if (result.isFailed()) {
            handleFailure(result.getFailure(), response);
          }
        } finally {
          if (req.isAsyncStarted()) {
            async.complete();
          }
        }
      }
    });
  }

  private static void handleFailure(Throwable t, HttpServletResponse response) {
    // Preserve the historical contract: a failed forward surfaces as a 500 (unless the upstream
    // failure was itself a SolrException carrying a specific code). Clients and tests
    // (e.g. testThatCantForwardToLeaderFails) rely on 500 here, not a gateway-style 502/504.
    int status = 500;
    Throwable root = t;
    while (root != null && !(root instanceof SolrException) && root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    if (root instanceof SolrException) {
      status = ((SolrException) root).code();
    }
    String msg = "Forwarding error: " + (t != null ? t.toString() : "unknown");
    log.warn("Proxy/forward failure", t);
    try {
      if (!response.isCommitted()) {
        response.sendError(status, msg);
      }
    } catch (IOException e) {
      log.error("Could not send proxy error response", e);
    }
  }

  private static void copyResponseHeaders(Response proxyResponse, HttpServletResponse response) {
    Set<String> hopHeaders = SolrCall.HOP_HEADERS;
    for (HttpField field : proxyResponse.getHeaders()) {
      String lc = field.getName().toLowerCase(Locale.ROOT);
      if (hopHeaders.contains(lc)) {
        continue;
      }
      response.addHeader(field.getName(), field.getValue());
    }
  }
}
