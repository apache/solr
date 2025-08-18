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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.solr.util.tracing.TraceUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.InputStreamRequestContent;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.Result;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;

/** Helper class for proxying the request to another Solr node. */
// Tried to use Jetty's ProxyServlet instead but ran into inexplicable difficulties:  EOF/reset.
// Perhaps was related to its use of ServletRequest.startAsync()/AsyncContext
class HttpSolrProxy {
  // TODO add X-Forwarded-For and with comma delimited

  private static final Set<HttpHeader> HOP_BY_HOP_HEADERS =
      EnumSet.of(
          HttpHeader.CONNECTION,
          HttpHeader.KEEP_ALIVE,
          HttpHeader.PROXY_AUTHENTICATE,
          HttpHeader.PROXY_AUTHORIZATION,
          HttpHeader.TE,
          HttpHeader.TRANSFER_ENCODING,
          HttpHeader.UPGRADE);

  // Methods that shouldn't have a body according to HTTP spec
  private static final Set<String> NO_BODY_METHODS = Set.of("GET", "HEAD", "DELETE");

  static void doHttpProxy(
      HttpClient httpClient,
      HttpServletRequest servletReq,
      HttpServletResponse servletRsp,
      String url)
      throws Throwable {
    Request proxyReq = httpClient.newRequest(url).method(servletReq.getMethod());

    // clearing them first to ensure there's no stock entries (e.g. user-agent)
    proxyReq.headers(proxyFields -> copyRequestHeaders(servletReq, proxyFields.clear()));

    // FYI see InstrumentedHttpListenerFactory
    TraceUtils.injectTraceContext(proxyReq);
    // TODO client spans.  See OTEL agent's approach:
    // https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/jetty-httpclient/jetty-httpclient-12.0

    if (!NO_BODY_METHODS.contains(servletReq.getMethod())) {
      proxyReq.body(
          new InputStreamRequestContent(servletReq.getContentType(), servletReq.getInputStream()));
    }

    CompletableFuture<Result> resultFuture = new CompletableFuture<>();

    proxyReq.send(
        new Response.Listener() {
          private final byte[] buffer = new byte[8192];

          @Override
          public void onBegin(Response response) {
            servletRsp.setStatus(response.getStatus());
          }

          @Override
          public void onHeaders(Response response) {
            copyResponseHeaders(response, servletRsp);
          }

          @Override
          public void onContent(Response response, ByteBuffer content) {
            try {
              final OutputStream clientOutputStream = servletRsp.getOutputStream();

              // Copy content to the client's output stream in chunks using the existing buffer
              int remaining = content.remaining();
              while (remaining > 0) {
                int chunkSize = Math.min(remaining, buffer.length);
                content.get(buffer, 0, chunkSize);
                clientOutputStream.write(buffer, 0, chunkSize);
                remaining -= chunkSize;
              }
              clientOutputStream.flush();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void onComplete(Result result) {
            resultFuture.complete(result);
          }
        });

    Result result = resultFuture.get(); // waits
    var failure = result.getFailure();
    if (failure != null) {
      throw failure;
    }
  }

  private static void copyRequestHeaders(
      HttpServletRequest servletReq, HttpFields.Mutable proxyFields) {
    servletReq
        .getHeaderNames()
        .asIterator()
        .forEachRemaining(
            headerName -> {
              HttpHeader knownHeader = HttpHeader.CACHE.get(headerName); // maybe null
              if (!HOP_BY_HOP_HEADERS.contains(knownHeader)) {
                servletReq
                    .getHeaders(headerName)
                    .asIterator()
                    .forEachRemaining(headerVal -> proxyFields.add(headerName, headerVal));
              }
            });
  }

  private static void copyResponseHeaders(Response proxyRsp, HttpServletResponse servletRsp) {
    for (HttpField headerField : proxyRsp.getHeaders()) {
      HttpHeader knownHeader = headerField.getHeader();
      if (!HOP_BY_HOP_HEADERS.contains(knownHeader)) {
        // HttpField: even if multiple values, it's encoded as one comma delimited value
        servletRsp.addHeader(headerField.getName(), headerField.getValue());
      }
    }
  }
}
