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
package org.apache.solr.handler.extraction;

import java.io.InputStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.tika.sax.BodyContentHandler;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.InputStreamRequestContent;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.xml.sax.helpers.DefaultHandler;

/** Extraction backend using the Tika Server. It uses a shared Jetty HttpClient. */
public class TikaServerExtractionBackend implements ExtractionBackend {
  private static volatile HttpClient SHARED_CLIENT;
  private static volatile ExecutorService SHARED_EXECUTOR;
  private static final Object INIT_LOCK = new Object();
  private static volatile boolean INITIALIZED = false;
  private static volatile boolean SHUTDOWN = false;
  private final String baseUrl; // e.g., http://localhost:9998
  private static final int DEFAULT_TIMEOUT_SECONDS = 3 * 60;
  private final Duration defaultTimeout;
  private final TikaServerParser tikaServerResponseParser = new TikaServerParser();

  public TikaServerExtractionBackend(String baseUrl) {
    this(baseUrl, DEFAULT_TIMEOUT_SECONDS);
  }

  public TikaServerExtractionBackend(String baseUrl, int timeoutSeconds) {
    if (baseUrl.endsWith("/")) {
      this.baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    } else {
      this.baseUrl = baseUrl;
    }
    this.defaultTimeout =
        Duration.ofSeconds(timeoutSeconds > 0 ? timeoutSeconds : DEFAULT_TIMEOUT_SECONDS);
  }

  public static final String NAME = "tikaserver";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public ExtractionResult extract(InputStream inputStream, ExtractionRequest request)
      throws Exception {
    try (InputStream tikaResponse = callTikaServer(inputStream, request)) {
      ExtractionMetadata md = buildMetadataFromRequest(request);
      BodyContentHandler bodyContentHandler = new BodyContentHandler(-1);
      if (request.tikaServerRecursive) {
        tikaServerResponseParser.parseRmetaJson(tikaResponse, bodyContentHandler, md);
      } else {
        tikaServerResponseParser.parseXml(tikaResponse, bodyContentHandler, md);
      }
      return new ExtractionResult(bodyContentHandler.toString(), md);
    }
  }

  @Override
  public void extractWithSaxHandler(
      InputStream inputStream,
      ExtractionRequest request,
      ExtractionMetadata md,
      DefaultHandler saxContentHandler)
      throws Exception {
    try (InputStream tikaResponse = callTikaServer(inputStream, request)) {
      if (request.tikaServerRecursive) {
        tikaServerResponseParser.parseRmetaJson(tikaResponse, saxContentHandler, md);
      } else {
        tikaServerResponseParser.parseXml(tikaResponse, saxContentHandler, md);
      }
    }
  }

  /**
   * Call the Tika Server to extract text and metadata. Depending on <code>request.recursive</code>,
   * will either return XML (false) or JSON array (true). <b>The recursive mode consumes more memory
   * both on the TikaServer side and on the Solr side</b>
   *
   * @return InputStream of the response body, either XML or JSON depending on <code>
   *     request.tikaserverRecursive</code>
   */
  InputStream callTikaServer(InputStream inputStream, ExtractionRequest request) throws Exception {
    String url = baseUrl + (request.tikaServerRecursive ? "/rmeta" : "/tika");

    ensureClientInitialized();
    HttpClient client = SHARED_CLIENT;

    Request req = client.newRequest(url).method("PUT");
    Duration effectiveTimeout =
        (request.tikaServerTimeoutSeconds != null && request.tikaServerTimeoutSeconds > 0)
            ? Duration.ofSeconds(request.tikaServerTimeoutSeconds)
            : defaultTimeout;
    req.timeout(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS);

    // Headers
    String accept = (request.tikaServerRecursive ? "application/json" : "text/xml");
    req.headers(h -> h.add("Accept", accept));
    String contentType = (request.streamType != null) ? request.streamType : request.contentType;
    if (contentType != null) {
      req.headers(h -> h.add("Content-Type", contentType));
    }
    if (!request.tikaServerRequestHeaders.isEmpty()) {
      req.headers(
          h ->
              request.tikaServerRequestHeaders.forEach(
                  (k, v) -> {
                    if (k != null && v != null) h.add(k, v);
                  }));
    }

    ExtractionMetadata md = buildMetadataFromRequest(request);
    if (request.resourcePassword != null || request.passwordsMap != null) {
      RegexRulesPasswordProvider passwordProvider = new RegexRulesPasswordProvider();
      if (request.resourcePassword != null) {
        passwordProvider.setExplicitPassword(request.resourcePassword);
      }
      if (request.passwordsMap != null) {
        passwordProvider.setPasswordMap(request.passwordsMap);
      }
      String pwd = passwordProvider.getPassword(md);
      if (pwd != null) {
        req.headers(h -> h.add("Password", pwd)); // Tika Server expects this header if provided
      }
    }
    if (request.resourceName != null) {
      req.headers(
          h ->
              h.add(
                  "Content-Disposition", "attachment; filename=\"" + request.resourceName + "\""));
    }

    if (contentType != null) {
      req.body(new InputStreamRequestContent(contentType, inputStream));
    } else {
      req.body(new InputStreamRequestContent(inputStream));
    }

    InputStreamResponseListener listener = new InputStreamResponseListener();
    req.send(listener);

    final Response response;
    try {
      response = listener.get(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      throw new SolrException(
          SolrException.ErrorCode.GATEWAY_TIMEOUT,
          "Timeout after "
              + effectiveTimeout.toMillis()
              + " ms while waiting for response from TikaServer "
              + url,
          te);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Interrupted while waiting for response from TikaServer " + url,
          ie);
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause instanceof ConnectException
          || cause instanceof SocketTimeoutException
          || cause instanceof EofException
          || cause instanceof ClosedChannelException) {
        throw new SolrException(
            SolrException.ErrorCode.SERVICE_UNAVAILABLE,
            "Error communicating with TikaServer "
                + url
                + ": "
                + cause.getClass().getSimpleName()
                + ": "
                + cause.getMessage(),
            cause);
      }
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error while calling TikaServer " + url,
          ee);
    }

    int code = response.getStatus();
    if (code < 200 || code >= 300) {
      SolrException.ErrorCode errorCode = SolrException.ErrorCode.getErrorCode(code);
      String reason = response.getReason();
      String msg =
          "TikaServer "
              + url
              + " returned status "
              + code
              + (reason != null ? " (" + reason + ")" : "");
      throw new SolrException(errorCode, msg);
    }

    return listener.getInputStream();
  }

  private static void ensureClientInitialized() {
    if (INITIALIZED) return;
    synchronized (INIT_LOCK) {
      if (INITIALIZED) return;
      ThreadFactory tf = new SolrNamedThreadFactory("TikaServerHttpClient");
      ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool(tf);
      HttpClient client = new HttpClient();
      client.setExecutor(exec);
      client.setScheduler(new ScheduledExecutorScheduler("TikaServerHttpClient-scheduler", true));
      try {
        client.start();
      } catch (Exception e) {
        try {
          exec.shutdownNow();
        } catch (Throwable ignore) {
        }
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Failed to start shared Jetty HttpClient", e);
      }
      SHARED_EXECUTOR = exec;
      SHARED_CLIENT = client;
      INITIALIZED = true;
      SHUTDOWN = false;
    }
  }

  @Override
  public void close() {
    if (SHUTDOWN) return;
    synchronized (INIT_LOCK) {
      if (SHUTDOWN) return;
      HttpClient client = SHARED_CLIENT;
      ExecutorService exec = SHARED_EXECUTOR;
      SHARED_CLIENT = null;
      SHARED_EXECUTOR = null;
      INITIALIZED = false;
      SHUTDOWN = true;
      if (client != null) {
        try {
          client.stop();
        } catch (Throwable ignore) {
        }
      }
      if (exec != null) {
        try {
          exec.shutdownNow();
        } catch (Throwable ignore) {
        }
      }
    }
  }
}
