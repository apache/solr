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

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.RefCounted;
import org.apache.tika.sax.BodyContentHandler;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.InputStreamRequestContent;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Extraction backend using the Tika Server. It uses a shared Jetty HttpClient. TODO: Get rid of the
 * import of org.apache.tika.sax.BodyContentHandler;
 */
public class TikaServerExtractionBackend implements ExtractionBackend {
  /**
   * Default maximum response size (100MB) to prevent excessive memory usage from large documents
   */
  public static final long DEFAULT_MAXCHARS_LIMIT = 100 * 1024 * 1024;

  private static final Object INIT_LOCK = new Object();
  private final String baseUrl;
  private static final int DEFAULT_TIMEOUT_SECONDS = 3 * 60;
  private final Duration defaultTimeout;
  private final TikaServerParser tikaServerResponseParser = new TikaServerParser();
  private boolean tikaMetadataCompatibility;
  private HashMap<String, Object> initArgsMap = new HashMap<>();
  private final long maxCharsLimit;

  // Singleton holder for the shared HttpClient/Executor resources (one per JVM)
  private static volatile RefCounted<HttpClientResources> SHARED_RESOURCES;
  // Per-backend handle (same RefCounted instance as SHARED_RESOURCES) that this instance will
  // decref() on close
  private RefCounted<HttpClientResources> acquiredResourcesRef;

  public TikaServerExtractionBackend(String baseUrl) {
    this(baseUrl, DEFAULT_TIMEOUT_SECONDS, null, DEFAULT_MAXCHARS_LIMIT);
  }

  public TikaServerExtractionBackend(
      String baseUrl, int timeoutSeconds, NamedList<?> initArgs, long maxCharsLimit) {
    // Validate baseUrl
    if (baseUrl == null || baseUrl.trim().isEmpty()) {
      throw new IllegalArgumentException("baseUrl cannot be null or empty");
    }
    // Validate URL format and scheme
    try {
      URI uri = new URI(baseUrl);
      String scheme = uri.getScheme();
      if (scheme == null
          || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
        throw new IllegalArgumentException(
            "baseUrl must use http or https scheme, got: " + baseUrl);
      }
      uri.toURL(); // Additional validation that it's a valid URL
    } catch (URISyntaxException | MalformedURLException e) {
      throw new IllegalArgumentException("Invalid baseUrl: " + baseUrl, e);
    }

    this.maxCharsLimit = maxCharsLimit;
    if (initArgs != null) {
      initArgs.toMap(this.initArgsMap);
    }
    Object metaCompatObh = this.initArgsMap.get(ExtractingParams.TIKASERVER_METADATA_COMPATIBILITY);
    if (metaCompatObh != null) {
      this.tikaMetadataCompatibility = Boolean.parseBoolean(metaCompatObh.toString());
    }
    if (timeoutSeconds <= 0) {
      timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
    }
    if (baseUrl.endsWith("/")) {
      this.baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    } else {
      this.baseUrl = baseUrl;
    }
    this.defaultTimeout =
        Duration.ofSeconds(timeoutSeconds > 0 ? timeoutSeconds : DEFAULT_TIMEOUT_SECONDS);

    // Acquire a reference to the shared resources; keep a handle so we can decref() on close
    acquiredResourcesRef = initializeHttpClient().incref();
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
      if (tikaMetadataCompatibility) {
        appendBackCompatTikaMetadata(md);
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
      if (tikaMetadataCompatibility) {
        appendBackCompatTikaMetadata(md);
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

    HttpClient client = acquiredResourcesRef.get().client;

    Request req = client.newRequest(url).method("PUT");
    Duration effectiveTimeout =
        (request.tikaServerTimeoutSeconds != null && request.tikaServerTimeoutSeconds > 0)
            ? Duration.ofSeconds(request.tikaServerTimeoutSeconds)
            : defaultTimeout;
    req.timeout(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS);
    // Also set idle timeout in case of heavy server side work like OCR
    req.idleTimeout(effectiveTimeout.toMillis(), TimeUnit.MILLISECONDS);

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
            "Error communicating with TikaServer at "
                + url
                + ": "
                + cause.getClass().getSimpleName()
                + ": "
                + cause.getMessage()
                + ". Check that a TikaServer is running.",
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

    InputStream responseStream = listener.getInputStream();
    // Bound the amount of data we read from Tika Server to avoid excessive memory/CPU usage
    return new LimitingInputStream(responseStream, maxCharsLimit);
  }

  private static class LimitingInputStream extends InputStream {
    private final InputStream in;
    private final long max;
    private long count;

    LimitingInputStream(InputStream in, long max) {
      this.in = in;
      this.max = max;
      this.count = 0L;
    }

    private void checkLimit(long toAdd) {
      if (max <= 0) return; // non-positive means unlimited
      long newCount = count + toAdd;
      if (newCount > max) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "TikaServer response exceeded the configured maximum size of " + max + " bytes");
      }
      count = newCount;
    }

    @Override
    public int read() throws IOException {
      int b = in.read();
      if (b != -1) {
        checkLimit(1);
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = in.read(b, off, len);
      if (n > 0) {
        checkLimit(n);
      }
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = in.skip(n);
      if (skipped > 0) {
        checkLimit(skipped);
      }
      return skipped;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public int available() throws IOException {
      return in.available();
    }
  }

  private static final class HttpClientResources {
    final HttpClient client;
    final ExecutorService executor;

    HttpClientResources(HttpClient client, ExecutorService executor) {
      this.client = client;
      this.executor = executor;
    }
  }

  private static final class ResourcesRef extends RefCounted<HttpClientResources> {
    ResourcesRef(HttpClientResources r) {
      super(r);
    }

    @Override
    protected void close() {
      // stop client and shutdown executor
      try {
        if (resource.client != null) resource.client.stop();
      } catch (Throwable ignore) {
      }
      try {
        if (resource.executor != null) resource.executor.shutdownNow();
      } catch (Throwable ignore) {
      }
      synchronized (INIT_LOCK) {
        // clear the shared reference when closed
        if (SHARED_RESOURCES == this) {
          SHARED_RESOURCES = null;
        }
      }
    }
  }

  private static RefCounted<HttpClientResources> initializeHttpClient() {
    RefCounted<HttpClientResources> ref = SHARED_RESOURCES;
    if (ref != null) return ref;
    synchronized (INIT_LOCK) {
      if (SHARED_RESOURCES != null) return SHARED_RESOURCES;
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
      SHARED_RESOURCES = new ResourcesRef(new HttpClientResources(client, exec));
      return SHARED_RESOURCES;
    }
  }

  private final Map<String, String> fieldMappings = new LinkedHashMap<>();

  // TODO: Improve backward compatibility by adding more mappings
  {
    fieldMappings.put("dc:title", "title");
    fieldMappings.put("dc:creator", "author");
    fieldMappings.put("dc:description", "description");
    fieldMappings.put("dc:subject", "subject");
    fieldMappings.put("dc:language", "language");
    fieldMappings.put("dc:publisher", "publisher");
    fieldMappings.put("dcterms:created", "created");
    fieldMappings.put("dcterms:modified", "modified");
    fieldMappings.put("meta:author", "Author");
    fieldMappings.put("meta:creation-date", "Creation-Date");
    fieldMappings.put("meta:save-date", "Last-Save-Date");
    fieldMappings.put("meta:keyword", "Keywords");
    fieldMappings.put("pdf:docinfo:keywords", "Keywords");
  }

  /*
   * Appends back-compatible metadata into the given {@code ExtractionMetadata} instance by mapping
   * source fields to target fields, provided that backward compatibility is enabled. If a source
   * field exists and the target field is not yet populated, the values from the source field will
   * be added to the target field.
   */
  private void appendBackCompatTikaMetadata(ExtractionMetadata md) {
    for (Map.Entry<String, String> mapping : fieldMappings.entrySet()) {
      String sourceField = mapping.getKey();
      String targetField = mapping.getValue();
      if (md.getFirst(sourceField) != null && md.getFirst(targetField) == null) {
        md.add(targetField, md.get(sourceField));
      }
    }
  }

  @Override
  public void close() {
    RefCounted<HttpClientResources> ref;
    synchronized (INIT_LOCK) {
      ref = acquiredResourcesRef;
      acquiredResourcesRef = null;
    }
    if (ref != null) {
      ref.decref();
    }
  }
}
