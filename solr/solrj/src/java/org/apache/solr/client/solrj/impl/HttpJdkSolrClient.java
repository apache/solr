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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.lang.invoke.MethodHandles;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SolrClient implementation that communicates to a Solr server using the built-in Java 11+ Http
 * Client. This client is targeted for those users who wish to minimize application dependencies.
 * This client will connect to solr using Http/2 but can seamlessly downgrade to Http/1.1 when
 * connecting to Solr hosts running on older versions.
 */
public class HttpJdkSolrClient extends HttpSolrClientBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String USER_AGENT =
      "Solr[" + MethodHandles.lookup().lookupClass().getName() + "] 1.0";

  protected HttpClient httpClient;

  protected ExecutorService executor;

  private boolean forceHttp11;

  private final boolean shutdownExecutor;

  protected HttpJdkSolrClient(String serverBaseUrl, HttpJdkSolrClient.Builder builder) {
    super(serverBaseUrl, builder);

    HttpClient.Redirect followRedirects =
        Boolean.TRUE.equals(builder.followRedirects)
            ? HttpClient.Redirect.NORMAL
            : HttpClient.Redirect.NEVER;
    HttpClient.Builder b = HttpClient.newBuilder().followRedirects(followRedirects);
    if (builder.sslContext != null) {
      b.sslContext(builder.sslContext);
    }

    if (builder.executor != null) {
      this.executor = builder.executor;
      this.shutdownExecutor = false;
    } else {
      BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(1024);
      this.executor =
          new ExecutorUtil.MDCAwareThreadPoolExecutor(
              4,
              256,
              60,
              TimeUnit.SECONDS,
              queue,
              new SolrNamedThreadFactory(this.getClass().getSimpleName()));
      this.shutdownExecutor = true;
    }
    b.executor(this.executor);

    if (builder.useHttp1_1) {
      this.forceHttp11 = true;
      b.version(HttpClient.Version.HTTP_1_1);
    }

    if (builder.cookieHandler != null) {
      b.cookieHandler(builder.cookieHandler);
    }

    if (builder.proxyHost != null) {
      if (builder.proxyIsSocks4) {
        log.warn(
            "Socks4 is likely not supported by this client.  See https://bugs.openjdk.org/browse/JDK-8214516");
      }
      b.proxy(ProxySelector.of(new InetSocketAddress(builder.proxyHost, builder.proxyPort)));
    }
    this.httpClient = b.build();
    updateDefaultMimeTypeForParser();

    assert ObjectReleaseTracker.track(this);
  }

  public Cancellable asyncRequest(
      SolrRequest<?> solrRequest,
      String collection,
      AsyncListener<NamedList<Object>> asyncListener) {
    try {
      PreparedRequest pReq = prepareRequest(solrRequest, collection);
      asyncListener.onStart();
      CompletableFuture<NamedList<Object>> response =
          httpClient
              .sendAsync(pReq.reqb.build(), HttpResponse.BodyHandlers.ofInputStream())
              .thenApply(
                  httpResponse -> {
                    try {
                      return processErrorsAndResponse(solrRequest, pReq.parserToUse, httpResponse, pReq.url);
                    } catch (SolrServerException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .whenComplete(
                  (nl, t) -> {
                    if (t != null) {
                      asyncListener.onFailure(t);
                    } else {
                      asyncListener.onSuccess(nl);
                    }
                  });
      return new HttpJdkSolrClientCancellable(response);
    } catch (Exception e) {
      asyncListener.onFailure(e);
      return () -> {};
    }
  }

  protected class HttpJdkSolrClientCancellable implements Cancellable {
    private final CompletableFuture<NamedList<Object>> response;

    HttpJdkSolrClientCancellable(CompletableFuture<NamedList<Object>> response) {
      this.response = response;
    }

    protected CompletableFuture<NamedList<Object>> getResponse() {
      return response;
    }


    @Override
    public void cancel() {
      response.cancel(true);
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    PreparedRequest pReq = prepareRequest(solrRequest, collection);
    HttpResponse<InputStream> response = null;
    try {
      response = httpClient.send(pReq.reqb.build(), HttpResponse.BodyHandlers.ofInputStream());
      return processErrorsAndResponse(solrRequest, pReq.parserToUse, response, pReq.url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (HttpTimeoutException e) {
      throw new SolrServerException(
          "Timeout occurred while waiting response from server at: " + pReq.url, e);
    } catch (SolrException se) {
      throw se;
    } catch (RuntimeException re) {
      throw new SolrServerException(re);
    } finally {
      if (pReq.contentWritingFuture != null) {
        pReq.contentWritingFuture.cancel(true);
      }

      // See
      // https://docs.oracle.com/en/java/javase/17/docs/api/java.net.http/java/net/http/HttpResponse.BodySubscribers.html#ofInputStream()
      if (!wantStream(pReq.parserToUse)) {
        try {
          response.body().close();
        } catch (Exception e1) {
          // ignore
        }
      }
    }
  }

  private PreparedRequest prepareRequest(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    checkClosed();
    if (ClientUtils.shouldApplyDefaultCollection(collection, solrRequest)) {
      collection = defaultCollection;
    }
    String url = getRequestPath(solrRequest, collection);
    ResponseParser parserToUse = responseParser(solrRequest);
    ModifiableSolrParams queryParams = initalizeSolrParams(solrRequest, parserToUse);
    var reqb = HttpRequest.newBuilder();
    PreparedRequest pReq = null;
    try {
      switch (solrRequest.getMethod()) {
        case GET:
          {
            pReq = prepareGet(url, reqb, solrRequest, queryParams);
            break;
          }
        case POST:
        case PUT:
          {
            pReq = preparePutOrPost(url, solrRequest.getMethod(), reqb, solrRequest, queryParams);
            break;
          }
        default:
          {
            throw new IllegalStateException("Unsupported method: " + solrRequest.getMethod());
          }
      }
    } catch (URISyntaxException | RuntimeException re) {
      throw new SolrServerException(re);
    }
    pReq.parserToUse = parserToUse;
    pReq.url = url;
    return pReq;
  }

  private PreparedRequest prepareGet(
      String url,
      HttpRequest.Builder reqb,
      SolrRequest<?> solrRequest,
      ModifiableSolrParams queryParams)
      throws IOException, URISyntaxException {
    validateGetRequest(solrRequest);
    reqb.GET();
    decorateRequest(reqb, solrRequest);
    reqb.uri(new URI(url + "?" + queryParams));
    return new PreparedRequest(reqb, null);
  }

  private PreparedRequest preparePutOrPost(
      String url,
      SolrRequest.METHOD method,
      HttpRequest.Builder reqb,
      SolrRequest<?> solrRequest,
      ModifiableSolrParams queryParams)
      throws IOException, URISyntaxException {

    final RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);

    final Collection<ContentStream> streams;
    if (contentWriter == null) {
      streams = requestWriter.getContentStreams(solrRequest);
    } else {
      streams = null;
    }

    String contentType = "application/x-www-form-urlencoded";
    if (contentWriter != null && contentWriter.getContentType() != null) {
      contentType = contentWriter.getContentType();
    }
    reqb.header("Content-Type", contentType);

    if (isMultipart(streams)) {
      throw new UnsupportedOperationException("This client does not support multipart.");
    }

    HttpRequest.BodyPublisher bodyPublisher;
    Future<?> contentWritingFuture = null;
    if (contentWriter != null) {
      boolean success = maybeTryHeadRequest(url);
      if (!success) {
        reqb.version(HttpClient.Version.HTTP_1_1);
      }

      final PipedOutputStream source = new PipedOutputStream();
      final PipedInputStream sink = new PipedInputStream(source);
      bodyPublisher = HttpRequest.BodyPublishers.ofInputStream(() -> sink);

      contentWritingFuture =
          executor.submit(
              () -> {
                try (source) {
                  contentWriter.write(source);
                } catch (Exception e) {
                  log.error("Cannot write Content Stream", e);
                }
              });
    } else if (streams != null && streams.size() == 1) {
      boolean success = maybeTryHeadRequest(url);
      if (!success) {
        reqb.version(HttpClient.Version.HTTP_1_1);
      }

      InputStream is = streams.iterator().next().getStream();
      bodyPublisher = HttpRequest.BodyPublishers.ofInputStream(() -> is);
    } else if (queryParams != null && urlParamNames != null) {
      ModifiableSolrParams requestParams = queryParams;
      queryParams = calculateQueryParams(urlParamNames, requestParams);
      queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), requestParams));
      bodyPublisher = HttpRequest.BodyPublishers.ofString(requestParams.toString());
    } else {
      bodyPublisher = HttpRequest.BodyPublishers.noBody();
    }

    decorateRequest(reqb, solrRequest);
    if (method == SolrRequest.METHOD.PUT) {
      reqb.method("PUT", bodyPublisher);
    } else {
      reqb.method("POST", bodyPublisher);
    }
    URI uriWithQueryParams = new URI(url + "?" + queryParams);
    reqb.uri(uriWithQueryParams);

    return new PreparedRequest(reqb, contentWritingFuture);
  }

  private static class PreparedRequest {
    Future<?> contentWritingFuture;
    HttpRequest.Builder reqb;

    ResponseParser parserToUse;

    String url;

    PreparedRequest(HttpRequest.Builder reqb, Future<?> contentWritingFuture) {
      this.reqb = reqb;
      this.contentWritingFuture = contentWritingFuture;
    }
  }

  /**
   * This is a workaround for the case where:
   *
   * <p>(1) no SSL/TLS (2) using POST with stream and (3) using Http/2
   *
   * <p>The JDK Http Client will send an upgrade request over Http/1 along with request content in
   * the same request. However, the Jetty Server underpinning Solr does not accept this.
   *
   * <p>We send a HEAD request first, then the client knows if Solr can accept Http/2, and no
   * additional upgrade requests will be sent.
   *
   * <p>See https://bugs.openjdk.org/browse/JDK-8287589 See
   * https://github.com/jetty/jetty.project/issues/9998#issuecomment-1614216870
   *
   * <p>We only try once, and if it fails, we downgrade to Http/1
   *
   * @param url the url with no request parameters
   * @return true if success
   */
  protected boolean maybeTryHeadRequest(String url) {
    if (forceHttp11 || url == null || url.toLowerCase(Locale.ROOT).startsWith("https://")) {
      return true;
    }
    return maybeTryHeadRequestSync(url);
  }

  protected volatile boolean headRequested; // must be threadsafe
  private boolean headSucceeded; // must be threadsafe

  private synchronized boolean maybeTryHeadRequestSync(String url) {
    if (headRequested) {
      return headSucceeded;
    }

    URI uriNoQueryParams;
    try {
      uriNoQueryParams = new URI(url);
    } catch (URISyntaxException e) {
      // If the url is invalid, let a subsequent request try again.
      return false;
    }
    HttpRequest.Builder headReqB =
        HttpRequest.newBuilder(uriNoQueryParams)
            .method("HEAD", HttpRequest.BodyPublishers.noBody());
    decorateRequest(headReqB, new QueryRequest());
    try {
      httpClient.send(headReqB.build(), HttpResponse.BodyHandlers.discarding());
      headSucceeded = true;
    } catch (IOException ioe) {
      log.warn("Could not issue HEAD request to {} ", url, ioe);
      headSucceeded = false;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      headSucceeded = false;
    } finally {

      // The HEAD request is tried only once.  All future requests will skip this check.
      headRequested = true;

      if (!headSucceeded) {
        log.info("All unencrypted POST requests with a chunked body will use http/1.1");
      }
    }

    return headSucceeded;
  }

  private void decorateRequest(HttpRequest.Builder reqb, SolrRequest<?> solrRequest) {
    if (requestTimeoutMillis > 0) {
      reqb.timeout(Duration.of(requestTimeoutMillis, ChronoUnit.MILLIS));
    } else {
      reqb.timeout(Duration.of(idleTimeoutMillis, ChronoUnit.MILLIS));
    }
    reqb.header("User-Agent", USER_AGENT);
    setBasicAuthHeader(solrRequest, reqb);
    Map<String, String> headers = solrRequest.getHeaders();
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        reqb.header(entry.getKey(), entry.getValue());
      }
    }
  }

  private void setBasicAuthHeader(SolrRequest<?> solrRequest, HttpRequest.Builder reqb) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String encoded =
          basicAuthCredentialsToAuthorizationString(
              solrRequest.getBasicAuthUser(), solrRequest.getBasicAuthPassword());
      reqb.header("Authorization", encoded);
    } else if (basicAuthAuthorizationStr != null) {
      reqb.header("Authorization", basicAuthAuthorizationStr);
    }
  }

  private static final Pattern MIME_TYPE_PATTERN = Pattern.compile("[^;]*");

  private String contentTypeToMimeType(String contentType) {
    Matcher mimeTypeMatcher = MIME_TYPE_PATTERN.matcher(contentType);
    return mimeTypeMatcher.find() ? mimeTypeMatcher.group() : null;
  }

  private static final Pattern CHARSET_PATTERN = Pattern.compile("(?i)^.*charset=(.*)$");

  protected String contentTypeToEncoding(String contentType) {
    Matcher encodingMatcher = CHARSET_PATTERN.matcher(contentType);
    return encodingMatcher.find() ? encodingMatcher.group(1) : null;
  }

  private NamedList<Object> processErrorsAndResponse(
      SolrRequest<?> solrRequest, ResponseParser parser, HttpResponse<InputStream> resp, String url)
      throws SolrServerException {
    String contentType = resp.headers().firstValue("Content-Type").orElse(null);
    contentType = contentType == null ? "" : contentType;
    String mimeType = contentTypeToMimeType(contentType);
    String encoding = contentTypeToEncoding(contentType);
    String method = resp.request() == null ? null : resp.request().method();
    int status = resp.statusCode();
    String reason = "" + status;
    InputStream is = resp.body();
    return processErrorsAndResponse(
        status, reason, method, parser, is, mimeType, encoding, isV2ApiRequest(solrRequest), url);
  }

  @Override
  public void close() throws IOException {
    // If used with Java 21+
    if (httpClient instanceof AutoCloseable) {
      try {
        ((AutoCloseable) httpClient).close();
      } catch (Exception e) {
        log.warn("Could not close the http client.", e);
      }
    }
    httpClient = null;

    if (shutdownExecutor) {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
    executor = null;

    assert ObjectReleaseTracker.release(this);
  }

  private void checkClosed() {
    if (httpClient == null) {
      throw new IllegalStateException("This is closed and cannot be reused.");
    }
  }

  @Override
  protected boolean isFollowRedirects() {
    return httpClient.followRedirects() != HttpClient.Redirect.NEVER;
  }

  @Override
  protected boolean processorAcceptsMimeType(
      Collection<String> processorSupportedContentTypes, String mimeType) {
    return processorSupportedContentTypes.stream()
        .map(this::contentTypeToMimeType)
        .filter(Objects::nonNull)
        .map(String::trim)
        .anyMatch(mimeType::equalsIgnoreCase);
  }

  @Override
  protected void updateDefaultMimeTypeForParser() {
    defaultParserMimeTypes =
        parser.getContentTypes().stream()
            .map(this::contentTypeToMimeType)
            .filter(Objects::nonNull)
            .map(s -> s.toLowerCase(Locale.ROOT).trim())
            .collect(Collectors.toSet());
  }

  @Override
  protected String allProcessorSupportedContentTypesCommaDelimited(
      Collection<String> processorSupportedContentTypes) {
    return processorSupportedContentTypes.stream()
        .map(this::contentTypeToMimeType)
        .filter(Objects::nonNull)
        .map(s -> s.toLowerCase(Locale.ROOT).trim())
        .collect(Collectors.joining(", "));
  }

  public static class Builder
      extends HttpSolrClientBuilderBase<HttpJdkSolrClient.Builder, HttpJdkSolrClient> {

    private SSLContext sslContext;

    private CookieHandler cookieHandler;

    public Builder() {
      super();
    }

    public Builder(String baseSolrUrl) {
      super();
      this.baseSolrUrl = baseSolrUrl;
    }

    @Override
    public HttpJdkSolrClient build() {
      if (idleTimeoutMillis == null || idleTimeoutMillis <= 0) {
        idleTimeoutMillis = (long) HttpClientUtil.DEFAULT_SO_TIMEOUT;
      }
      if (connectionTimeoutMillis == null) {
        connectionTimeoutMillis = (long) HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
      }
      return new HttpJdkSolrClient(baseSolrUrl, this);
    }

    /**
     * Use the provided SSLContext. See {@link
     * java.net.http.HttpClient.Builder#sslContext(SSLContext)}.
     *
     * @param sslContext the ssl context to use
     * @return this Builder
     */
    public HttpJdkSolrClient.Builder withSSLContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    /**
     * Use the provided CookieHandler.
     *
     * @param cookieHandler the cookie handler to use
     * @return this Builder
     */
    public HttpJdkSolrClient.Builder withCookieHandler(CookieHandler cookieHandler) {
      this.cookieHandler = cookieHandler;
      return this;
    }
  }
}
