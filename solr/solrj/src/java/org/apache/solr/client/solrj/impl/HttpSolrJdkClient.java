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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSolrJdkClient extends HttpSolrClientBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String USER_AGENT =
      "Solr[" + MethodHandles.lookup().lookupClass().getName() + "] 1.0";

  protected HttpClient client;

  protected ExecutorService executor;

  private boolean shutdownExecutor;

  protected HttpSolrJdkClient(String serverBaseUrl, HttpSolrJdkClient.Builder builder) {
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
      BlockingArrayQueue<Runnable> queue = new BlockingArrayQueue<>(32, 32);
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
    this.client = b.build();
    updateDefaultMimeTypeForParser();

    // This is a workaround for the case where the first request using
    // this client:
    //
    // (1) no SSL/TLS (2) using POST with stream and (3) using Http/2
    //
    // The JDK Http Client will send an upgrade request over Http/1
    // along with request content in the same request.  However,
    // the Jetty Server underpinning Solr does not accept this.
    //
    // By sending a ping before any real requests occur, the client
    // knows if Solr can accept Http/2, and no additional
    // upgrade requests will be sent.
    //
    // See https://bugs.openjdk.org/browse/JDK-8287589
    // See https://github.com/jetty/jetty.project/issues/9998#issuecomment-1614216870
    //
    try {
      ping();
    } catch (Exception e) {
      // ignore
    }

    assert ObjectReleaseTracker.track(this);
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    checkClosed();
    if (ClientUtils.shouldApplyDefaultCollection(collection, solrRequest)) {
      collection = defaultCollection;
    }
    String url = getRequestPath(solrRequest, collection);
    ResponseParser parser = responseParser(solrRequest);
    ModifiableSolrParams queryParams = initalizeSolrParams(solrRequest);
    HttpResponse<InputStream> resp = null;
    try {
      var reqb = HttpRequest.newBuilder();
      switch (solrRequest.getMethod()) {
        case GET:
          {
            resp = doGet(url, reqb, solrRequest, queryParams);
            break;
          }
        case POST:
          {
            resp = doPutOrPost(url, false, reqb, solrRequest, queryParams);
            break;
          }
        case PUT:
          {
            resp = doPutOrPost(url, true, reqb, solrRequest, queryParams);
            break;
          }
        default:
          {
            throw new IllegalStateException("Unsupported method: " + solrRequest.getMethod());
          }
      }
      return processErrorsAndResponse(solrRequest, parser, resp, url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (HttpTimeoutException e) {
      throw new SolrServerException(
          "Timeout occurred while waiting response from server at: " + url, e);
    } catch (SolrException se) {
      throw se;
    } catch (URISyntaxException | RuntimeException re) {
      throw new SolrServerException(re);
    } finally {
      // See
      // https://docs.oracle.com/en/java/javase/17/docs/api/java.net.http/java/net/http/HttpResponse.BodySubscribers.html#ofInputStream()
      if (!wantStream(parser)) {
        try {
          resp.body().close();
        } catch (Exception e1) {
          // ignore
        }
      }
    }
  }

  private HttpResponse<InputStream> doGet(
      String url,
      HttpRequest.Builder reqb,
      SolrRequest<?> solrRequest,
      ModifiableSolrParams queryParams)
      throws IOException, InterruptedException, URISyntaxException {
    validateGetRequest(solrRequest);
    reqb.GET();
    decorateRequest(reqb, solrRequest);
    reqb.uri(new URI(url + "?" + queryParams));
    return client.send(reqb.build(), HttpResponse.BodyHandlers.ofInputStream());
  }

  private HttpResponse<InputStream> doPutOrPost(
      String url,
      boolean isPut,
      HttpRequest.Builder reqb,
      SolrRequest<?> solrRequest,
      ModifiableSolrParams queryParams)
      throws IOException, InterruptedException, URISyntaxException {

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

    if (isPut) {
      reqb.PUT(bodyPublisher);
    } else {
      reqb.POST(bodyPublisher);
    }
    decorateRequest(reqb, solrRequest);
    reqb.uri(new URI(url + "?" + queryParams));

    HttpResponse<InputStream> response;
    try {
      response = client.send(reqb.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (IOException | InterruptedException | RuntimeException e) {
      if (contentWritingFuture != null) {
        contentWritingFuture.cancel(true);
      }
      throw e;
    }
    return response;
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
    if (shutdownExecutor) {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
    executor = null;

    // TODO: Java 21 adds close/autoclosable to HttpClient.  We should use it.
    client = null;

    assert ObjectReleaseTracker.release(this);
  }

  private void checkClosed() {
    if (client == null) {
      throw new IllegalStateException("This is closed and cannot be reused.");
    }
  }

  @Override
  protected boolean isFollowRedirects() {
    return client.followRedirects() != HttpClient.Redirect.NEVER;
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
      extends HttpSolrClientBuilderBase<HttpSolrJdkClient.Builder, HttpSolrJdkClient> {

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
    public HttpSolrJdkClient build() {
      if (idleTimeoutMillis == null || idleTimeoutMillis <= 0) {
        idleTimeoutMillis = (long) HttpClientUtil.DEFAULT_SO_TIMEOUT;
      }
      if (connectionTimeoutMillis == null) {
        connectionTimeoutMillis = (long) HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
      }
      return new HttpSolrJdkClient(baseSolrUrl, this);
    }

    /**
     * Use the provided SSLContext. See {@link
     * java.net.http.HttpClient.Builder#sslContext(SSLContext)}.
     *
     * @param sslContext the ssl context to use
     * @return this Builder
     */
    public HttpSolrJdkClient.Builder withSSLContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    /**
     * Use the provided CookieHandler.
     *
     * @param cookieHandler the cookie handler to use
     * @return this Builder
     */
    public HttpSolrJdkClient.Builder withCookieHandler(CookieHandler cookieHandler) {
      this.cookieHandler = cookieHandler;
      return this;
    }
  }
}
