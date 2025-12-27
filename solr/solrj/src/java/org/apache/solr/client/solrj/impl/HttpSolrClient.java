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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.JavaBinRequestWriter;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.client.solrj.response.ResponseParser;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple/direct {@link SolrClient} using HTTP.
 *
 * <p>The protected methods can be considered as internal / unstable APIs.
 *
 * @see #builder(String)
 * @see org.apache.solr.client.solrj.jetty.HttpJettySolrClient
 * @see org.apache.solr.client.solrj.impl.HttpJdkSolrClient
 */
public abstract class HttpSolrClient extends SolrClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;

  protected final String baseUrl;
  protected final long requestTimeoutMillis;

  protected final Set<String> urlParamNames;

  protected RequestWriter requestWriter = new JavaBinRequestWriter();

  // updating parser instance needs to go via the setter to ensure update of defaultParserMimeTypes
  protected ResponseParser parser = new JavaBinResponseParser();

  protected Set<String> defaultParserMimeTypes;

  protected final String basicAuthAuthorizationStr;

  protected HttpSolrClient(String serverBaseUrl, BuilderBase<?, ?> builder) {
    this.baseUrl = extractBaseUrl(serverBaseUrl);
    this.requestTimeoutMillis = builder.getRequestTimeoutMillis();
    this.basicAuthAuthorizationStr = builder.basicAuthAuthorizationStr;
    if (builder.requestWriter != null) {
      this.requestWriter = builder.requestWriter;
    }
    if (builder.responseParser != null) {
      this.parser = builder.responseParser;
    }
    this.defaultCollection = builder.defaultCollection;
    if (builder.urlParamNames != null) {
      this.urlParamNames = builder.urlParamNames;
    } else {
      this.urlParamNames = Set.of();
    }
  }

  private static String extractBaseUrl(String serverBaseUrl) {
    if (serverBaseUrl == null) {
      return null;
    }
    if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
      serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
    }

    if (serverBaseUrl.startsWith("//")) {
      serverBaseUrl = serverBaseUrl.substring(1);
    }
    return serverBaseUrl;
  }

  /** Typically looks like {@code http://localhost:8983/solr} (no core or collection) */
  public String getBaseURL() {
    return baseUrl;
  }

  /**
   * @lucene.internal
   */
  protected abstract LBSolrClient createLBSolrClient();

  protected String getRequestUrl(SolrRequest<?> solrRequest, String collection)
      throws MalformedURLException {
    return ClientUtils.buildRequestUrl(solrRequest, getBaseURL(), collection);
  }

  protected ResponseParser responseParser(SolrRequest<?> solrRequest) {
    // TODO add invariantParams support
    return solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
  }

  public RequestWriter getRequestWriter() {
    return requestWriter;
  }

  protected ModifiableSolrParams initializeSolrParams(
      SolrRequest<?> solrRequest, ResponseParser parserToUse) {
    // The parser 'wt=' param is used instead of the original params
    ModifiableSolrParams wparams = new ModifiableSolrParams(solrRequest.getParams());
    wparams.set(CommonParams.WT, parserToUse.getWriterType());
    return wparams;
  }

  protected boolean isMultipart(Collection<ContentStream> streams) {
    boolean isMultipart = false;
    if (streams != null) {
      boolean hasNullStreamName = false;
      hasNullStreamName = streams.stream().anyMatch(cs -> cs.getName() == null);
      isMultipart = !hasNullStreamName && streams.size() > 1;
    }
    return isMultipart;
  }

  protected ModifiableSolrParams calculateQueryParams(
      Set<String> queryParamNames, ModifiableSolrParams wparams) {
    ModifiableSolrParams queryModParams = new ModifiableSolrParams();
    if (queryParamNames != null) {
      for (String param : queryParamNames) {
        String[] value = wparams.getParams(param);
        if (value != null) {
          for (String v : value) {
            queryModParams.add(param, v);
          }
          wparams.remove(param);
        }
      }
    }
    return queryModParams;
  }

  protected void validateGetRequest(SolrRequest<?> solrRequest) throws IOException {
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams =
        contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    if (contentWriter != null || streams != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
    }
  }

  protected abstract boolean isFollowRedirects();

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected NamedList<Object> processErrorsAndResponse(
      int httpStatus,
      String responseReason,
      String responseMethod,
      final ResponseParser processor,
      InputStream is,
      String mimeType,
      String encoding,
      final boolean isV2Api,
      String urlExceptionMessage)
      throws SolrServerException {
    boolean shouldClose = true;
    try {
      // handle some http level checks before trying to parse the response
      switch (httpStatus) {
        case 200: // OK
        case 400: // Bad Request
        case 409: // Conflict
          break;
        case 301: // Moved Permanently
        case 302: // Moved Temporarily
          if (!isFollowRedirects()) {
            throw new SolrServerException(
                "Server at " + urlExceptionMessage + " sent back a redirect (" + httpStatus + ").");
          }
          break;
        default:
          if (processor == null || mimeType == null) {
            throw new RemoteSolrException(
                urlExceptionMessage,
                httpStatus,
                "non ok status: " + httpStatus + ", message:" + responseReason,
                null);
          }
      }

      if (wantStream(processor)) {
        // Only case where stream should not be closed
        shouldClose = false;
        // no processor specified, return raw stream
        return InputStreamResponseParser.createInputStreamNamedList(httpStatus, is);
      }

      checkContentType(processor, is, mimeType, encoding, httpStatus, urlExceptionMessage);

      NamedList<Object> rsp;
      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        throw new RemoteSolrException(urlExceptionMessage, httpStatus, e.getMessage(), e);
      }

      Object error = rsp == null ? null : rsp.get("error");
      if (error != null && isV2Api) {
        throw new RemoteSolrException(urlExceptionMessage, httpStatus, error, true);
      }
      if (httpStatus != 200 && !isV2Api) {
        if (error == null) {
          StringBuilder msg =
              new StringBuilder()
                  .append(responseReason)
                  .append("\n")
                  .append("request: ")
                  .append(responseMethod);
          String reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
          throw new RemoteSolrException(urlExceptionMessage, httpStatus, reason, null);
        } else {
          throw new RemoteSolrException(urlExceptionMessage, httpStatus, error);
        }
      }
      return rsp;
    } finally {
      if (shouldClose) {
        try {
          is.close();
        } catch (IOException e) {
          // quietly
        }
      }
    }
  }

  protected boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  protected abstract boolean processorAcceptsMimeType(
      Collection<String> processorSupportedContentTypes, String mimeType);

  protected abstract String allProcessorSupportedContentTypesCommaDelimited(
      Collection<String> processorSupportedContentTypes);

  /**
   * Validates that the content type in the response can be processed by the Response Parser. Throws
   * a {@code RemoteSolrException} if not.
   */
  private void checkContentType(
      ResponseParser processor,
      InputStream is,
      String mimeType,
      String encoding,
      int httpStatus,
      String urlExceptionMessage) {
    if (mimeType == null
        || (processor == this.parser && defaultParserMimeTypes.contains(mimeType))) {
      // Shortcut the default scenario
      return;
    }
    final Collection<String> processorSupportedContentTypes = processor.getContentTypes();
    if (!processorSupportedContentTypes.isEmpty()) {
      boolean processorAcceptsMimeType =
          processorAcceptsMimeType(processorSupportedContentTypes, mimeType);
      if (!processorAcceptsMimeType) {
        // unexpected mime type
        final String allSupportedTypes =
            allProcessorSupportedContentTypesCommaDelimited(processorSupportedContentTypes);
        String prefix =
            "Expected mime type in [" + allSupportedTypes + "] but got " + mimeType + ". ";
        String exceptionEncoding = encoding != null ? encoding : FALLBACK_CHARSET.name();
        try {
          ByteArrayOutputStream body = new ByteArrayOutputStream();
          is.transferTo(body);
          throw new RemoteSolrException(
              urlExceptionMessage, httpStatus, prefix + body.toString(exceptionEncoding), null);
        } catch (IOException e) {
          throw new RemoteSolrException(
              urlExceptionMessage,
              httpStatus,
              "Could not parse response with encoding " + exceptionEncoding,
              e);
        }
      }
    }
  }

  protected static String basicAuthCredentialsToAuthorizationString(String user, String pass) {
    String userPass = user + ":" + pass;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(FALLBACK_CHARSET));
  }

  protected void setParser(ResponseParser parser) {
    this.parser = parser;
    updateDefaultMimeTypeForParser();
  }

  protected abstract void updateDefaultMimeTypeForParser();

  /**
   * Executes a SolrRequest using the provided URL to temporarily override any "base URL" currently
   * used by this client
   *
   * @param baseUrl a URL to a root Solr path (i.e. "/solr") that should be used for this request
   * @param solrRequest the SolrRequest to send
   * @param collection an optional collection or core name used to override the client's "default
   *     collection". May be 'null' for any requests that don't require a collection or wish to rely
   *     on the client's default
   * @see SolrRequest#processWithBaseUrl(HttpSolrClient, String, String)
   */
  public abstract NamedList<Object> requestWithBaseUrl(
      String baseUrl, SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException;

  /**
   * Execute an asynchronous request against a Solr server for a given collection.
   *
   * @param request the request to execute
   * @param collection the collection to execute the request against
   * @return a {@link CompletableFuture} that tracks the progress of the async request. Supports
   *     cancelling requests via {@link CompletableFuture#cancel(boolean)}, adding callbacks/error
   *     handling using {@link CompletableFuture#whenComplete(BiConsumer)} and {@link
   *     CompletableFuture#exceptionally(Function)} methods, and other CompletableFuture
   *     functionality. Will complete exceptionally in case of either an {@link IOException} or
   *     {@link SolrServerException} during the request. Once completed, the CompletableFuture will
   *     contain a {@link NamedList} with the response from the server.
   */
  public abstract CompletableFuture<NamedList<Object>> requestAsync(
      final SolrRequest<?> request, String collection);

  /**
   * Execute an asynchronous request against a Solr server using the default collection.
   *
   * @param request the request to execute
   * @return a {@link CompletableFuture} see {@link #requestAsync(SolrRequest, String)}.
   */
  public CompletableFuture<NamedList<Object>> requestAsync(final SolrRequest<?> request) {
    return requestAsync(request, null);
  }

  protected boolean isV2ApiRequest(final SolrRequest<?> request) {
    return request.getApiVersion() == SolrRequest.ApiVersion.V2;
  }

  public ResponseParser getParser() {
    return parser;
  }

  public Set<String> getUrlParamNames() {
    return urlParamNames;
  }

  /**
   * @lucene.internal
   */
  protected abstract BuilderBase<?, ?> toBuilder(String baseUrl);

  // If the Jetty-based HttpJettySolrClient builder is on the classpath, this will be its no-arg
  // constructor; otherwise it will be null and we will fall back to the JDK HTTP client.
  private static final Constructor<? extends BuilderBase<?, ?>> HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR;

  static {
    Constructor<? extends HttpSolrClient.BuilderBase<?, ?>> ctor = null;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HttpSolrClient.BuilderBase<?, ?>> builderClass =
          (Class<? extends HttpSolrClient.BuilderBase<?, ?>>)
              Class.forName("org.apache.solr.client.solrj.jetty.HttpJettySolrClient$Builder");
      ctor = builderClass.getDeclaredConstructor();
      ctor.newInstance(); // perhaps fails because Jetty libs aren't on the classpath
    } catch (Throwable t) {
      // Class not present or incompatible; leave ctor as null to indicate unavailability
      if (log.isTraceEnabled()) {
        log.trace(
            "HttpJettySolrClient$Builder not available on classpath; will use HttpJdkSolrClient",
            t);
      }
    }
    HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR = ctor;
  }

  /**
   * Provides a new builder of an {@link HttpSolrClient}. The implementation will try to create a
   * {@link org.apache.solr.client.solrj.jetty.HttpJettySolrClient} if available, otherwise will
   * fall back on {@link HttpJdkSolrClient}.
   *
   * @param baseUrl for {@link BuilderBase#withBaseSolrUrl(String)}.
   */
  public static BuilderBase<?, ?> builder(String baseUrl) {
    HttpSolrClient.BuilderBase<?, ?> builder;
    if (HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR != null) {
      try {
        log.debug("Using HttpJettySolrClient as the delegate http client");
        builder = HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR.newInstance();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      log.debug("Using HttpJdkSolrClient as the delegate http client");
      builder = new HttpJdkSolrClient.Builder();
    }
    return builder.withBaseSolrUrl(baseUrl);
  }

  public abstract static class BuilderBase<B extends BuilderBase<?, ?>, C extends HttpSolrClient> {

    protected Long idleTimeoutMillis;
    protected Long connectionTimeoutMillis;
    protected Long requestTimeoutMillis;
    protected String basicAuthAuthorizationStr;
    protected Boolean followRedirects;
    protected String baseSolrUrl;
    protected RequestWriter requestWriter;
    protected ResponseParser responseParser;
    protected String defaultCollection;
    protected Set<String> urlParamNames;
    protected Integer maxConnectionsPerHost;
    protected ExecutorService executor;
    protected final boolean defaultUseHttp1_1 = Boolean.getBoolean("solr.http1");
    protected Boolean useHttp1_1;
    protected String proxyHost;
    protected int proxyPort;
    protected boolean proxyIsSocks4;
    protected boolean proxyIsSecure;

    public abstract C build();

    /**
     * Provide a seed HttpSolrClient for the builder values, values can still be overridden by using
     * builder methods
     */
    @SuppressWarnings("unchecked")
    public B withHttpClient(C httpSolrClient) {
      if (this.basicAuthAuthorizationStr == null) {
        this.basicAuthAuthorizationStr = httpSolrClient.basicAuthAuthorizationStr;
      }
      if (this.requestTimeoutMillis == null) {
        this.requestTimeoutMillis = httpSolrClient.requestTimeoutMillis;
      }
      if (this.requestWriter == null) {
        this.requestWriter = httpSolrClient.requestWriter;
      }
      if (this.responseParser == null) {
        this.responseParser = httpSolrClient.parser;
      }
      if (this.urlParamNames == null) {
        this.urlParamNames = httpSolrClient.urlParamNames;
      }
      return (B) (this);
    }

    /** Provides the Base Solr Url. */
    @SuppressWarnings("unchecked")
    public B withBaseSolrUrl(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
      return (B) this;
    }

    /** Provides a {@link RequestWriter} for created clients to use when handing requests. */
    @SuppressWarnings("unchecked")
    public B withRequestWriter(RequestWriter requestWriter) {
      this.requestWriter = requestWriter;
      return (B) this;
    }

    /** Provides a {@link ResponseParser} for created clients to use when handling requests. */
    @SuppressWarnings("unchecked")
    public B withResponseParser(ResponseParser responseParser) {
      this.responseParser = responseParser;
      return (B) this;
    }

    /** Sets a default for core or collection based requests. */
    @SuppressWarnings("unchecked")
    public B withDefaultCollection(String defaultCoreOrCollection) {
      this.defaultCollection = defaultCoreOrCollection;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B withFollowRedirects(boolean followRedirects) {
      this.followRedirects = followRedirects;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B withExecutor(ExecutorService executor) {
      this.executor = executor;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B withBasicAuthCredentials(String user, String pass) {
      if (user != null || pass != null) {
        if (user == null || pass == null) {
          throw new IllegalStateException(
              "Invalid Authentication credentials. Either both username and password or none must be provided");
        }
      }
      this.basicAuthAuthorizationStr = basicAuthCredentialsToAuthorizationString(user, pass);
      return (B) this;
    }

    /**
     * Expert Method
     *
     * @param urlParamNames set of param keys that are only sent via the query string. Note that the
     *     param will be sent as a query string if the key is part of this Set or the SolrRequest's
     *     query params.
     * @see SolrRequest#getQueryParams
     */
    @SuppressWarnings("unchecked")
    public B withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
      this.urlParamNames = urlParamNames;
      return (B) this;
    }

    /**
     * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited
     * to 4
     */
    @SuppressWarnings("unchecked")
    public B withMaxConnectionsPerHost(int max) {
      this.maxConnectionsPerHost = max;
      return (B) this;
    }

    /**
     * The max time a connection can be idle (that is, without traffic of bytes in either
     * direction). Sometimes called a "socket timeout". Note: not applicable to the JDK HttpClient.
     */
    @SuppressWarnings("unchecked")
    public B withIdleTimeout(long idleConnectionTimeout, TimeUnit unit) {
      this.idleTimeoutMillis = TimeUnit.MILLISECONDS.convert(idleConnectionTimeout, unit);
      return (B) this;
    }

    public long getIdleTimeoutMillis() {
      return idleTimeoutMillis != null && idleTimeoutMillis > 0
          ? idleTimeoutMillis
          : SolrHttpConstants.DEFAULT_SO_TIMEOUT;
    }

    /** The max time a connection can take to connect to destinations. */
    @SuppressWarnings("unchecked")
    public B withConnectionTimeout(long connectionTimeout, TimeUnit unit) {
      this.connectionTimeoutMillis = TimeUnit.MILLISECONDS.convert(connectionTimeout, unit);
      return (B) this;
    }

    public long getConnectionTimeoutMillis() {
      return connectionTimeoutMillis != null && connectionTimeoutMillis > 0
          ? connectionTimeoutMillis
          : SolrHttpConstants.DEFAULT_CONNECT_TIMEOUT;
    }

    /** Set a timeout for requests to receive a response. */
    @SuppressWarnings("unchecked")
    public B withRequestTimeout(long requestTimeout, TimeUnit unit) {
      this.requestTimeoutMillis = TimeUnit.MILLISECONDS.convert(requestTimeout, unit);
      return (B) this;
    }

    public long getRequestTimeoutMillis() {
      return requestTimeoutMillis != null && requestTimeoutMillis > 0
          ? requestTimeoutMillis
          : getIdleTimeoutMillis();
    }

    /**
     * If true, prefer http1.1 over http2. If not set, the default is determined by system property
     * 'solr.http1'. Otherwise, false.
     *
     * @param useHttp1_1 prefer http1.1?
     * @return this Builder
     */
    @SuppressWarnings("unchecked")
    public B useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return (B) this;
    }

    /**
     * Return whether the HttpSolrClient built will prefer http1.1 over http2.
     *
     * @return whether to prefer http1.1
     */
    public boolean shouldUseHttp1_1() {
      return useHttp1_1 != null ? useHttp1_1 : defaultUseHttp1_1;
    }

    /**
     * Setup a proxy
     *
     * @param host The proxy host
     * @param port The proxy port
     * @param isSocks4 If true creates an SOCKS 4 proxy, otherwise creates an HTTP proxy
     * @param isSecure If true enables the secure flag on the proxy
     * @return this Builder
     */
    @SuppressWarnings("unchecked")
    public B withProxyConfiguration(String host, int port, boolean isSocks4, boolean isSecure) {
      this.proxyHost = host;
      this.proxyPort = port;
      this.proxyIsSocks4 = isSocks4;
      this.proxyIsSecure = isSecure;
      return (B) this;
    }

    /**
     * Setup basic authentication from a string formatted as username:password. If the string is
     * Null then it doesn't do anything.
     *
     * @param credentials The username and password formatted as username:password
     * @return this Builder
     */
    @SuppressWarnings("unchecked")
    public B withOptionalBasicAuthCredentials(String credentials) {
      if (credentials != null) {
        if (credentials.indexOf(':') == -1) {
          throw new IllegalStateException(
              "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");
        }
        String username = credentials.substring(0, credentials.indexOf(':'));
        String password = credentials.substring(credentials.indexOf(':') + 1, credentials.length());
        withBasicAuthCredentials(username, password);
      }
      return (B) this;
    }

    public Integer getMaxConnectionsPerHost() {
      return maxConnectionsPerHost;
    }

    public Boolean getFollowRedirects() {
      return followRedirects;
    }

    public String getProxyHost() {
      return proxyHost;
    }

    public int getProxyPort() {
      return proxyPort;
    }

    public boolean isProxyIsSocks4() {
      return proxyIsSocks4;
    }

    public boolean isProxyIsSecure() {
      return proxyIsSecure;
    }

    public ExecutorService getExecutor() {
      return executor;
    }
  }
}
