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

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.CookieStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.HttpListenerFactory.RequestResponseListener;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin.Address;
import org.eclipse.jetty.client.Origin.Protocol;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.FormRequestContent;
import org.eclipse.jetty.client.util.InputStreamRequestContent;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartRequestContent;
import org.eclipse.jetty.client.util.OutputStreamRequestContent;
import org.eclipse.jetty.client.util.StringRequestContent;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.KeyStoreScanner;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Difference between this {@link Http2SolrClient} and {@link HttpSolrClient}:
 *
 * <ul>
 *   <li>{@link Http2SolrClient} sends requests in HTTP/2
 *   <li>{@link Http2SolrClient} can point to multiple urls
 *   <li>{@link Http2SolrClient} does not expose its internal httpClient like {@link
 *       HttpSolrClient#getHttpClient()}, sharing connection pools should be done by {@link
 *       Http2SolrClient.Builder#withHttpClient(Http2SolrClient)}
 * </ul>
 *
 * @lucene.experimental
 */
public class Http2SolrClient extends Http2SolrClientBase {
  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";

  private static volatile SSLConfig defaultSSLConfig;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";

  private final HttpClient httpClient;

  private SSLConfig sslConfig;
  ;

  private List<HttpListenerFactory> listenerFactory = new ArrayList<>();
  private final AsyncTracker asyncTracker = new AsyncTracker();

  private final boolean closeClient;
  private ExecutorService executor;
  private boolean shutdownExecutor;

  private AuthenticationStoreHolder authenticationStore;

  private KeyStoreScanner scanner;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    super(serverBaseUrl, builder);

    if (builder.httpClient != null) {
      this.httpClient = builder.httpClient;
      this.closeClient = false;
    } else {
      this.httpClient = createHttpClient(builder);
      this.closeClient = true;
    }

    updateDefaultMimeTypeForParser();

    this.httpClient.setFollowRedirects(Boolean.TRUE.equals(builder.followRedirects));

    assert ObjectReleaseTracker.track(this);
  }

  public void addListenerFactory(HttpListenerFactory factory) {
    this.listenerFactory.add(factory);
  }

  // internal usage only
  HttpClient getHttpClient() {
    return httpClient;
  }

  // internal usage only
  ProtocolHandlers getProtocolHandlers() {
    return httpClient.getProtocolHandlers();
  }

  private HttpClient createHttpClient(Builder builder) {
    HttpClient httpClient;

    executor = builder.executor;
    if (executor == null) {
      BlockingArrayQueue<Runnable> queue = new BlockingArrayQueue<>(256, 256);
      this.executor =
          new ExecutorUtil.MDCAwareThreadPoolExecutor(
              32, 256, 60, TimeUnit.SECONDS, queue, new SolrNamedThreadFactory("h2sc"));
      shutdownExecutor = true;
    } else {
      shutdownExecutor = false;
    }

    SslContextFactory.Client sslContextFactory;
    if (builder.sslConfig == null) {
      sslContextFactory = getDefaultSslContextFactory();
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
    }

    if (sslContextFactory != null
        && sslContextFactory.getKeyStoreResource() != null
        && builder.keyStoreReloadIntervalSecs != null
        && builder.keyStoreReloadIntervalSecs > 0) {
      scanner = new KeyStoreScanner(sslContextFactory);
      try {
        scanner.setScanInterval(
            (int) Math.min(builder.keyStoreReloadIntervalSecs, Integer.MAX_VALUE));
        scanner.start();
        if (log.isDebugEnabled()) {
          log.debug("Key Store Scanner started");
        }
      } catch (Exception e) {
        RuntimeException startException =
            new RuntimeException("Unable to start key store scanner", e);
        try {
          scanner.stop();
        } catch (Exception stopException) {
          startException.addSuppressed(stopException);
        }
        throw startException;
      }
    }

    ClientConnector clientConnector = new ClientConnector();
    clientConnector.setReuseAddress(true);
    clientConnector.setSslContextFactory(sslContextFactory);
    clientConnector.setSelectors(2);

    HttpClientTransport transport;
    if (builder.useHttp1_1) {
      if (log.isDebugEnabled()) {
        log.debug("Create Http2SolrClient with HTTP/1.1 transport");
      }

      transport = new HttpClientTransportOverHTTP(clientConnector);
      httpClient = new HttpClient(transport);
      if (builder.maxConnectionsPerHost != null) {
        httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("Create Http2SolrClient with HTTP/2 transport");
      }

      HTTP2Client http2client = new HTTP2Client(clientConnector);
      transport = new HttpClientTransportOverHTTP2(http2client);
      httpClient = new HttpClient(transport);
      httpClient.setMaxConnectionsPerDestination(4);
    }

    httpClient.setExecutor(this.executor);
    httpClient.setStrictEventOrdering(false);
    httpClient.setConnectBlocking(true);
    httpClient.setFollowRedirects(false);
    httpClient.setMaxRequestsQueuedPerDestination(
        asyncTracker.getMaxRequestsQueuedPerDestination());
    httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));
    httpClient.setIdleTimeout(idleTimeoutMillis);

    if (builder.cookieStore != null) {
      httpClient.setCookieStore(builder.cookieStore);
    }

    this.authenticationStore = new AuthenticationStoreHolder();
    httpClient.setAuthenticationStore(this.authenticationStore);

    httpClient.setConnectTimeout(builder.connectionTimeoutMillis);

    setupProxy(builder, httpClient);

    try {
      httpClient.start();
    } catch (Exception e) {
      close(); // make sure we clean up
      throw new RuntimeException(e);
    }

    return httpClient;
  }

  private void setupProxy(Builder builder, HttpClient httpClient) {
    if (builder.proxyHost == null) {
      return;
    }
    Address address = new Address(builder.proxyHost, builder.proxyPort);

    final ProxyConfiguration.Proxy proxy;
    if (builder.proxyIsSocks4) {
      proxy = new Socks4Proxy(address, builder.proxyIsSecure);
    } else {
      final Protocol protocol;
      if (builder.useHttp1_1) {
        protocol = HttpClientTransportOverHTTP.HTTP11;
      } else {
        // see HttpClientTransportOverHTTP2#newOrigin
        String protocolName = builder.proxyIsSecure ? "h2" : "h2c";
        protocol = new Protocol(List.of(protocolName), false);
      }
      proxy = new HttpProxy(address, builder.proxyIsSecure, protocol);
    }
    httpClient.getProxyConfiguration().addProxy(proxy);
  }

  @Override
  public void close() {
    // we wait for async requests, so far devs don't want to give sugar for this
    asyncTracker.waitForComplete();
    try {
      if (closeClient) {
        httpClient.stop();
        httpClient.destroy();

        if (scanner != null) {
          scanner.stop();
          if (log.isDebugEnabled()) {
            log.debug("Key Store Scanner stopped");
          }
          scanner = null;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception on closing client", e);
    } finally {
      if (shutdownExecutor) {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }
    }
    assert ObjectReleaseTracker.release(this);
  }

  public void setAuthenticationStore(AuthenticationStore authenticationStore) {
    this.authenticationStore.updateAuthenticationStore(authenticationStore);
  }

  public static class OutStream implements Closeable {
    private final String origCollection;
    private final ModifiableSolrParams origParams;
    private final OutputStreamRequestContent content;
    private final InputStreamResponseListener responseListener;
    private final boolean isXml;

    public OutStream(
        String origCollection,
        ModifiableSolrParams origParams,
        OutputStreamRequestContent content,
        InputStreamResponseListener responseListener,
        boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.content = content;
      this.responseListener = responseListener;
      this.isXml = isXml;
    }

    boolean belongToThisStream(SolrRequest<?> solrRequest, String collection) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams(solrRequest.getParams());
      return origParams.toNamedList().equals(solrParams.toNamedList())
          && Objects.equals(origCollection, collection);
    }

    public void write(byte b[]) throws IOException {
      this.content.getOutputStream().write(b);
    }

    public void flush() throws IOException {
      this.content.getOutputStream().flush();
    }

    @Override
    public void close() throws IOException {
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      this.content.getOutputStream().close();
    }

    // TODO this class should be hidden
    public InputStreamResponseListener getResponseListener() {
      return responseListener;
    }
  }

  public OutStream initOutStream(String baseUrl, UpdateRequest updateRequest, String collection)
      throws IOException {
    String contentType = requestWriter.getUpdateContentType();
    final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

    ModifiableSolrParams requestParams = initalizeSolrParams(null);

    String basePath = baseUrl;
    if (collection != null) basePath += "/" + collection;
    if (!basePath.endsWith("/")) basePath += "/";

    OutputStreamRequestContent content = new OutputStreamRequestContent(contentType);
    Request postRequest =
        httpClient
            .newRequest(basePath + "update" + requestParams.toQueryString())
            .method(HttpMethod.POST)
            .body(content);
    decorateRequest(postRequest, updateRequest, false);
    InputStreamResponseListener responseListener = new InputStreamReleaseTrackingResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, content, responseListener, isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  public void send(OutStream outStream, SolrRequest<?> req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    this.requestWriter.write(req, outStream.content.getOutputStream());
    if (outStream.isXml) {
      // check for commit or optimize
      SolrParams params = req.getParams();
      if (params != null) {
        String fmt = null;
        if (params.getBool(UpdateParams.OPTIMIZE, false)) {
          fmt = "<optimize waitSearcher=\"%s\" />";
        } else if (params.getBool(UpdateParams.COMMIT, false)) {
          fmt = "<commit waitSearcher=\"%s\" />";
        }
        if (fmt != null) {
          byte[] content =
              String.format(
                      Locale.ROOT, fmt, params.getBool(UpdateParams.WAIT_SEARCHER, false) + "")
                  .getBytes(FALLBACK_CHARSET);
          outStream.write(content);
        }
      }
    }
    outStream.flush();
  }

  @SuppressWarnings("StaticAssignmentOfThrowable")
  private static final Exception CANCELLED_EXCEPTION = new Exception();

  private static final Cancellable FAILED_MAKING_REQUEST_CANCELLABLE = () -> {};

  public Cancellable asyncRequest(
      SolrRequest<?> solrRequest,
      String collection,
      AsyncListener<NamedList<Object>> asyncListener) {
    MDCCopyHelper mdcCopyHelper = new MDCCopyHelper();

    Request req;
    try {
      String url = getRequestPath(solrRequest, collection);
      InputStreamResponseListener listener =
          new InputStreamReleaseTrackingResponseListener() {
            @Override
            public void onHeaders(Response response) {
              super.onHeaders(response);
              executor.execute(
                  () -> {
                    InputStream is = getInputStream();
                    try {
                      NamedList<Object> body =
                          processErrorsAndResponse(solrRequest, response, is, url);
                      mdcCopyHelper.onBegin(null);
                      log.debug("response processing success");
                      asyncListener.onSuccess(body);
                    } catch (RemoteSolrException e) {
                      if (SolrException.getRootCause(e) != CANCELLED_EXCEPTION) {
                        mdcCopyHelper.onBegin(null);
                        log.debug("response processing failed", e);
                        asyncListener.onFailure(e);
                      }
                    } catch (SolrServerException e) {
                      mdcCopyHelper.onBegin(null);
                      log.debug("response processing failed", e);
                      asyncListener.onFailure(e);
                    } finally {
                      log.debug("response processing completed");
                      mdcCopyHelper.onComplete(null);
                    }
                  });
            }

            @Override
            public void onFailure(Response response, Throwable failure) {
              super.onFailure(response, failure);
              if (failure != CANCELLED_EXCEPTION) {
                asyncListener.onFailure(new SolrServerException(failure.getMessage(), failure));
              }
            }
          };

      req = makeRequestAndSend(solrRequest, url, listener, true);
    } catch (SolrServerException | IOException e) {
      asyncListener.onFailure(e);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    return () -> req.abort(CANCELLED_EXCEPTION);
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    if (ClientUtils.shouldApplyDefaultCollection(collection, solrRequest)) {
      collection = defaultCollection;
    }
    String url = getRequestPath(solrRequest, collection);
    Throwable abortCause = null;
    Request req = null;
    try {
      InputStreamResponseListener listener = new InputStreamReleaseTrackingResponseListener();
      req = makeRequestAndSend(solrRequest, url, listener, false);
      Response response = listener.get(idleTimeoutMillis, TimeUnit.MILLISECONDS);
      url = req.getURI().toString();
      InputStream is = listener.getInputStream();
      return processErrorsAndResponse(solrRequest, response, is, url);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      abortCause = e;
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new SolrServerException(
          "Timeout occurred while waiting response from server at: " + url, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      abortCause = cause;
      if (cause instanceof ConnectException) {
        throw new SolrServerException("Server refused connection at: " + url, cause);
      }
      if (cause instanceof SolrServerException) {
        throw (SolrServerException) cause;
      } else if (cause instanceof IOException) {
        throw new SolrServerException(
            "IOException occurred when talking to server at: " + url, cause);
      }
      throw new SolrServerException(cause.getMessage(), cause);
    } catch (SolrServerException | RuntimeException sse) {
      abortCause = sse;
      throw sse;
    } finally {
      if (abortCause != null && req != null) {
        req.abort(abortCause);
      }
    }
  }

  private NamedList<Object> processErrorsAndResponse(
      SolrRequest<?> solrRequest, Response response, InputStream is, String urlExceptionMessage)
      throws SolrServerException {
    ResponseParser parser =
        solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
    String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);
    String mimeType = null;
    String encoding = null;
    if (contentType != null) {
      mimeType = MimeTypes.getContentTypeWithoutCharset(contentType);
      encoding = MimeTypes.getCharsetFromContentType(contentType);
    }
    String responseMethod = response.getRequest() == null ? "" : response.getRequest().getMethod();
    return processErrorsAndResponse(
        response.getStatus(),
        response.getReason(),
        responseMethod,
        parser,
        is,
        mimeType,
        encoding,
        isV2ApiRequest(solrRequest),
        urlExceptionMessage);
  }

  private void setBasicAuthHeader(SolrRequest<?> solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String encoded =
          basicAuthCredentialsToAuthorizationString(
              solrRequest.getBasicAuthUser(), solrRequest.getBasicAuthPassword());
      req.headers(headers -> headers.put("Authorization", encoded));
    } else if (basicAuthAuthorizationStr != null) {
      req.headers(headers -> headers.put("Authorization", basicAuthAuthorizationStr));
    }
  }

  private void decorateRequest(Request req, SolrRequest<?> solrRequest, boolean isAsync) {
    req.headers(headers -> headers.remove(HttpHeader.ACCEPT_ENCODING));

    if (requestTimeoutMillis > 0) {
      req.timeout(requestTimeoutMillis, TimeUnit.MILLISECONDS);
    } else {
      req.timeout(idleTimeoutMillis, TimeUnit.MILLISECONDS);
    }
    if (solrRequest.getUserPrincipal() != null) {
      req.attribute(REQ_PRINCIPAL_KEY, solrRequest.getUserPrincipal());
    }

    setBasicAuthHeader(solrRequest, req);
    for (HttpListenerFactory factory : listenerFactory) {
      HttpListenerFactory.RequestResponseListener listener = factory.get();
      listener.onQueued(req);
      req.onRequestBegin(listener);
      req.onComplete(listener);
    }

    if (isAsync) {
      req.onRequestQueued(asyncTracker.queuedListener);
      req.onComplete(asyncTracker.completeListener);
    }

    Map<String, String> headers = solrRequest.getHeaders();
    if (headers != null) {
      req.headers(
          h ->
              headers.entrySet().stream()
                  .forEach(entry -> h.add(entry.getKey(), entry.getValue())));
    }
  }

  private Request makeRequestAndSend(
      SolrRequest<?> solrRequest, String url, InputStreamResponseListener listener, boolean isAsync)
      throws IOException, SolrServerException {

    ResponseParser parser = responseParser(solrRequest);
    ModifiableSolrParams wparams = initalizeSolrParams(solrRequest);

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      validateGetRequest(solrRequest);
      var r = httpClient.newRequest(url + wparams.toQueryString()).method(HttpMethod.GET);
      decorateRequest(r, solrRequest, isAsync);
      r.send(listener);
      return r;
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      var r = httpClient.newRequest(url + wparams.toQueryString()).method(HttpMethod.DELETE);
      decorateRequest(r, solrRequest, isAsync);
      r.send(listener);
      return r;
    }

    if (SolrRequest.METHOD.POST == solrRequest.getMethod()
        || SolrRequest.METHOD.PUT == solrRequest.getMethod()) {
      RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
      Collection<ContentStream> streams =
          contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;

      boolean isMultipart = isMultipart(streams);

      HttpMethod method =
          SolrRequest.METHOD.POST == solrRequest.getMethod() ? HttpMethod.POST : HttpMethod.PUT;

      if (contentWriter != null) {
        var content = new OutputStreamRequestContent(contentWriter.getContentType());
        var r = httpClient.newRequest(url + wparams.toQueryString()).method(method).body(content);
        decorateRequest(r, solrRequest, isAsync);
        r.send(listener);
        try (var output = content.getOutputStream()) {
          contentWriter.write(output);
        }
        return r;

      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.urlParamNames, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));
        Request req = httpClient.newRequest(url + queryParams.toQueryString()).method(method);
        var r = fillContentStream(req, streams, wparams, isMultipart);
        decorateRequest(r, solrRequest, isAsync);
        r.send(listener);
        return r;

      } else {
        // If is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();
        var content =
            new InputStreamRequestContent(
                contentStream.getContentType(), contentStream.getStream());
        var r = httpClient.newRequest(url + wparams.toQueryString()).method(method).body(content);
        decorateRequest(r, solrRequest, isAsync);
        r.send(listener);
        return r;
      }
    }

    throw new SolrServerException("Unsupported method: " + solrRequest.getMethod());
  }

  private Request fillContentStream(
      Request req,
      Collection<ContentStream> streams,
      ModifiableSolrParams wparams,
      boolean isMultipart)
      throws IOException {
    if (isMultipart) {
      // multipart/form-data
      try (MultiPartRequestContent content = new MultiPartRequestContent()) {
        Iterator<String> iter = wparams.getParameterNamesIterator();
        while (iter.hasNext()) {
          String key = iter.next();
          String[] vals = wparams.getParams(key);
          if (vals != null) {
            for (String val : vals) {
              content.addFieldPart(key, new StringRequestContent(val), null);
            }
          }
        }
        if (streams != null) {
          for (ContentStream contentStream : streams) {
            String contentType = contentStream.getContentType();
            if (contentType == null) {
              contentType = "multipart/form-data"; // default
            }
            String name = contentStream.getName();
            if (name == null) {
              name = "";
            }
            HttpFields.Mutable fields = HttpFields.build(1);
            fields.add(HttpHeader.CONTENT_TYPE, contentType);
            content.addFilePart(
                name,
                contentStream.getName(),
                new InputStreamRequestContent(contentStream.getStream()),
                fields);
          }
        }
        req.body(content);
      }
    } else {
      // application/x-www-form-urlencoded
      Fields fields = new Fields();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            fields.add(key, val);
          }
        }
      }
      req.body(new FormRequestContent(fields, FALLBACK_CHARSET));
    }

    return req;
  }

  @Override
  protected boolean isFollowRedirects() {
    return httpClient.isFollowRedirects();
  }

  @Override
  protected boolean processorAcceptsMimeType(
      Collection<String> processorSupportedContentTypes, String mimeType) {

    return processorSupportedContentTypes.stream()
        .map(ct -> MimeTypes.getContentTypeWithoutCharset(ct).trim())
        .anyMatch(mimeType::equalsIgnoreCase);
  }

  @Override
  protected void updateDefaultMimeTypeForParser() {
    defaultParserMimeTypes =
        parser.getContentTypes().stream()
            .map(ct -> MimeTypes.getContentTypeWithoutCharset(ct).trim().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
  }

  @Override
  protected String allProcessorSupportedContentTypesCommaDelimited(
      Collection<String> processorSupportedContentTypes) {
    return processorSupportedContentTypes.stream()
        .map(ct -> MimeTypes.getContentTypeWithoutCharset(ct).trim().toLowerCase(Locale.ROOT))
        .collect(Collectors.joining(", "));
  }

  protected RequestWriter getRequestWriter() {
    return requestWriter;
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  private static class AsyncTracker {
    private static final int MAX_OUTSTANDING_REQUESTS = 1000;

    // wait for async requests
    private final Phaser phaser;
    // maximum outstanding requests left
    private final Semaphore available;
    private final Request.QueuedListener queuedListener;
    private final Response.CompleteListener completeListener;

    AsyncTracker() {
      // TODO: what about shared instances?
      phaser = new Phaser(1);
      available = new Semaphore(MAX_OUTSTANDING_REQUESTS, false);
      queuedListener =
          request -> {
            phaser.register();
            try {
              available.acquire();
            } catch (InterruptedException ignored) {

            }
          };
      completeListener =
          result -> {
            phaser.arriveAndDeregister();
            available.release();
          };
    }

    int getMaxRequestsQueuedPerDestination() {
      // comfortably above max outstanding requests
      return MAX_OUTSTANDING_REQUESTS * 3;
    }

    public void waitForComplete() {
      phaser.arriveAndAwaitAdvance();
      phaser.arriveAndDeregister();
    }
  }

  public static class Builder
      extends HttpSolrClientBuilderBase<Http2SolrClient.Builder, Http2SolrClient> {

    private HttpClient httpClient;

    private SSLConfig sslConfig;

    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");

    protected Long keyStoreReloadIntervalSecs;

    public Builder() {
      super();
    }

    public Builder(String baseSolrUrl) {
      super();
      this.baseSolrUrl = baseSolrUrl;
    }

    public HttpSolrClientBuilderBase<Http2SolrClient.Builder, Http2SolrClient> withSSLConfig(
        SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    /**
     * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited
     * to 4
     *
     * @deprecated Please use {@link #withMaxConnectionsPerHost(int)}
     */
    @Deprecated(since = "9.2")
    public Http2SolrClient.Builder maxConnectionsPerHost(int max) {
      withMaxConnectionsPerHost(max);
      return this;
    }

    /**
     * Set the scanning interval to check for updates in the Key Store used by this client. If the
     * interval is unset, 0 or less, then the Key Store Scanner is not created, and the client will
     * not attempt to update key stores. The minimum value between checks is 1 second.
     *
     * @param interval Interval between checks
     * @param unit The unit for the interval
     * @return This builder
     */
    public Http2SolrClient.Builder withKeyStoreReloadInterval(long interval, TimeUnit unit) {
      this.keyStoreReloadIntervalSecs = unit.toSeconds(interval);
      if (this.keyStoreReloadIntervalSecs == 0 && interval > 0) {
        this.keyStoreReloadIntervalSecs = 1L;
      }
      return this;
    }

    /**
     * @deprecated Please use {@link #withIdleTimeout(long, TimeUnit)}
     */
    @Deprecated(since = "9.2")
    public Http2SolrClient.Builder idleTimeout(int idleConnectionTimeout) {
      withIdleTimeout(idleConnectionTimeout, TimeUnit.MILLISECONDS);
      return this;
    }

    /**
     * @deprecated Please use {@link #withConnectionTimeout(long, TimeUnit)}
     */
    @Deprecated(since = "9.2")
    public Http2SolrClient.Builder connectionTimeout(int connectionTimeout) {
      withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS);
      return this;
    }

    /**
     * Set a timeout in milliseconds for requests issued by this client.
     *
     * @param requestTimeout The timeout in milliseconds
     * @return this Builder.
     * @deprecated Please use {@link #withRequestTimeout(long, TimeUnit)}
     */
    @Deprecated(since = "9.2")
    public Http2SolrClient.Builder requestTimeout(int requestTimeout) {
      withRequestTimeout(requestTimeout, TimeUnit.MILLISECONDS);
      return this;
    }

    private CookieStore getDefaultCookieStore() {
      if (Boolean.getBoolean("solr.http.disableCookies")) {
        return new HttpCookieStore.Empty();
      }
      /*
       * We could potentially have a Supplier<CookieStore> if we ever need further customization support,
       * but for now it's only either Empty or default (in-memory).
       */
      return null;
    }

    protected <B extends Http2SolrClientBase> B build(Class<B> type) {
      return type.cast(build());
    }

    public Http2SolrClient build() {
      if (sslConfig == null) {
        sslConfig = Http2SolrClient.defaultSSLConfig;
      }
      if (cookieStore == null) {
        cookieStore = getDefaultCookieStore();
      }
      if (idleTimeoutMillis == null || idleTimeoutMillis <= 0) {
        idleTimeoutMillis = (long) HttpClientUtil.DEFAULT_SO_TIMEOUT;
      }
      if (connectionTimeoutMillis == null) {
        connectionTimeoutMillis = (long) HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
      }

      if (keyStoreReloadIntervalSecs != null
          && keyStoreReloadIntervalSecs > 0
          && this.httpClient != null) {
        log.warn("keyStoreReloadIntervalSecs can't be set when using external httpClient");
        keyStoreReloadIntervalSecs = null;
      } else if (keyStoreReloadIntervalSecs == null
          && this.httpClient == null
          && Boolean.getBoolean("solr.keyStoreReload.enabled")) {
        keyStoreReloadIntervalSecs = Long.getLong("solr.jetty.sslContext.reload.scanInterval", 30);
      }

      Http2SolrClient client = new Http2SolrClient(baseSolrUrl, this);
      try {
        httpClientBuilderSetup(client);
      } catch (RuntimeException e) {
        try {
          client.close();
        } catch (Exception exceptionOnClose) {
          e.addSuppressed(exceptionOnClose);
        }
        throw e;
      }
      return client;
    }

    private void httpClientBuilderSetup(Http2SolrClient client) {
      String factoryClassName =
          System.getProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY);
      if (factoryClassName != null) {
        log.debug("Using Http Builder Factory: {}", factoryClassName);
        HttpClientBuilderFactory factory;
        try {
          factory =
              Class.forName(factoryClassName)
                  .asSubclass(HttpClientBuilderFactory.class)
                  .getDeclaredConstructor()
                  .newInstance();
        } catch (InstantiationException
            | IllegalAccessException
            | ClassNotFoundException
            | InvocationTargetException
            | NoSuchMethodException e) {
          throw new RuntimeException("Unable to instantiate " + Http2SolrClient.class.getName(), e);
        }
        factory.setup(client);
      }
    }

    /**
     * Provide a seed Http2SolrClient for the builder values, values can still be overridden by
     * using builder methods
     */
    public Builder withHttpClient(Http2SolrClient http2SolrClient) {
      this.httpClient = http2SolrClient.httpClient;

      if (this.basicAuthAuthorizationStr == null) {
        this.basicAuthAuthorizationStr = http2SolrClient.basicAuthAuthorizationStr;
      }
      if (this.followRedirects == null) {
        this.followRedirects = http2SolrClient.httpClient.isFollowRedirects();
      }
      if (this.idleTimeoutMillis == null) {
        this.idleTimeoutMillis = http2SolrClient.idleTimeoutMillis;
      }
      if (this.requestWriter == null) {
        this.requestWriter = http2SolrClient.requestWriter;
      }
      if (this.requestTimeoutMillis == null) {
        this.requestTimeoutMillis = http2SolrClient.requestTimeoutMillis;
      }
      if (this.responseParser == null) {
        this.responseParser = http2SolrClient.parser;
      }
      if (this.urlParamNames == null) {
        this.urlParamNames = http2SolrClient.urlParamNames;
      }
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return this;
    }
  }

  public static void setDefaultSSLConfig(SSLConfig sslConfig) {
    Http2SolrClient.defaultSSLConfig = sslConfig;
  }

  // public for testing, only used by tests
  public static void resetSslContextFactory() {
    Http2SolrClient.defaultSSLConfig = null;
  }

  /* package-private for testing */
  static SslContextFactory.Client getDefaultSslContextFactory() {
    String checkPeerNameStr = System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
    boolean sslCheckPeerName = !"false".equalsIgnoreCase(checkPeerNameStr);

    SslContextFactory.Client sslContextFactory = new SslContextFactory.Client(!sslCheckPeerName);

    if (null != System.getProperty("javax.net.ssl.keyStore")) {
      sslContextFactory.setKeyStorePath(System.getProperty("javax.net.ssl.keyStore"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
      sslContextFactory.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStoreType")) {
      sslContextFactory.setKeyStoreType(System.getProperty("javax.net.ssl.keyStoreType"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStore")) {
      sslContextFactory.setTrustStorePath(System.getProperty("javax.net.ssl.trustStore"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
      sslContextFactory.setTrustStorePassword(
          System.getProperty("javax.net.ssl.trustStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStoreType")) {
      sslContextFactory.setTrustStoreType(System.getProperty("javax.net.ssl.trustStoreType"));
    }

    return sslContextFactory;
  }

  /**
   * Helper class in change of copying MDC context across all threads involved in processing a
   * request. This does not strictly need to be a RequestResponseListener, but using it since it
   * already provides hooks into the request processing lifecycle.
   */
  private static class MDCCopyHelper extends RequestResponseListener {
    private final Map<String, String> submitterContext = MDC.getCopyOfContextMap();
    private Map<String, String> threadContext;

    @Override
    public void onBegin(Request request) {
      threadContext = MDC.getCopyOfContextMap();
      updateContextMap(submitterContext);
    }

    @Override
    public void onComplete(Result result) {
      updateContextMap(threadContext);
    }

    private static void updateContextMap(Map<String, String> context) {
      if (context != null && !context.isEmpty()) {
        MDC.setContextMap(context);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Extension of InputStreamResponseListener that handles Object release tracking of the
   * InputStreams
   *
   * @see ObjectReleaseTracker
   */
  private static class InputStreamReleaseTrackingResponseListener
      extends InputStreamResponseListener {

    @Override
    public InputStream getInputStream() {
      return new ObjectReleaseTrackedInputStream(super.getInputStream());
    }

    private static final class ObjectReleaseTrackedInputStream extends FilterInputStream {
      public ObjectReleaseTrackedInputStream(final InputStream in) {
        super(in);
        assert ObjectReleaseTracker.track(in);
      }

      @Override
      public void close() throws IOException {
        assert ObjectReleaseTracker.release(in);
        super.close();
      }
    }
  }
}
