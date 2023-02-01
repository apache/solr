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

import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import static org.apache.solr.common.util.Utils.getObjectByPath;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.http.entity.ContentType;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartContentProvider;
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Http2SolrClient extends SolrClient {
  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";

  private static volatile SSLConfig defaultSSLConfig;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final String DEFAULT_PATH = "/select";
  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  private HttpClient httpClient;
  private volatile Set<String> urlParamNames = Set.of();
  private int idleTimeout;
  private int requestTimeout;

  protected ResponseParser parser = new BinaryResponseParser();
  protected RequestWriter requestWriter = new BinaryRequestWriter();
  private List<HttpListenerFactory> listenerFactory = new ArrayList<>();
  private AsyncTracker asyncTracker = new AsyncTracker();
  /** The URL of the Solr server. */
  private String serverBaseUrl;

  private boolean closeClient;
  private ExecutorService executor;
  private boolean shutdownExecutor;

  private final String basicAuthAuthorizationStr;
  private AuthenticationStoreHolder authenticationStore;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    if (serverBaseUrl != null) {
      if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
      }
      this.serverBaseUrl = serverBaseUrl;
    }

    if (builder.idleTimeout != null && builder.idleTimeout > 0) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.http2SolrClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.http2SolrClient.httpClient;
    }
    if (builder.basicAuthUser != null && builder.basicAuthPassword != null) {
      basicAuthAuthorizationStr =
          basicAuthCredentialsToAuthorizationString(
              builder.basicAuthUser, builder.basicAuthPassword);
    } else {
      basicAuthAuthorizationStr = null;
    }
    if (builder.requestWriter != null) {
      requestWriter = builder.requestWriter;
    }
    if (builder.responseParser != null) {
      parser = builder.responseParser;
    }
    if (builder.requestTimeout == null) {
      requestTimeout = -1;
    } else {
      requestTimeout = builder.requestTimeout;
    }
    httpClient.setFollowRedirects(builder.followRedirects);
    this.urlParamNames = builder.urlParamNames;
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
    boolean sslEnabled;
    if (builder.sslConfig == null) {
      sslEnabled =
          System.getProperty("javax.net.ssl.keyStore") != null
              || System.getProperty("javax.net.ssl.trustStore") != null;
      sslContextFactory = sslEnabled ? getDefaultSslContextFactory() : null;
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
      sslEnabled = true;
    }

    HttpClientTransport transport;
    if (builder.useHttp1_1) {
      if (log.isDebugEnabled()) {
        log.debug("Create Http2SolrClient with HTTP/1.1 transport");
      }

      ClientConnector clientConnector = new ClientConnector();
      clientConnector.setReuseAddress(true);
      clientConnector.setSslContextFactory(sslContextFactory);
      clientConnector.setSelectors(2);
      transport = new HttpClientTransportOverHTTP(clientConnector);

      httpClient = new HttpClient(transport);

      if (builder.maxConnectionsPerHost != null)
        httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
    } else {
      log.debug("Create Http2SolrClient with HTTP/2 transport");
      ClientConnector clientConnector = new ClientConnector();
      clientConnector.setReuseAddress(true);
      clientConnector.setSslContextFactory(sslContextFactory);
      clientConnector.setSelectors(2);

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
    httpClient.setIdleTimeout(idleTimeout);

    this.authenticationStore = new AuthenticationStoreHolder();
    httpClient.setAuthenticationStore(this.authenticationStore);

    if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);

    try {
      httpClient.start();
    } catch (Exception e) {
      close(); // make sure we clean up
      throw new RuntimeException(e);
    }

    return httpClient;
  }

  @Override
  public void close() {
    // we wait for async requests, so far devs don't want to give sugar for this
    asyncTracker.waitForComplete();
    try {
      if (closeClient) {
        httpClient.stop();
        httpClient.destroy();
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

  public boolean isV2ApiRequest(final SolrRequest<?> request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  public static class OutStream implements Closeable {
    private final String origCollection;
    private final ModifiableSolrParams origParams;
    private final OutputStreamContentProvider outProvider;
    private final InputStreamResponseListener responseListener;
    private final boolean isXml;

    public OutStream(
        String origCollection,
        ModifiableSolrParams origParams,
        OutputStreamContentProvider outProvider,
        InputStreamResponseListener responseListener,
        boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.outProvider = outProvider;
      this.responseListener = responseListener;
      this.isXml = isXml;
    }

    boolean belongToThisStream(SolrRequest<?> solrRequest, String collection) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams(solrRequest.getParams());
      if (!origParams.toNamedList().equals(solrParams.toNamedList())
          || !StringUtils.equals(origCollection, collection)) {
        return false;
      }
      return true;
    }

    public void write(byte b[]) throws IOException {
      this.outProvider.getOutputStream().write(b);
    }

    public void flush() throws IOException {
      this.outProvider.getOutputStream().flush();
    }

    @Override
    public void close() throws IOException {
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      this.outProvider.getOutputStream().close();
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

    // The parser 'wt=' and 'version=' params are used instead of the
    // original params
    ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
    requestParams.set(CommonParams.WT, parser.getWriterType());
    requestParams.set(CommonParams.VERSION, parser.getVersion());

    String basePath = baseUrl;
    if (collection != null) basePath += "/" + collection;
    if (!basePath.endsWith("/")) basePath += "/";

    OutputStreamContentProvider provider = new OutputStreamContentProvider();
    Request postRequest =
        httpClient
            .newRequest(basePath + "update" + requestParams.toQueryString())
            .method(HttpMethod.POST)
            .header(HttpHeader.CONTENT_TYPE, contentType)
            .content(provider);
    decorateRequest(postRequest, updateRequest);
    InputStreamResponseListener responseListener = new InputStreamResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, provider, responseListener, isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  public void send(OutStream outStream, SolrRequest<?> req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    this.requestWriter.write(req, outStream.outProvider.getOutputStream());
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
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
    } catch (SolrServerException | IOException e) {
      asyncListener.onFailure(e);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    final ResponseParser parser =
        solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
    req.onRequestQueued(asyncTracker.queuedListener)
        .onComplete(asyncTracker.completeListener)
        .send(
            new InputStreamResponseListener() {
              @Override
              public void onHeaders(Response response) {
                super.onHeaders(response);
                InputStreamResponseListener listener = this;
                executor.execute(
                    () -> {
                      InputStream is = listener.getInputStream();
                      assert ObjectReleaseTracker.track(is);
                      try {
                        NamedList<Object> body =
                            processErrorsAndResponse(solrRequest, parser, response, is);
                        asyncListener.onSuccess(body);
                      } catch (RemoteSolrException e) {
                        if (SolrException.getRootCause(e) != CANCELLED_EXCEPTION) {
                          asyncListener.onFailure(e);
                        }
                      } catch (SolrServerException e) {
                        asyncListener.onFailure(e);
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
            });
    return () -> req.abort(CANCELLED_EXCEPTION);
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = makeRequest(solrRequest, collection);
    final ResponseParser parser =
        solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();

    Throwable abortCause = null;
    try {
      InputStreamResponseListener listener = new InputStreamResponseListener();
      req.send(listener);
      Response response = listener.get(idleTimeout, TimeUnit.MILLISECONDS);
      InputStream is = listener.getInputStream();
      assert ObjectReleaseTracker.track(is);
      return processErrorsAndResponse(solrRequest, parser, response, is);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      abortCause = e;
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new SolrServerException(
          "Timeout occurred while waiting response from server at: " + req.getURI(), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      abortCause = cause;
      if (cause instanceof ConnectException) {
        throw new SolrServerException("Server refused connection at: " + req.getURI(), cause);
      }
      if (cause instanceof SolrServerException) {
        throw (SolrServerException) cause;
      } else if (cause instanceof IOException) {
        throw new SolrServerException(
            "IOException occurred when talking to server at: " + getBaseURL(), cause);
      }
      throw new SolrServerException(cause.getMessage(), cause);
    } catch (SolrServerException | RuntimeException sse) {
      abortCause = sse;
      throw sse;
    } finally {
      if (abortCause != null) {
        req.abort(abortCause);
      }
    }
  }

  private NamedList<Object> processErrorsAndResponse(
      SolrRequest<?> solrRequest, ResponseParser parser, Response response, InputStream is)
      throws SolrServerException {
    String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);
    String mimeType = null;
    String encoding = null;
    if (contentType != null) {
      mimeType = MimeTypes.getContentTypeWithoutCharset(contentType);
      encoding = MimeTypes.getCharsetFromContentType(contentType);
    }
    return processErrorsAndResponse(
        response, parser, is, mimeType, encoding, isV2ApiRequest(solrRequest));
  }

  private void setBasicAuthHeader(SolrRequest<?> solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String encoded =
          basicAuthCredentialsToAuthorizationString(
              solrRequest.getBasicAuthUser(), solrRequest.getBasicAuthPassword());
      req.header("Authorization", encoded);
    } else if (basicAuthAuthorizationStr != null) {
      req.header("Authorization", basicAuthAuthorizationStr);
    }
  }

  private String basicAuthCredentialsToAuthorizationString(String user, String pass) {
    String userPass = user + ":" + pass;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(FALLBACK_CHARSET));
  }

  private Request makeRequest(SolrRequest<?> solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = createRequest(solrRequest, collection);
    decorateRequest(req, solrRequest);
    return req;
  }

  private void decorateRequest(Request req, SolrRequest<?> solrRequest) {
    req.header(HttpHeader.ACCEPT_ENCODING, null);
    if (requestTimeout > 0) {
      req.timeout(requestTimeout, TimeUnit.MILLISECONDS);
    } else {
      req.timeout(idleTimeout, TimeUnit.MILLISECONDS);
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

    Map<String, String> headers = solrRequest.getHeaders();
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
    }
  }

  private String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
    URL oldURL = new URL(basePath);
    String newPath = oldURL.getPath().replaceFirst("/solr", "/api");
    return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
  }

  private Request createRequest(SolrRequest<?> solrRequest, String collection)
      throws IOException, SolrServerException {
    if (solrRequest.getBasePath() == null && serverBaseUrl == null)
      throw new IllegalArgumentException("Destination node is not provided!");

    if (solrRequest instanceof V2RequestSupport) {
      solrRequest = ((V2RequestSupport) solrRequest).getV2Request();
    }
    SolrParams params = solrRequest.getParams();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams =
        contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    String path = requestWriter.getPath(solrRequest);
    if (path == null || !path.startsWith("/")) {
      path = DEFAULT_PATH;
    }

    ResponseParser parser = solrRequest.getResponseParser();
    if (parser == null) {
      parser = this.parser;
    }

    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    ModifiableSolrParams wparams = new ModifiableSolrParams(params);
    if (parser != null) {
      wparams.set(CommonParams.WT, parser.getWriterType());
      wparams.set(CommonParams.VERSION, parser.getVersion());
    }

    // TODO add invariantParams support

    String basePath = solrRequest.getBasePath() == null ? serverBaseUrl : solrRequest.getBasePath();
    if (collection != null) basePath += "/" + collection;

    if (solrRequest instanceof V2Request) {
      if (System.getProperty("solr.v2RealPath") == null) {
        basePath = changeV2RequestEndpoint(basePath);
      } else {
        basePath = serverBaseUrl + "/____v2";
      }
    }

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      return httpClient
          .newRequest(basePath + path + wparams.toQueryString())
          .method(HttpMethod.GET);
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      return httpClient
          .newRequest(basePath + path + wparams.toQueryString())
          .method(HttpMethod.DELETE);
    }

    if (SolrRequest.METHOD.POST == solrRequest.getMethod()
        || SolrRequest.METHOD.PUT == solrRequest.getMethod()) {

      String url = basePath + path;
      boolean hasNullStreamName = false;
      if (streams != null) {
        hasNullStreamName = streams.stream().anyMatch(cs -> cs.getName() == null);
      }

      boolean isMultipart = streams != null && streams.size() > 1 && !hasNullStreamName;

      HttpMethod method =
          SolrRequest.METHOD.POST == solrRequest.getMethod() ? HttpMethod.POST : HttpMethod.PUT;

      if (contentWriter != null) {
        Request req = httpClient.newRequest(url + wparams.toQueryString()).method(method);
        BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
        contentWriter.write(baos);

        // SOLR-16265: TODO reduce memory usage
        return req.content(
            // We're throwing this BAOS away, so no need to copy the byte[], just use the raw buf
            new ByteBufferContentProvider(
                contentWriter.getContentType(), ByteBuffer.wrap(baos.getbuf(), 0, baos.size())));
      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.urlParamNames, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));
        Request req = httpClient.newRequest(url + queryParams.toQueryString()).method(method);
        return fillContentStream(req, streams, wparams, isMultipart);
      } else {
        // It is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();
        return httpClient
            .newRequest(url + wparams.toQueryString())
            .method(method)
            .content(
                new InputStreamContentProvider(contentStream.getStream()),
                contentStream.getContentType());
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
      MultiPartContentProvider content = new MultiPartContentProvider();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            content.addFieldPart(key, new StringContentProvider(val), null);
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
              new InputStreamContentProvider(contentStream.getStream()),
              fields);
        }
      }
      req.content(content);
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
      req.content(new FormContentProvider(fields, FALLBACK_CHARSET));
    }

    return req;
  }

  private boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private NamedList<Object> processErrorsAndResponse(
      Response response,
      final ResponseParser processor,
      InputStream is,
      String mimeType,
      String encoding,
      final boolean isV2Api)
      throws SolrServerException {
    boolean shouldClose = true;
    try {
      // handle some http level checks before trying to parse the response
      int httpStatus = response.getStatus();

      switch (httpStatus) {
        case HttpStatus.OK_200:
        case HttpStatus.BAD_REQUEST_400:
        case HttpStatus.CONFLICT_409:
          break;
        case HttpStatus.MOVED_PERMANENTLY_301:
        case HttpStatus.MOVED_TEMPORARILY_302:
          if (!httpClient.isFollowRedirects()) {
            throw new SolrServerException(
                "Server at " + getBaseURL() + " sent back a redirect (" + httpStatus + ").");
          }
          break;
        default:
          if (processor == null || mimeType == null) {
            throw new RemoteSolrException(
                serverBaseUrl,
                httpStatus,
                "non ok status: " + httpStatus + ", message:" + response.getReason(),
                null);
          }
      }

      if (wantStream(parser)) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
        rsp.add("stream", is);
        // Only case where stream should not be closed
        shouldClose = false;
        return rsp;
      }

      final Collection<String> processorSupportedContentTypes = processor.getContentTypes();
      if (processorSupportedContentTypes != null && !processorSupportedContentTypes.isEmpty()) {
        final Collection<String> processorMimeTypes =
            processorSupportedContentTypes.stream()
                .map(ct -> ContentType.parse(ct).getMimeType().trim().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        if (!processorMimeTypes.contains(mimeType)) {
          // unexpected mime type
          final String allSupportedTypes = String.join(", ", processorMimeTypes);
          String prefix =
              "Expected mime type in [" + allSupportedTypes + "] but got " + mimeType + ". ";
          String exceptionEncoding = encoding != null ? encoding : FALLBACK_CHARSET.name();
          try {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            is.transferTo(body);
            throw new RemoteSolrException(
                serverBaseUrl, httpStatus, prefix + body.toString(exceptionEncoding), null);
          } catch (IOException e) {
            throw new RemoteSolrException(
                serverBaseUrl,
                httpStatus,
                "Could not parse response with encoding " + exceptionEncoding,
                e);
          }
        }
      }

      NamedList<Object> rsp;
      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        throw new RemoteSolrException(serverBaseUrl, httpStatus, e.getMessage(), e);
      }

      Object error = rsp == null ? null : rsp.get("error");
      if (error != null
          && (String.valueOf(getObjectByPath(error, true, errPath))
              .endsWith("ExceptionWithErrObject"))) {
        throw RemoteExecutionException.create(serverBaseUrl, rsp);
      }
      if (httpStatus != HttpStatus.OK_200 && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          if (error != null) {
            reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
            if (reason == null) {
              reason =
                  (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
            }
            Object metadataObj =
                Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
            if (metadataObj instanceof NamedList) {
              metadata = (NamedList<String>) metadataObj;
            } else if (metadataObj instanceof List) {
              // NamedList parsed as List convert to NamedList again
              List<Object> list = (List<Object>) metadataObj;
              metadata = new NamedList<>(list.size() / 2);
              for (int i = 0; i < list.size(); i += 2) {
                metadata.add((String) list.get(i), (String) list.get(i + 1));
              }
            } else if (metadataObj instanceof Map) {
              metadata = new NamedList((Map) metadataObj);
            }
          }
        } catch (Exception ex) {
          /* Ignored */
        }
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getReason())
              .append("\n\n")
              .append("request: ")
              .append(response.getRequest().getMethod());
          reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
        }
        RemoteSolrException rss = new RemoteSolrException(serverBaseUrl, httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }
      return rsp;
    } finally {
      if (shouldClose) {
        try {
          is.close();
          assert ObjectReleaseTracker.release(is);
        } catch (IOException e) {
          // quitely
        }
      }
    }
  }

  /**
   * Choose the {@link RequestWriter} to use.
   *
   * <p>By default, {@link BinaryRequestWriter} is used.
   *
   * <p>Note: This setter method is <b>not thread-safe</b>.
   *
   * @deprecated use {@link Http2SolrClient.Builder#withRequestWriter(RequestWriter)} instead
   */
  @Deprecated
  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  protected RequestWriter getRequestWriter() {
    return requestWriter;
  }

  /**
   * Configure whether the client should follow redirects or not.
   *
   * <p>This defaults to false under the assumption that if you are following a redirect to get to a
   * Solr installation, something is configured wrong somewhere.
   *
   * @deprecated use {@link Http2SolrClient.Builder#withFollowRedirects(boolean)}
   *     Redirects(boolean)} instead
   */
  @Deprecated
  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
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

  public static class Builder {

    private Http2SolrClient http2SolrClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout;
    private Integer connectionTimeout;
    private Integer requestTimeout;
    private Integer maxConnectionsPerHost;
    private String basicAuthUser;
    private String basicAuthPassword;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    private boolean followRedirects = false;
    protected String baseSolrUrl;
    private ExecutorService executor;
    protected RequestWriter requestWriter;
    protected ResponseParser responseParser;
    private Set<String> urlParamNames = Set.of();

    public Builder() {}

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
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
              (HttpClientBuilderFactory)
                  Class.forName(factoryClassName).getConstructor().newInstance();
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

    /** Reuse {@code httpClient} connections pool */
    public Builder withHttpClient(Http2SolrClient httpClient) {
      this.http2SolrClient = httpClient;
      return this;
    }

    /** Provides a {@link RequestWriter} for created clients to use when handing requests. */
    public Builder withRequestWriter(RequestWriter requestWriter) {
      this.requestWriter = requestWriter;
      return this;
    }

    /** Provides a {@link ResponseParser} for created clients to use when handling requests. */
    public Builder withResponseParser(ResponseParser responseParser) {
      this.responseParser = responseParser;
      return this;
    }

    public Builder withFollowRedirects(boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
    }

    public Builder withExecutor(ExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    public Builder withBasicAuthCredentials(String user, String pass) {
      if (user != null || pass != null) {
        if (user == null || pass == null) {
          throw new IllegalStateException(
              "Invalid Authentication credentials. Either both username and password or none must be provided");
        }
      }
      this.basicAuthUser = user;
      this.basicAuthPassword = pass;
      return this;
    }

    /**
     * Expert Method
     *
     * @param urlParamNames set of param keys that are only sent via the query string. Note that the
     *     param will be sent as a query string if the key is part of this Set or the SolrRequest's
     *     query params.
     * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
     */
    public Builder withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
      this.urlParamNames = urlParamNames;
      return this;
    }

    /**
     * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited
     * by 4
     */
    public Builder maxConnectionsPerHost(int max) {
      this.maxConnectionsPerHost = max;
      return this;
    }

    public Builder idleTimeout(int idleConnectionTimeout) {
      this.idleTimeout = idleConnectionTimeout;
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return this;
    }

    public Builder connectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    /**
     * Set a timeout in milliseconds for requests issued by this client.
     *
     * @param requestTimeout The timeout in milliseconds
     * @return this Builder.
     */
    public Builder requestTimeout(int requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }
  }

  /**
   * @deprecated use {@link #getUrlParamNames()}
   */
  @Deprecated
  public Set<String> getQueryParams() {
    return getUrlParamNames();
  }

  public Set<String> getUrlParamNames() {
    return urlParamNames;
  }

  /**
   * Expert Method
   *
   * @param urlParamNames set of param keys that are only sent via the query string. Note that the
   *     param will be sent as a query string if the key is part of this Set or the SolrRequest's
   *     query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   * @deprecated use {@link Http2SolrClient.Builder#withTheseParamNamesInTheUrl(Set)} instead
   */
  @Deprecated
  public void setUrlParamNames(Set<String> urlParamNames) {
    this.urlParamNames = urlParamNames;
  }

  private ModifiableSolrParams calculateQueryParams(
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

  public ResponseParser getParser() {
    return parser;
  }

  /**
   * Note: This setter method is <b>not thread-safe</b>.
   *
   * @param parser Default Response Parser chosen to parse the response if the parser were not
   *     specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   * @deprecated use {@link Http2SolrClient.Builder#withResponseParser(ResponseParser)} instead
   */
  @Deprecated
  public void setParser(ResponseParser parser) {
    this.parser = parser;
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
    boolean sslCheckPeerName = true;
    if (checkPeerNameStr == null || "false".equalsIgnoreCase(checkPeerNameStr)) {
      sslCheckPeerName = false;
    }

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

    sslContextFactory.setEndpointIdentificationAlgorithm(
        System.getProperty("solr.jetty.ssl.verifyClientHostName"));

    return sslContextFactory;
  }
}
