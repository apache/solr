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

import com.google.api.client.util.escape.CharEscapers;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import org.agrona.MutableDirectBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.*;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.*;
import org.eclipse.jetty.client.*;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.client.http.HttpClientConnectionFactory;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.*;
import org.eclipse.jetty.http.*;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.ClientConnectionFactoryOverHTTP2;
import org.eclipse.jetty.http2.frames.Frame;
import org.eclipse.jetty.io.ClientConnectionFactory;
import org.eclipse.jetty.io.ClientConnector;

import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.Pool;
import org.eclipse.jetty.util.ProcessorUtils;
import org.eclipse.jetty.util.SocketAddressResolver;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import static org.apache.solr.common.util.Utils.getObjectByPath;

/**
 * Difference between this {@link Http2SolrClient} and the former HttpSolrClient:
 * <ul>
 *  <li>{@link Http2SolrClient} sends requests in HTTP/2</li>
 *  <li>{@link Http2SolrClient} can point to multiple urls</li>
 *  <li>{@link Http2SolrClient} does not expose its internal httpClient like the former HttpSolrClient.getHttpClient(),
 * sharing connection pools should be done by {@link Builder#withHttpClient(Http2SolrClient)} </li>
 * </ul>
 */
public class Http2SolrClient extends SolrClient {

  public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";
  private static final Pattern COMPILE = Pattern.compile("/solr");

  private static volatile SSLConfig defaultSSLConfig;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String GET = "GET";
  private static final String DELETE = "DELETE";
  private static final String HEAD = "HEAD";

  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final String DEFAULT_PATH = "/select";

  private final Map<String, String> headers;
  private final boolean markedInternal;

  private CloseTracker closeTracker;

  private final SolrInternalHttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private final int idleTimeout;
  private final boolean strictEventOrdering;
  private volatile ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();
  private final Set<HttpListenerFactory> listenerFactory;
  private final AsyncTracker asyncTracker;
  /**
   * The URL of the Solr server.
   */
  private volatile String serverBaseUrl;
  private volatile boolean closeClient;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    assert (closeTracker = new CloseTracker()) != null;
    assert ObjectReleaseTracker.getInstance().track(this);
    if (serverBaseUrl == null && builder.baseSolrUrl != null) {
      serverBaseUrl = builder.baseSolrUrl;
    }
    //System.out.println("serverBaseUrl=" + serverBaseUrl);
    //new RuntimeException().printStackTrace();
    if (serverBaseUrl != null)  {
      if (!serverBaseUrl.equals("/") && !serverBaseUrl.isEmpty() && serverBaseUrl.charAt(serverBaseUrl.length() - 1) == '/') {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1);
      }
    }
    this.serverBaseUrl = serverBaseUrl;
    int maxOutstandingAsyncRequests = SysStats.PROC_COUNT;
    if (builder.maxOutstandingAsyncRequests != null) maxOutstandingAsyncRequests = builder.maxOutstandingAsyncRequests;
    asyncTracker = new AsyncTracker(maxOutstandingAsyncRequests);
    this.headers = builder.headers;
    this.markedInternal = builder.markedInternal;

    this.strictEventOrdering = builder.strictEventOrdering;

    if (builder.idleTimeout != null && builder.idleTimeout > 0) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.http2SolrClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
      this.listenerFactory  = ConcurrentHashMap.newKeySet();
    } else {
      httpClient = builder.http2SolrClient.httpClient;
      this.listenerFactory = builder.http2SolrClient.listenerFactory;
    }
  }

  public void addListenerFactory(HttpListenerFactory factory) {
    this.listenerFactory.add(factory);
  }

  // internal usage only
  public HttpClient getHttpClient() {
    return httpClient;
  }

  public void addHeaders(Map<String,String> headers) {
    this.headers.putAll(headers);
  }

  // internal usage only
  ProtocolHandlers getProtocolHandlers() {
    return httpClient.getProtocolHandlers();
  }

  private SolrInternalHttpClient createHttpClient(Builder builder) {
    SolrInternalHttpClient httpClient;

    SslContextFactory.Client sslContextFactory = null;

    if (builder.sslConfig == null) {
      sslContextFactory = getDefaultSslContextFactory();
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
    }
    // MRM TODO: - look at config again as well
    int minThreads = Integer.getInteger("solr.minHttp2ClientThreads", 6);

    minThreads = Math.min(builder.maxThreadPoolSize, minThreads);

    int maxThreads = Math.max(builder.maxThreadPoolSize, minThreads);

    //SolrQueuedThreadPool httpClientExecutor = new SolrQueuedThreadPool("http2Client", maxThreads, minThreads, 15000, null, -1, null);
    //httpClientExecutor.setLowThreadsThreshold(-1);
    // section SolrQTP
    SolrQTP httpClientExecutor = new SolrQTP("Http2SolrClient-" + builder.name, maxThreads, minThreads);
    httpClientExecutor.setStopTimeout(0);
    // Disable reserved threads so EatWhatYouKill degrades to ProduceExecuteConsume: a response callback
    // (which may block while a distributed update forwards/retries) never occupies the connection's IO
    // producer thread, so other multiplexed streams' frames keep being read. See JettySolrRunner.

        // SolrQTP httpClientExecutor = new SolrQTP("Http2SolrClient-" + builder.name, maxThreads, minThreads, new MPMCQueue.RunnableBlockingQueue());
//    try {
//      httpClientExecutor.start();
//    } catch (Exception e) {
//     throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }
    SecurityManager s = System.getSecurityManager();
    ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    ScheduledExecutorScheduler scheduler = new ScheduledExecutorScheduler("http2client-scheduler", true, null, group);

    if (builder.useHttp1_1) {
      if (log.isTraceEnabled()) log.trace("Create Http2SolrClient with HTTP/1.1 transport");
      ClientConnector clientConnector = new ClientConnector();
      clientConnector.setReuseAddress(true);
      clientConnector.setSslContextFactory(sslContextFactory);
      SolrHttpClientTransportOverHTTP transport = new SolrHttpClientTransportOverHTTP(clientConnector);
      httpClient = new SolrInternalHttpClient(transport);
    } else {
      if (log.isTraceEnabled()) log.trace("Create Http2SolrClient with HTTP/2 transport");
//
//          if (log.isDebugEnabled()) {
//            RuntimeException e = new RuntimeException();
//            StackTraceElement[] stack = e.getStackTrace();
//            for (int i = 0; i < Math.min(8, stack.length - 1); i++) {
//              log.debug(stack[i].toString());
//            }
//
//            log.debug("create http2solrclient {}", this);
//          }
   //   HttpClientTransportOverHTTP2 transport;
      HTTP2Client http2client;
      ClientConnector clientConnector = new ClientConnector();
      if (sslContextFactory == null) {

        clientConnector.setReuseAddress(true);
        http2client = new HTTP2Client(clientConnector);
      } else {

        clientConnector.setSslContextFactory(sslContextFactory);
        clientConnector.setReuseAddress(true);
        http2client = new HTTP2Client(clientConnector);
      }

     clientConnector.setByteBufferPool(new NonBlockingMappedByteBufferPool(
         httpClientExecutor instanceof ThreadPool.SizedThreadPool
             ? ((ThreadPool.SizedThreadPool)httpClientExecutor).getMaxThreads() / 2
             : ProcessorUtils.availableProcessors() << 1));
      http2client.setSelectors(-1);

     // http2client.setMaxConcurrentPushedStreams(512);
      http2client.setInputBufferSize(Frame.DEFAULT_MAX_LENGTH +  + Frame.HEADER_LENGTH);
    //  transport = new HttpClientTransportOverHTTP2(http2client);

      //ClientConnector clientConnector = new ClientConnector();
      // Configure the clientConnector.

      // Prepare the application protocols.
      ClientConnectionFactory.Info h1 = HttpClientConnectionFactory.HTTP11;

      ClientConnectionFactory.Info h2 = new ClientConnectionFactoryOverHTTP2.HTTP2(http2client);

      // Create the HttpClientTransportDynamic, preferring h2 over h1.
      HttpClientTransport transport = new HttpClientTransportDynamic(http2client.getClientConnector(), h2, h1);

      transport.setConnectionPoolFactory(new MyFactory(builder.maxConnectionsPerHost, builder.maxRequestsQueuedPerDestination, builder.useHttp1_1));
      httpClient = new SolrInternalHttpClient(transport);
    }

    try {
      httpClient.setScheduler(scheduler);
      httpClient.addBean(scheduler);
      httpClient.manage(scheduler);
      httpClient.setExecutor(httpClientExecutor);
      httpClient.addBean(httpClientExecutor);
      httpClient.manage(httpClientExecutor);

      httpClient.setStrictEventOrdering(strictEventOrdering);
      httpClient.setSocketAddressResolver(new SocketAddressResolver.Async(ParWork.getRootSharedIOExecutor(), scheduler, 3000));
      httpClient.setConnectBlocking(false);
      httpClient.setFollowRedirects(false);
      if (builder.maxConnectionsPerHost != null) httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
      httpClient.setMaxRequestsQueuedPerDestination(builder.maxRequestsQueuedPerDestination);
      httpClient.setRequestBufferSize(8192);
      httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));
      httpClient.setIdleTimeout(builder.idleTimeout);
      httpClient.setConnectTimeout(5000);
      httpClient.setTCPNoDelay(true);

      //httpClient.setRemoveIdleDestinations(true);
      httpClient.setAddressResolutionTimeout(3000);

      if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
      httpClient.start();
    } catch (Exception e) {
      log.error("Exception creating HttpClient", e);
      try {
        close();
      } catch (Exception e1) {
        e.addSuppressed(e1);
      }
      throw new RuntimeException(e);
    }
    return httpClient;
  }

  public void close() {
    if (log.isTraceEnabled()) log.trace("Closing {} closeClient={}", this.getClass().getSimpleName(), closeClient);
    assert closeTracker != null ? closeTracker.close() : true;
    boolean closed = true;
    try {
//      try {
//        waitForOutstandingRequests(30, TimeUnit.SECONDS);
//      } catch (TimeoutException timeoutException) {
//        log.info("Timeout waiting for outstanding requests on close");
//      }
      org.apache.solr.common.util.IOUtils.closeQuietly(asyncTracker);

      if (closeClient) {
        try {
          httpClient.stop();
          try {
            ((LifeCycle) httpClient.getExecutor()).stop();
          } catch (Exception e) {
            log.error("Exception closing httpClient scheduler", e);
          }
        //  httpClient.getScheduler().stop();
        } catch (Exception e) {
          log.error("Exception closing httpClient", e);
        }

      }
      if (log.isTraceEnabled()) log.trace("Done closing {}", this.getClass().getSimpleName());
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

  public void waitForOutstandingRequests(long timeout, TimeUnit timeUnit) throws TimeoutException {
    asyncTracker.waitForComplete(timeout, timeUnit);
  }

  public static boolean isV2ApiRequest(final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  /**
   * Accumulates a batch of streamed updates into a single in-memory (Agrona) buffer and then sends
   * them as one fully-buffered HTTP/2 request with an explicit {@code Content-Length}.
   *
   * <p>The previous implementation streamed the body via Jetty's {@code OutputStreamContentProvider}
   * (chunked, no Content-Length). Under the per-runner client lifecycle the HTTP/2 connection could be
   * torn down while streamed adds were still in flight ("IllegalStateException: STOPPING"), truncating
   * the body so the server saw an incomplete javabin/xml stream and returned a 500 with an EOFException.
   * Buffering with a known length mirrors the proven {@link #request(SolrRequest, Request)} path and
   * sends the body atomically, avoiding the mid-stream teardown.</p>
   */
  public class OutStream implements Closeable {
    private final String origCollection;
    private final ModifiableSolrParams origParams;
    private final String baseUrl;
    private final boolean isXml;

    private final MutableDirectBuffer buffer;
    private final ExpandableDirectBufferOutputStream bufferOut;

    // Populated once the buffered body is sent (on close()).
    private SolrInputStreamResponseListener responseListener;
    private boolean sent;

    public OutStream(String origCollection, ModifiableSolrParams origParams, String baseUrl, boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.baseUrl = baseUrl;
      this.isXml = isXml;
      this.buffer = ExpandableBuffers.getInstance().acquire(-1, true);
      this.bufferOut = new ExpandableDirectBufferOutputStream(buffer);
    }

    boolean belongToThisStream(@SuppressWarnings({"rawtypes"})SolrRequest solrRequest, String collection) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams(solrRequest.getParams());
      return origParams.toNamedList().equals(solrParams.toNamedList()) && StringUtils.equals(origCollection, collection);
    }

    public void write(byte[] b) throws IOException {
      this.bufferOut.write(b);
    }

    public void flush() throws IOException {
      this.bufferOut.flush();
    }

    ExpandableDirectBufferOutputStream getBufferOut() {
      return bufferOut;
    }

    boolean isXml() {
      return isXml;
    }

    /**
     * Finalizes the body (closing the xml {@code <stream>} element if needed) and sends the whole
     * buffered batch as a single request with an explicit Content-Length, then blocks for the
     * response headers so the caller can read the response body and status.
     */
    @Override
    public void close() throws IOException {
      if (sent) {
        // Idempotent: the body is sent on the first close(); try-with-resources may call again.
        return;
      }
      sent = true;
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      bufferOut.flush();

      String contentType = requestWriter.getUpdateContentType();

      ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
      requestParams.set(CommonParams.WT, parser.getWriterType());
      requestParams.set(CommonParams.VERSION, parser.getVersion());

      ByteBuffer byteBuffer = bufferOut.buffer().byteBuffer().asReadOnlyBuffer();
      byteBuffer.position(bufferOut.offset() + bufferOut.buffer().wrapAdjustment());
      byteBuffer.limit(bufferOut.position() + bufferOut.buffer().wrapAdjustment());
      final int contentLength = bufferOut.position();

      ByteBufferRequestContent bcp = new ByteBufferRequestContent(contentType, byteBuffer);

      String updateUrl = baseUrl + (baseUrl.endsWith("/") ? "" : "/") + "update" + requestParams.toQueryString();

      Request postRequest = httpClient
          .newSolrRequest(updateUrl, buffer)
          .method(HttpMethod.POST)
          .headers(httpFields -> httpFields.add(HttpHeader.CONTENT_LENGTH, String.valueOf(contentLength)))
          // Release the pooled request buffer when the exchange completes. The CUSC Runner normally
          // frees it via rspBody.close(), but if responseListener.get() times out or fails before the
          // response InputStream is obtained, that close never runs and the Agrona buffer leaks. This
          // onComplete backstop matches the canonical createRequest() path; freeBuffer() is idempotent
          // (AtomicBoolean) so the buffer is released exactly once even when both paths fire.
          .onComplete(result -> ((SolrHttpRequest) result.getRequest()).freeBuffer())
          .body(bcp);
      headers.forEach(postRequest::header);

      decorateRequest(postRequest, new UpdateRequest());

      this.responseListener = new SolrInputStreamResponseListener((SolrHttpRequest) postRequest);
      postRequest.send(responseListener);
    }

    //TODO this class should be hidden
    public SolrInputStreamResponseListener getResponseListener() {
      return responseListener;
    }
  }

  public OutStream initOutStream(String baseUrl,
                                 UpdateRequest updateRequest,
                                 String collection) throws IOException {
    final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

    // Route per-collection: ConcurrentUpdateHttp2SolrClient.add(collection, doc) supplies a
    // collection that must be appended to the base URL, otherwise every streamed batch hits the
    // bare base path. All updates within one OutStream share the same collection (enforced by
    // belongToThisStream), so it is safe to bake the collection into the destination URL here.
    String destUrl = baseUrl;
    if (collection != null) {
      destUrl = destUrl + (destUrl.endsWith("/") ? "" : "/") + collection;
    }

    updateRequest.setBasePath(destUrl);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, destUrl, isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  public void send(OutStream outStream, SolrRequest req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    this.requestWriter.write(req, new FastOutputStream(outStream.getBufferOut()));
    if (outStream.isXml()) {
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
          byte[] content = String.format(Locale.ROOT,
              fmt, String.valueOf(params.getBool(UpdateParams.WAIT_SEARCHER, false)))
              .getBytes(FALLBACK_CHARSET);
          outStream.write(content);
        }
      }
    }
  }

  public static class CancelledException extends RuntimeException {

  }
  private static final Cancellable FAILED_MAKING_REQUEST_CANCELLABLE = () -> {};

  public Cancellable asyncRequest(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<NamedList<Object>> asyncListener) {
    return asyncRequest(solrRequest, collection,null, asyncListener);
  }

  public Cancellable asyncRequest(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, Object context, AsyncListener<NamedList<Object>> asyncListener) {
    // section asyncRequest
    SolrParams params = solrRequest.getParams();
    Integer idleTimeout = params == null ? null : params.getInt("idleTimeout");
    Integer requestTimeoutMs = params == null ? null : params.getInt("requestTimeoutMs");
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
      // Apply a per-request (per-stream) idle timeout. HTTP/2 multiplexes many streams over a single
      // connection, so the connection-level idle timeout never fires for one wedged stream while other
      // streams keep the connection active. Without a per-stream deadline a stuck peer leaves this async
      // callback un-invoked, leaking the AsyncTracker phaser party and hanging the caller (e.g.
      // SolrCmdDistributor.finish()) for its full multi-minute wait.
      //
      // Only force the client default for UPDATE requests (forwarded updates/commits sent by
      // SolrCmdDistributor). Distributed SEARCH sub-requests also go through asyncRequest, and they
      // legitimately fan out to shards of several collections; imposing the default idle timeout on
      // them aborts a slower collection's shard request and silently drops its results (e.g. a
      // multi-collection alias query returning only one collection's docs). For those, preserve the
      // baseline behavior of honoring only an explicitly-requested idleTimeout.
      if (idleTimeout != null && idleTimeout > 0) {
        req.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      } else if (solrRequest instanceof UpdateRequest) {
        req.idleTimeout(this.idleTimeout, TimeUnit.MILLISECONDS);
      }
      // A hard, total request deadline. Unlike idleTimeout (which Jetty only enforces per-stream once
      // the stream sees no data, and which can fail to fire for a wedged HTTP/2 stream on an otherwise
      // active multiplexed connection), Request.timeout() is enforced by the scheduler over the whole
      // request lifecycle including time spent queued waiting for a connection. Callers that forward
      // updates/commits set this so a wedged peer cannot block them indefinitely.
      if (requestTimeoutMs != null && requestTimeoutMs > 0) {
        req.timeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      asyncListener.onFailure(e, 0, context);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    final ResponseParser parser = solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();
    boolean tracking = false;
    MutableDirectBuffer expandableBuffer = ExpandableBuffers.getInstance().acquire(-1, true);
    SolrBufferingResponseListener listener = new SolrBufferingResponseListener(expandableBuffer) {
      @Override public void onComplete(Result result) {
        httpClient.getExecutor().execute( () -> {

          MutableDirectBuffer buff = getContent();
          // Length = bytes actually received (getContent() set the ByteBuffer limit accordingly),
          // not the full buffer capacity, which would include trailing NUL padding.
          int len = buff == null ? 0 : buff.byteBuffer().remaining();
          InputStream is = new DirectBufferInputStream(buff, 0, len);
          try {
            Response response = result.getResponse();
            if (result.isSucceeded()) {

              NamedList<Object> body = processErrorsAndResponse(req, solrRequest, parser, response, is);
              if (response.getStatus() == 200) {
                asyncListener.onSuccess(body, response.getStatus(), context);
              } else {
                asyncListener.onFailure(response.getRequest().getAbortCause(), response.getStatus(), context);
              }
            } else {

              Throwable failure = result.getRequestFailure();

              if (failure == null) {
                failure = result.getResponseFailure();
              }


              if (failure instanceof CancelledException) {
                asyncListener.onFailure(failure, 0, context);
                return;
              }
              int status = response.getStatus();

              log.error("Request exception code={} request={}", status, req, failure);

              if (failure instanceof ClosedChannelException) { // success but no response
                asyncListener.onFailure(failure, 503, context);
                return;
              }
              if (failure == null) {
                processErrorsAndResponse(req, solrRequest, parser, response, is);
              }

              asyncListener.onFailure(failure, status, context);
            }
          } catch (Exception e) {
            int status = 0;
            if (e instanceof SolrException) {
              status = ((SolrException) e).code();
            }
            log.warn("Unexpected failure while inspecting response", e);
            asyncListener.onFailure(e, status, context); // TODO handle response better

          } finally {
            try {
              org.apache.solr.common.util.IOUtils.closeQuietly(is);
              ((SolrHttpRequest) req).freeBuffer();
              // Return the response buffer to the pool alongside the request buffer. It was never released
              // on this path, losing pooling and adding GC pressure on every async request.
              ExpandableBuffers.getInstance().release(expandableBuffer);
            } finally {
              asyncTracker.arrive();
            }

          }
        });
      }
    };
    try {
      asyncTracker.register();
      tracking = true;
      asyncListener.onStart();
      req.send(listener);

    } catch (Exception e) {
      try {
        log.warn("failed sending request", e);
        if (!(e instanceof CancelledException)) {
          asyncListener.onFailure(e, 0, context);
        } else {
          asyncListener.onFailure(e, 0, context);
        }
      } finally {
        if (tracking) {
          asyncTracker.arrive();
        }
      }
    }

    return new AbortRequest(req);
  }

  public Cancellable asyncRequestRaw(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<InputStream> asyncListener)
      throws SolrServerException {
    // section asyncRaw
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
    } catch (Exception e) {
      asyncListener.onFailure(e, 0, null);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    MyInputStreamResponseListener mysl = new MyInputStreamResponseListener(httpClient, req, asyncListener);
    try {
      req.send(mysl);
    } catch (Exception e) {
      throw new SolrServerException(e);
    }

    Response response;
    try {
      response = mysl.get(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
            InputStream is = mysl.getInputStream();
     org.apache.solr.common.util.IOUtils.closeQuietly(is);
      throw new SolrServerException(e);
    } catch (TimeoutException timeoutException) {
      InputStream is = mysl.getInputStream();
      org.apache.solr.common.util.IOUtils.closeQuietly(is);
      throw new SolrServerException(timeoutException);
    } catch (ExecutionException e) {
      InputStream is = mysl.getInputStream();
      org.apache.solr.common.util.IOUtils.closeQuietly(is);
      throw new SolrServerException(e.getCause());
    }

//    if (response.getStatus() != 200) {
//      InputStream is = mysl.getInputStream();
//      org.apache.solr.common.util.IOUtils.closeQuietly(is);
//
//      throw new SolrServerException("Request failed with status " + response.getStatus());
//    }

    return new MyCancellable(req, mysl);
  }

  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection) throws SolrServerException, IOException {
    Request req = makeRequest(solrRequest, collection);
    SolrParams params = solrRequest.getParams();
    Integer reqIdleTimeout = params == null ? null : params.getInt("idleTimeout");
    // Only apply a per-request idle timeout when the caller explicitly asked for one. This is the
    // synchronous request path used by streaming reads (CloudSolrStream/SolrStream over /export and
    // /stream): a long-running stream (e.g. an executor() or daemon() expression) legitimately sends
    // no bytes for long stretches while the server computes, so imposing the client's default
    // idleTimeout here would abort it with "TimeoutException: idle_timeout". The wedged-stream
    // protection for forwarded updates/commits lives on the async path (see asyncRequest), which is
    // where SolrCmdDistributor sends them.
    if (reqIdleTimeout != null && reqIdleTimeout > 0) {
      req.idleTimeout(reqIdleTimeout, TimeUnit.MILLISECONDS);
    }
    // Honor a hard, total request deadline on the synchronous path too (symmetric with asyncRequest).
    // idleTimeout only fires per-stream when the stream sees no data, which a wedged HTTP/2 stream on
    // a half-open connection (e.g. a leader that died/partitioned behind a still-open proxy) can fail
    // to trigger -- and the blocking get() below otherwise falls back to a 10-MINUTE ceiling. A
    // scheduler-enforced Request.timeout() aborts the whole request regardless, so a caller that sets
    // requestTimeoutMs (e.g. recovery's commit-on-leader) cannot be stalled indefinitely.
    Integer requestTimeoutMs = params == null ? null : params.getInt("requestTimeoutMs");
    if (requestTimeoutMs != null && requestTimeoutMs > 0) {
      req.timeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
    }
    return request(solrRequest, req);
  }

  /**
   * Best-effort extraction of a human-readable error message from a javabin-decoded error body.
   * Handles both standard error responses ({@code {error:{msg:...}}}) and the streaming-handler
   * format used by /export and /stream ({@code {response:{docs:[{EXCEPTION:...}]}}}). Works whether
   * the decoded container is a {@link NamedList} or a plain {@link Map} (the fork's JavaBinCodec
   * deserializes maps as fastutil maps, not NamedLists).
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static String extractErrorMessage(Object root) {
    if (root == null) return null;
    // Standard error response: error -> msg
    Object error = mapGet(root, "error");
    if (error != null) {
      Object msg = mapGet(error, "msg");
      if (msg != null) return String.valueOf(msg);
    }
    // Streaming handler (/export, /stream) error: response -> docs -> [0] -> EXCEPTION
    Object resp = mapGet(root, "response");
    if (resp != null) {
      Object docs = mapGet(resp, "docs");
      if (docs instanceof List && !((List) docs).isEmpty()) {
        Object doc0 = ((List) docs).get(0);
        Object exc = mapGet(doc0, "EXCEPTION");
        if (exc != null) return String.valueOf(exc);
      }
    }
    return null;
  }

  /** Reads a key from either a {@link NamedList} or a {@link Map}. */
  private static Object mapGet(Object container, String key) {
    if (container instanceof NamedList) return ((NamedList<?>) container).get(key);
    if (container instanceof Map) return ((Map<?, ?>) container).get(key);
    return null;
  }

  public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, Request req) throws SolrServerException, IOException {

    // section request
    final ResponseParser parser = solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();

    Response response;
    if (wantStream(parser)) {
      SolrInputStreamResponseListener listener = new SolrInputStreamResponseListener((SolrHttpRequest) req);
      req.send(listener);
      try {
        response = listener.get(30000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | TimeoutException e) {
        throw new SolrServerException(e);
      } catch (ExecutionException e) {
        throw new SolrServerException(e.getCause());
      }

      if (response.getStatus() == 200) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>(1);
        rsp.add("stream", listener.getInputStream());
        return rsp;
      }
      // Non-200: read the error body to extract the actual error message before closing.
      InputStream errorIs = listener.getInputStream();
      String errorMsg = "Request failed with status " + response.getStatus();
      try {
        if (errorIs != null) {
          // Read the error body and try to extract a meaningful message from it.
          try {
            // Try to parse the error body to get a meaningful message.
            String contentType = response.getHeaders().get("Content-Type");
            // javabin content type is "application/octet-stream"
            if (contentType != null && (contentType.contains("octet-stream") || contentType.contains("javabin"))) {
              // NOTE: do NOT cast to NamedList here. The fork's JavaBinCodec deserializes maps as
              // it.unimi.dsi.fastutil Object2ObjectLinkedOpenHashMap, and streaming handlers (/export,
              // /stream) write errors as a plain map, not a NamedList. Navigate generically over either.
              Object errObj;
              try (JavaBinCodec codec = new JavaBinCodec()) {
                errObj = codec.unmarshal(org.apache.solr.common.util.FastInputStream.wrap(errorIs));
              }
              String msg = extractErrorMessage(errObj);
              if (msg != null) errorMsg = "Request failed with status " + response.getStatus() + ": " + msg;
            } else {
              // Read as text and truncate to 512 chars
              byte[] buf = new byte[512];
              int n = errorIs.read(buf);
              if (n > 0) {
                String body = new String(buf, 0, n, java.nio.charset.StandardCharsets.UTF_8).trim();
                errorMsg = "Request failed with status " + response.getStatus() + ": " + body;
              }
            }
          } catch (Exception parseEx) {
            // ignore, use the generic message
          }
        }
      } finally {
        org.apache.solr.common.util.IOUtils.closeQuietly(errorIs);
        ((SolrHttpRequest)req).freeBuffer();
      }
      throw new SolrServerException(errorMsg);
    }


    InputStream is = null;
    SolrFutureResponseListener listener;
    MutableDirectBuffer expandableBuffer = ExpandableBuffers.getInstance().acquire(-1, true);
    try {
      listener = new SolrFutureResponseListener(req, expandableBuffer);
      req.send(listener);

      ContentResponse res = null; // Timed block
      try {
        // Bound the blocking wait. Prefer the hard total deadline (Request.timeout()) when the caller
        // set one, then the per-request idle timeout, else fall back to 10 minutes. Without this a
        // request on a wedged half-open connection whose idle timeout never fires would block here for
        // the full 10-minute fallback.
        long timeoutMs;
        if (req.getTimeout() > 0) {
          // small grace so Jetty's scheduler aborts the request first and we surface its real cause
          timeoutMs = req.getTimeout() + 1000;
        } else if (req.getIdleTimeout() > 0) {
          timeoutMs = req.getIdleTimeout();
        } else {
          timeoutMs = TimeUnit.MINUTES.toMillis(10);
        }
        res = listener.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new SolrServerException(e);
      } catch (ExecutionException e) {
        throw new SolrServerException(e.getCause());
      } catch (TimeoutException e) {
        throw new SolrServerException(e);
      }


      MutableDirectBuffer buff = listener.getContent();
      try {
        // getContent() sets the backing ByteBuffer's limit to the number of bytes actually
        // received; use that as the stream length. DirectBufferInputStream(buff) alone would
        // read the full buffer capacity (trailing NUL padding), corrupting the response.
        int len = buff == null ? 0 : buff.byteBuffer().remaining();
        is = new DirectBufferInputStream(buff, 0, len);

        return processErrorsAndResponse(req, solrRequest, parser, res, is);
      } finally {

      }
    } finally {
      org.apache.solr.common.util.IOUtils.closeQuietly(is);
      ((SolrHttpRequest)req).freeBuffer();
      // freeBuffer() only releases the request-body buffer; the response buffer was never returned (lost
      // from the pool, extra GC pressure). Released once here after the stream is fully read into the result.
      ExpandableBuffers.getInstance().release(expandableBuffer);
    }
  }

  private static void setBasicAuthHeader(SolrRequest solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String userPass = solrRequest.getBasicAuthUser() + ':' + solrRequest.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(FALLBACK_CHARSET));
      req.headers(httpFields -> httpFields.put("Authorization", "Basic " + encoded));
    }
  }

  public void setBaseUrl(String baseUrl) {
    this.serverBaseUrl = baseUrl;
  }

  public Request makeRequest(SolrRequest solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = createRequest(solrRequest, collection);
    decorateRequest(req, solrRequest);
    return req;
  }

  private void decorateRequest(Request req, SolrRequest solrRequest) {
    req.headers(httpFields -> httpFields.remove(HttpHeader.ACCEPT_ENCODING));

    if (solrRequest.getUserPrincipal() != null) {
      req.attribute(REQ_PRINCIPAL_KEY, solrRequest.getUserPrincipal());
    }
    setBasicAuthHeader(solrRequest, req);
    for (HttpListenerFactory factory : listenerFactory) {
      HttpListenerFactory.RequestResponseListener listener = factory.get();
     // req.onRequestQueued(request -> {
        listener.onQueued(req, solrRequest);
     // });
      req.onRequestBegin(listener);
      req.onComplete(listener);
    }

//    Map<String, String> headers = solrRequest.getHeaders();
//    if (headers != null) {
//      headers.forEach((k, v) -> {
//        log.info("add header {} {}", k, v);
//        req.headers(httpFields -> httpFields.put(k,v));
//      });
//    }

    // Per-request headers carried on the SolrRequest itself (e.g. the OpenTracing span context
    // injected by SolrCmdDistributor / HttpShardHandler, or forwarded client headers). createRequest()
    // only applies the client-level `headers`; without copying these the injected trace context never
    // reaches the wire, so the receiving node starts a brand-new trace (two root /update spans for one
    // update -> TestDistributedTracing).
    Map<String, String> reqHeaders = solrRequest.getHeaders();
    if (reqHeaders != null && !reqHeaders.isEmpty()) {
      req.headers(httpFields -> reqHeaders.forEach(httpFields::put));
    }
  }

  private static String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
    URL oldURL = new URL(basePath);
    String newPath = COMPILE.matcher(oldURL.getPath()).replaceFirst("/api");
    return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
  }

  public boolean isMarkedInternal() {
    return markedInternal;
  }

  private Request createRequest(SolrRequest solrRequest, String collection) throws IOException, SolrServerException {
    if (solrRequest.getBasePath() == null && serverBaseUrl == null)
      throw new IllegalArgumentException("Destination node is not provided!");

    if (solrRequest instanceof V2RequestSupport) {
      solrRequest = ((V2RequestSupport) solrRequest).getV2Request();
    }
    SolrParams params = solrRequest.getParams();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams = contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    String path = RequestWriter.getPath(solrRequest);
    if (path == null) {
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

    //TODO add invariantParams support

    String basePath = solrRequest.getBasePath() == null ? serverBaseUrl : solrRequest.getBasePath();
    if (collection != null) {
      if (solrRequest.getBasePath() == null) {
        basePath += '/' + collection;
      }
    }

    if (solrRequest instanceof V2Request) {
      // A V2Request whose path already carries the full "/api/..." routing prefix (e.g. the package
      // manager's PackageUtils.postFile builds resource "/api/cluster/files/...") is already a
      // complete, server-routable path -- the client must not prepend another V2 prefix. Without this
      // guard the solr.v2RealPath test mode appends "/____v2" and yields "/____v2/api/..." (a path no
      // servlet context matches), so the request 404s before reaching SolrDispatchFilter. Leaving such
      // paths untouched makes the URL base + path == "http://host:port/api/..." which the /api rewrite
      // routes correctly, matching the raw-GET path the package manager already uses elsewhere.
      if (!path.startsWith("/api/")) {
        if (System.getProperty("solr.v2RealPath") == null) {
          basePath = changeV2RequestEndpoint(basePath);
        } else {
          basePath = (solrRequest.getBasePath() == null ? serverBaseUrl  : solrRequest.getBasePath()) + "/____v2";
        }
      }
    }

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      Request req = httpClient.newSolrRequest(basePath + path + wparams.toQueryString(), null).method(HttpMethod.GET);

      req = req.headers(new AddHeaders(headers));

      return req;
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      Request req = httpClient.newSolrRequest(basePath + path + wparams.toQueryString(), null).method(HttpMethod.DELETE);

      req = req.headers(new AddHeaders(headers));

      return req;
    }

    if (SolrRequest.METHOD.POST == solrRequest.getMethod() || SolrRequest.METHOD.PUT == solrRequest.getMethod()) {

      String url = basePath + path;
      boolean hasNullStreamName = false;
      if (streams != null) {
        hasNullStreamName = streams.stream().anyMatch(new ContentStreamPredicate());
      }

      boolean isMultipart = streams != null && streams.size() > 1 && !hasNullStreamName;

      HttpMethod method = SolrRequest.METHOD.POST == solrRequest.getMethod() ? HttpMethod.POST : HttpMethod.PUT;

      if (contentWriter != null) {

        MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
        ExpandableDirectBufferOutputStream outStream = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        contentWriter.write(outStream);
        Request req;
        try {
          req = httpClient.newSolrRequest(url + wparams.toQueryString(), expandableBuffer1).method(method);
        } catch (IllegalArgumentException e) {
          throw new SolrServerException("Illegal url for request url=" + url, e);
        }

        req = req.headers(new AddHeaders(headers));

        //req = req.idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS);



        ByteBuffer buffer = outStream.buffer().byteBuffer().asReadOnlyBuffer();
        buffer.position(outStream.offset() + outStream.buffer().wrapAdjustment());
        buffer.limit( outStream.position() + outStream.buffer().wrapAdjustment());
     //   BufferUtil.flipToFlush(expandableBuffer1.byteBuffer(), pos);
        ByteBufferRequestContent bcp = new ByteBufferRequestContent(contentWriter.getContentType(), buffer);
      //  return req.headers(httpFields -> httpFields.add(HttpHeader.CONTENT_LENGTH, String.valueOf(outStream.position()))).body(bcp);
        return req.headers(httpFields -> httpFields.add(HttpHeader.CONTENT_LENGTH, String.valueOf(outStream.position()))).
            onComplete(result -> ((SolrHttpRequest) result.getRequest()).freeBuffer()).body(bcp);
      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));

        Request req = httpClient
            .newSolrRequest(url + queryParams.toQueryString(), null)
            .method(method);

        req = req.headers(new AddHeaders(headers));

        return fillContentStream(req, streams, wparams, isMultipart);
      } else {
        // It is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();


        MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);

        ExpandableDirectBufferOutputStream outStream = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        contentWriter.write(outStream);

        ByteBuffer buffer = outStream.buffer().byteBuffer().asReadOnlyBuffer();
        buffer.position(outStream.offset() + outStream.buffer().wrapAdjustment());
        buffer.limit( outStream.position() + outStream.buffer().wrapAdjustment());



        InputStream is = contentStream.getStream();

        is.transferTo(outStream);

        ByteBufferRequestContent bcp = new ByteBufferRequestContent(contentStream.getContentType(), buffer);

        Request req = httpClient.newSolrRequest(url + wparams.toQueryString(), expandableBuffer1).method(method).body(bcp)
            .onComplete(result -> ((SolrHttpRequest) result.getRequest()).freeBuffer()).headers(new AddHeaders(headers));

        return req;
      }
    }

    throw new SolrServerException("Unsupported method: " + solrRequest.getMethod());
  }

  private static Request fillContentStream(Request req, Collection<ContentStream> streams, ModifiableSolrParams wparams, boolean isMultipart) throws IOException {
    if (isMultipart) {
      // multipart/form-data
      MultiPartRequestContent content = new MultiPartRequestContent();
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
            contentType = BinaryResponseParser.BINARY_CONTENT_TYPE; // default
          }
          String name = contentStream.getName();
          if (name == null) {
            name = "";
          }
          HttpFields.Mutable fields = HttpFields.build();
          fields.add(HttpHeader.CONTENT_TYPE, contentType);
          Long sz = contentStream.getSize();
//          if (sz != null) {
//            fields.add(HttpHeader.CONTENT_LENGTH, sz.toString());
//          }
          content.addFilePart(name, contentStream.getName(), new InputStreamRequestContent(contentType, contentStream.getStream()), fields);
        }
      }
      content.close();
      req = req.body(content);
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
      SolrFormEncoder provider = new SolrFormEncoder(fields, FALLBACK_CHARSET);
      req.body(provider);
    }

    return req;
  }

  private static boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  @SuppressWarnings({"unchecked", "rawtypes"})
  private NamedList<Object> processErrorsAndResponse(Request req, SolrRequest solrRequest, final ResponseParser processor, Response response, InputStream is) {
    NamedList<Object> rsp;

    int httpStatus = -1;

    if (response != null) {
      try {
        httpStatus = response.getStatus();
      } catch (Exception e) {
        log.warn("", e);
      }
    }

    Throwable failure = response == null ? null : response.getRequest().getAbortCause();
    {
//      if (failure != null && (failure instanceof ClosedChannelException)) {
//        httpClient.getDestinations().forEach(dest1 -> {
//          if (new Origin.Address(dest1.getHost(), dest1.getPort()).equals(new Origin.Address(req.getHost(), req.getPort()))) {
////            ((HttpDestination)dest1).close();
////            try {
////              ((HttpDestination)dest1).stop();
////            } catch (Exception e) {
////              e.printStackTrace();
////            }
//            ((SolrInternalHttpClient) httpClient).removeDestination((HttpDestination) dest1);
//            try {
//              ((HttpDestination)dest1).stop();
//            } catch (Exception e) {
//              e.printStackTrace();
//            }
//            // dest1.getConnectionPool().close();
//           // ((SolrInternalHttpClient) httpClient).getDestinationsMap().values().remove(dest1);
//          }
//
//        });
        // we may get success but be told the peer is shutting down via exception
//        if (solrRequest instanceof UpdateRequest) {
//          NamedList<Object> body = new NamedList<>();
//          body.add("status", "200");
//          body.add("msg", "Success but no response");
//          return body;
//        }

//
//     }
      //        HttpClientTransport transport = httpClient.getTransport();
      //        //  Origin origin = transport.newOrigin((HttpRequest)req);
      //        //
      //        //
      //
      //        ((SolrInternalHttpClient) httpClient).getDestinationsMap().forEach((origin1, dest1) -> {
      //          if (origin1.getAddress().equals(new Origin.Address(req.getHost(), req.getPort()))) {
      //            dest1.close();
      //            dest1.getConnectionPool().close();
      //            ((SolrInternalHttpClient) httpClient).getDestinationsMap().values().remove(dest1);
      //          }
      //
      //        });

      //      HttpDestination dest = ((SolrInternalHttpClient)httpClient).getDestination(origin);
      //      if (dest != null) {
      //      //  ((SolrInternalHttpClient) httpClient).removeDestination(dest);
      //        dest.close();
      //       // httpClient.getTransport().getConnectionPoolFactory().newConnectionPool(dest);
      //      }
      //req.abort(CANCELLED_EXCEPTION);
      // dest.getConnectionPool().close();
      // org.apache.solr.common.util.IOUtils.closeQuietly(is);
      //   throw new RemoteSolrException(req.getHost() + ':' + req.getPort() + "/" + req.getPath(), 0, "Connection has been closed", failure);
      //     }

      String remoteHost = solrRequest.getBasePath();
      if (remoteHost == null) {
        remoteHost = serverBaseUrl;
      }

      //      Throwable requestFailure = response.getRequestFailure();
      //      if (requestFailure != null) {
      //        throw new RemoteSolrException(remoteHost, 0, "Request failure: " + requestFailure.getMessage(), requestFailure);
      //      }

      //    if (response == null) {
      //      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Unknown failure, no response");
      //    }

      boolean isV2Api = isV2ApiRequest(solrRequest);
      String mimeType = null;
      String encoding = null;


      if (response != null) {
        String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);


        if (contentType != null) {
          encoding = StringUtils.isEmpty(contentType) ? null : MimeTypes.getCharsetFromContentType(contentType);
          mimeType = MimeTypes.getContentTypeWithoutCharset(contentType);

        }
      }

      String procCt = processor.getContentType();
      if (procCt != null && mimeType != null) {
        String procMimeType = MimeTypes.getContentTypeWithoutCharset(procCt).trim().toLowerCase(Locale.ROOT);

        if (!procMimeType.equalsIgnoreCase(mimeType)) {
          // unexpected mime type
          String msg = "Expected mime type " + procMimeType + " but got " + mimeType + '.';
          String exceptionEncoding = FALLBACK_CHARSET.name();
          if (is != null) {
            try {
              msg = msg + " text=" + IOUtils.toString(is, exceptionEncoding);
            } catch (Exception e) {
              try {
                throw new RemoteSolrException(remoteHost, response.getStatus(), "Could not parse response with encoding " + exceptionEncoding, e);
              } catch (Exception e1) {
                log.warn("", e1);
              }
            }
          }
          throw new RemoteSolrException(remoteHost, response.getStatus(), msg, null);
        }
      }

      if (httpStatus == 404) {

        throw new RemoteSolrException(remoteHost, httpStatus, "not found: " + httpStatus + ", message:" + response.getReason(), failure);
      }

      if (is == null) {
        throw new RemoteSolrException(remoteHost, response != null ? response.getStatus() : httpStatus,
            response != null && response.getReason() != null ? response.getReason() : String.valueOf(httpStatus), null);
      }

      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        log.warn("Could not parse", e);
        if (response != null) {
          try {
            //
            //          if (response.getStatus() == 200) {
            //            return new NamedList<>();
            //          }

            if (remoteHost == null) {
              remoteHost = serverBaseUrl;
            }
            String txt = IOUtils.toString(is, StandardCharsets.UTF_8);
            log.debug("Could not parse response, status={} txt={}", httpStatus, txt, e);
            throw new RemoteSolrException(remoteHost, httpStatus, txt == null || txt.isEmpty() ? (org.eclipse.jetty.http.HttpStatus.getCode(httpStatus) != null ? org.eclipse.jetty.http.HttpStatus.getCode(httpStatus).getMessage() : String.valueOf(httpStatus)) : txt, failure);
          } catch (Exception e1) {
            log.warn("", e1);
          }
        }

        if (remoteHost == null) {
          remoteHost = serverBaseUrl;
        }
        String txt = null;
        try {
           is.reset();
           txt = IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (Exception ioException) {
          // ignore - we tried to reset and re-read the stream for a better error message
        }
        String msg = "";
        if (httpStatus == 200) {
          msg = "Request was succesful on the server, but we failed parsing the response. ";
        }

        throw new RemoteSolrException(remoteHost, httpStatus, msg + (txt == null || txt.isEmpty() ? (httpStatus > 0 && org.eclipse.jetty.http.HttpStatus.getCode(httpStatus) != null ? org.eclipse.jetty.http.HttpStatus.getCode(httpStatus).getMessage() : String.valueOf(httpStatus)) : txt), e);
      }

      // log.error("rsp:{}", rsp);

      Object error = rsp == null ? null : rsp.get("error");

      Object err = rsp == null ? null : rsp.get("error");
      if (error != null && (isV2Api || String.valueOf(getObjectByPath(err, true, errPath)).endsWith("ExceptionWithErrObject"))) {
        throw RemoteExecutionException.create(remoteHost, rsp);
      }

      if (error instanceof NamedList && ((NamedList<?>) error).get("metadata") == null) {
        throw RemoteExecutionException.create(remoteHost, rsp);
      }

      if (httpStatus != HttpStatus.SC_OK && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          if (error != null) {
            reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
            if (reason == null) {
              reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
            }
            Object metadataObj = Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
            if (metadataObj instanceof NamedList) {
              metadata = (NamedList<String>) metadataObj;
            } else if (metadataObj instanceof List) {
              // NamedList parsed as List convert to NamedList again
              List<Object> list = (List<Object>) metadataObj;
              final int size = list.size();
              metadata = new NamedList<>(size / 2);
              for (int i = 0; i < size; i += 2) {
                metadata.add((String) list.get(i), (String) list.get(i + 1));
              }
            } else if (metadataObj instanceof Map) {
              metadata = new NamedList((Map) metadataObj);
            }
            List details = (ArrayList) Utils.getObjectByPath(error, false, Collections.singletonList("details"));
            if (details != null) {
              reason = reason + ' ' + details;
            }

          }
        } catch (Exception ex) {
          log.warn("Exception parsing error response", ex);
        }
        if (reason == null) {
          String msg = response == null ? null : response.getReason() + "\n\n" + "request: " + response.getRequest().getMethod();
          if (msg != null) {
            reason = URLDecoder.decode(msg, FALLBACK_CHARSET);
          }
        }
        log.error("rsp {}", rsp);

        if (remoteHost == null) {
          remoteHost = serverBaseUrl;
        }

        RemoteSolrException rss = new RemoteSolrException(remoteHost, httpStatus, reason, failure);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }
    }

    return rsp;
  }

  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  private static class MyCancellable implements Cancellable {
    private final Request req;
    private final MyInputStreamResponseListener mysl;

    public MyCancellable(Request req, MyInputStreamResponseListener mysl) {
      this.req = req;
      this.mysl = mysl;
    }

    @Override
    public void cancel() {

      boolean success = req.abort(new CancelledException());
      if (!success) {
        org.apache.solr.common.util.IOUtils.closeQuietly( mysl.getInputStream());
      }
    }

    @Override
    public InputStream getStream() {
      try {
        Response resp = mysl.get(30, TimeUnit.SECONDS);
        if (resp.getStatus() != 200) {
          org.apache.solr.common.util.IOUtils.closeQuietly( mysl.getInputStream());
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Request failed status=" + resp.getStatus());
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
      return mysl.getInputStream();
    }
  }

  private static class AbortRequest implements Cancellable {
    private final Request req;

    public AbortRequest(Request req) {
      this.req = req;
    }

    @Override
    public void cancel() {
      boolean success = req.abort(new CancelledException());
    }
  }

  private static class ContentStreamPredicate implements Predicate<ContentStream> {
    @Override
    public boolean test(ContentStream cs) {
      return cs.getName() == null;
    }
  }

  public abstract static class Abortable {
    public abstract void abort();
  }

  public static class Builder {

    public static int DEFAULT_MAX_THREADS = Integer.getInteger("solr.maxHttp2ClientThreads", 36);
    private static final Integer DEFAULT_IDLE_TIME = Integer.getInteger("solr.http2solrclient.default.idletimeout", 120000);
    public int maxThreadPoolSize = DEFAULT_MAX_THREADS;
    public int maxRequestsQueuedPerDestination = 2048;
    private Http2SolrClient http2SolrClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout = DEFAULT_IDLE_TIME;
    private Integer connectionTimeout;
    private Integer maxConnectionsPerHost = 12;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    protected String baseSolrUrl;
    protected Map<String,String> headers = new Object2ObjectArrayMap(8);
    protected boolean strictEventOrdering = false;
    private Integer maxOutstandingAsyncRequests;
    private boolean markedInternal;

    private String name = "Def";

    public Builder() {

    }

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Reuse {@code httpClient} connections pool
     */
    public Builder withHttpClient(Http2SolrClient httpClient) {
      this.http2SolrClient = httpClient;
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    /**
     * Set maxConnectionsPerHost
     */
    public Builder maxConnectionsPerHost(int max) {
      this.maxConnectionsPerHost = max;
      return this;
    }

    public Builder maxRequestsQueuedPerDestination(int max) {
      this.maxRequestsQueuedPerDestination = max;
      return this;
    }

    public Builder maxThreadPoolSize(int max) {
      this.maxThreadPoolSize = max;
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

    public Builder strictEventOrdering(boolean strictEventOrdering) {
      this.strictEventOrdering = strictEventOrdering;
      return this;
    }

    public Builder connectionTimeout(int connectionTimeOut) {
      this.connectionTimeout = connectionTimeOut;
      return this;
    }

    //do not set this from an external client
    public Builder markInternalRequest() {
      this.headers.put(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
      this.markedInternal = true;
      return this;
    }

    public Builder withBaseUrl(String url) {
      this.baseSolrUrl = url;
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder withHeader(String header, String value) {
      this.headers.put(header, value);
      return this;
    }

    public Builder maxOutstandingAsyncRequests(int maxOutstandingAsyncRequests) {
      this.maxOutstandingAsyncRequests = maxOutstandingAsyncRequests;
      return this;
    }
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method
   *
   * @param queryParams set of param keys to only send via the query string
   *                    Note that the param will be sent as a query string if the key is part
   *                    of this Set or the SolrRequest's query params.
   * @see SolrRequest#getQueryParams
   */
  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }

  private static ModifiableSolrParams calculateQueryParams(Set<String> queryParamNames, ModifiableSolrParams wparams) {
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

  public void setParser(ResponseParser processor) {
    parser = processor;
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
    boolean sslCheckPeerName = checkPeerNameStr != null && !"false".equalsIgnoreCase(checkPeerNameStr);

    SslContextFactory.Client sslContextFactory = new SslContextFactory.Client(!sslCheckPeerName);
    boolean sslset = false;
    if (null != System.getProperty("javax.net.ssl.keyStore")) {
      sslset = true;
      sslContextFactory.setKeyStorePath
          (System.getProperty("javax.net.ssl.keyStore"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
      sslset = true;
      sslContextFactory.setKeyStorePassword
          (System.getProperty("javax.net.ssl.keyStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStore")) {
      sslset = true;
      sslContextFactory.setTrustStorePath
          (System.getProperty("javax.net.ssl.trustStore"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
      sslset = true;
      sslContextFactory.setTrustStorePassword
          (System.getProperty("javax.net.ssl.trustStorePassword"));
    }

    if (!sslset) {
      return null;
    }
    sslContextFactory.setEndpointIdentificationAlgorithm(System.getProperty("solr.jetty.ssl.verifyClientHostName", null));

    return sslContextFactory;
  }

  public static int HEAD(String url, Http2SolrClient httpClient) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response;
    Request req = httpClient.httpClient.newSolrRequest(url, null);
    response = req.method(HEAD).send();
    if (response.getStatus() != 200) {
      throw new RemoteSolrException(url, response.getStatus(), response.getReason(), null);
    }
    return response.getStatus();
  }

  public static class SimpleResponse {
    public String asString;
    public String contentType;
    public int size;
    public int status;
    public byte[] bytes;
    /** Response headers; keys are case-insensitive (HTTP/2 lowercases header names). */
    public Map<String, String> headers = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  }


  public static SimpleResponse DELETE(String url)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doDelete(url, Collections.emptyMap());
  }

  public static SimpleResponse GET(String url)
      throws InterruptedException, ExecutionException, TimeoutException {
    try (Http2SolrClient httpClient = new Http2SolrClient(url, new Builder())) {
      return doGet(url, httpClient, Collections.emptyMap());
    }
  }

  public static SimpleResponse GET(String url, Http2SolrClient httpClient)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, Collections.emptyMap());
  }

  public static SimpleResponse GET(String url, Http2SolrClient httpClient, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, headers);
  }

  public static SimpleResponse POST(String url, ByteBuffer bytes, String contentType, Map headers)
      throws ExecutionException, InterruptedException, TimeoutException {
    try (Http2SolrClient http2SolrClient = new Builder().build()) {
      return POST(url, http2SolrClient, bytes, contentType, headers);
    }
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, headers);
  }

  public static SimpleResponse PUT(String url, Http2SolrClient httpClient, byte[] bytes, String contentType, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doPut(url, httpClient, bytes, contentType, headers);
  }

  private static SimpleResponse doGet(String url, Http2SolrClient httpClient, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    Request req = httpClient.httpClient.newSolrRequest(url, null).method(GET);
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    sResponse.bytes = response.getContent();
    response.getHeaders().forEach(field -> sResponse.headers.put(field.getName(), field.getValue()));
    return sResponse;
  }

  private static SimpleResponse doDelete(String url, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    try (Http2SolrClient http2SolrClient = new Builder().build()) {
      Request req = http2SolrClient.httpClient.newSolrRequest(url, null).method(DELETE);
      return getSimpleResponse(req);
    }
  }

  public String httpDelete(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(URI.create(url)).method(DELETE).send();
    return response.getContentAsString();
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.httpClient.newRequest(url).method(POST).body(new BytesRequestContent(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse doPut(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
      Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.httpClient.newRequest(url).method(PUT).body(new BytesRequestContent(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.httpClient.newRequest(url).method(POST).body(new ByteBufferRequestContent(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse getSimpleResponse(Request req) throws InterruptedException, TimeoutException, ExecutionException {
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }

  public static String httpPut(String url, HttpClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(url).method(PUT).body(new BytesRequestContent(contentType, bytes)).send();
    return response.getContentAsString();
  }

  private static class SolrHttpClientTransportOverHTTP extends HttpClientTransportOverHTTP {
    public SolrHttpClientTransportOverHTTP(int selectors) {
      super(selectors);
    }

    public SolrHttpClientTransportOverHTTP(ClientConnector connector) {
      super(connector);
    }

    public HttpClient getHttpClient() {
      return super.getHttpClient();
    }
  }

  private static class MyInputStreamResponseListener extends SolrInputStreamResponseListener {
    private final AsyncListener<InputStream> asyncListener;
    private final Request request;

    public MyInputStreamResponseListener(HttpClient httpClient, Request request, AsyncListener<InputStream> asyncListener) {
      super((SolrHttpRequest) request);
      this.asyncListener = asyncListener;
      this.request = request;
    }

    //    @Override public void onHeaders(Response response) {
    //      super.onHeaders(response);
    //      if (response.getStatus() == 200) {
    //        ParWork.submitIO("Http2SolrClientAsync", () -> asyncListener.onSuccess(new CloseShieldInputStream(getInputStream()), response.getStatus()));
    //      } else {
    //       // org.apache.solr.common.util.IOUtils.closeQuietly(getInputStream());
    //      }
    //    }

    @Override public void onFailure(Response response, Throwable failure) {
      super.onFailure(response, failure);
      ParWork.submitIO("Http2SolrClientAsync", () -> {
        try {

          if (SolrException.getRootCause(failure) instanceof CancelledException) {
            asyncListener.onFailure(failure, 0, null);
            return;
          }

          asyncListener.onFailure(failure, response.getStatus(), null);

        } catch (Exception e) {
          log.error("Exception in async failure listener", e);
        } finally {
         // InputStream is = getInputStream();
         // org.apache.solr.common.util.IOUtils.closeQuietly(is);
        }
      });

    }
  }

  private static class MyFactory implements ConnectionPool.Factory {

    private final int maxConnectionsPerDest;
    private final int maxQueuedRequestsPerDest;
    private final boolean useHttp1_1;

    MyFactory(int maxConnectionsPerDest, int maxQueuedRequestsPerDest, boolean useHttp1_1) {
      this.maxConnectionsPerDest = maxConnectionsPerDest;
      this.maxQueuedRequestsPerDest = maxQueuedRequestsPerDest;
      this.useHttp1_1 = useHttp1_1;
    }

    @Override
    public ConnectionPool newConnectionPool(HttpDestination destination) {
     Pool pool = new Pool(Pool.StrategyType.THREAD_ID, maxConnectionsPerDest, true);
     pool.setMaxMultiplex(512);
     // RoundRobinConnectionPool thePool = new RoundRobinConnectionPool(destination, maxConnectionsPerDest, destination, maxQueuedRequestsPerDest);
      // MRM TODO: should be configurable
      MultiplexConnectionPool thePool = new MultiplexConnectionPool(destination, pool, destination, maxQueuedRequestsPerDest);
      thePool.setMaxMultiplex(512);
  //    thePool.setMaximizeConnections(true);

    //  mulitplexPool.preCreateConnections(12);
      thePool.preCreateConnections(useHttp1_1 ? 4 : 1);
      return thePool;
    }
  }

  private static class RequestInputStreamResponseListener extends InputStreamResponseListener {
    @Override
    public void onHeaders(Response response)
    {
      super.onHeaders(response);
      log.error("HEADERS: {}", response.getStatus());
    }

    @Override
    public void onFailure(Response response, Throwable failure)
    {
      super.onFailure(response, failure);
      log.error("FAILURE: {}", response.getStatus(), failure);
    }
  }

  private static class AddHeaders implements Consumer<HttpFields.Mutable> {
    private final Map<String,String> headers;

    public AddHeaders(Map<String,String> entry) {
      this.headers = entry;
    }

    @Override
    public void accept(HttpFields.Mutable httpFields) {
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        httpFields.add(entry.getKey(), entry.getValue());
      }
    }
  }

  //    @Override public void onSuccess(Response response) {
//      super.onSuccess(response);
//      log.debug("onSuccess response={}", response);
//      result.set(response);
//      latch.countDown();
//    }
//
//    @Override public void onFailure(Response response, Throwable failure) {
//      super.onFailure(response, failure);
//      log.debug("onFailure response={}", response, failure);
//      result.set(response);
//      latch.countDown();
//    }


}
