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
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SynchronizedNamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;
/**
 * LBHttp2SolrClient or "LoadBalanced LBHttp2SolrClient" is a load balancing wrapper around
 * {@link Http2SolrClient}. This is useful when you
 * have multiple Solr servers and the requests need to be Load Balanced among them.
 *
 * Do <b>NOT</b> use this class for indexing in master/slave scenarios since documents must be sent to the
 * correct master; no inter-node routing is done.
 *
 * In SolrCloud (leader/replica) scenarios, it is usually better to use
 * {@link CloudHttp2SolrClient}, but this class may be used
 * for updates because the server will forward them to the appropriate leader.
 *
 * <p>
 * It offers automatic failover when a server goes down and it detects when the server comes back up.
 * <p>
 * Load balancing is done using a simple round-robin on the list of servers.
 * <p>
 * If a request to a server fails by an IOException due to a connection timeout or read timeout then the host is taken
 * off the list of live servers and moved to a 'dead server list' and the request is resent to the next live server.
 * This process is continued till it tries all the live servers. If at least one server is alive, the request succeeds,
 * and if not it fails.
 * <blockquote><pre>
 * SolrClient lbHttp2SolrClient = new LBHttp2SolrClient(http2SolrClient, "http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * </pre></blockquote>
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in a dedicated thread.
 * This interval can be set using {@link #setAliveCheckInterval} , the default is set to one minute.
 * <p>
 * <b>When to use this?</b><br> This can be used as a software load balancer when you do not wish to setup an external
 * load balancer. Alternatives to this code are to use
 * a dedicated hardware load balancer or using Apache httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @lucene.experimental
 * @since solr 8.0
 */
public class LBHttp2SolrClient extends LBSolrClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Http2SolrClient httpClient;

  public LBHttp2SolrClient(Http2SolrClient httpClient, String... baseSolrUrls) {
    this(httpClient, httpClient == null ? false : httpClient.isMarkedInternal(), baseSolrUrls);
  }

  public LBHttp2SolrClient(Http2SolrClient httpClient, boolean markInternal, String... baseSolrUrls) {
    super(new ObjectArrayList<>(baseSolrUrls));
    // MRM TODO: - should only be internal for us
//    Http2SolrClient.Builder builder = new Http2SolrClient.Builder().withHttpClient(httpClient);
//    if (markInternal) {
//      builder = builder.maxOutstandingAsyncRequests(300).markInternalRequest(); // MRM TODO configurable
//    }
    this.httpClient = httpClient;
  }

  public LBHttp2SolrClient(String... baseSolrUrls) {
    super(new ObjectArrayList<>(baseSolrUrls) {
    });
    // MRM TODO: - should only be internal for us
    Http2SolrClient.Builder builder = new Http2SolrClient.Builder();
    this.httpClient = builder.build();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  protected SolrClient getClient(String baseUrl) {
    return httpClient;
  }

  public Cancellable asyncReq(Req req, AsyncListener<Rsp> asyncListener) {
    Rsp rsp = new Rsp();
    boolean isNonRetryable = req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    ServerIterator it = new ServerIterator(req, zombieServers);
    asyncListener.onStart();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicReference<Cancellable> currentCancellable = new AtomicReference<>();
    RetryListener retryListener = new RetryListener() {

      @Override
      public void onSuccess(Rsp rsp) {
        asyncListener.onSuccess(rsp, 200, req);
      }

      @Override
      public void onFailure(Exception e, boolean retryReq) {
        if (retryReq) {
          String url;
          try {
            url = it.nextOrError(e);
          } catch (Exception ex) {
            req.retryCount.incrementAndGet();
            asyncListener.onFailure(e, 500, req);
            return;
          }
          try {
            MDC.put("LBSolrClient.url", url);
            req.retryCount.incrementAndGet();
            Cancellable cancellable = doRequest(url, req, rsp, isNonRetryable, it.isServingZombieServer(), this);
            currentCancellable.set(cancellable);

          } finally {
            MDC.remove("LBSolrClient.url");
          }
        } else {
          int code = -1;
          if (e instanceof SolrException) {
            code = ((SolrException) e).code();
          }
          req.retryCount.incrementAndGet();
          asyncListener.onFailure(e, code, req);
        }
      }
    };
    try {
      Cancellable cancellable = doRequest(it.nextOrError(), req, rsp, isNonRetryable, it.isServingZombieServer(), retryListener);
      currentCancellable.set(cancellable);
    } catch (SolrServerException e) {
      asyncListener.onFailure(e, 500, req);
    }
    return () -> {

      cancelled.set(true);
      if (currentCancellable.get() != null) {
        currentCancellable.get().cancel();
      }

    };
  }

  private interface RetryListener {
    void onSuccess(Rsp rsp);
    void onFailure(Exception e, boolean retryRetry);
  }

  private Cancellable doRequest(String baseUrl, Req req, Rsp rsp, boolean isNonRetryable,
                         boolean isZombie, RetryListener listener) {
    rsp.server = baseUrl;
    req.getRequest().setBasePath(baseUrl);

    return ((Http2SolrClient) getClient(baseUrl)).asyncRequest(req.getRequest(), null, new AsyncListener<>() {
      @Override public void onSuccess(NamedList<Object> result, int statusCode, Object context) {
        rsp.rsp = new SynchronizedNamedList<>(result);
        if (isZombie) {
          zombieServers.remove(baseUrl);
        }
        listener.onSuccess(rsp);
      }

      @Override public void onFailure(Throwable oe, int code, Object context) {
        try {

          if (code == 200) {
            log.warn("Request success, but exception", oe);
            if (isZombie) {
              zombieServers.remove(baseUrl);
            }
            listener.onSuccess(rsp);
            return;
          }

          throw (Exception) oe;
        } catch (BaseHttpSolrClient.RemoteExecutionException e) {
          req.retryCount.incrementAndGet();
          listener.onFailure(e, false);
        } catch (SolrException e) {
          // we retry on 404 or 403 or 502, 503 or 500
          // unless it's an update - then we only retry on connect exception
          if (!isNonRetryable && (RETRY_CODES.contains(e.code())) || e.getMessage().contains("Connection refused")) {
            req.retryCount.incrementAndGet();
            listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
          } else {
            // Server is alive but the request was likely malformed or invalid
            if (isZombie) {
              zombieServers.remove(baseUrl);
            }
            req.retryCount.incrementAndGet();
            listener.onFailure(e, false);
          }
        } catch (SocketException e) {
          req.retryCount.incrementAndGet();
          listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
        } catch (SocketTimeoutException e) {
          if (!isNonRetryable) {
            req.retryCount.incrementAndGet();
            listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
          } else {
            req.retryCount.incrementAndGet();
            listener.onFailure(e, false);
          }
        } catch (SolrServerException e) {
          Throwable rootCause = e.getRootCause();
          if (!isNonRetryable && (rootCause instanceof IOException)) {
            listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
          } else if (isNonRetryable && rootCause instanceof ConnectException) {
            req.retryCount.incrementAndGet();
            listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
          } else {
            req.retryCount.incrementAndGet();
            listener.onFailure(e, false);
          }
        } catch (Exception e) {
          req.retryCount.incrementAndGet();
          listener.onFailure(new SolrServerException(e), false);
        }

      }
    });
  }
}
