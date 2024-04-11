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

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.slf4j.MDC;

/**
 * LBHttp2SolrClient or "LoadBalanced LBHttp2SolrClient" is a load balancing wrapper around {@link
 * Http2SolrClient}. This is useful when you have multiple Solr endpoints and requests need to be
 * Load Balanced among them.
 *
 * <p>Do <b>NOT</b> use this class for indexing in leader/follower scenarios since documents must be
 * sent to the correct leader; no inter-node routing is done.
 *
 * <p>In SolrCloud (leader/replica) scenarios, it is usually better to use {@link CloudSolrClient},
 * but this class may be used for updates because the server will forward them to the appropriate
 * leader.
 *
 * <p>It offers automatic failover when a server goes down, and it detects when the server comes
 * back up.
 *
 * <p>Load balancing is done using a simple round-robin on the list of endpoints. Endpoint URLs are
 * expected to point to the Solr "root" path (i.e. "/solr").
 *
 * <blockquote>
 *
 * <pre>
 * SolrClient client = new LBHttp2SolrClient.Builder(http2SolrClient,
 *         new LBSolrClient.Endpoint("http://host1:8080/solr"), new LBSolrClient.Endpoint("http://host2:8080/solr"))
 *     .build();
 * </pre>
 *
 * </blockquote>
 *
 * Users who wish to balance traffic across a specific set of replicas or cores may specify each
 * endpoint as a root-URL and core-name pair. For example:
 *
 * <blockquote>
 *
 * <pre>
 * SolrClient client = new LBHttp2SolrClient.Builder(http2SolrClient,
 *         new LBSolrClient.Endpoint("http://host1:8080/solr", "coreA"),
 *         new LBSolrClient.Endpoint("http://host2:8080/solr", "coreB"))
 *     .build();
 * </pre>
 *
 * </blockquote>
 *
 * <p>If a request to an endpoint fails by an IOException due to a connection timeout or read
 * timeout then the host is taken off the list of live endpoints and moved to a 'dead endpoint list'
 * and the request is resent to the next live endpoint. This process is continued till it tries all
 * the live endpoints. If at least one endpoint is alive, the request succeeds, and if not it fails.
 *
 * <p>Dead endpoints are periodically healthchecked on a fixed interval controlled by {@link
 * LBHttp2SolrClient.Builder#setAliveCheckInterval(int, TimeUnit)}. The default is set to one
 * minute.
 *
 * <p><b>When to use this?</b><br>
 * This can be used as a software load balancer when you do not wish to set up an external load
 * balancer. Alternatives to this code are to use a dedicated hardware load balancer or using Apache
 * httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @since solr 8.0
 */
public class LBHttp2SolrClient extends LBSolrClient {
  private final Http2SolrClient solrClient;

  private LBHttp2SolrClient(Builder builder) {
    super(Arrays.asList(builder.solrEndpoints));
    this.solrClient = builder.http2SolrClient;
    this.aliveCheckIntervalMillis = builder.aliveCheckIntervalMillis;
    this.defaultCollection = builder.defaultCollection;
  }

  @Override
  protected SolrClient getClient(Endpoint endpoint) {
    return solrClient;
  }

  @Override
  public ResponseParser getParser() {
    return solrClient.getParser();
  }

  @Override
  public RequestWriter getRequestWriter() {
    return solrClient.getRequestWriter();
  }

  public Set<String> getUrlParamNames() {
    return solrClient.getUrlParamNames();
  }

  public Cancellable asyncReq(Req req, AsyncListener<Rsp> asyncListener) {
    asyncListener.onStart();
    CompletableFuture<Rsp> cf =
        requestAsync(req)
            .whenComplete(
                (rsp, t) -> {
                  if (t != null) {
                    asyncListener.onFailure(t);
                  } else {
                    asyncListener.onSuccess(rsp);
                  }
                });
    return () -> cf.cancel(true);
  }

  public CompletableFuture<Rsp> requestAsync(Req req) {
    CompletableFuture<Rsp> apiFuture = new CompletableFuture<>();
    Rsp rsp = new Rsp();
    boolean isNonRetryable =
        req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    EndpointIterator it = new EndpointIterator(req, zombieServers);
    AtomicReference<CompletableFuture<NamedList<Object>>> currentFuture = new AtomicReference<>();
    RetryListener retryListener =
        new RetryListener() {

          @Override
          public void onSuccess(Rsp rsp) {
            apiFuture.complete(rsp);
          }

          @Override
          public void onFailure(Exception e, boolean retryReq) {
            if (retryReq) {
              Endpoint url;
              try {
                url = it.nextOrError(e);
              } catch (SolrServerException ex) {
                apiFuture.completeExceptionally(e);
                return;
              }
              MDC.put("LBSolrClient.url", url.toString());
              if (!apiFuture.isCancelled()) {
                CompletableFuture<NamedList<Object>> future =
                    doAsyncRequest(url, req, rsp, isNonRetryable, it.isServingZombieServer(), this);
                currentFuture.set(future);
              }
            } else {
              apiFuture.completeExceptionally(e);
            }
          }
        };
    try {
      CompletableFuture<NamedList<Object>> future =
          doAsyncRequest(
              it.nextOrError(),
              req,
              rsp,
              isNonRetryable,
              it.isServingZombieServer(),
              retryListener);
      currentFuture.set(future);
    } catch (SolrServerException e) {
      apiFuture.completeExceptionally(e);
      return apiFuture;
    }
    apiFuture.exceptionally(
        (error) -> {
          if (apiFuture.isCancelled()) {
            currentFuture.get().cancel(true);
          }
          return null;
        });
    return apiFuture;
  }

  private interface RetryListener {
    void onSuccess(Rsp rsp);

    void onFailure(Exception e, boolean retryReq);
  }

  private CompletableFuture<NamedList<Object>> doAsyncRequest(
      Endpoint endpoint,
      Req req,
      Rsp rsp,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    String baseUrl = endpoint.toString();
    rsp.server = baseUrl;
    req.getRequest().setBasePath(baseUrl);
    CompletableFuture<NamedList<Object>> future =
        getClient(endpoint).requestAsync(req.getRequest());
    future.whenComplete(
        (result, throwable) -> {
          if (!future.isCompletedExceptionally()) {
            onSuccessfulRequest(result, endpoint, rsp, isZombie, listener);
          } else if (!future.isCancelled()) {
            onFailedRequest(throwable, endpoint, isNonRetryable, isZombie, listener);
          }
        });
    return future;
  }

  private void onSuccessfulRequest(
      NamedList<Object> result,
      Endpoint endpoint,
      Rsp rsp,
      boolean isZombie,
      RetryListener listener) {
    rsp.rsp = result;
    if (isZombie) {
      zombieServers.remove(endpoint);
    }
    listener.onSuccess(rsp);
  }

  private void onFailedRequest(
      Throwable oe,
      Endpoint endpoint,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    try {
      throw (Exception) oe;
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      listener.onFailure(e, false);
    } catch (SolrException e) {
      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        listener.onFailure((!isZombie) ? addZombie(endpoint, e) : e, true);
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          zombieServers.remove(endpoint);
        }
        listener.onFailure(e, false);
      }
    } catch (SocketException e) {
      if (!isNonRetryable || e instanceof ConnectException) {
        listener.onFailure((!isZombie) ? addZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (SocketTimeoutException e) {
      if (!isNonRetryable) {
        listener.onFailure((!isZombie) ? addZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (SolrServerException e) {
      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        listener.onFailure((!isZombie) ? addZombie(endpoint, e) : e, true);
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        listener.onFailure((!isZombie) ? addZombie(endpoint, e) : e, true);
      } else {
        listener.onFailure(e, false);
      }
    } catch (Exception e) {
      listener.onFailure(new SolrServerException(e), false);
    }
  }

  public static class Builder {

    private final Http2SolrClient http2SolrClient;
    private final Endpoint[] solrEndpoints;
    private long aliveCheckIntervalMillis =
        TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 minute between checks
    protected String defaultCollection;

    public Builder(Http2SolrClient http2Client, Endpoint... endpoints) {
      this.http2SolrClient = http2Client;
      this.solrEndpoints = endpoints;
    }

    /**
     * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use
     * this to set that interval
     *
     * @param aliveCheckInterval how often to ping for aliveness
     */
    public LBHttp2SolrClient.Builder setAliveCheckInterval(int aliveCheckInterval, TimeUnit unit) {
      if (aliveCheckInterval <= 0) {
        throw new IllegalArgumentException(
            "Alive check interval must be " + "positive, specified value = " + aliveCheckInterval);
      }
      this.aliveCheckIntervalMillis = TimeUnit.MILLISECONDS.convert(aliveCheckInterval, unit);
      return this;
    }

    /** Sets a default for core or collection based requests. */
    public LBHttp2SolrClient.Builder withDefaultCollection(String defaultCoreOrCollection) {
      this.defaultCollection = defaultCoreOrCollection;
      return this;
    }

    public LBHttp2SolrClient build() {
      return new LBHttp2SolrClient(this);
    }
  }
}
