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
import java.util.HashSet;
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
 * Http2SolrClient}. This is useful when you have multiple Solr servers and the requests need to be
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
 * <p>Load balancing is done using a simple round-robin on the list of servers.
 *
 * <p>If a request to a server fails by an IOException due to a connection timeout or read timeout
 * then the host is taken off the list of live servers and moved to a 'dead server list' and the
 * request is resent to the next live server. This process is continued till it tries all the live
 * servers. If at least one server is alive, the request succeeds, and if not it fails.
 *
 * <blockquote>
 *
 * <pre>
 * SolrClient lbHttp2SolrClient = new LBHttp2SolrClient(http2SolrClient, "http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * </pre>
 *
 * </blockquote>
 *
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in
 * a dedicated thread. This interval can be set using {@link
 * LBHttp2SolrClient.Builder#setAliveCheckInterval(int)} , the default is set to one minute.
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

  /**
   * @deprecated Use {@link LBHttp2SolrClient.Builder} instead
   */
  @Deprecated
  public LBHttp2SolrClient(Http2SolrClient solrClient, String... baseSolrUrls) {
    super(Arrays.asList(baseSolrUrls));
    this.solrClient = solrClient;
  }

  private LBHttp2SolrClient(Builder builder) {
    super(Arrays.asList(builder.baseSolrUrls));
    this.solrClient = builder.http2SolrClient;
    this.aliveCheckIntervalMillis = builder.aliveCheckIntervalMillis;
    this.defaultCollection = builder.defaultCollection;
  }

  /**
   * @deprecated Use {@link #getClient(Endpoint)} instead.
   */
  @Deprecated
  @Override
  protected SolrClient getClient(String baseUrl) {
    return solrClient;
  }

  @Override
  protected SolrClient getClient(Endpoint endpoint) {
    return solrClient;
  }

  /**
   * Note: This setter method is <b>not thread-safe</b>.
   *
   * @param parser Default Response Parser chosen to parse the response if the parser were not
   *     specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   * @deprecated Pass in a configured {@link Http2SolrClient} instead
   */
  @Deprecated
  @Override
  public void setParser(ResponseParser parser) {
    super.setParser(parser);
    this.solrClient.setParser(parser);
  }

  @Override
  public ResponseParser getParser() {
    return solrClient.getParser();
  }

  /**
   * Choose the {@link RequestWriter} to use.
   *
   * <p>By default, {@link BinaryRequestWriter} is used.
   *
   * <p>Note: This setter method is <b>not thread-safe</b>.
   *
   * @deprecated Pass in a configured {@link Http2SolrClient} instead
   */
  @Deprecated
  @Override
  public void setRequestWriter(RequestWriter writer) {
    super.setRequestWriter(writer);
    this.solrClient.setRequestWriter(writer);
  }

  @Override
  public RequestWriter getRequestWriter() {
    return solrClient.getRequestWriter();
  }

  public Set<String> getUrlParamNames() {
    return solrClient.getUrlParamNames();
  }

  /**
   * @deprecated You should instead set this on the passed in Http2SolrClient used by the Builder.
   */
  @Deprecated
  public void setQueryParams(Set<String> queryParams) {
    this.solrClient.setUrlParamNames(queryParams);
  }

  /**
   * This method should be removed as being able to add a query parameter isn't compatible with the
   * idea that query params are an immutable property of a solr client.
   *
   * @deprecated you should instead set this on the passed in Http2SolrClient used by the Builder.
   */
  @Deprecated
  public void addQueryParams(String queryOnlyParam) {
    Set<String> urlParamNames = new HashSet<>(this.solrClient.getUrlParamNames());
    urlParamNames.add(queryOnlyParam);
    this.solrClient.setUrlParamNames(urlParamNames);
  }

  /**
   * Execute an asynchronous request against a one or more hosts for a given collection.
   *
   * @param req the wrapped request to perform
   * @param asyncListener callers should provide an implementation to handle events: start, success,
   *     exception
   * @return Cancellable allowing the caller to attempt cancellation
   * @deprecated Use {@link #requestAsync(Req)}.
   */
  @Deprecated
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

  /**
   * Execute an asynchronous request against one or more hosts for a given collection. The passed-in
   * Req object includes a List of Endpoints. This method always begins with the first Endpoint in
   * the list and if unsuccessful tries each in turn until the request is successful. Consequently,
   * this method does not actually Load Balance. It is up to the caller to shuffle the List of
   * Endpoints if Load Balancing is desired.
   *
   * @param req the wrapped request to perform
   * @return a {@link CompletableFuture} that tracks the progress of the async request.
   */
  public CompletableFuture<Rsp> requestAsync(Req req) {
    CompletableFuture<Rsp> apiFuture = new CompletableFuture<>();
    Rsp rsp = new Rsp();
    boolean isNonRetryable =
        req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    ServerIterator it = new ServerIterator(req, zombieServers);
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
              String url;
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
      String endpoint,
      Req req,
      Rsp rsp,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    String baseUrl = endpoint.toString();
    rsp.server = baseUrl;
    req.getRequest().setBasePath(baseUrl);
    CompletableFuture<NamedList<Object>> future =
        ((Http2SolrClient) getClient(endpoint)).requestAsync(req.getRequest());
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
      String endpoint,
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
      String endpoint,
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
    private final String[] baseSolrUrls;
    private long aliveCheckIntervalMillis =
        TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 minute between checks
    protected String defaultCollection;

    /**
     * Create a Builder object, based on the provided solrClient and Solr URLs.
     *
     * <p>Two different paths can be specified as a part of the provided URLs:
     *
     * <p>1) A path pointing directly at a particular core
     *
     * <pre>
     *   SolrClient client = new LBHttp2SolrClient.Builder(client, "http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     *
     * Note that when a core is provided in the base URL, queries and other requests can be made
     * without mentioning the core explicitly. However, the client can only send requests to that
     * core. Attempts to make core-agnostic requests, or requests for other cores will fail.
     *
     * <p>Use of these core-based URLs is deprecated and will not be supported in Solr 10.0 Users
     * should instead provide base URLs as described below, and provide a "default collection" as
     * desired using {@link #withDefaultCollection(String)}
     *
     * <p>2) The path of the root Solr path ("/solr")
     *
     * <pre>
     *   SolrClient client = new LBHttp2SolrClient.Builder(client, "http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     *
     * In this case the client is more flexible and can be used to send requests to any cores. Users
     * can still provide a "default" collection if desired through use of {@link
     * #withDefaultCollection(String)}.
     *
     * @deprecated use {@link #Builder(Http2SolrClient, Endpoint...)} instead
     */
    @Deprecated
    public Builder(Http2SolrClient http2Client, String... baseSolrUrls) {
      this.http2SolrClient = http2Client;
      this.baseSolrUrls = baseSolrUrls;
    }

    /**
     * Create a Builder object, based on the provided solrClient and endpoint objects.
     *
     * <p>Endpoint instances come in two main flavors:
     *
     * <p>1) Endpoints representing a particular core or collection
     *
     * <pre>
     *   SolrClient client = new LBHttp2SolrClient.Builder(
     *           client, new LBSolrClient.Endpoint("http://my-solr-server:8983/solr", "core1"))
     *       .build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     *
     * Note that when a core is provided in the endpoint, queries and other requests can be made
     * without mentioning the core explicitly. However, the client can only send requests to that
     * core. Attempts to make core-agnostic requests, or requests for other cores will fail.
     *
     * <p>2) Endpoints representing the root Solr path (i.e. "/solr")
     *
     * <pre>
     *   SolrClient client = new LBHttp2SolrClient.Builder(
     *           client, new LBSolrClient.Endpoint("http://my-solr-server:8983/solr"))
     *       .build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     *
     * In this case the client is more flexible and can be used to send requests to any cores. Users
     * can still provide a "default" collection if desired through use of {@link
     * #withDefaultCollection(String)}.
     */
    public Builder(Http2SolrClient http2Client, Endpoint... endpoints) {
      this.http2SolrClient = http2Client;

      this.baseSolrUrls = new String[endpoints.length];
      for (int i = 0; i < endpoints.length; i++) {
        this.baseSolrUrls[i] = endpoints[i].getUrl();
      }
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

    /**
     * Sets a default for core or collection based requests.
     *
     * <p>This method should not be used if the client is provided a Solr URL which already contains
     * a core or collection name.
     */
    public LBHttp2SolrClient.Builder withDefaultCollection(String defaultCoreOrCollection) {
      this.defaultCollection = defaultCoreOrCollection;
      return this;
    }

    public LBHttp2SolrClient build() {
      return new LBHttp2SolrClient(this);
    }
  }
}
