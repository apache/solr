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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * <p><b>When to use this?</b><br>
 * This can be used as a software load balancer when you do not wish to set up an external load
 * balancer. Alternatives to this code are to use a dedicated hardware load balancer or using Apache
 * httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @lucene.experimental
 * @since solr 8.0
 */
public class LBHttp2SolrClient extends LBSolrClient {
  private final Http2SolrClient solrClient;

  private LBHttp2SolrClient(Builder builder) {
    super(Arrays.asList(builder.baseSolrUrls));
    this.solrClient = builder.http2SolrClient;
    this.aliveCheckIntervalMillis = builder.aliveCheckIntervalMillis;
    this.defaultCollection = builder.defaultCollection;
  }

  @Override
  protected SolrClient getClient(String baseUrl) {
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
    Rsp rsp = new Rsp();
    boolean isNonRetryable =
        req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    ServerIterator it = new ServerIterator(req, zombieServers);
    asyncListener.onStart();
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicReference<Cancellable> currentCancellable = new AtomicReference<>();
    RetryListener retryListener =
        new RetryListener() {

          @Override
          public void onSuccess(Rsp rsp) {
            asyncListener.onSuccess(rsp);
          }

          @Override
          public void onFailure(Exception e, boolean retryReq) {
            if (retryReq) {
              String url;
              try {
                url = it.nextOrError(e);
              } catch (SolrServerException ex) {
                asyncListener.onFailure(e);
                return;
              }
              try {
                MDC.put("LBSolrClient.url", url);
                synchronized (cancelled) {
                  if (cancelled.get()) {
                    return;
                  }
                  Cancellable cancellable =
                      doRequest(url, req, rsp, isNonRetryable, it.isServingZombieServer(), this);
                  currentCancellable.set(cancellable);
                }
              } finally {
                MDC.remove("LBSolrClient.url");
              }
            } else {
              asyncListener.onFailure(e);
            }
          }
        };
    try {
      Cancellable cancellable =
          doRequest(
              it.nextOrError(),
              req,
              rsp,
              isNonRetryable,
              it.isServingZombieServer(),
              retryListener);
      currentCancellable.set(cancellable);
    } catch (SolrServerException e) {
      asyncListener.onFailure(e);
    }
    return () -> {
      synchronized (cancelled) {
        cancelled.set(true);
        if (currentCancellable.get() != null) {
          currentCancellable.get().cancel();
        }
      }
    };
  }

  private interface RetryListener {
    void onSuccess(Rsp rsp);

    void onFailure(Exception e, boolean retryReq);
  }

  private Cancellable doRequest(
      String baseUrl,
      Req req,
      Rsp rsp,
      boolean isNonRetryable,
      boolean isZombie,
      RetryListener listener) {
    rsp.server = baseUrl;
    req.getRequest().setBasePath(baseUrl);
    return ((Http2SolrClient) getClient(baseUrl))
        .asyncRequest(
            req.getRequest(),
            null,
            new AsyncListener<>() {
              @Override
              public void onSuccess(NamedList<Object> result) {
                rsp.rsp = result;
                if (isZombie) {
                  zombieServers.remove(baseUrl);
                }
                listener.onSuccess(rsp);
              }

              @Override
              public void onFailure(Throwable oe) {
                try {
                  throw (Exception) oe;
                } catch (BaseHttpSolrClient.RemoteExecutionException e) {
                  listener.onFailure(e, false);
                } catch (SolrException e) {
                  // we retry on 404 or 403 or 503 or 500
                  // unless it's an update - then we only retry on connect exception
                  if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
                    listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
                  } else {
                    // Server is alive but the request was likely malformed or invalid
                    if (isZombie) {
                      zombieServers.remove(baseUrl);
                    }
                    listener.onFailure(e, false);
                  }
                } catch (SocketException e) {
                  if (!isNonRetryable || e instanceof ConnectException) {
                    listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
                  } else {
                    listener.onFailure(e, false);
                  }
                } catch (SocketTimeoutException e) {
                  if (!isNonRetryable) {
                    listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
                  } else {
                    listener.onFailure(e, false);
                  }
                } catch (SolrServerException e) {
                  Throwable rootCause = e.getRootCause();
                  if (!isNonRetryable && rootCause instanceof IOException) {
                    listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
                  } else if (isNonRetryable && rootCause instanceof ConnectException) {
                    listener.onFailure((!isZombie) ? addZombie(baseUrl, e) : e, true);
                  } else {
                    listener.onFailure(e, false);
                  }
                } catch (Exception e) {
                  listener.onFailure(new SolrServerException(e), false);
                }
              }
            });
  }

  public static class Builder {

    private final Http2SolrClient http2SolrClient;
    private final String[] baseSolrUrls;


    //Boolean parameter to make zombie ping checks configurable. If true, zombie ping checks are enabled.
    // If false, zombieServers are monitored to check for servers that have spent at least minZombieReleaseTimeMillis as zombies and release them
    private boolean enableZombiePingChecks;

    //min time a server is regarded to be in zombie state before being released to the alive set. This param is relevant if enableZombiePingChecks = false
    private long minZombieReleaseTimeMillis;

    //If enableZombiePingChecks = true, configure aliveCheckExecutor thread to run every zombiePingIntervalMillis to ping zombie servers
    private long zombiePingIntervalMillis =
        TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 min between checks


    //If enableZombiePingChecks=false, zombieServers are monitored every zombieStateMonitoringIntervalMillis before releasing them
    private long zombieStateMonitoringIntervalMillis =
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS); //5 sec between checks


    public Builder(Http2SolrClient http2Client, String... baseSolrUrls) {
      this.http2SolrClient = http2Client;
      this.baseSolrUrls = baseSolrUrls;
    }

    /**
     * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
     * interval
     *
     * @param zombiePingIntervalMillis time in milliseconds
     */
    public LBHttp2SolrClient.Builder setZombiePingIntervalMillis(long zombiePingIntervalMillis){
      if(zombiePingIntervalMillis <=0 ){
        throw new IllegalArgumentException(("Zombie check interval must be positive, specified value = " + zombiePingIntervalMillis));
      }
      this.zombiePingIntervalMillis = zombiePingIntervalMillis;
      return this;
    }

    /**
     * LBHttpSolrServer monitors the zombieServers list at fixed interval to see if any of the servers have spent atleast minZombieReleaseTimeMillis as zombies. Use this to set that
     * interval. Note with enableZombiePingChecks, either zombie checking (ping dead servers at fixed interval) or zombie tracking (monitor zombieServers to check who can be released as zombies, minus the pings) is supported
     *
     * @param zombieStateMonitoringIntervalMillis time in milliseconds
     */
    public LBHttp2SolrClient.Builder setZombieStateMonitoringIntervalMillis(long zombieStateMonitoringIntervalMillis){
      if(zombieStateMonitoringIntervalMillis <=0 ){
        throw new IllegalArgumentException(("Zombie track interval must be positive, specified value = " + zombieStateMonitoringIntervalMillis));
      }
      this.zombieStateMonitoringIntervalMillis = zombieStateMonitoringIntervalMillis;
      return this;
    }


    /**
     * With this parameter, zombie checking (ping dead servers at fixed interval) or zombie tracking (monitor zombieServers to check who can be released as zombies, minus the pings) is supported
     * @param enableZombiePingChecks If set to true, this would enable zombie ping checks, else only do zombie tracking, thereby holding a server as zombie for atleast minZombieReleaseTimeMillis
     */
    public LBHttp2SolrClient.Builder setEnableZombiePingChecks(boolean enableZombiePingChecks){
      this.enableZombiePingChecks = enableZombiePingChecks;
      return this;
    }

    /**
     * This param should be set if enableZombieChecks=false. This param configures the time a server should be regarded, at a minimum, as a zombie before being released
     * @param minZombieReleaseTimeMillis This corresponds to the amount of time in milliseconds a server would be held as zombie if enableZombieChecks is set to false
     */
    public LBHttp2SolrClient.Builder setMinZombieReleaseTimeMillis(long minZombieReleaseTimeMillis) {
      if( minZombieReleaseTimeMillis < 0){
        throw new IllegalArgumentException("Jail time should be positive, specified value = " + minZombieReleaseTimeMillis);
      }
      this.minZombieReleaseTimeMillis = minZombieReleaseTimeMillis;
      return this;
    }

    public LBHttp2SolrClient build() {
      LBHttp2SolrClient solrClient =
          new LBHttp2SolrClient(this.http2SolrClient, Arrays.asList(this.baseSolrUrls));
      solrClient.enableZombiePingChecks = this.enableZombiePingChecks;
      solrClient.zombieCheckIntervalMillis = this.enableZombiePingChecks ? this.zombiePingIntervalMillis: this.zombieStateMonitoringIntervalMillis;
      solrClient.minZombieReleaseTimeMillis = this.minZombieReleaseTimeMillis;
      return solrClient;
      
      
    /** Sets a default collection for collection-based requests. */
    public LBHttp2SolrClient.Builder withDefaultCollection(String defaultCollection) {
      this.defaultCollection = defaultCollection;
      return this;
    }

    }
  }
}
