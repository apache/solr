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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * LBHttpSolrClient or "LoadBalanced HttpSolrClient" is a load balancing wrapper around {@link
 * HttpSolrClient}. This is useful when you have multiple Solr servers (also called endpoints) and
 * requests need to be Load Balanced among them.
 *
 * <p>Do <b>NOT</b> use this class for indexing in leader/follower scenarios since documents must be
 * sent to the correct leader; no inter-node routing is done.
 *
 * <p>In SolrCloud (leader/replica) scenarios, it is usually better to use {@link CloudSolrClient},
 * but this class may be used for updates because the server will forward them to the appropriate
 * leader.
 *
 * <p>It offers automatic failover when a server goes down and it detects when the server comes back
 * up.
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
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient.Builder()
 *     .withBaseEndpoints("http://host1:8080/solr", "http://host2:8080/solr", "http://host3:8080/solr")
 *     .build();
 * //or if you wish to pass the HttpClient do as follows
 * HttpClient httpClient = new HttpClient();
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient.Builder()
 *     .withBaseEndpoints("http://host1:8080/solr", "http://host2:8080/solr", "http://host3:8080/solr")
 *     .withHttpClient(httpClient)
 *     .build();
 * </pre>
 *
 * </blockquote>
 *
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in
 * a dedicated thread. This interval can be set using {@link
 * LBHttpSolrClient.Builder#setAliveCheckInterval(int)} , the default is set to one minute.
 *
 * <p><b>When to use this?</b><br>
 * This can be used as a software load balancer when you do not wish to setup an external load
 * balancer. Alternatives to this code are to use a dedicated hardware load balancer or using Apache
 * httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @since solr 1.4
 * @deprecated Please use {@link LBHttp2SolrClient}
 */
@Deprecated(since = "9.0")
public class LBHttpSolrClient extends LBSolrClient {

  private final HttpClient httpClient;
  private final boolean clientIsInternal;
  private final ConcurrentHashMap<String, HttpSolrClient> urlToClient = new ConcurrentHashMap<>();
  private final HttpSolrClient.Builder httpSolrClientBuilder;
  private volatile Set<String> urlParamNames = new HashSet<>();

  final int connectionTimeoutMillis;
  final int soTimeoutMillis;

  /** The provided httpClient should use a multi-threaded connection manager */
  protected LBHttpSolrClient(Builder builder) {
    super(builder.solrEndpoints);
    this.clientIsInternal = builder.httpClient == null;
    this.httpSolrClientBuilder = builder.httpSolrClientBuilder;
    this.httpClient =
        builder.httpClient == null ? constructClient(builder.solrEndpoints) : builder.httpClient;
    this.defaultCollection = builder.defaultCollection;
    if (httpSolrClientBuilder != null && this.defaultCollection != null) {
      httpSolrClientBuilder.defaultCollection = this.defaultCollection;
    }
    this.connectionTimeoutMillis = builder.connectionTimeoutMillis;
    this.soTimeoutMillis = builder.socketTimeoutMillis;
    this.parser = builder.responseParser;
    this.aliveCheckIntervalMillis = builder.aliveCheckInterval;
    for (Endpoint endpoint : builder.solrEndpoints) {
      urlToClient.put(endpoint.toString(), makeSolrClient(endpoint));
    }
  }

  private HttpClient constructClient(List<Endpoint> solrEndpoints) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (solrEndpoints != null && solrEndpoints.size() > 1) {
      // we prefer retrying another server
      params.set(HttpClientUtil.PROP_USE_RETRY, false);
    } else {
      params.set(HttpClientUtil.PROP_USE_RETRY, true);
    }
    return HttpClientUtil.createClient(params);
  }

  protected HttpSolrClient makeSolrClient(Endpoint server) {
    HttpSolrClient client;
    if (httpSolrClientBuilder != null) {
      synchronized (this) {
        httpSolrClientBuilder
            .withBaseSolrUrl(server.getBaseUrl())
            .withDefaultCollection(server.getCore())
            .withHttpClient(httpClient)
            .withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
            .withSocketTimeout(soTimeoutMillis, TimeUnit.MILLISECONDS);
        if (defaultCollection != null) {
          httpSolrClientBuilder.withDefaultCollection(defaultCollection);
        }
        if (requestWriter != null) {
          httpSolrClientBuilder.withRequestWriter(requestWriter);
        }
        if (urlParamNames != null) {
          httpSolrClientBuilder.withTheseParamNamesInTheUrl(urlParamNames);
        }
        client = httpSolrClientBuilder.build();
      }
    } else {
      final var clientBuilder =
          new HttpSolrClient.Builder(server.getBaseUrl())
              .withDefaultCollection(server.getCore())
              .withHttpClient(httpClient)
              .withResponseParser(parser)
              .withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
              .withSocketTimeout(soTimeoutMillis, TimeUnit.MILLISECONDS);
      if (defaultCollection != null) {
        clientBuilder.withDefaultCollection(defaultCollection);
      }
      if (requestWriter != null) {
        clientBuilder.withRequestWriter(requestWriter);
      }
      if (urlParamNames != null) {
        clientBuilder.withTheseParamNamesInTheUrl(urlParamNames);
      }
      client = clientBuilder.build();
    }

    return client;
  }

  @Override
  protected SolrClient getClient(Endpoint endpoint) {
    SolrClient client = urlToClient.get(endpoint.toString());
    if (client == null) {
      return makeSolrClient(endpoint);
    } else {
      return client;
    }
  }

  @Override
  public String removeSolrServer(Endpoint server) {
    urlToClient.remove(server.toString());
    return super.removeSolrServer(server);
  }

  @Override
  public void close() {
    super.close();
    if (clientIsInternal) {
      HttpClientUtil.close(httpClient);
    }
  }

  /** Return the HttpClient this instance uses. */
  public HttpClient getHttpClient() {
    return httpClient;
  }

  /** Constructs {@link LBHttpSolrClient} instances from provided configuration. */
  public static class Builder extends SolrClientBuilder<Builder> {

    public static final int CHECK_INTERVAL = 60 * 1000; // 1 minute between checks
    protected final List<Endpoint> solrEndpoints;
    protected HttpSolrClient.Builder httpSolrClientBuilder;
    private int aliveCheckInterval = CHECK_INTERVAL;

    public Builder() {
      this.solrEndpoints = new ArrayList<>();
      this.responseParser = new BinaryResponseParser();
    }

    public HttpSolrClient.Builder getHttpSolrClientBuilder() {
      return httpSolrClientBuilder;
    }

    /**
     * Provide a "base" Solr URL to be used when configuring {@link LBHttpSolrClient} instances.
     *
     * <p>Method may be called multiple times. All provided values will be used. However, all
     * endpoints must be of the same type: providing a mix of"base" endpoints via this method and
     * core/collection endpoints via {@link #withCollectionEndpoint(String, String)} is prohibited.
     *
     * <p>Users who use this method to provide base Solr URLs may specify a "default collection" for
     * their requests using {@link #withDefaultCollection(String)} if they wish to avoid needing to
     * specify a collection or core on relevant requests.
     *
     * @param baseSolrUrl the base URL for a Solr node, in the form "http[s]://hostname:port/solr"
     */
    public Builder withBaseEndpoint(String baseSolrUrl) {
      solrEndpoints.add(new Endpoint(baseSolrUrl));
      return this;
    }

    /**
     * Provide multiple "base" Solr URLs to be used when configuring {@link LBHttpSolrClient}
     * instances.
     *
     * <p>Method may be called multiple times. All provided values will be used. However, all
     * endpoints must be of the same type: providing a mix of"base" endpoints via this method and
     * core/collection endpoints via {@link #withCollectionEndpoint(String, String)} is prohibited.
     *
     * <p>Users who use this method to provide base Solr URLs may specify a "default collection" for
     * their requests using {@link #withDefaultCollection(String)} if they wish to avoid needing to
     * specify a collection or core on relevant requests.
     *
     * @param baseSolrUrls Solr base URLs, in the form "http[s]://hostname:port/solr"
     */
    public Builder withBaseEndpoints(String... baseSolrUrls) {
      for (String baseSolrUrl : baseSolrUrls) {
        solrEndpoints.add(new Endpoint(baseSolrUrl));
      }
      return this;
    }

    /**
     * Provide a core/collection Solr endpoint to be used when configuring {@link LBHttpSolrClient}
     * instances.
     *
     * <p>Method may be called multiple times. All provided values will be used. However, all
     * endpoints must be of the same type: providing a mix of "core" endpoints via this method and
     * base endpoints via {@link #withBaseEndpoint(String)} is prohibited.
     *
     * @param baseSolrUrl the base URL for a Solr node, in the form "http[s]://hostname:port/solr"
     * @param core the Solr core or collection to target
     */
    public Builder withCollectionEndpoint(String baseSolrUrl, String core) {
      solrEndpoints.add(new Endpoint(baseSolrUrl, core));
      return this;
    }

    /**
     * Provide multiple core/collection endpoints to be used when configuring {@link
     * LBHttpSolrClient} instances.
     *
     * <p>Method may be called multiple times. All provided values will be used. However, all
     * endpoints must be of the same type: providing a mix of "core" endpoints via this method and
     * base endpoints via {@link #withBaseEndpoint(String)} is prohibited.
     *
     * @param endpoints endpoint instances pointing to distinct cores/collections
     */
    public Builder withCollectionEndpoints(Endpoint... endpoints) {
      if (endpoints != null) {
        for (Endpoint e : endpoints) {
          solrEndpoints.add(e);
        }
      }

      return this;
    }

    /**
     * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use
     * this to set that interval
     *
     * @param aliveCheckInterval time in milliseconds
     */
    public Builder setAliveCheckInterval(int aliveCheckInterval) {
      if (aliveCheckInterval <= 0) {
        throw new IllegalArgumentException(
            "Alive check interval must be " + "positive, specified value = " + aliveCheckInterval);
      }
      this.aliveCheckInterval = aliveCheckInterval;
      return this;
    }

    /**
     * Provides a {@link HttpSolrClient.Builder} to be used for building the internally used
     * clients.
     */
    public Builder withHttpSolrClientBuilder(HttpSolrClient.Builder builder) {
      this.httpSolrClientBuilder = builder;
      return this;
    }

    /** Create a {@link LBHttpSolrClient} based on provided configuration. */
    public LBHttpSolrClient build() {
      return new LBHttpSolrClient(this);
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
