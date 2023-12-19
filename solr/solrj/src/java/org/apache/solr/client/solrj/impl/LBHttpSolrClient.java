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
 * HttpSolrClient}. This is useful when you have multiple Solr servers and the requests need to be
 * Load Balanced among them.
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
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient("http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * //or if you wish to pass the HttpClient do as follows
 * httpClient httpClient = new HttpClient();
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient(httpClient, "http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * </pre>
 *
 * </blockquote>

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
    super(builder.baseSolrUrls);
    this.clientIsInternal = builder.httpClient == null;
    this.httpSolrClientBuilder = builder.httpSolrClientBuilder;
    this.httpClient =
        builder.httpClient == null
            ? constructClient(builder.baseSolrUrls.toArray(new String[0]))
            : builder.httpClient;
    this.defaultCollection = builder.defaultCollection;
    if (httpSolrClientBuilder != null && this.defaultCollection != null) {
      httpSolrClientBuilder.defaultCollection = this.defaultCollection;
    }
    this.connectionTimeoutMillis = builder.connectionTimeoutMillis;
    this.soTimeoutMillis = builder.socketTimeoutMillis;
    this.parser = builder.responseParser;
    this.enableZombiePingChecks = builder.enableZombiePingChecks;
    this.zombieCheckIntervalMillis = builder.enableZombiePingChecks? builder.zombiePingIntervalMillis: builder.zombieStateMonitoringIntervalMillis;
    this.minZombieReleaseTimeMillis = builder.minZombieReleaseTimeMillis;
    for (String baseUrl : builder.baseSolrUrls) {
      urlToClient.put(baseUrl, makeSolrClient(baseUrl));
    }
  }

  private HttpClient constructClient(String[] solrServerUrl) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (solrServerUrl != null && solrServerUrl.length > 1) {
      // we prefer retrying another server
      params.set(HttpClientUtil.PROP_USE_RETRY, false);
    } else {
      params.set(HttpClientUtil.PROP_USE_RETRY, true);
    }
    return HttpClientUtil.createClient(params);
  }

  protected HttpSolrClient makeSolrClient(String server) {
    HttpSolrClient client;
    if (httpSolrClientBuilder != null) {
      synchronized (this) {
        httpSolrClientBuilder.withBaseSolrUrl(server).withHttpClient(httpClient);
        httpSolrClientBuilder.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
        httpSolrClientBuilder.withSocketTimeout(soTimeoutMillis, TimeUnit.MILLISECONDS);

        if (requestWriter != null) {
          httpSolrClientBuilder.withRequestWriter(requestWriter);
        }
        if (urlParamNames != null) {
          httpSolrClientBuilder.withTheseParamNamesInTheUrl(urlParamNames);
        }
        client = httpSolrClientBuilder.build();
      }
    } else {
      final HttpSolrClient.Builder clientBuilder =
          new HttpSolrClient.Builder(server).withHttpClient(httpClient).withResponseParser(parser);
      clientBuilder.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
      clientBuilder.withSocketTimeout(soTimeoutMillis, TimeUnit.MILLISECONDS);
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
  protected SolrClient getClient(String baseUrl) {
    SolrClient client = urlToClient.get(baseUrl);
    if (client == null) {
      return makeSolrClient(baseUrl);
    } else {
      return client;
    }
  }

  @Override
  public String removeSolrServer(String server) {
    urlToClient.remove(server);
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
    protected final List<String> baseSolrUrls;
    protected HttpSolrClient.Builder httpSolrClientBuilder;

    //Boolean parameter to make zombie ping checks configurable. If true, zombie ping checks are enabled.
    // If false, zombieServers are monitored to check for servers that have spent at least minZombieReleaseTimeMillis as zombies and release them
    private boolean enableZombiePingChecks;

    //If enableZombiePingChecks = true, configure aliveCheckExecutor thread to run every zombiePingIntervalMillis to ping zombie servers
    private long zombiePingIntervalMillis;

    //If enableZombiePingChecks=false, zombieServers are monitored every zombieStateMonitoringIntervalMillis before releasing them
    private long zombieStateMonitoringIntervalMillis;

    //min time a server is regarded to be in zombie state before being released to the alive set. This param is relevant if enableZombiePingChecks = false
    private long minZombieReleaseTimeMillis;

    public Builder() {
      this.baseSolrUrls = new ArrayList<>();
      this.responseParser = new BinaryResponseParser();
    }

    public HttpSolrClient.Builder getHttpSolrClientBuilder() {
      return httpSolrClientBuilder;
    }

    /**
     * Provide a Solr endpoint to be used when configuring {@link LBHttpSolrClient} instances.
     *
     * <p>Method may be called multiple times. All provided values will be used.
     *
     * <p>Two different paths can be specified as a part of the URL:
     *
     * <p>1) A path pointing directly at a particular core
     *
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrl("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     *
     * Note that when a core is provided in the base URL, queries and other requests can be made
     * without mentioning the core explicitly. However, the client can only send requests to that
     * core.
     *
     * <p>2) The path of the root Solr path ("/solr")
     *
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrl("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     *
     * In this case the client is more flexible and can be used to send requests to any cores. This
     * flexibility though requires that the core is specified on all requests.
     */
    public Builder withBaseSolrUrl(String baseSolrUrl) {
      this.baseSolrUrls.add(baseSolrUrl);
      return this;
    }

    /**
     * Provide Solr endpoints to be used when configuring {@link LBHttpSolrClient} instances.
     *
     * <p>Method may be called multiple times. All provided values will be used.
     *
     * <p>Two different paths can be specified as a part of each URL:
     *
     * <p>1) A path pointing directly at a particular core
     *
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrls("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     *
     * Note that when a core is provided in the base URL, queries and other requests can be made
     * without mentioning the core explicitly. However, the client can only send requests to that
     * core.
     *
     * <p>2) The path of the root Solr path ("/solr")
     *
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrls("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     *
     * In this case the client is more flexible and can be used to send requests to any cores. This
     * flexibility though requires that the core is specified on all requests.
     */
    public Builder withBaseSolrUrls(String... solrUrls) {
      for (String baseSolrUrl : solrUrls) {
        this.baseSolrUrls.add(baseSolrUrl);
      }
      return this;
    }

    /**
     * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
     * interval
     *
     * @param zombiePingIntervalMillis time in milliseconds
     */
    public Builder setZombiePingIntervalMillis(long zombiePingIntervalMillis){
      if(zombiePingIntervalMillis <=0 ){
        throw new IllegalArgumentException(("Zombie check interval must be positive, specified value = " + zombiePingIntervalMillis));
      }
      this.zombiePingIntervalMillis = zombiePingIntervalMillis;
      return this;
    }

    /**
     * LBHttpSolrServer monitors the zombieServers list at fixed interval to see if any of the servers have spent atleast #zombieJalTime as zombies. Use this to set that
     * interval. Note with enableZombieChecks, either zombie checking (ping dead servers at fixed interval) or zombie tracking (monitor zombieServers to check who can be released as zombies, minus the pings) is supported
     *
     * @param zombieStateMonitoringIntervalMillis time in milliseconds a thread would track which servers could be released from zombie state
     */
    public Builder setZombieStateMonitoringIntervalMillis(long zombieStateMonitoringIntervalMillis){
      if(zombieStateMonitoringIntervalMillis <=0 ){
        throw new IllegalArgumentException(("Zombie track interval must be positive, specified value = " + zombieStateMonitoringIntervalMillis));
      }
      this.zombieStateMonitoringIntervalMillis = zombieStateMonitoringIntervalMillis;
      return this;
    }

    /**
     * With this parameter, zombie checking (ping dead servers at fixed interval) or zombie tracking (monitor zombieServers to check who can be released as zombies, minus the pings) is supported
     * @param enableZombiePingChecks If set to true, this would enable zombie ping checks, else only do zombie tracking, thereby holding a server as zombie for atleast zombieJailTime milliseconds
     */
    public Builder setEnableZombiePingChecks(boolean enableZombiePingChecks){
      this.enableZombiePingChecks = enableZombiePingChecks;
      return this;
    }

    /**
     * This param should be set if enableZombieChecks=false. This param configures the time a server should be regarded, at a minimum, as a zombie before being released
     * @param jailTimeInMillis this corresponds to the amount of time in milliseconds a server would be held as zombie if enableZombieChecks is set to false
     */
    public Builder setMinZombieReleaseTimeMillis(long jailTimeInMillis) {
      if( jailTimeInMillis < 0){
        throw new IllegalArgumentException("Jail time should be positive, specified value = " + jailTimeInMillis);
      }
      this.minZombieReleaseTimeMillis = jailTimeInMillis;
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

    /** Create a {@link HttpSolrClient} based on provided configuration. */
    public LBHttpSolrClient build() {
      return new LBHttpSolrClient(this);
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
