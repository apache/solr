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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout.SolrZkClientTimeoutAware;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;

/**
 * SolrJ client class to communicate with SolrCloud using Http2SolrClient. Instances of this class
 * communicate with Zookeeper to discover Solr endpoints for SolrCloud collections, and then use the
 * {@link LBHttp2SolrClient} to issue requests.
 *
 * @lucene.experimental
 * @since solr 8.0
 */
@SuppressWarnings("serial")
public class CloudHttp2SolrClient extends CloudSolrClient {

  private final ClusterStateProvider stateProvider;
  private final LBHttp2SolrClient lbClient;
  private final Http2SolrClient myClient;
  private final boolean clientIsInternal;

  /**
   * Create a new client object that connects to Zookeeper and is always aware of the SolrCloud
   * state. If there is a fully redundant Zookeeper quorum and SolrCloud has enough replicas for
   * every shard in a collection, there is no single point of failure. Updates will be sent to shard
   * leaders by default.
   *
   * @param builder a {@link Http2SolrClient.Builder} with the options used to create the client.
   */
  protected CloudHttp2SolrClient(Builder builder) {
    super(builder.shardLeadersOnly, builder.parallelUpdates, builder.directUpdatesToLeadersOnly);
    if (builder.httpClient == null) {
      this.clientIsInternal = true;
      if (builder.internalClientBuilder == null) {
        this.myClient = new Http2SolrClient.Builder().build();
      } else {
        this.myClient = builder.internalClientBuilder.build();
      }
    } else {
      this.clientIsInternal = false;
      this.myClient = builder.httpClient;
    }
    this.retryExpiryTimeNano = builder.retryExpiryTimeNano;
    this.defaultCollection = builder.defaultCollection;
    if (builder.requestWriter != null) {
      this.myClient.requestWriter = builder.requestWriter;
    }
    if (builder.responseParser != null) {
      this.myClient.setParser(builder.responseParser);
    }
    this.stateProvider = builder.stateProvider;

    this.collectionStateCache.timeToLiveMs =
        TimeUnit.MILLISECONDS.convert(builder.timeToLiveSeconds, TimeUnit.SECONDS);

    //  If caches are expired then they are refreshed after acquiring a lock. Set the number of
    // locks.
    this.locks = objectList(builder.parallelCacheRefreshesLocks);

    this.lbClient = new LBHttp2SolrClient.Builder(myClient).build();
  }

  @Override
  public void close() throws IOException {
    stateProvider.close();
    lbClient.close();

    if (clientIsInternal && myClient != null) {
      myClient.close();
    }

    super.close();
  }

  @Override
  public LBHttp2SolrClient getLbClient() {
    return lbClient;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return stateProvider;
  }

  public Http2SolrClient getHttpClient() {
    return myClient;
  }

  @Override
  protected boolean wasCommError(Throwable rootCause) {
    return false;
  }

  /** Constructs {@link CloudHttp2SolrClient} instances from provided configuration. */
  public static class Builder {
    protected Collection<String> zkHosts = new ArrayList<>();
    protected List<String> solrUrls = new ArrayList<>();
    protected String zkChroot;
    protected Http2SolrClient httpClient;
    protected boolean shardLeadersOnly = true;
    protected boolean directUpdatesToLeadersOnly = false;
    protected boolean parallelUpdates = true;
    protected ClusterStateProvider stateProvider;
    protected Http2SolrClient.Builder internalClientBuilder;
    private RequestWriter requestWriter;
    private ResponseParser responseParser;
    private long retryExpiryTimeNano =
        TimeUnit.NANOSECONDS.convert(3, TimeUnit.SECONDS); // 3 seconds or 3 million nanos

    private String defaultCollection;
    private long timeToLiveSeconds = 60;
    private int parallelCacheRefreshesLocks = 3;
    private int zkConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;
    private int zkClientTimeout = SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT;

    /**
     * Provide a series of Solr URLs to be used when configuring {@link CloudHttp2SolrClient}
     * instances. The solr client will use these urls to understand the cluster topology, which solr
     * nodes are active etc.
     *
     * <p>Provided Solr URLs are expected to point to the root Solr path
     * ("http://hostname:8983/solr"); they should not include any collections, cores, or other path
     * components.
     *
     * <p>Usage example:
     *
     * <pre>
     *   final List&lt;String&gt; solrBaseUrls = new ArrayList&lt;String&gt;();
     *   solrBaseUrls.add("http://solr1:8983/solr"); solrBaseUrls.add("http://solr2:8983/solr"); solrBaseUrls.add("http://solr3:8983/solr");
     *   final SolrClient client = new CloudHttp2SolrClient.Builder(solrBaseUrls).build();
     * </pre>
     */
    public Builder(List<String> solrUrls) {
      this.solrUrls = solrUrls;
    }

    /**
     * Provide a series of ZK hosts which will be used when configuring {@link CloudHttp2SolrClient}
     * instances.
     *
     * <p>Usage example when Solr stores data at the ZooKeeper root ('/'):
     *
     * <pre>
     *   final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *   zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *   final SolrClient client = new CloudHttp2SolrClient.Builder(zkServers, Optional.empty()).build();
     * </pre>
     *
     * Usage example when Solr data is stored in a ZooKeeper chroot:
     *
     * <pre>
     *    final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *    zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *    final SolrClient client = new CloudHttp2SolrClient.Builder(zkServers, Optional.of("/solr")).build();
     *  </pre>
     *
     * @param zkHosts a List of at least one ZooKeeper host and port (e.g. "zookeeper1:2181")
     * @param zkChroot the path to the root ZooKeeper node containing Solr data. Provide {@code
     *     java.util.Optional.empty()} if no ZK chroot is used.
     */
    public Builder(List<String> zkHosts, Optional<String> zkChroot) {
      this.zkHosts = zkHosts;
      if (zkChroot.isPresent()) this.zkChroot = zkChroot.get();
    }

    /**
     * Tells {@link Builder} that created clients should be configured such that {@link
     * CloudSolrClient#isUpdatesToLeaders} returns <code>true</code>.
     *
     * @see #sendUpdatesToAnyReplica
     * @see CloudSolrClient#isUpdatesToLeaders
     */
    public Builder sendUpdatesOnlyToShardLeaders() {
      shardLeadersOnly = true;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients should be configured such that {@link
     * CloudSolrClient#isUpdatesToLeaders} returns <code>false</code>.
     *
     * @see #sendUpdatesOnlyToShardLeaders
     * @see CloudSolrClient#isUpdatesToLeaders
     */
    public Builder sendUpdatesToAnyReplica() {
      shardLeadersOnly = false;
      return this;
    }

    /**
     * Tells {@link CloudHttp2SolrClient.Builder} that created clients should send direct updates to
     * shard leaders only.
     *
     * <p>UpdateRequests whose leaders cannot be found will "fail fast" on the client side with a
     * {@link SolrException}
     *
     * @see #sendDirectUpdatesToAnyShardReplica
     * @see CloudSolrClient#isDirectUpdatesToLeadersOnly
     */
    public Builder sendDirectUpdatesToShardLeadersOnly() {
      directUpdatesToLeadersOnly = true;
      return this;
    }

    /**
     * Tells {@link CloudHttp2SolrClient.Builder} that created clients can send updates to any shard
     * replica (shard leaders and non-leaders).
     *
     * <p>Shard leaders are still preferred, but the created clients will fallback to using other
     * replicas if a leader cannot be found.
     *
     * @see #sendDirectUpdatesToShardLeadersOnly
     * @see CloudSolrClient#isDirectUpdatesToLeadersOnly
     */
    public Builder sendDirectUpdatesToAnyShardReplica() {
      directUpdatesToLeadersOnly = false;
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

    /**
     * Tells {@link CloudHttp2SolrClient.Builder} whether created clients should send shard updates
     * serially or in parallel
     *
     * <p>When an {@link UpdateRequest} affects multiple shards, {@link CloudHttp2SolrClient} splits
     * it up and sends a request to each affected shard. This setting chooses whether those
     * sub-requests are sent serially or in parallel.
     *
     * <p>If not set, this defaults to 'true' and sends sub-requests in parallel.
     */
    public Builder withParallelUpdates(boolean parallelUpdates) {
      this.parallelUpdates = parallelUpdates;
      return this;
    }

    /**
     * When caches are expired then they are refreshed after acquiring a lock. Use this to set the
     * number of locks.
     *
     * <p>Defaults to 3.
     *
     * @deprecated Please use {@link #withParallelCacheRefreshes(int)}
     */
    @Deprecated(since = "9.2")
    public Builder setParallelCacheRefreshes(int parallelCacheRefreshesLocks) {
      this.withParallelCacheRefreshes(parallelCacheRefreshesLocks);
      return this;
    }

    /**
     * When caches are expired then they are refreshed after acquiring a lock. Use this to set the
     * number of locks.
     *
     * <p>Defaults to 3.
     */
    public Builder withParallelCacheRefreshes(int parallelCacheRefreshesLocks) {
      this.parallelCacheRefreshesLocks = parallelCacheRefreshesLocks;
      return this;
    }

    /**
     * This is the time to wait to refetch the state after getting the same state version from ZK
     *
     * @deprecated Please use {@link #withRetryExpiryTime(long, TimeUnit)}
     */
    @Deprecated(since = "9.2")
    public Builder setRetryExpiryTime(int secs) {
      this.withRetryExpiryTime(secs, TimeUnit.SECONDS);
      return this;
    }

    /**
     * This is the time to wait to refetch the state after getting the same state version from ZK
     */
    public Builder withRetryExpiryTime(long expiryTime, TimeUnit unit) {
      this.retryExpiryTimeNano = TimeUnit.NANOSECONDS.convert(expiryTime, unit);
      return this;
    }

    /** Sets the default collection for request. */
    public Builder withDefaultCollection(String collection) {
      this.defaultCollection = collection;
      return this;
    }
    /**
     * Sets the cache ttl for DocCollection Objects cached.
     *
     * @param timeToLiveSeconds ttl value in seconds
     * @deprecated Please use {@link #withCollectionCacheTtl(long, TimeUnit)}
     */
    @Deprecated(since = "9.2")
    public Builder withCollectionCacheTtl(int timeToLiveSeconds) {
      withCollectionCacheTtl(timeToLiveSeconds, TimeUnit.SECONDS);
      return this;
    }

    /**
     * Sets the cache ttl for DocCollection Objects cached.
     *
     * @param timeToLive ttl value
     */
    public Builder withCollectionCacheTtl(long timeToLive, TimeUnit unit) {
      assert timeToLive > 0;
      this.timeToLiveSeconds = TimeUnit.SECONDS.convert(timeToLive, unit);
      return this;
    }

    /**
     * Set the internal http client.
     *
     * <p>Note: closing the httpClient instance is at the responsibility of the caller.
     *
     * @param httpClient http client
     * @return this
     */
    public Builder withHttpClient(Http2SolrClient httpClient) {
      if (this.internalClientBuilder != null) {
        throw new IllegalStateException(
            "The builder can't accept an httpClient AND an internalClientBuilder, only one of those can be provided");
      }
      this.httpClient = httpClient;
      return this;
    }

    /**
     * If provided, the CloudHttp2SolrClient will build it's internal Http2SolrClient using this
     * builder (instead of the empty default one). Providing this builder allows users to configure
     * the internal clients (authentication, timeouts, etc).
     *
     * @param internalClientBuilder the builder to use for creating the internal http client.
     * @return this
     */
    public Builder withInternalClientBuilder(Http2SolrClient.Builder internalClientBuilder) {
      if (this.httpClient != null) {
        throw new IllegalStateException(
            "The builder can't accept an httpClient AND an internalClientBuilder, only one of those can be provided");
      }
      this.internalClientBuilder = internalClientBuilder;
      return this;
    }

    /**
     * Sets the Zk connection timeout
     *
     * @param zkConnectTimeout timeout value
     * @param unit time unit
     */
    public Builder withZkConnectTimeout(int zkConnectTimeout, TimeUnit unit) {
      this.zkConnectTimeout = Math.toIntExact(unit.toMillis(zkConnectTimeout));
      return this;
    }

    /**
     * Sets the Zk client session timeout
     *
     * @param zkClientTimeout timeout value
     * @param unit time unit
     */
    public Builder withZkClientTimeout(int zkClientTimeout, TimeUnit unit) {
      this.zkClientTimeout = Math.toIntExact(unit.toMillis(zkClientTimeout));
      return this;
    }

    /** Create a {@link CloudHttp2SolrClient} based on the provided configuration. */
    public CloudHttp2SolrClient build() {
      if (stateProvider == null) {
        if (!zkHosts.isEmpty() && !solrUrls.isEmpty()) {
          throw new IllegalArgumentException(
              "Both zkHost(s) & solrUrl(s) have been specified. Only specify one.");
        } else if (!zkHosts.isEmpty()) {
          stateProvider = ClusterStateProvider.newZkClusterStateProvider(zkHosts, zkChroot);
          if (stateProvider instanceof SolrZkClientTimeoutAware) {
            var timeoutAware = (SolrZkClientTimeoutAware) stateProvider;
            timeoutAware.setZkClientTimeout(zkClientTimeout);
            timeoutAware.setZkConnectTimeout(zkConnectTimeout);
          }
        } else if (!solrUrls.isEmpty()) {
          try {
            stateProvider = new Http2ClusterStateProvider(solrUrls, httpClient);
          } catch (Exception e) {
            throw new RuntimeException(
                "Couldn't initialize a HttpClusterStateProvider (is/are the "
                    + "Solr server(s), "
                    + solrUrls
                    + ", down?)",
                e);
          }
        } else {
          throw new IllegalArgumentException("Both zkHosts and solrUrl cannot be null.");
        }
      }
      return new CloudHttp2SolrClient(this);
    }
  }
}
