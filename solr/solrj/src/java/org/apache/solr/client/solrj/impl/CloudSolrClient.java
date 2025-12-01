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

import static org.apache.solr.common.params.CommonParams.ID;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.ResponseParser;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A {@link SolrClient} that routes requests to ideal nodes, including splitting update batches to
 * the correct shards. It uses {@link LBSolrClient} as well, thus offering fail-over abilities if a
 * core or node becomes unavailable. It's able to know where to route requests due to its knowledge
 * of the SolrCloud "cluster state".
 */
public abstract class CloudSolrClient extends SolrClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // no of times collection state to be reloaded if stale state error is received
  private static final int MAX_STALE_RETRIES =
      Integer.parseInt(System.getProperty("solr.solrj.cloud.max.stale.retries", "5"));
  private final Random rand = new Random();

  private final boolean updatesToLeaders;
  private final boolean directUpdatesToLeadersOnly;
  private final RequestReplicaListTransformerGenerator requestRLTGenerator;
  private final boolean parallelUpdates;
  private ExecutorService threadPool =
      ExecutorUtil.newMDCAwareCachedThreadPool(
          new SolrNamedThreadFactory("CloudSolrClient ThreadPool"));

  public static final String STATE_VERSION = "_stateVer_";
  protected long retryExpiryTimeNano =
      TimeUnit.NANOSECONDS.convert(3, TimeUnit.SECONDS); // 3 seconds or 3 million nanos
  private static final Set<String> NON_ROUTABLE_PARAMS =
      Set.of(
          UpdateParams.EXPUNGE_DELETES,
          UpdateParams.MAX_OPTIMIZE_SEGMENTS,
          UpdateParams.COMMIT,
          UpdateParams.WAIT_SEARCHER,
          UpdateParams.OPEN_SEARCHER,
          UpdateParams.SOFT_COMMIT,
          UpdateParams.PREPARE_COMMIT,
          UpdateParams.OPTIMIZE

          // Not supported via SolrCloud
          // UpdateParams.ROLLBACK
          );

  protected volatile Object[] locks = objectList(3);

  /**
   * Constructs {@link CloudSolrClient} instances from provided configuration. It will use a Jetty
   * based {@code HttpClient} if available, or will otherwise use the JDK.
   */
  public static class Builder {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // If the Jetty-based HttpJettySolrClient builder is on the classpath, this will be its no-arg
    // constructor; otherwise it will be null and we will fall back to the JDK HTTP client.
    private static final Constructor<? extends HttpSolrClientBuilderBase<?, ?>>
        HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR;

    static {
      Constructor<? extends HttpSolrClientBuilderBase<?, ?>> ctor = null;
      try {
        @SuppressWarnings("unchecked")
        Class<? extends HttpSolrClientBuilderBase<?, ?>> builderClass =
            (Class<? extends HttpSolrClientBuilderBase<?, ?>>)
                Class.forName("org.apache.solr.client.solrj.jetty.HttpJettySolrClient$Builder");
        ctor = builderClass.getDeclaredConstructor();
        ctor.newInstance(); // perhaps fails because Jetty libs aren't on the classpath
      } catch (Throwable t) {
        // Class not present or incompatible; leave ctor as null to indicate unavailability
        if (log.isTraceEnabled()) {
          log.trace(
              "HttpJettySolrClient$Builder not available on classpath; will use HttpJdkSolrClient",
              t);
        }
      }
      HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR = ctor;
    }

    protected Collection<String> zkHosts = new ArrayList<>();
    protected List<String> solrUrls = new ArrayList<>();
    protected String zkChroot;
    protected HttpSolrClientBase httpClient;
    protected boolean shardLeadersOnly = true;
    protected boolean directUpdatesToLeadersOnly = false;
    protected boolean parallelUpdates = true;
    protected ClusterStateProvider stateProvider;
    protected HttpSolrClientBuilderBase<?, ?> internalClientBuilder;
    protected RequestWriter requestWriter;
    protected ResponseParser responseParser;
    protected long retryExpiryTimeNano =
        TimeUnit.NANOSECONDS.convert(3, TimeUnit.SECONDS); // 3 seconds or 3 million nanos

    protected String defaultCollection;
    protected long timeToLiveSeconds = 60;
    protected int parallelCacheRefreshesLocks = 3;
    protected int zkConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;
    protected int zkClientTimeout = SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT;
    protected boolean canUseZkACLs = true;

    /**
     * Provide a series of Solr URLs to be used when configuring {@link CloudSolrClient} instances.
     * The solr client will use these urls to understand the cluster topology, which solr nodes are
     * active etc.
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
     *   final SolrClient client = new CloudSolrClient.Builder(solrBaseUrls).build();
     * </pre>
     */
    public Builder(List<String> solrUrls) {
      this.solrUrls = solrUrls;
    }

    /**
     * Provide a series of ZK hosts which will be used when configuring {@link CloudSolrClient}
     * instances.
     *
     * <p>Usage example when Solr stores data at the ZooKeeper root ('/'):
     *
     * <pre>
     *   final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *   zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *   final SolrClient client = new CloudSolrClient.Builder(zkServers, Optional.empty()).build();
     * </pre>
     *
     * Usage example when Solr data is stored in a ZooKeeper chroot:
     *
     * <pre>
     *    final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *    zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *    final SolrClient client = new CloudSolrClient.Builder(zkServers, Optional.of("/solr")).build();
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

    /** for an expert use-case */
    public Builder(ClusterStateProvider stateProvider) {
      this.stateProvider = stateProvider;
    }

    /** Whether to use the default ZK ACLs when building a ZK Client. */
    public Builder canUseZkACLs(boolean canUseZkACLs) {
      this.canUseZkACLs = canUseZkACLs;
      return this;
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
     * Tells {@link CloudSolrClient.Builder} that created clients should send direct updates to
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
     * Tells {@link CloudSolrClient.Builder} that created clients can send updates to any shard
     * replica (shard leaders and non-leaders).
     *
     * <p>Shard leaders are still preferred, but the created clients will fall back to using other
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
     * Tells {@link CloudSolrClient.Builder} whether created clients should send shard updates
     * serially or in parallel
     *
     * <p>When an {@link UpdateRequest} affects multiple shards, {@link CloudSolrClient} splits it
     * up and sends a request to each affected shard. This setting chooses whether those
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
     */
    public Builder withParallelCacheRefreshes(int parallelCacheRefreshesLocks) {
      this.parallelCacheRefreshesLocks = parallelCacheRefreshesLocks;
      return this;
    }

    /**
     * This is the time to wait to re-fetch the state after getting the same state version from ZK
     */
    public Builder withRetryExpiryTime(long expiryTime, TimeUnit unit) {
      this.retryExpiryTimeNano = TimeUnit.NANOSECONDS.convert(expiryTime, unit);
      return this;
    }

    /** Sets the default collection for request. */
    public Builder withDefaultCollection(String defaultCollection) {
      this.defaultCollection = defaultCollection;
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
     * Set the internal Solr HTTP client.
     *
     * <p>Note: closing the client instance is the responsibility of the caller.
     *
     * @return this
     */
    public Builder withHttpClient(HttpSolrClientBase httpSolrClient) {
      if (this.internalClientBuilder != null) {
        throw new IllegalStateException(
            "The builder can't accept an httpClient AND an internalClientBuilder, only one of those can be provided");
      }
      this.httpClient = httpSolrClient;
      return this;
    }

    /**
     * If provided, the CloudSolrClient will build it's internal client using this builder (instead
     * of the empty default one). Providing this builder allows users to configure the internal
     * clients (authentication, timeouts, etc.).
     *
     * @param internalClientBuilder the builder to use for creating the internal http client.
     * @return this
     */
    public Builder withHttpClientBuilder(HttpSolrClientBuilderBase<?, ?> internalClientBuilder) {
      if (this.httpClient != null) {
        throw new IllegalStateException(
            "The builder can't accept an httpClient AND an internalClientBuilder, only one of those can be provided");
      }
      this.internalClientBuilder = internalClientBuilder;
      return this;
    }

    @Deprecated(since = "9.10")
    public Builder withInternalClientBuilder(
        HttpSolrClientBuilderBase<?, ?> internalClientBuilder) {
      return withHttpClientBuilder(internalClientBuilder);
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

    /** Create a {@link CloudSolrClient} based on the provided configuration. */
    public CloudHttp2SolrClient build() {
      int providedOptions = 0;
      if (!zkHosts.isEmpty()) providedOptions++;
      if (!solrUrls.isEmpty()) providedOptions++;
      if (stateProvider != null) providedOptions++;

      if (providedOptions > 1) {
        throw new IllegalArgumentException(
            "Only one of zkHost(s), solrUrl(s), or stateProvider should be specified.");
      } else if (providedOptions == 0) {
        throw new IllegalArgumentException(
            "One of zkHosts, solrUrls, or stateProvider must be specified.");
      }

      return new CloudHttp2SolrClient(this);
    }

    protected HttpSolrClientBase createOrGetHttpClient() {
      if (httpClient != null) {
        return httpClient;
      } else if (internalClientBuilder != null) {
        return internalClientBuilder.build();
      }

      HttpSolrClientBuilderBase<?, ?> builder;
      if (HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR != null) {
        try {
          log.debug("Using HttpJettySolrClient as the delegate http client");
          builder = HTTP_JETTY_SOLR_CLIENT_BUILDER_CTOR.newInstance();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        log.debug("Using HttpJdkSolrClient as the delegate http client");
        builder = new HttpJdkSolrClient.Builder();
      }
      return builder.build();
    }

    protected LBSolrClient createOrGetLbClient(HttpSolrClientBase myClient) {
      return myClient.createLBSolrClient();
    }

    protected ClusterStateProvider createZkClusterStateProvider() {
      ClusterStateProvider stateProvider =
          ClusterStateProvider.newZkClusterStateProvider(zkHosts, zkChroot, canUseZkACLs);
      if (stateProvider instanceof SolrZkClientTimeout.SolrZkClientTimeoutAware timeoutAware) {
        timeoutAware.setZkClientTimeout(zkClientTimeout);
        timeoutAware.setZkConnectTimeout(zkConnectTimeout);
      }
      return stateProvider;
    }

    protected ClusterStateProvider createHttpClusterStateProvider(HttpSolrClientBase httpClient) {
      try {
        return new HttpClusterStateProvider<>(solrUrls, httpClient);
      } catch (Exception e) {
        throw new RuntimeException(
            "Couldn't initialize a HttpClusterStateProvider (is/are the "
                + "Solr server(s), "
                + solrUrls
                + ", down?)",
            e);
      }
    }
  }

  protected static class StateCache extends ConcurrentHashMap<String, ExpiringCachedDocCollection> {
    final AtomicLong puts = new AtomicLong();
    final AtomicLong hits = new AtomicLong();
    final Lock evictLock = new ReentrantLock(true);
    public volatile long timeToLiveMs = 60 * 1000L;

    @Override
    public ExpiringCachedDocCollection get(Object key) {
      ExpiringCachedDocCollection val = super.get(key);
      if (val == null) {
        // a new collection is likely to be added now.
        // check if there are stale items and remove them
        evictStale();
        return null;
      }
      if (val.isExpired(timeToLiveMs)) {
        super.remove(key);
        return null;
      }
      hits.incrementAndGet();
      return val;
    }

    @Override
    public ExpiringCachedDocCollection put(String key, ExpiringCachedDocCollection value) {
      puts.incrementAndGet();
      return super.put(key, value);
    }

    void evictStale() {
      if (!evictLock.tryLock()) return;
      try {
        for (Entry<String, ExpiringCachedDocCollection> e : entrySet()) {
          if (e.getValue().isExpired(timeToLiveMs)) {
            super.remove(e.getKey());
          }
        }
      } finally {
        evictLock.unlock();
      }
    }
  }

  protected final StateCache collectionStateCache = new StateCache();

  class ExpiringCachedDocCollection {
    final DocCollection cached;
    final long cachedAtNano;
    // This is the time at which the collection is retried and got the same old version
    volatile long retriedAtNano = -1;
    // flag that suggests that this is potentially to be rechecked
    volatile boolean maybeStale = false;

    ExpiringCachedDocCollection(DocCollection cached) {
      this.cached = cached;
      this.cachedAtNano = System.nanoTime();
    }

    boolean isExpired(long timeToLiveMs) {
      return (System.nanoTime() - cachedAtNano)
          > TimeUnit.NANOSECONDS.convert(timeToLiveMs, TimeUnit.MILLISECONDS);
    }

    boolean shouldRetry() {
      if (maybeStale) { // we are not sure if it is stale so check with retry time
        if ((retriedAtNano == -1 || (System.nanoTime() - retriedAtNano) > retryExpiryTimeNano)) {
          return true; // we retried a while back. and we could not get anything new.
          // it's likely that it is not going to be available now also.
        }
      }
      return false;
    }

    void setRetriedAt() {
      retriedAtNano = System.nanoTime();
    }
  }

  protected CloudSolrClient(
      boolean updatesToLeaders, boolean parallelUpdates, boolean directUpdatesToLeadersOnly) {
    this.updatesToLeaders = updatesToLeaders;
    this.parallelUpdates = parallelUpdates;
    this.directUpdatesToLeadersOnly = directUpdatesToLeadersOnly;
    this.requestRLTGenerator = new RequestReplicaListTransformerGenerator();
  }

  protected abstract LBSolrClient getLbClient();

  public abstract ClusterStateProvider getClusterStateProvider();

  /**
   * @deprecated problematic as a 'get' method, since one implementation will do a remote request
   *     each time this is called, potentially return lots of data that isn't even needed.
   */
  @Deprecated
  public ClusterState getClusterState() {
    // The future of "ClusterState" isn't clear.  Could make it more of a cache instead of a
    // snapshot, so we un-deprecate. Or we avoid it and maybe make the ClusterStateProvider as that
    // cache.  SOLR-17604 is related.
    return getClusterStateProvider().getClusterState();
  }

  /** Is this a communication error? We will retry if so. */
  protected boolean wasCommError(Throwable t) {
    return t instanceof SocketException || t instanceof UnknownHostException;
  }

  @Override
  public void close() {
    if (this.threadPool != null && !ExecutorUtil.isShutdown(this.threadPool)) {
      ExecutorUtil.shutdownAndAwaitTermination(this.threadPool);
      this.threadPool = null;
    }
  }

  public ResponseParser getParser() {
    return getLbClient().getParser();
  }

  public RequestWriter getRequestWriter() {
    return getLbClient().getRequestWriter();
  }

  /** Gets whether direct updates are sent in parallel */
  public boolean isParallelUpdates() {
    return parallelUpdates;
  }

  /**
   * Connect to the zookeeper ensemble. This is an optional method that may be used to force a
   * connection before any other requests are sent.
   *
   * @deprecated Call {@link ClusterStateProvider#getLiveNodes()} instead.
   */
  @Deprecated
  public void connect() {
    getClusterStateProvider().connect();
  }

  /**
   * Connect to a cluster. If the cluster is not ready, retry connection up to a given timeout.
   *
   * @param duration the timeout
   * @param timeUnit the units of the timeout
   * @throws TimeoutException if the cluster is not ready after the timeout
   * @throws InterruptedException if the wait is interrupted
   */
  @Deprecated
  public void connect(long duration, TimeUnit timeUnit)
      throws TimeoutException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(
          "Waiting for {} {} for cluster at {} to be ready",
          duration,
          timeUnit,
          getClusterStateProvider());
    }
    long timeout = System.nanoTime() + timeUnit.toNanos(duration);
    while (System.nanoTime() < timeout) {
      try {
        connect();
        if (log.isInfoEnabled()) {
          log.info("Cluster at {} ready", getClusterStateProvider());
        }
        return;
      } catch (RuntimeException e) {
        // not ready yet, then...
      }
      TimeUnit.MILLISECONDS.sleep(250);
    }
    throw new TimeoutException("Timed out waiting for cluster");
  }

  @SuppressWarnings({"unchecked"})
  private NamedList<Object> directUpdate(UpdateRequest request, String collection)
      throws SolrServerException {
    SolrParams params = request.getParams();
    ModifiableSolrParams routableParams = new ModifiableSolrParams();
    ModifiableSolrParams nonRoutableParams = new ModifiableSolrParams();

    if (params != null) {
      nonRoutableParams.add(params);
      routableParams.add(params);
      for (String param : NON_ROUTABLE_PARAMS) {
        routableParams.remove(param);
      }
    } else {
      params = new ModifiableSolrParams();
    }

    if (collection == null) {
      throw new SolrServerException(
          "No collection param specified on request and no default collection has been set.");
    }

    // Check to see if the collection is an alias. Updates to multi-collection aliases are ok as
    // long as they are routed aliases
    List<String> aliasedCollections =
        new ArrayList<>(resolveAliases(Collections.singletonList(collection)));
    if (aliasedCollections.size() == 1 || getClusterStateProvider().isRoutedAlias(collection)) {
      collection = aliasedCollections.get(0); // pick 1st (consistent with HttpSolrCall behavior)
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Update request to non-routed multi-collection alias not supported: "
              + collection
              + " -> "
              + aliasedCollections);
    }

    DocCollection col = getDocCollection(collection, null);

    DocRouter router = col.getRouter();

    if (router instanceof ImplicitDocRouter) {
      // short circuit as optimization
      return null;
    }

    ReplicaListTransformer replicaListTransformer =
        requestRLTGenerator.getReplicaListTransformer(params);

    // Create the URL map, which is keyed on slice name.
    // The value is a list of URLs for each replica in the slice.
    // The first value in the list is the leader for the slice.
    final Map<String, List<String>> urlMap = buildUrlMap(col, replicaListTransformer);
    String routeField =
        (col.getRouter().getRouteField(col) == null) ? ID : col.getRouter().getRouteField(col);
    final Map<String, ? extends LBSolrClient.Req> routes =
        createRoutes(request, routableParams, col, router, urlMap, routeField);
    if (routes == null) {
      if (directUpdatesToLeadersOnly && hasInfoToFindLeaders(request, routeField)) {
        // we have info (documents with ids and/or ids to delete) with
        // which to find the leaders, but we could not find (all of) them
        throw new SolrException(
            SolrException.ErrorCode.SERVICE_UNAVAILABLE,
            "directUpdatesToLeadersOnly==true but could not find leader(s)");
      } else {
        // we could not find a leader or routes yet - use unoptimized general path
        return null;
      }
    }

    final NamedList<Throwable> exceptions = new NamedList<>();
    final NamedList<NamedList<?>> shardResponses =
        new NamedList<>(routes.size() + 1); // +1 for deleteQuery

    long start = System.nanoTime();

    if (parallelUpdates) {
      final Map<String, Future<NamedList<?>>> responseFutures =
          CollectionUtil.newHashMap(routes.size());
      for (final Map.Entry<String, ? extends LBSolrClient.Req> entry : routes.entrySet()) {
        final String url = entry.getKey();
        final LBSolrClient.Req lbRequest = entry.getValue();
        try {
          MDC.put("CloudSolrClient.url", url);
          responseFutures.put(
              url,
              threadPool.submit(
                  () -> {
                    return getLbClient().request(lbRequest).getResponse();
                  }));
        } finally {
          MDC.remove("CloudSolrClient.url");
        }
      }

      for (final Map.Entry<String, Future<NamedList<?>>> entry : responseFutures.entrySet()) {
        final String url = entry.getKey();
        final Future<NamedList<?>> responseFuture = entry.getValue();
        try {
          shardResponses.add(url, responseFuture.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          exceptions.add(url, e.getCause());
        }
      }

      if (exceptions.size() > 0) {
        Throwable firstException = exceptions.getVal(0);
        if (firstException instanceof SolrException e) {
          throw getRouteException(
              SolrException.ErrorCode.getErrorCode(e.code()), exceptions, routes);
        } else {
          throw getRouteException(SolrException.ErrorCode.SERVER_ERROR, exceptions, routes);
        }
      }
    } else {
      for (Map.Entry<String, ? extends LBSolrClient.Req> entry : routes.entrySet()) {
        String url = entry.getKey();
        LBSolrClient.Req lbRequest = entry.getValue();
        try {
          NamedList<Object> rsp = getLbClient().request(lbRequest).getResponse();
          shardResponses.add(url, rsp);
        } catch (Exception e) {
          if (e instanceof SolrException) {
            throw (SolrException) e;
          } else {
            throw new SolrServerException(e);
          }
        }
      }
    }

    UpdateRequest nonRoutableRequest = null;
    List<String> deleteQuery = request.getDeleteQuery();
    if (deleteQuery != null && deleteQuery.size() > 0) {
      UpdateRequest deleteQueryRequest = new UpdateRequest();
      deleteQueryRequest.setDeleteQuery(deleteQuery);
      nonRoutableRequest = deleteQueryRequest;
    }

    Set<String> paramNames = nonRoutableParams.getParameterNames();

    Set<String> intersection = new HashSet<>(paramNames);
    intersection.retainAll(NON_ROUTABLE_PARAMS);

    if (nonRoutableRequest != null || intersection.size() > 0) {
      if (nonRoutableRequest == null) {
        nonRoutableRequest = new UpdateRequest();
      }
      nonRoutableRequest.setParams(nonRoutableParams);
      nonRoutableRequest.setBasicAuthCredentials(
          request.getBasicAuthUser(), request.getBasicAuthPassword());
      final var endpoints =
          routes.keySet().stream()
              .map(url -> LBSolrClient.Endpoint.from(url))
              .collect(Collectors.toList());
      Collections.shuffle(endpoints, rand);
      LBSolrClient.Req req = new LBSolrClient.Req(nonRoutableRequest, endpoints);
      try {
        LBSolrClient.Rsp rsp = getLbClient().request(req);
        shardResponses.add(endpoints.get(0).toString(), rsp.getResponse());
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, endpoints.get(0).toString(), e);
      }
    }

    long end = System.nanoTime();

    @SuppressWarnings({"rawtypes"})
    RouteResponse rr =
        condenseResponse(
            shardResponses, (int) TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));
    rr.setRouteResponses(shardResponses);
    rr.setRoutes(routes);
    return rr;
  }

  protected RouteException getRouteException(
      SolrException.ErrorCode serverError,
      NamedList<Throwable> exceptions,
      Map<String, ? extends LBSolrClient.Req> routes) {
    return new RouteException(serverError, exceptions, routes);
  }

  protected Map<String, ? extends LBSolrClient.Req> createRoutes(
      UpdateRequest updateRequest,
      ModifiableSolrParams routableParams,
      DocCollection col,
      DocRouter router,
      Map<String, List<String>> urlMap,
      String routeField) {
    return urlMap == null
        ? null
        : updateRequest.getRoutesToCollection(router, col, urlMap, routableParams, routeField);
  }

  private Map<String, List<String>> buildUrlMap(
      DocCollection col, ReplicaListTransformer replicaListTransformer) {
    Map<String, List<String>> urlMap = new HashMap<>();
    Collection<Slice> slices = col.getActiveSlices();
    Set<String> liveNodes = getClusterStateProvider().getLiveNodes();
    for (Slice slice : slices) {
      String name = slice.getName();
      List<Replica> sortedReplicas = new ArrayList<>();
      Replica leader = slice.getLeader();
      if (directUpdatesToLeadersOnly && leader == null) {
        for (Replica replica :
            slice.getReplicas(
                replica -> replica.isActive(liveNodes) && replica.getType() == Replica.Type.NRT)) {
          leader = replica;
          break;
        }
      }
      if (leader == null) {
        if (directUpdatesToLeadersOnly) {
          continue;
        }
        // take unoptimized general path - we cannot find a leader yet
        return null;
      }

      if (!directUpdatesToLeadersOnly) {
        for (Replica replica : slice.getReplicas()) {
          if (!replica.equals(leader)) {
            sortedReplicas.add(replica);
          }
        }
      }

      // Sort the non-leader replicas according to the request parameters
      replicaListTransformer.transform(sortedReplicas);

      // put the leaderUrl first.
      sortedReplicas.add(0, leader);

      urlMap.put(
          name, sortedReplicas.stream().map(Replica::getCoreUrl).collect(Collectors.toList()));
    }
    return urlMap;
  }

  protected <T extends RouteResponse<?>> T condenseResponse(
      NamedList<?> response, int timeMillis, Supplier<T> supplier) {
    T condensed = supplier.get();
    int status = 0;
    Integer rf = null;

    // TolerantUpdateProcessor
    List<SimpleOrderedMap<String>> toleratedErrors = null;
    int maxToleratedErrors = Integer.MAX_VALUE;

    // For "adds", "deletes", "deleteByQuery" etc.
    Map<String, NamedList<Object>> versions = new HashMap<>();

    for (int i = 0; i < response.size(); i++) {
      NamedList<?> shardResponse = (NamedList<?>) response.getVal(i);
      NamedList<?> header = (NamedList<?>) shardResponse.get("responseHeader");
      Integer shardStatus = (Integer) header.get("status");
      int s = shardStatus.intValue();
      if (s > 0) {
        status = s;
      }
      Object rfObj = header.get(UpdateRequest.REPFACT);
      if (rfObj != null && rfObj instanceof Integer routeRf) {
        if (rf == null || routeRf < rf) rf = routeRf;
      }

      @SuppressWarnings("unchecked")
      List<SimpleOrderedMap<String>> shardTolerantErrors =
          (List<SimpleOrderedMap<String>>) header.get("errors");
      if (null != shardTolerantErrors) {
        Integer shardMaxToleratedErrors = (Integer) header.get("maxErrors");
        assert null != shardMaxToleratedErrors
            : "TolerantUpdateProcessor reported errors but not maxErrors";
        // if we get into some weird state where the nodes disagree about the effective maxErrors,
        // assume the min value seen to decide if we should fail.
        maxToleratedErrors =
            Math.min(
                maxToleratedErrors,
                ToleratedUpdateError.getEffectiveMaxErrors(shardMaxToleratedErrors.intValue()));

        if (null == toleratedErrors) {
          toleratedErrors = new ArrayList<SimpleOrderedMap<String>>(shardTolerantErrors.size());
        }
        for (SimpleOrderedMap<String> err : shardTolerantErrors) {
          toleratedErrors.add(err);
        }
      }
      for (String updateType : Arrays.asList("adds", "deletes", "deleteByQuery")) {
        Object obj = shardResponse.get(updateType);
        if (obj instanceof NamedList<?> nl) {
          NamedList<Object> versionsList =
              versions.containsKey(updateType) ? versions.get(updateType) : new NamedList<>();
          versionsList.addAll(nl);
          versions.put(updateType, versionsList);
        }
      }
    }

    NamedList<Object> cheader = new NamedList<>();
    cheader.add("status", status);
    cheader.add("QTime", timeMillis);
    if (rf != null) cheader.add(UpdateRequest.REPFACT, rf);
    if (null != toleratedErrors) {
      cheader.add("maxErrors", ToleratedUpdateError.getUserFriendlyMaxErrors(maxToleratedErrors));
      cheader.add("errors", toleratedErrors);
      if (maxToleratedErrors < toleratedErrors.size()) {
        // cumulative errors are too high, we need to throw a client exception w/correct metadata

        // NOTE: it shouldn't be possible for 1 == toleratedErrors.size(), because if that were the
        // case then at least one shard should have thrown a real error before this, so we don't
        // worry about having a more "singular" exception msg for that situation
        StringBuilder msgBuf =
            new StringBuilder()
                .append(toleratedErrors.size())
                .append(" Async failures during distributed update: ");

        NamedList<String> metadata = new NamedList<>();
        for (SimpleOrderedMap<String> err : toleratedErrors) {
          ToleratedUpdateError te = ToleratedUpdateError.parseMap(err);
          metadata.add(te.getMetadataKey(), te.getMetadataValue());

          msgBuf.append("\n").append(te.getMessage());
        }

        SolrException toThrow =
            new SolrException(SolrException.ErrorCode.BAD_REQUEST, msgBuf.toString());
        toThrow.setMetadata(metadata);
        throw toThrow;
      }
    }
    for (Map.Entry<String, NamedList<Object>> entry : versions.entrySet()) {
      condensed.add(entry.getKey(), entry.getValue());
    }
    condensed.add("responseHeader", cheader);
    return condensed;
  }

  @SuppressWarnings({"rawtypes"})
  public RouteResponse condenseResponse(NamedList<?> response, int timeMillis) {
    return condenseResponse(response, timeMillis, RouteResponse::new);
  }

  @SuppressWarnings({"rawtypes"})
  public static class RouteResponse<T extends LBSolrClient.Req> extends NamedList<Object> {
    private NamedList<NamedList<?>> routeResponses;
    private Map<String, T> routes;

    public void setRouteResponses(NamedList<NamedList<?>> routeResponses) {
      this.routeResponses = routeResponses;
    }

    public NamedList<NamedList<?>> getRouteResponses() {
      return routeResponses;
    }

    public void setRoutes(Map<String, T> routes) {
      this.routes = routes;
    }

    public Map<String, T> getRoutes() {
      return routes;
    }
  }

  public static class RouteException extends SolrException {

    private NamedList<Throwable> throwables;
    private Map<String, ? extends LBSolrClient.Req> routes;

    public RouteException(
        ErrorCode errorCode,
        NamedList<Throwable> throwables,
        Map<String, ? extends LBSolrClient.Req> routes) {
      super(errorCode, throwables.getVal(0).getMessage(), throwables.getVal(0));
      this.throwables = throwables;
      this.routes = routes;

      // create a merged copy of the metadata from all wrapped exceptions
      NamedList<String> metadata = new NamedList<String>();
      for (int i = 0; i < throwables.size(); i++) {
        Throwable t = throwables.getVal(i);
        if (t instanceof SolrException e) {
          NamedList<String> eMeta = e.getMetadata();
          if (null != eMeta) {
            metadata.addAll(eMeta);
          }
        }
      }
      if (0 < metadata.size()) {
        this.setMetadata(metadata);
      }
    }

    public NamedList<Throwable> getThrowables() {
      return throwables;
    }

    public Map<String, ? extends LBSolrClient.Req> getRoutes() {
      return this.routes;
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest<?> request, String collection)
      throws SolrServerException, IOException {
    // the collection parameter of the request overrides that of the parameter to this method
    String requestCollection = request.getCollection();
    if (requestCollection != null) {
      collection = requestCollection;
    } else if (collection == null) {
      collection = defaultCollection;
    }

    List<String> inputCollections =
        collection == null ? Collections.emptyList() : StrUtils.splitSmart(collection, ",", true);
    return requestWithRetryOnStaleState(request, 0, inputCollections);
  }

  /**
   * As this class doesn't watch external collections on the client side, there's a chance that the
   * request will fail due to cached stale state, which means the state must be refreshed from ZK
   * and retried.
   */
  protected NamedList<Object> requestWithRetryOnStaleState(
      SolrRequest<?> request, int retryCount, List<String> inputCollections)
      throws SolrServerException, IOException {
    // build up a _stateVer_ param to pass to the server containing all the
    // external collection state versions involved in this request, which allows
    // the server to notify us that our cached state for one or more of the external
    // collections is stale and needs to be refreshed ... this code has no impact on internal
    // collections
    String stateVerParam = null;
    List<DocCollection> requestedCollections = null;
    boolean isCollectionRequestOfV2 = false;
    if (request instanceof V2Request) {
      isCollectionRequestOfV2 = ((V2Request) request).isPerCollectionRequest();
    }
    boolean isAdmin =
        request.getRequestType() == SolrRequestType.ADMIN && !request.requiresCollection();
    if (!inputCollections.isEmpty()
        && !isAdmin
        && !isCollectionRequestOfV2) { // don't do _stateVer_ checking for admin, v2 api requests
      Set<String> requestedCollectionNames = resolveAliases(inputCollections);

      StringBuilder stateVerParamBuilder = null;
      for (String requestedCollection : requestedCollectionNames) {
        // track the version of state we're using on the client side using the _stateVer_ param
        DocCollection coll = getDocCollection(requestedCollection, null);
        if (coll == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Collection not found: " + requestedCollection);
        }
        int collVer = coll.getZNodeVersion();
        if (requestedCollections == null)
          requestedCollections = new ArrayList<>(requestedCollectionNames.size());
        requestedCollections.add(coll);

        if (stateVerParamBuilder == null) {
          stateVerParamBuilder = new StringBuilder();
        } else {
          stateVerParamBuilder.append(
              "|"); // hopefully pipe is not an allowed char in a collection name
        }

        stateVerParamBuilder.append(coll.getName()).append(":").append(collVer);
      }

      if (stateVerParamBuilder != null) {
        stateVerParam = stateVerParamBuilder.toString();
      }
    }

    if (request.getParams() instanceof ModifiableSolrParams params) {
      if (stateVerParam != null) {
        params.set(STATE_VERSION, stateVerParam);
      } else {
        params.remove(STATE_VERSION);
      }
    } // else: ??? how to set this ???

    NamedList<Object> resp = null;
    try {
      resp = sendRequest(request, inputCollections);
      // to avoid an O(n) operation we always add STATE_VERSION to the last and try to read it from
      // there
      Object o = resp == null || resp.size() == 0 ? null : resp.get(STATE_VERSION, resp.size() - 1);
      if (o != null && o instanceof Map<?, ?> invalidStates) {
        // remove this because no one else needs this and tests would fail if they are comparing
        // responses
        resp.remove(resp.size() - 1);
        for (Map.Entry<?, ?> e : invalidStates.entrySet()) {
          getDocCollection((String) e.getKey(), (Integer) e.getValue());
        }
      }
    } catch (Exception exc) {

      Throwable rootCause = SolrException.getRootCause(exc);
      // don't do retry support for admin requests
      // or if the request doesn't have a collection specified
      // or request is v2 api and its method is not GET
      if (inputCollections.isEmpty()
          || isAdmin
          || (request.getApiVersion() == SolrRequest.ApiVersion.V2
              && request.getMethod() != SolrRequest.METHOD.GET)) {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException) exc;
        } else if (exc instanceof IOException) {
          throw (IOException) exc;
        } else if (exc instanceof RuntimeException) {
          throw (RuntimeException) exc;
        } else {
          throw new SolrServerException(rootCause);
        }
      }

      int errorCode =
          (rootCause instanceof SolrException)
              ? ((SolrException) rootCause).code()
              : SolrException.ErrorCode.UNKNOWN.code;

      final boolean wasCommError = wasCommError(rootCause);

      if (wasCommError
          || (exc instanceof RouteException
              && (errorCode == 503)) // 404 because the core does not exist 503 service unavailable
      // TODO there are other reasons for 404. We need to change the solr response format from HTML
      // to structured data to know that
      ) {
        // it was a communication error. it is likely that
        // the node to which the request to be sent is down . So , expire the state
        // so that the next attempt would fetch the fresh state
        // just re-read state for all of them, if it has not been retried
        // in retryExpiryTime time
        if (requestedCollections != null) {
          for (DocCollection ext : requestedCollections) {
            ExpiringCachedDocCollection cacheEntry = collectionStateCache.get(ext.getName());
            if (cacheEntry == null) continue;
            cacheEntry.maybeStale = true;
          }
        }
        if (retryCount < MAX_STALE_RETRIES) { // if it is a communication error , we must try again
          // may be, we have a stale version of the collection state,
          // and we could not get any information from the server
          // it is probably not worth trying again and again because
          // the state would not have been updated
          log.info(
              "Request to collection {} failed due to ({}) {}, retry={} maxRetries={} commError={} errorCode={} - retrying",
              inputCollections,
              errorCode,
              rootCause,
              retryCount,
              MAX_STALE_RETRIES,
              wasCommError,
              errorCode);
          return requestWithRetryOnStaleState(request, retryCount + 1, inputCollections);
        }
      } else {
        log.info("request was not communication error it seems");
      }
      log.info(
          "Request to collection {} failed due to ({}) {}, retry={} maxRetries={} commError={} errorCode={} ",
          inputCollections,
          errorCode,
          rootCause,
          retryCount,
          MAX_STALE_RETRIES,
          wasCommError,
          errorCode);

      boolean stateWasStale = false;
      if (retryCount < MAX_STALE_RETRIES
          && requestedCollections != null
          && !requestedCollections.isEmpty()
          && (SolrException.ErrorCode.getErrorCode(errorCode)
                  == SolrException.ErrorCode.INVALID_STATE
              || errorCode == 404)) {
        // cached state for one or more external collections was stale
        // re-issue request using updated state
        stateWasStale = true;

        // just re-read state for all of them, which is a little heavy-handed but hopefully a rare
        // occurrence
        for (DocCollection ext : requestedCollections) {
          collectionStateCache.remove(ext.getName());
        }
      }

      // if we experienced a communication error, it's worth checking the state
      // with ZK just to make sure the node we're trying to hit is still part of the collection
      if (retryCount < MAX_STALE_RETRIES
          && !stateWasStale
          && requestedCollections != null
          && !requestedCollections.isEmpty()
          && wasCommError) {
        for (DocCollection ext : requestedCollections) {
          DocCollection latestStateFromZk = getDocCollection(ext.getName(), null);
          if (latestStateFromZk.getZNodeVersion() != ext.getZNodeVersion()) {
            // looks like we couldn't reach the server because the state was stale == retry
            stateWasStale = true;
            // we just pulled state from ZK, so update the cache so that the retry uses it
            collectionStateCache.put(
                ext.getName(), new ExpiringCachedDocCollection(latestStateFromZk));
          }
        }
      }

      if (requestedCollections != null) {
        requestedCollections.clear(); // done with this
      }

      // if the state was stale, then we retry the request once with new state pulled from Zk
      if (stateWasStale) {
        log.warn(
            "Re-trying request to collection(s) {} after stale state error from server.",
            inputCollections);
        resp = requestWithRetryOnStaleState(request, retryCount + 1, inputCollections);
      } else {
        if (exc instanceof SolrException
            || exc instanceof SolrServerException
            || exc instanceof IOException) {
          throw exc;
        } else {
          throw new SolrServerException(rootCause);
        }
      }
    }

    return resp;
  }

  protected NamedList<Object> sendRequest(SolrRequest<?> request, List<String> inputCollections)
      throws SolrServerException, IOException {
    boolean sendToLeaders = false;

    if (request.getRequestType() == SolrRequestType.UPDATE) {
      sendToLeaders = this.isUpdatesToLeaders();

      if (sendToLeaders && request instanceof UpdateRequest updateRequest) {
        sendToLeaders = sendToLeaders && updateRequest.isSendToLeaders();

        // Check if we can do a "directUpdate" ...
        if (sendToLeaders) {
          if (inputCollections.size() > 1) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Update request must be sent to a single collection "
                    + "or an alias: "
                    + inputCollections);
          }
          String collection =
              inputCollections.isEmpty()
                  ? null
                  : inputCollections.get(0); // getting first mimics HttpSolrCall
          NamedList<Object> response = directUpdate(updateRequest, collection);
          if (response != null) {
            return response;
          }
        }
      }
    }

    SolrParams reqParams = request.getParams();
    assert reqParams != null;

    ReplicaListTransformer replicaListTransformer =
        requestRLTGenerator.getReplicaListTransformer(reqParams);

    final ClusterStateProvider provider = getClusterStateProvider();
    final String urlScheme = provider.getClusterProperty(ClusterState.URL_SCHEME, "http");
    final Set<String> liveNodes = provider.getLiveNodes();

    final List<LBSolrClient.Endpoint> requestEndpoints =
        new ArrayList<>(); // we populate this as follows...

    if (request.getApiVersion() == SolrRequest.ApiVersion.V2) {
      if (!liveNodes.isEmpty()) {
        List<String> liveNodesList = new ArrayList<>(liveNodes);
        Collections.shuffle(liveNodesList, rand);
        final var chosenNodeUrl = Utils.getBaseUrlForNodeName(liveNodesList.get(0), urlScheme);
        requestEndpoints.add(new LBSolrClient.Endpoint(chosenNodeUrl));
      }

    } else if (!request.requiresCollection()) {
      for (String liveNode : liveNodes) {
        final var nodeBaseUrl = Utils.getBaseUrlForNodeName(liveNode, urlScheme);
        requestEndpoints.add(new LBSolrClient.Endpoint(nodeBaseUrl));
      }
    } else { // API call to a particular collection / core / alias (i.e.
      // request.requiresCollection() == true)
      Set<String> collectionNames = resolveAliases(inputCollections);
      if (collectionNames.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No collection param specified on request and no default collection has been set: "
                + inputCollections);
      }

      List<String> preferredNodes = request.getPreferredNodes();
      if (preferredNodes != null && !preferredNodes.isEmpty()) {
        String joinedInputCollections = StrUtils.join(inputCollections, ',');
        final var endpoints =
            preferredNodes.stream()
                .map(nodeName -> Utils.getBaseUrlForNodeName(nodeName, urlScheme))
                .map(nodeUrl -> new LBSolrClient.Endpoint(nodeUrl, joinedInputCollections))
                .collect(Collectors.toList());
        if (!endpoints.isEmpty()) {
          LBSolrClient.Req req = new LBSolrClient.Req(request, endpoints);
          LBSolrClient.Rsp rsp = getLbClient().request(req);
          return rsp.getResponse();
        }
      }

      // TODO: not a big deal because of the caching, but we could avoid looking
      //   at every shard when getting leaders if we tweaked some things

      // Retrieve slices from the cloud state and, for each collection specified, add it to the Map
      // of slices.
      Map<String, Slice> slices = new HashMap<>();
      String shardKeys = reqParams.get(ShardParams._ROUTE_);
      for (String collectionName : collectionNames) {
        DocCollection col = getDocCollection(collectionName, null);
        if (col == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Collection not found: " + collectionName);
        }
        Collection<Slice> routeSlices = col.getRouter().getSearchSlices(shardKeys, reqParams, col);
        ClientUtils.addSlices(slices, collectionName, routeSlices, true);
      }

      // Gather URLs, grouped by leader or replica
      List<Replica> sortedReplicas = new ArrayList<>();
      List<Replica> replicas = new ArrayList<>();
      for (Slice slice : slices.values()) {
        Replica leader = slice.getLeader();
        for (Replica replica : slice.getReplicas()) {
          String node = replica.getNodeName();
          if (!liveNodes.contains(node) // Must be a live node to continue
              || replica.getState()
                  != Replica.State.ACTIVE) { // Must be an ACTIVE replica to continue
            continue;
          }
          if (sendToLeaders && replica.equals(leader)) {
            sortedReplicas.add(replica); // put leaders here eagerly (if sendToLeader mode)
          } else {
            replicas.add(replica); // replicas here
          }
        }
      }

      // Sort the leader replicas, if any, according to the request preferences    (none if
      // !sendToLeaders)
      replicaListTransformer.transform(sortedReplicas);

      // Sort the replicas, if any, according to the request preferences and append to our list
      replicaListTransformer.transform(replicas);

      sortedReplicas.addAll(replicas);

      String joinedInputCollections = StrUtils.join(inputCollections, ',');
      Set<String> seenNodes = new HashSet<>();
      sortedReplicas.forEach(
          replica -> {
            if (seenNodes.add(replica.getNodeName())) {
              if (inputCollections.size() == 1 && collectionNames.size() == 1) {
                // If we have a single collection name (and not an alias to multiple collection),
                // send the query directly to a replica of this collection.
                requestEndpoints.add(
                    new LBSolrClient.Endpoint(replica.getBaseUrl(), replica.getCoreName()));
              } else {
                requestEndpoints.add(
                    new LBSolrClient.Endpoint(replica.getBaseUrl(), joinedInputCollections));
              }
            }
          });

      if (requestEndpoints.isEmpty()) {
        collectionStateCache.keySet().removeAll(collectionNames);
        throw new SolrException(
            SolrException.ErrorCode.INVALID_STATE,
            "Could not find a healthy node to handle the request.");
      }
    }

    LBSolrClient.Req req = new LBSolrClient.Req(request, requestEndpoints);
    LBSolrClient.Rsp rsp = getLbClient().request(req);
    return rsp.getResponse();
  }

  /**
   * Resolves the input collections to their possible aliased collections. Doesn't validate
   * collection existence.
   */
  private Set<String> resolveAliases(List<String> inputCollections) {
    if (inputCollections.isEmpty()) {
      return Collections.emptySet();
    }
    LinkedHashSet<String> uniqueNames = new LinkedHashSet<>(); // consistent ordering
    for (String collectionName : inputCollections) {
      if (getDocCollection(collectionName, -1) == null) {
        // perhaps it's an alias
        uniqueNames.addAll(getClusterStateProvider().resolveAlias(collectionName));
      } else {
        uniqueNames.add(collectionName); // it's a collection
      }
    }
    return uniqueNames;
  }

  /**
   * If true, this client has been configured such that it will generally prefer to send {@link
   * SolrRequestType#UPDATE} requests to a shard leader, if and only if {@link
   * UpdateRequest#isSendToLeaders} is also true. If false, then this client has been configured to
   * obey normal routing preferences when dealing with {@link SolrRequestType#UPDATE} requests.
   *
   * @see #isDirectUpdatesToLeadersOnly
   */
  public boolean isUpdatesToLeaders() {
    return updatesToLeaders;
  }

  /**
   * If true, this client has been configured such that "direct updates" will <em>only</em> be sent
   * to the current leader of the corresponding shard, and will not be retried with other replicas.
   * This method has no effect if {@link #isUpdatesToLeaders()} or {@link
   * UpdateRequest#isSendToLeaders} returns false.
   *
   * <p>A "direct update" is any update that can be sent directly to a single shard, and does not
   * need to be broadcast to every shard. (Example: document updates or "delete by id" when using
   * the default router; non-direct updates are things like commits and "delete by query").
   *
   * <p>NOTE: If a single {@link UpdateRequest} contains multiple "direct updates" for different
   * shards, this client may break the request up and merge the responses.
   *
   * @return true if direct updates are sent to shard leaders only
   */
  public boolean isDirectUpdatesToLeadersOnly() {
    return directUpdatesToLeadersOnly;
  }

  protected static Object[] objectList(int n) {
    Object[] l = new Object[n];
    for (int i = 0; i < n; i++) {
      l[i] = new Object();
    }
    return l;
  }

  protected DocCollection getDocCollection(String collection, Integer expectedVersion)
      throws SolrException {
    if (expectedVersion == null) expectedVersion = -1;
    if (collection == null) return null;
    ExpiringCachedDocCollection cacheEntry = collectionStateCache.get(collection);
    DocCollection col = cacheEntry == null ? null : cacheEntry.cached;
    if (col != null) {
      if (expectedVersion <= col.getZNodeVersion() && !cacheEntry.shouldRetry()) return col;
    }

    Object[] locks = this.locks;
    int lockId =
        Math.abs(Hash.murmurhash3_x86_32(collection, 0, collection.length(), 0) % locks.length);
    final Object lock = locks[lockId];
    synchronized (lock) {
      /*we have waited for some time just check once again*/
      cacheEntry = collectionStateCache.get(collection);
      col = cacheEntry == null ? null : cacheEntry.cached;
      if (col != null) {
        if (expectedVersion <= col.getZNodeVersion() && !cacheEntry.shouldRetry()) return col;
      }
      ClusterState.CollectionRef ref = getCollectionRef(collection);
      if (ref == null) {
        // no such collection exists
        return null;
      }
      // We are going to fetch a new version
      // we MUST try to get a new version
      DocCollection fetchedCol = ref.get(); // this is a call to ZK
      if (fetchedCol == null) return null; // this collection no more exists
      if (col != null && fetchedCol.getZNodeVersion() == col.getZNodeVersion()) {
        cacheEntry.setRetriedAt(); // we retried and found that it is the same version
        cacheEntry.maybeStale = false;
      } else {
        collectionStateCache.put(collection, new ExpiringCachedDocCollection(fetchedCol));
      }
      return fetchedCol;
    }
  }

  ClusterState.CollectionRef getCollectionRef(String collection) {
    return getClusterStateProvider().getState(collection);
  }

  /**
   * Useful for determining the minimum achieved replication factor across all shards involved in
   * processing an update request, typically useful for gauging the replication factor of a batch.
   */
  public int getMinAchievedReplicationFactor(String collection, NamedList<?> resp) {
    // it's probably already on the top-level header set by condense
    NamedList<?> header = (NamedList<?>) resp.get("responseHeader");
    Integer achRf = (Integer) header.get(UpdateRequest.REPFACT);
    if (achRf != null) return achRf.intValue();

    // not on the top-level header, walk the shard route tree
    Map<String, Integer> shardRf = getShardReplicationFactor(collection, resp);
    for (Integer rf : shardRf.values()) {
      if (achRf == null || rf < achRf) {
        achRf = rf;
      }
    }
    return (achRf != null) ? achRf.intValue() : -1;
  }

  /**
   * Walks the NamedList response after performing an update request looking for the replication
   * factor that was achieved in each shard involved in the request. For single doc updates, there
   * will be only one shard in the return value.
   */
  public Map<String, Integer> getShardReplicationFactor(String collection, NamedList<?> resp) {
    Map<String, Integer> results = new HashMap<>();
    if (resp instanceof RouteResponse) {
      NamedList<NamedList<?>> routes = ((RouteResponse<?>) resp).getRouteResponses();
      DocCollection coll = getDocCollection(collection, null);
      Map<String, String> leaders = new HashMap<>();
      for (Slice slice : coll.getActiveSlices()) {
        Replica leader = slice.getLeader();
        if (leader != null) {
          String leaderUrl = leader.getBaseUrl() + "/" + leader.getCoreName();
          leaders.put(leaderUrl, slice.getName());
          String altLeaderUrl = leader.getBaseUrl() + "/" + collection;
          leaders.put(altLeaderUrl, slice.getName());
        }
      }

      Iterator<Map.Entry<String, NamedList<?>>> routeIter = routes.iterator();
      while (routeIter.hasNext()) {
        Map.Entry<String, NamedList<?>> next = routeIter.next();
        String host = next.getKey();
        NamedList<?> hostResp = next.getValue();
        Integer rf =
            (Integer) ((NamedList<?>) hostResp.get("responseHeader")).get(UpdateRequest.REPFACT);
        if (rf != null) {
          String shard = leaders.get(host);
          if (shard == null) {
            if (host.endsWith("/")) shard = leaders.get(host.substring(0, host.length() - 1));
            if (shard == null) {
              shard = host;
            }
          }
          results.put(shard, rf);
        }
      }
    }
    return results;
  }

  private static boolean hasInfoToFindLeaders(UpdateRequest updateRequest, String idField) {
    final Map<SolrInputDocument, Map<String, Object>> documents = updateRequest.getDocumentsMap();
    final Map<String, Map<String, Object>> deleteById = updateRequest.getDeleteByIdMap();

    final boolean hasNoDocuments = (documents == null || documents.isEmpty());
    final boolean hasNoDeleteById = (deleteById == null || deleteById.isEmpty());
    if (hasNoDocuments && hasNoDeleteById) {
      // no documents and no delete-by-id, so no info to find leader(s)
      return false;
    }

    if (documents != null) {
      for (final Map.Entry<SolrInputDocument, Map<String, Object>> entry : documents.entrySet()) {
        final SolrInputDocument doc = entry.getKey();
        final Object fieldValue = doc.getFieldValue(idField);
        if (fieldValue == null) {
          // a document with no id field value, so can't find leader for it
          return false;
        }
      }
    }

    return true;
  }
}
