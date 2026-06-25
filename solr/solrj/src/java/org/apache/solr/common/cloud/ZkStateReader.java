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
package org.apache.solr.common.cloud;

import com.codahale.metrics.Meter;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.cloud.CloudInspectUtil;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.Callable;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.emptySortedSet;
import static org.apache.solr.common.util.Utils.fromJSON;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ZkStateReader implements SolrCloseable, Watcher, Replica.NodeNameToBaseUrl {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String STRUCTURE_CHANGE_NOTIFIER = "_scn";

  private static final Meter docCollLiveUpdateRequests = Metrics.MARKS_METRICS.meter("zkreader_doccollliveupdates_requests");
  private static final Meter stateUpdateNotifications = Metrics.MARKS_METRICS.meter("zkreader_stateupdates_notifications");

  //private final ExecutorService dispatchExecutor = Executors.newWorkStealingPool();

  public static final byte[] emptyJson = Utils.toJSON(EMPTY_MAP);

  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";

  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";

  /**
   * SolrCore name.
   */
  public static final String CORE_NAME_PROP = "core";
  public static final String COLLECTION_PROP = "collection";
  public static final String ELECTION_NODE_PROP = "election_node";
  public static final String SHARD_ID_PROP = "shard";
  public static final String REPLICA_PROP = "replica";
  public static final String SHARD_RANGE_PROP = "shard_range";
  public static final String SHARD_STATE_PROP = "shard_state";
  public static final String SHARD_PARENT_PROP = "shard_parent";
  public static final String NUM_SHARDS_PROP = "numShards";
  public static final String LEADER_PROP = "leader";
  public static final String SHARED_STORAGE_PROP = "shared_storage";
  public static final String PROPERTY_PROP = "property";
  public static final String PROPERTY_PROP_PREFIX = "property.";
  public static final String PROPERTY_VALUE_PROP = "property.value";
  public static final String MAX_AT_ONCE_PROP = "maxAtOnce";
  public static final String MAX_WAIT_SECONDS_PROP = "maxWaitSeconds";
  public static final String STATE_TIMESTAMP_PROP = "stateTimestamp";
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String ALIASES = "/aliases.json";
  public static final String STATE_JSON = "/state.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String COLLECTION_PROPS_ZKNODE = "collectionprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";
  public static final String SOLR_PKGS_PATH = "/packages.json";

  public static final String DEFAULT_SHARD_PREFERENCES = "defaultShardPreferences";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String MAX_CORES_PER_NODE = "maxCoresPerNode";
  public static final String PULL_REPLICAS = "pullReplicas";
  public static final String NRT_REPLICAS = "nrtReplicas";
  public static final String TLOG_REPLICAS = "tlogReplicas";
  public static final String READ_ONLY = "readOnly";

  public static final String ROLES = "/roles.json";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP = "configName";

  public static final String SAMPLE_PERCENTAGE = "samplePercentage";

  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionAdminParams#DEFAULTS} instead.
   */
  @Deprecated
  public static final String COLLECTION_DEF = "collectionDefaults";

  public static final String URL_SCHEME = "urlScheme";

  private static final String SOLR_ENVIRONMENT = "environment";

  public static final String REPLICA_TYPE = "type";
  public static final String COLLECTION_CANNOT_BE_NULL = "Collection cannot be null";

  private CloseTracker closeTracker;

  private final ReentrantLock createClusterStateWatchersAndUpdateLock = new ReentrantLock();
  private final ZkStateReaderQueue zkStateReaderQueue;
  private volatile CollectionRemoved collectionRemovedListener;

  public String getCollectionInfo() {
    return "clusterstate: interesting [" + collectionWatches.keySet().size() + "] watched [" + watchedCollectionStates.size() + "] lazy ["+ lazyCollectionStates.keySet().size() + "] total [" + allCollections.size() + ']';
  }

  private static class AllCollections {

    final Map<String,DocCollection> watchedCollectionStates;
    private final Map<String,ClusterState.CollectionRef> lazyCollectionStates;

    AllCollections(Map<String, DocCollection> watchedCollectionStates, Map<String, ClusterState.CollectionRef> lazyCollectionStates) {
      this.watchedCollectionStates = watchedCollectionStates;
      this.lazyCollectionStates = lazyCollectionStates;
    }

    public long size() {
      return watchedCollectionStates.size() + lazyCollectionStates.size();
    }

    public ClusterState.CollectionRef get(String collection) {
      DocCollection docCollection = watchedCollectionStates.get(collection);
      if (docCollection != null) {
        return new ClusterState.CollectionRef(docCollection);
      }
      return lazyCollectionStates.get(collection);
    }
  }

  private final int GET_LEADER_RETRY_DEFAULT_TIMEOUT = Integer.parseInt(System.getProperty("zkReaderGetLeaderRetryTimeoutMs", "1000"));

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";

  /**
   * "Interesting" and actively watched Collections.
   */
  final Map<String, DocCollection> watchedCollectionStates = new ConcurrentHashMap<>(64);


  /**
   * "Interesting" but not actively watched Collections.
   */
  final Map<String, ClusterState.CollectionRef> lazyCollectionStates = new ConcurrentHashMap<>(64);

  private final AllCollections allCollections = new AllCollections(watchedCollectionStates, lazyCollectionStates);

  private volatile Set<String> liveNodes = emptySortedSet();

  private final AtomicInteger liveNodesVersion = new AtomicInteger(-1);

  /**
   * Collection properties being actively watched
   */
  final Map<String,PropsWatcher.VersionedCollectionProps> watchedCollectionProps = new ConcurrentHashMap<>();

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();

  /**
   * JVM-global cache of the cluster's {@link #URL_SCHEME} (http/https). {@link Replica#getBaseUrl()}
   * reads this to build node base URLs: a {@link Replica} is a bare data object with no
   * {@link ZkStateReader} reference, so it cannot read the cluster property directly. Updated on
   * every {@link #loadClusterProperties()} (set to the loaded value, or reset to "http" when the
   * property is absent/empty), so it tracks the active cluster's scheme. Defaults to "http"; only
   * an SSL cluster (which writes {@code urlScheme=https} to /clusterprops.json) flips it to "https".
   * Without this, replica URLs were always http:// and an HTTPS client would speak plaintext to the
   * TLS server (ClosedChannelException / "Illegal character CNTL=0x15").
   */
  private static volatile String urlScheme = "http";

  /** @return the cluster's url scheme (http/https) last loaded from cluster properties. */
  public static String getUrlScheme() {
    return urlScheme;
  }


  private final ZkConfigManager configManager;

  private ConfigData securityData;

  private final Runnable securityNodeListener;

  private final Map<String, CollectionWatch<DocCollectionWatcher>> collectionWatches = new ConcurrentHashMap<>(64);

  private final Set<String> registeredCores = ConcurrentHashMap.newKeySet();


  // named this observers so there's less confusion between CollectionPropsWatcher map and the PropsWatcher map.
  final Map<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers = new ConcurrentHashMap<>();

  /**
   * Watchers of Collection properties
   */
  private final Map<String, PropsWatcher> collectionPropsWatchers = new ConcurrentHashMap<>();

  private final Set<CloudCollectionsListener> cloudCollectionsListeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService notifications = ParWork.getRootSharedExecutor();

  private final Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();

  private final Set<ClusterPropertiesListener> clusterPropertiesListeners = ConcurrentHashMap.newKeySet();

  private static final long LAZY_CACHE_TIME = TimeUnit.NANOSECONDS.convert(5000, TimeUnit.MILLISECONDS);

  private final Future<?> collectionPropsCacheCleaner = ParWork.submit("collectionPropsCacheCleaner", new CacheCleaner());
  volatile String node = "client";
  private volatile LiveNodeWatcher liveNodesWatcher;
 // private volatile CollectionsChildWatcher collectionsChildWatcher;
  public volatile IsLocalLeader isLocalLeader; // MRM TODO: not public

  public interface CollectionRemoved {
    void removed(String collection);
  }


  static class CollectionWatch<T> {

    private final String collection;
    volatile AtomicInteger coreRefCount = new AtomicInteger();
    final Set<DocCollectionWatcher> stateWatchers = ConcurrentHashMap.newKeySet();

    final Set<CollectionPropsWatcher> propStateWatchers = ConcurrentHashMap.newKeySet();

    public CollectionWatch(String collection) {
      this.collection = collection;
    }

    public boolean canBeRemoved(int currentWatchedCollections) {
      int refCount = coreRefCount.get();
      int watcherCount = stateWatchers.size();
      int propWatcherCount = propStateWatchers.size();

      log.debug("{} check if watcher can be removed coreRefCount={}, stateWatchers={} propStateWatchers={} currentWatchedCollections={}", collection, refCount, watcherCount, propWatcherCount, currentWatchedCollections);

//      if (currentWatchedCollections != -1 && currentWatchedCollections < 10) { // currentWatchedCollections < 10 - if there are few watched collections, might as well keep some not lazy?
//        return  false;
//      }
      // Must also account for collection-property watchers: removing one props watcher (or a props
      // watcher self-removing by returning true from onStateChanged) must NOT tear down the whole
      // collection watch while other props watchers are still registered, or they silently stop
      // receiving notifications.
      return refCount <= 0 && watcherCount <= 0 && propWatcherCount <= 0;
    }
  }


  public static final Set<String> KNOWN_CLUSTER_PROPS = Set.of(
      URL_SCHEME,
      CoreAdminParams.BACKUP_LOCATION,
      DEFAULT_SHARD_PREFERENCES,
      MAX_CORES_PER_NODE,
      SAMPLE_PERCENTAGE,
      SOLR_ENVIRONMENT,
      CollectionAdminParams.DEFAULTS);

  /**
   * Returns config set name for collection.
   * TODO move to DocCollection (state.json).
   *
   * @param collection to return config set name for
   */
  public String readConfigName(String collection) throws KeeperException {

    String configName;
//
//    DocCollection docCollection = getCollectionOrNull(collection);
//    if (docCollection != null) {
//      configName = docCollection.getStr(CONFIGNAME_PROP);
//      if (configName != null) {
//        return configName;
//      }
//    }

    String path = COLLECTIONS_ZKNODE + '/' + collection;
    log.debug("Loading collection config from: [{}]", path);

    try {

      byte[] data = zkClient.getData(path, null, null, true, true);
      if (data == null) {
        log.warn("No config data found at path {}.", path);
        throw new KeeperException.NoNodeException(path);
      }

      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.getStr(CONFIGNAME_PROP);

      if (configName == null) {
        log.warn("No config data found at path{}. ", path);
        throw new KeeperException.NoNodeException("No config data found at path: " + path);
      }
    } catch (InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      log.warn("Thread interrupted when loading config name for collection {}", collection);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Thread interrupted when loading config name for collection " + collection, e);
    }

    return configName;
  }


  private final SolrZkClient zkClient;

  protected final boolean closeClient;

  private volatile boolean closed = false;

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = zkClient;
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = false;
    this.securityNodeListener = securityNodeListener;
    zkStateReaderQueue = new ZkStateReaderQueue(this);
    assert ObjectReleaseTracker.getInstance().track(this);
  }


  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    // MRM TODO: check this out
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
            // on reconnect, reload cloud info
            new OnReconnect() {
              @Override
              public void command() {
                ZkStateReader.this.createClusterStateWatchersAndUpdate();
              }

              @Override
              public String getName() {
                return "createClusterStateWatchersAndUpdate";
              }
            });

    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = true;
    this.securityNodeListener = null;
    // Client/reader connection (holds no ephemerals): enable the test fast-close (skips ZooKeeper's
    // hardcoded ~100ms cleanup nap; graceful closeSession still sent). No-op in production. Before start().
    this.zkClient.setFastCloseForTests(Boolean.getBoolean("solr.zkClientFastCloseForTests"));
    zkStateReaderQueue = new ZkStateReaderQueue(this);
    try {
      zkClient.start();
    } catch (RuntimeException re) {
      log.error("Exception starting zkClient", re);
      zkClient.close(); // stuff has been opened inside the zkClient
      throw re;
    }
    assert ObjectReleaseTracker.getInstance().track(this);
  }

  public ZkConfigManager getConfigManager() {
    return configManager;
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
  /**
   * Refresh the set of live nodes.
   */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes();
  }

  public String compareStateVersions(String coll, int version, int updateHash) {
    log.debug("compareStateVersions {} {} {}", coll, version, updateHash);
    DocCollection collection = watchedCollectionStates.get(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() == version && updateHash == collection.getStateUpdates().hashCode()) {
      return null;
    }

    if (log.isDebugEnabled()) {
      log.debug("Wrong version from client [{}]!=[{}] updatesHash {}!={}", version, collection.getZNodeVersion(), updateHash, collection.getStateUpdates().hashCode());
    }

    return collection.getZNodeVersion() + ">" + collection.getStateUpdates().hashCode();
  }

  @SuppressWarnings({"unchecked"})
  public void createClusterStateWatchersAndUpdate() {
    createClusterStateWatchersAndUpdateLock.lock();
    try {
      if (closed) {
        throw new AlreadyClosedException();
      }


      if (log.isDebugEnabled()) log.debug("createClusterStateWatchersAndUpdate");
      CountDownLatch latch = new CountDownLatch(1);

      Watcher watcher = event -> {
        if (closed || !EventType.NodeCreated.equals(event.getType())) {
          return;
        }

        latch.countDown();
      };

      try {
        Stat stat = zkClient.exists("/cluster_init_done", watcher);
        if (stat == null) {
          boolean success = latch.await(10000, TimeUnit.MILLISECONDS);
          if (!success) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "cluster not found/not ready");
          }
        } else {
          zkClient.removeWatches("/cluster_init_done", watcher, Watcher.WatcherType.Any, true, (rc, path, ctx) -> {
          }, "");
        }

      } catch (Exception e) {
        log.error("Error waiting for cluster init", e);
        if (e instanceof RuntimeException) {
          throw (RuntimeException ) e;
        }
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, e);
      }

      try {

        if (closed || !zkClient.isConnected()) {
          throw new AlreadyClosedException();
        }


        if (log.isDebugEnabled()) {
          log.debug("Updating cluster state from ZooKeeper... ");
        }

        this.liveNodesWatcher = new LiveNodeWatcher();
        this.liveNodesWatcher.createWatch();
        this.liveNodesWatcher.refresh();

        zkStateReaderQueue.start();

        // on reconnect of SolrZkClient force refresh and re-add watches.
        loadClusterProperties();

        List<String> currentCollections = zkClient.getChildren(ZkStateReader.COLLECTIONS_ZKNODE, null, true);
        for (String collection : currentCollections) {
          collectionAdded(collection);
        }

        // Scoped state-plane subscriptions (review finding #4): the broad watch on /collections is
        // NON-recursive (PERSISTENT, not PERSISTENT_RECURSIVE), so it delivers ONLY collection
        // add/remove — a NodeChildrenChanged on /collections, reconciled by refreshCollectionSet().
        // High-frequency state-plane leaf events (deltas/snapshot/manifest) and structure `_scn` are
        // NOT delivered cluster-wide any more; they arrive through per-collection PERSISTENT_RECURSIVE
        // watches registered on /collections/<coll> ONLY for collections this node is interested in
        // (registerStatePlaneWatch, driven by registerCore / registerDocCollectionWatcher and torn down
        // by colectionRemoved). An uninterested node is never woken for another collection's leaf change,
        // so notification delivery now scales with the collections a node hosts/watches, not with the
        // whole cluster. The per-collection watch lifetime intentionally mirrors the process() gate
        // (watchedCollectionStates || collectionWatches): registered on first interest, released on
        // collection deletion — so a lingering cached collection keeps receiving pushes exactly as before.
        zkClient.addWatch(ZkStateReader.COLLECTIONS_ZKNODE, this, AddWatchMode.PERSISTENT, true);

        // Re-establish per-collection scoped watches after a session reconnect (ZK drops watches on a
        // new session). statePlaneWatched tracks the client-side intent only; clear it and rebuild from
        // the live interest set so the synchronous addWatch below actually re-arms each one in ZK. The
        // interest set is collectionWatches (active local cores/watchers) ONLY: a collection this node has
        // churned away from had its scoped watch torn down by releaseLocalInterest, and must not be
        // resurrected on reconnect. registerStatePlaneWatch issues a catch-up fetch as it re-arms, so each
        // still-interested collection re-syncs any change missed during the disconnect.
        statePlaneWatched.clear();
        for (String collection : collectionWatches.keySet()) {
          registerStatePlaneWatch(collection);
        }

        refreshAliases(aliasesManager);

        if (securityNodeListener != null) {
          addSecurityNodeWatcher(pair -> {
            ConfigData cd = new ConfigData();
            cd.data = pair.first() == null || pair.first().length == 0 ? EMPTY_MAP : Utils.getDeepCopy((Map) fromJSON(pair.first()), 4, false);
            cd.version = pair.second() == null ? -1 : pair.second().getVersion();
            securityData = cd;
            securityNodeListener.run();
          });
          securityData = getSecurityProps(true);
        }
        collectionPropsObservers.forEach((k, v) -> collectionPropsWatchers.computeIfAbsent(k, s -> new PropsWatcher(zkClient, notifications, node, k, 0,
            watchedCollectionProps, collectionPropsObservers, collectionPropsWatchers)).refreshAndWatch(true));
      } catch (Exception e) {
        log.warn("", e);
        return;
      }
    } finally {
      createClusterStateWatchersAndUpdateLock.unlock();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event == null || EventType.None.equals(event.getType()) || event.getPath() == null) {
      return;
    }
    // section process
    try {
      MDCLoggingContext.setNode(node);

      String[] parts = event.getPath().substring(1).split("/");

      if (log.isDebugEnabled()) {
        log.debug("Recursive Watch event={} parts={}", event, Arrays.asList(parts));
      }

      switch (event.getType()) {
        case NodeCreated:
          CompletableFuture<DocCollection> future;
          DocCollection docCollection;
          if (parts.length == 2 && parts[0].equals(ZkStateReader.COLLECTIONS_ZKNODE.substring(1))) {
            collectionAdded(parts[1]);
          } else if (parts[parts.length - 1].equals("_scn") && (watchedCollectionStates.containsKey(parts[1]) || collectionWatches.containsKey(parts[1]))) {
            String collection = parts[1];
            zkStateReaderQueue.fetchStateUpdates(collection, false);
          } else if (isStatePlaneNode(parts[parts.length - 1]) && (watchedCollectionStates.containsKey(parts[1]) || collectionWatches
              .containsKey(parts[1]))) {
            // PR-3 delta plane node created (ring/snapshot/manifest) — route to the delta apply path,
            // scoped to the changed shard when the path identifies one (finding #2).
            String collection = parts[1];
            zkStateReaderQueue.fetchStateUpdates(collection, shardFromStatePlanePath(parts), true);
          }
          break;
        case NodeDeleted:
          if (parts.length == 2 && parts[0].equals(ZkStateReader.COLLECTIONS_ZKNODE.substring(1))) {
            colectionRemoved(parts[1]);
          }
          break;
        case NodeChildrenChanged:
          // The non-recursive /collections watch (finding #4) reports add/remove as a child-list change
          // with no per-child path; reconcile the live children against the known set. The scoped
          // per-collection recursive watches also fire NodeChildrenChanged for sub-directories (e.g.
          // .../state/shards) — those are ignored here (length>1); their data changes arrive as
          // NodeCreated/NodeDataChanged on the leaves below.
          if (parts.length == 1 && parts[0].equals(ZkStateReader.COLLECTIONS_ZKNODE.substring(1))) {
            refreshCollectionSet();
          }
          break;
        case NodeDataChanged:
          if (log.isDebugEnabled()) {
            log.debug("NodeDataChanged lastPart={} collection={} watchedCollectionStates={} collectionWatches={} lazyCollectionStates={}", parts[parts.length - 1], parts[1], watchedCollectionStates,
                collectionWatches, lazyCollectionStates);
          }
          if (isStatePlaneNode(parts[parts.length - 1]) && (watchedCollectionStates.containsKey(parts[1]) || collectionWatches.containsKey(parts[1]))) {
            // Delta plane: a per-shard delta ring / snapshot / manifest changed. Re-fetch state
            // updates (the queue routes to the delta apply path), scoped to the changed shard when the
            // path identifies one (finding #2); a manifest change folds all shards.
            String collection = parts[1];
            zkStateReaderQueue.fetchStateUpdates(collection, shardFromStatePlanePath(parts), true);
          } else if (parts[parts.length - 1].equals("_scn") && (watchedCollectionStates.containsKey(parts[1]) || collectionWatches.containsKey(parts[1]) || lazyCollectionStates.containsKey(parts[1]))) {
            String collection = parts[1];
            zkStateReaderQueue.fetchStateUpdates(collection, false);
          }

          break;

        default:
      }
    } catch (Exception e) {
      log.error("Exception parsing in process", e);
    }
  }

  /** True for the delta-plane leaf znode names (PR-3): per-shard {@code deltas}/{@code snapshot} and the {@code manifest}. */
  private static boolean isStatePlaneNode(String lastPart) {
    return "deltas".equals(lastPart) || "snapshot".equals(lastPart) || "manifest".equals(lastPart);
  }

  /**
   * The shard name encoded in a per-shard state-plane leaf path (finding #2), or null for the
   * collection-level {@code manifest} node. Path shapes (after stripping the leading slash and
   * splitting on {@code /}): {@code collections/<coll>/state/shards/<shard>/deltas} and
   * {@code .../snapshot} carry the shard at {@code parts[len-2]}; {@code collections/<coll>/state/manifest}
   * has no shard (a collection-level switch onto the plane, so all shards are folded).
   */
  private static String shardFromStatePlanePath(String[] parts) {
    String lastPart = parts[parts.length - 1];
    if (("deltas".equals(lastPart) || "snapshot".equals(lastPart)) && parts.length >= 2) {
      return parts[parts.length - 2];
    }
    return null;
  }

  private void colectionRemoved(String collection) {
    log.debug("Collection removed {}", collection);
    // The collection is gone — drop its scoped state-plane watch (finding #4). Safe from the event
    // thread: removeWatches is async and a NoWatcher result is ignored.
    unregisterStatePlaneWatch(collection);
    if (collectionRemovedListener != null) {
      collectionRemovedListener.removed(collection);
    }
    lazyCollectionStates.remove(collection);
    watchedCollectionStates.remove(collection);

    CollectionWatch<DocCollectionWatcher> watchers = collectionWatches.get(collection);
    if (watchers != null) {
      watchers.stateWatchers.forEach(watcher -> CompletableFuture.runAsync(() -> {
        log.debug("Notify DocCollectionWatcher {} {}", watcher, null);
        try {
          if (!watcher.onStateChanged(null)) {
            if (watcher instanceof DocCollectionAndLiveNodesWatcherWrapper) {
               ((DocCollectionAndLiveNodesWatcherWrapper) watcher).latch.close("collection removed, " + collection);
            }
          }
        } catch (Exception exception) {
          log.warn("Error on calling state watcher", exception);
        }
      }));
    }
    notifyCloudCollectionsListeners();
    collectionWatches.remove(collection);
    lastFetchedCollectionSet.set(getCurrentCollections());
  }

  private void collectionAdded(String collection) {
    log.debug("Collection added {}", collection);

    collectionWatches.compute(collection, (k, v) -> {
      if (v != null) {
       zkStateReaderQueue.fetchStateUpdates(collection, false);
      }

      return v;
    });

    watchedCollectionStates.compute(collection, (s, docCollection) -> {
      if (docCollection == null) {
        LazyCollectionRef docRef = new LazyCollectionRef(collection);
        lazyCollectionStates.put(collection, docRef);
      }
      return docCollection;
    });

    notifyCloudCollectionsListeners();
    lastFetchedCollectionSet.set(getCurrentCollections());
  }

  // Collections for which this node currently wants a scoped per-collection PERSISTENT_RECURSIVE
  // watch on /collections/<coll>.  This is local-interest intent, retained across transient addWatch
  // failures and reconnects so watch arming can be retried without waiting for another registration.
  private final Set<String> statePlaneWatchWanted = ConcurrentHashMap.newKeySet();

  // Collections whose scoped state-plane watch is known to be armed in ZooKeeper for the current
  // session.  Cleared on reconnect and rebuilt from local interest / wanted-watch intent.
  private final Set<String> statePlaneWatched = ConcurrentHashMap.newKeySet();

  /**
   * Register a scoped recursive watch on {@code /collections/<collection>} so this node receives that
   * collection's structure (`_scn`) and high-frequency state-plane leaf events (deltas/snapshot/manifest)
   * WITHOUT the broad cluster-wide fanout (review finding #4). Idempotent and best-effort: a transient ZK
   * failure is logged and the wanted intent retained so delayed retry, a later interest registration, or reconnect
   * can arm the watch.
   */
  private void registerStatePlaneWatch(String collection) {
    if (closed || collection == null) {
      return;
    }
    statePlaneWatchWanted.add(collection);
    armStatePlaneWatch(collection);
  }

  private void armStatePlaneWatch(String collection) {
    if (closed || collection == null || !statePlaneWatchWanted.contains(collection)) {
      return;
    }
    if (!statePlaneWatched.add(collection)) {
      return;
    }
    try {
      // retryOnConnLoss=true: addWatch blocks until ZK confirms, so on return the watch is armed and
      // no post-registration leaf event can be missed.
      zkClient.addWatch(
          COLLECTIONS_ZKNODE + "/" + collection, this, AddWatchMode.PERSISTENT_RECURSIVE, true);
    } catch (Exception e) {
      statePlaneWatched.remove(collection);
      log.warn(
          "Could not register scoped state-plane watch for collection {}; will retry while local interest remains",
          collection,
          e);
      retryStatePlaneWatchRegistration(collection);
      return;
    }

    // Catch up on any state-plane change that landed while this collection had no scoped watch —
    // first interest, or interest regained after a churn teardown (review finding #4). The armed
    // watch covers all FUTURE leaf events; this fetch covers the gap so a re-interested watcher never
    // misses a transition that completed during the unwatched window. Best-effort + idempotent.
    try {
      if (zkStateReaderQueue != null) {
        zkStateReaderQueue.fetchStateUpdates(collection, true);
      }
    } catch (Exception e) {
      log.debug("Scoped-watch catch-up fetch failed for collection {}", collection, e);
    }
    log.debug("Scoped-watch registered for collection {}", collection);
  }

  private void retryStatePlaneWatchRegistration(String collection) {
    CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS)
        .execute(
            () -> {
              if (!closed
                  && collection != null
                  && statePlaneWatchWanted.contains(collection)
                  && !statePlaneWatched.contains(collection)) {
                armStatePlaneWatch(collection);
              }
            });
  }

  private void unregisterStatePlaneWatch(String collection) {
    if (collection == null) {
      return;
    }
    statePlaneWatchWanted.remove(collection);
    if (!statePlaneWatched.remove(collection)) {
      return;
    }
    try {
      // Async removal: a NoWatcher result (watch already gone with the deleted node) is reported via rc
      // and ignored — never thrown — so this is safe to call from the ZK event thread (collectionRemoved).
      zkClient.removeWatches(
          COLLECTIONS_ZKNODE + "/" + collection,
          this,
          Watcher.WatcherType.Any,
          true,
          (rc, path, ctx) -> {},
          null);
    } catch (Exception e) {
      log.debug(
          "Could not remove scoped state-plane watch for collection {}", collection, e);
    }
  }


  /**
   * The node has dropped its last local interest (core or watcher) in {@code collection} while the
   * collection still exists cluster-wide (review finding #4). Release the scoped recursive watch so this
   * node stops receiving — and reprocessing — the collection's high-frequency state-plane leaf events
   * (the whole point of scoping). This is the teardown half that makes the fanout reduction real for a
   * node that has churned away from a collection (core moved, watcher removed) without that collection
   * being deleted cluster-wide.
   *
   * <p>The cached entry in {@code watchedCollectionStates} is intentionally NOT dropped here: it is still
   * read synchronously by an immediately-firing registration ({@code registerCollectionStateWatcher}'s
   * on-register live-nodes notify), so removing it would surface a transient null to a watcher predicate.
   * Staleness of that retained-but-unwatched entry is bounded — {@link #registerStatePlaneWatch} issues a
   * catch-up fetch whenever interest (and the scoped watch) returns, re-materializing current state.
   */
  private void releaseLocalInterest(String collection) {
    // Keep the scoped state-plane watch armed while a cached entry for this collection is still served
    // from watchedCollectionStates. Dropping the last DocCollectionWatcher (e.g. a satisfied
    // waitForState predicate self-removing) clears collectionWatches but intentionally RETAINS the cached
    // watchedCollectionStates entry (see method javadoc), and getClusterState() serves that retained entry
    // (getCollectionRefs overlays watchedCollectionStates on top of the lazy ref). Tearing the watch down
    // here would freeze that cached state at its last-applied delta: a consumer that polls getClusterState()
    // without registering its own watcher (e.g. HttpPartitionTest.waitForState) would then never observe a
    // later transition (a replica publishing DOWN during a partition), because no leaf event is delivered
    // and interest never returns to trigger registerStatePlaneWatch's catch-up fetch — the staleness the
    // javadoc calls "bounded" is in fact unbounded for that consumer (ForceLeaderTest DOWN timeout). The
    // watch is still released on the genuine no-more-interest path: colectionRemoved (collection deleted
    // cluster-wide) drops the cached entry AND unregisters the watch directly.
    if (watchedCollectionStates.containsKey(collection)) {
      return;
    }
    unregisterStatePlaneWatch(collection);
    // The scoped-watch arm/release is NOT atomic with the collectionWatches membership decision that
    // routed us here (review finding #4). A concurrent registerCore / registerDocCollectionWatcher for the
    // same collection can re-create the collectionWatches entry AND skip its own arm (statePlaneWatched
    // still held the collection in the window before this teardown removed it) — leaving the collection
    // interested-but-unwatched, which would stall state-plane delivery to a freshly-hosted core. Resolve it
    // by re-checking interest AFTER the teardown: if a register raced us, re-arm. This turns the lost update
    // into a self-correcting reconcile — registerStatePlaneWatch is idempotent and its catch-up fetch covers
    // any gap, and because this teardown removed the collection from statePlaneWatched first, the re-add here
    // (or the racing register's own add) is guaranteed to win exactly once. Teardown is the reconciler.
    if (collectionWatches.containsKey(collection)) {
      registerStatePlaneWatch(collection);
    }
    // The cached watchedCollectionStates entry is intentionally retained — see the method javadoc.
    // (M7 reconsidered evicting it here, but registerCollectionStateWatcher's immediately-firing
    // registration reads it synchronously, so evicting surfaces a transient null to a watcher predicate.)
  }

  /**
   * Reconcile the local collection set against the authoritative children of {@code /collections}
   * (review finding #4). The non-recursive /collections watch reports add/remove only as a child-list
   * change with no per-child path, so we diff: new children become known (collectionAdded), vanished
   * children are removed (colectionRemoved).
   */
  private void refreshCollectionSet() {
    if (closed) {
      return;
    }
    try {
      List<String> children = zkClient.getChildren(COLLECTIONS_ZKNODE, null, true);
      Set<String> live = new HashSet<>(children);
      Set<String> known = getCurrentCollections();
      for (String collection : children) {
        if (!known.contains(collection)) {
          collectionAdded(collection);
        }
      }
      for (String collection : known) {
        if (!live.contains(collection)) {
          colectionRemoved(collection);
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // /collections itself is gone — nothing to reconcile.
    } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
      // A reconnect will re-run createClusterStateWatchersAndUpdate and reconcile from scratch.
      log.debug("Transient ZK error reconciling collection set; reconnect will recover", e);
    } catch (Exception e) {
      log.warn("Could not reconcile collection set after a /collections child change", e);
    }
  }

  private void addSecurityNodeWatcher(final Callable<Pair<byte[], Stat>> callback) throws KeeperException, InterruptedException {
    zkClient.exists(SOLR_SECURITY_CONF_PATH,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events, and do not remove the watcher
            if (closed || EventType.None.equals(event.getType())) {
              return;
            }
            try {

              log.info("Updating [{}] ... ", SOLR_SECURITY_CONF_PATH);

              // remake watch
              final Stat stat = new Stat();
              byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
              if (EventType.NodeDeleted.equals(event.getType())) {
                // Node deleted, just recreate watch without attempting a read - SOLR-9679
                getZkClient().exists(SOLR_SECURITY_CONF_PATH, this);
              } else {
                data = getZkClient().getData(SOLR_SECURITY_CONF_PATH, this, stat);
              }
              try {
                callback.call(new Pair<>(data, stat));
              } catch (Exception e) {
                log.error("Error running collections node listener", e);
                return;
              }

            } catch (KeeperException e) {
              log.error("A ZK error has adding the security node watcher", e);
              return;
            } catch (InterruptedException e) {
              log.warn("", e);
              return;
            }
          }
        });
  }

  private final AtomicReference<Set<String>> lastFetchedCollectionSet = new AtomicReference<>(Collections.emptySet());

  @SuppressWarnings("unchecked")
  private static PropsWatcher.VersionedCollectionProps fetchCollectionProperties(SolrZkClient zkClient, String collection, PropsWatcher watcher) throws KeeperException, InterruptedException {
    final String znodePath = getCollectionPropsPath(collection);
    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, watcher, stat, true, false);
      return new PropsWatcher.VersionedCollectionProps(stat.getVersion(), (Map<String,String>) Utils.fromJSON(data));
    } catch (ClassCastException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to parse collection properties for collection " + collection, e);
    } catch (KeeperException.NoNodeException e) {
      return new PropsWatcher.VersionedCollectionProps(-1, EMPTY_MAP);
    }
  }

  public static String getCollectionPropsPath(final String collection) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/' + COLLECTION_PROPS_ZKNODE;
  }


  /**
   * Register a CloudCollectionsListener to be called when the set of collections within a cloud changes.
   */
  public void registerCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.add(cloudCollectionsListener);
    // Notify synchronously so a freshly registered listener observes the current collection set
    // before this method returns. Dispatching this on the async notifications executor lets the
    // caller race ahead and read a not-yet-populated result.
    cloudCollectionsListener.onChange(Collections.emptySet(), getCurrentCollections());
  }

  /**
   * Remove a registered CloudCollectionsListener.
   */
  public void removeCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.remove(cloudCollectionsListener);
  }

  private void notifyCloudCollectionsListeners() {

    Set<String> newCollections = getCurrentCollections();
    Set<String> oldCollections = lastFetchedCollectionSet.get();

    log.debug("Notify cloud collection listeners {} count={}", newCollections, cloudCollectionsListeners.size());

    cloudCollectionsListeners.forEach(cloudCollectionsListener -> CompletableFuture.runAsync(() -> cloudCollectionsListener.onChange(oldCollections, newCollections)));
  }

  private Set<String> getCurrentCollections() {
    Set<String> collections = new ObjectLinkedOpenHashSet<>(watchedCollectionStates.size() + lazyCollectionStates.size(), .5f) {
    };
    collections.addAll(watchedCollectionStates.keySet());
    collections.addAll(lazyCollectionStates.keySet());
    return collections;
  }

  public class LazyCollectionRef extends ClusterState.CollectionRef {
    private final String collName;
    private long lastUpdateTime;
    private volatile DocCollection cachedDocCollection;

//    public LazyCollectionRef(String collName, DocCollection cachedDocCollection) {
//      super(null);
//      this.collName = collName;
//      this.lastUpdateTime = -1;
//      this.cachedDocCollection = cachedDocCollection;
//    }

    public LazyCollectionRef(String collName) {
      super(null);
      this.collName = collName;
      this.lastUpdateTime = -1;
    }

    @Override
    public CompletableFuture<DocCollection> get(boolean allowCached) {
      log.debug("LAZY Collection GET for {}", collName);
      gets.incrementAndGet();
      if (!allowCached || lastUpdateTime < 0 || System.nanoTime() - lastUpdateTime > LAZY_CACHE_TIME) {

        try {
          return getCollectionLive(collName).thenApplyAsync(docCollection -> {
            lastUpdateTime = System.nanoTime();
            cachedDocCollection = docCollection;
            return docCollection;
          });

        } catch (Exception e) {
          return CompletableFuture.failedFuture(new SolrException(ErrorCode.SERVER_ERROR, e));
        }
      }

      return CompletableFuture.completedFuture(cachedDocCollection);
    }


    public DocCollection getCachedDocCollection() {
      return cachedDocCollection;
    }

    @Override
    public boolean isLazilyLoaded() {
      return true;
    }

    @Override
    public String toString() {
      return "LazyCollectionRef(" + collName + "::" + cachedDocCollection + ')';
    }
  }

  /**
   * Refresh live_nodes.
   */
  private void refreshLiveNodes() throws KeeperException, InterruptedException {

    Set<String> oldLiveNodes = Collections.emptySet();
    AtomicReference<Set<String>> newLiveNodes = new AtomicReference<>();
    try {

      Stat existsStat = zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null, true);
      AtomicBoolean continueOn = new AtomicBoolean(false);
      liveNodesVersion.updateAndGet(cversion -> {
        if (existsStat != null) {
          if (existsStat.getCversion() > cversion) {
            continueOn.set(true);
            return cversion;
          }
        }
        return cversion;
      });

      if (!continueOn.get()) {
        return;
      }

      continueOn.set(false);

      Stat childrenStat = new Stat();
      List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, null, childrenStat, true);

      AtomicReference<Set<String>> oldLiveNodesRef = new AtomicReference<>();
      liveNodesVersion.updateAndGet(cversion -> {
        if (childrenStat.getCversion() > cversion) {
          // Publish liveNodes inside the version-CAS so the field and liveNodesVersion advance
          // together; otherwise a concurrent refresher can leave liveNodes older than the
          // version claims and a node-down event is missed until the next children-changed.
          // (updateAndGet may retry this lambda, so it must only mutate via this CAS winner.)
          oldLiveNodesRef.set(this.liveNodes);
          newLiveNodes.set(new TreeSet<>(nodeList));
          this.liveNodes = newLiveNodes.get();
          continueOn.set(true);
          return childrenStat.getCversion();
        }
        return cversion;
      });

      if (!continueOn.get()) {
        return;
      }

      oldLiveNodes = oldLiveNodesRef.get();

    } catch (KeeperException.NoNodeException e) {
      log.warn("No node exception in live nodes watcher", e);
      return;
    }

    if (log.isInfoEnabled()) {
      log.info("Updated live nodes from ZooKeeper... ({}) -> ({})", oldLiveNodes.size(), newLiveNodes.get().size());
    }

    if (log.isTraceEnabled()) {
      log.trace("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
    }

    if (log.isDebugEnabled()) log.debug("Fire live node listeners");
    Set<String> finalNewLiveNodes = newLiveNodes.get();
    Set<String> finalOldLiveNodes = oldLiveNodes;
    liveNodesListeners.forEach(listener -> notifications.submit(() -> {
      if (listener.onChange(new TreeSet<>(finalOldLiveNodes), new TreeSet<>(finalNewLiveNodes))) {
        removeLiveNodesListener(listener);
      }
    }));
  }

  public void registerClusterPropertiesListener(ClusterPropertiesListener listener) {
    // fire it once with current properties
    if (listener.onChange(getClusterProperties())) {
      removeClusterPropertiesListener(listener);
    } else {
      clusterPropertiesListeners.add(listener);
    }
  }

  public void removeClusterPropertiesListener(ClusterPropertiesListener listener) {
    clusterPropertiesListeners.remove(listener);
  }

  public void registerLiveNodesListener(LiveNodesListener listener) {
    // fire it once with current live nodes

    if (listener.onChange(new TreeSet<>(), new TreeSet<>(liveNodes))) {
      removeLiveNodesListener(listener);
    }

    liveNodesListeners.add(listener);
  }

  public void removeLiveNodesListener(LiveNodesListener listener) {
    liveNodesListeners.remove(listener);
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return new ClusterState(lazyCollectionStates, watchedCollectionStates);
  }

  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing ZkStateReader");
    assert closeTracker != null ? closeTracker.close() : true;

    closed = true;
    try {
//      if (createdCollectionsWatch) {
//        try {
//          zkClient.removeWatches(ZkStateReader.COLLECTIONS_ZKNODE, this, Watcher.WatcherType.Any, true);
//        } catch (Exception e) {
//          log.debug("Exception removing watch on close", e);
//        }
//      }
      IOUtils.closeQuietly(zkStateReaderQueue);
      IOUtils.closeQuietly(this.liveNodesWatcher);
      IOUtils.closeQuietly(clusterPropertiesWatcher);
      Future<?> cpc = collectionPropsCacheCleaner;
      if (cpc != null) {
        cpc.cancel(true);
      }
      collectionWatches.forEach((s, collectionWatch) -> {
        collectionWatch.stateWatchers.forEach(docCollectionWatcher -> {
          if (docCollectionWatcher instanceof DocCollectionAndLiveNodesWatcherWrapper) {
            IOUtils.closeQuietly((Closeable) docCollectionWatcher);
          }
        });
      });
      if (closeClient) {
        IOUtils.closeQuietly(zkClient);
      }
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

  public String getLeaderUrl(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    Replica replica = getLeaderRetry(collection, shard, timeout);
    return replica.getCoreUrl();
  }

  public Replica getLeader(String collection, String shard) {
    try {
      return getLeaderRetry(collection, shard, 3000);
    } catch (InterruptedException | TimeoutException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

//  public Replica getLeader(String collection, String shard) {
//    if (clusterState != null) {
//      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
//      Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
//      if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
//        return replica;
//      }
//    }
//    return null;
//  }

  public boolean isNodeLive(String node) {
    return getLiveNodes().contains(node);
  }

  public void setNode(String node) {
    this.node = node;
  }

  public void setLeaderChecker(IsLocalLeader isLocalLeader) {
    this.isLocalLeader = isLocalLeader;
  }

  public interface IsLocalLeader {
    boolean isLocalLeader(String name);
  }


  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, GET_LEADER_RETRY_DEFAULT_TIMEOUT, false);
  }

  public Replica getLeaderRetry(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, timeout, false);
  }

  public Replica getLeaderRetry(String collection, String shard, int timeout, boolean checkValidLeader) throws InterruptedException, TimeoutException {
    return getLeaderRetry(null, collection, shard, timeout, checkValidLeader);
  }
  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(Http2SolrClient client, String collection, String shard, int timeout, boolean checkValidLeader) throws InterruptedException, TimeoutException {
    log.debug("get leader timeout={}", timeout);

    DocCollection coll;

    boolean closeHttpClient = false;
    if (checkValidLeader && client == null) {
      closeHttpClient = true;
      client = new Http2SolrClient.Builder().markInternalRequest().build();
    }
    try {
      TimeOut leaderVerifyTimeout = null;
      if (checkValidLeader) {
        leaderVerifyTimeout = new TimeOut(timeout, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      }
      CoreAdminRequest.WaitForState prepCmd = null;
      AtomicReference<DocCollection> state = new AtomicReference<>();
      AtomicReference<Replica> returnLeader = new AtomicReference<>();
      while (!closed) {
        state.set(null);
        returnLeader.set(null);
        final boolean higherLevelClosed = higherLevelClosed();
        if (higherLevelClosed) break;
        returnLeader.set(null);
        try {
          waitForState(collection, timeout, TimeUnit.MILLISECONDS, (n, c) -> {
            if (log.isDebugEnabled()) {
              log.debug("wait for leader notified collection={}  watchedCollections={} lazyCollections={} collectionWatches={}", c == null ? "(null)" : c, watchedCollectionStates, lazyCollectionStates, collectionWatches);
            }
            state.set(c);
            if (c == null) return false;
            Slice slice = c.getSlice(shard);
            if (slice == null) return false;
            Replica leader = slice.getLeader(liveNodes);

            if (leader != null && leader.getState() == Replica.State.ACTIVE) {
              log.debug("Found ACTIVE leader for slice={} leader={}", slice.getName(), leader);
              returnLeader.set(leader);
              return true;
            }

            return false;
          });
        } catch (TimeoutException e) {
          coll = watchedCollectionStates.get(collection);
          log.debug("timeout out while waiting to see leader in cluster state {} {}", shard, coll);
          throw new TimeoutException(
              "No registered leader was found after waiting for " + timeout + "ms " + ", collection: " + collection + " slice: " + shard + " saw state=" + coll + " with live_nodes=" + liveNodes);
        }

        Replica leader = returnLeader.get();
        if (leader == null) {
          log.debug("return leader is null");
          throw new TimeoutException(
              "No registered leader was found " + "collection: " + collection + " slice: " + shard + " saw state=" + watchedCollectionStates.get(collection) + " with live_nodes=" + liveNodes);
        }

        DocCollection lastState = state.get();
        int replicaCount = lastState.getSlice(shard).getReplicas().size();
        if (checkValidLeader &&  replicaCount > 1) {

          log.debug("checking if found leader is valid {} {}", leader, replicaCount);
          if (isLocalLeader != null && leader.getNodeName().equals(node)) {
            if (isLocalLeader.isLocalLeader(leader.getName())) {
              log.debug("leader is local and valid {}", leader);
              return leader;
            }
          }

          if (leaderVerifyTimeout.hasTimedOut()) {
            log.debug("timeout out while checking if found leader is valid {}", leader);
            throw new TimeoutException(
                "No registered leader was found " + "collection: " + collection + " slice: " + shard + " saw state=" + watchedCollectionStates.get(collection) + " with live_nodes=" + liveNodes);
          }
          if (prepCmd == null) {
            prepCmd = new CoreAdminRequest.WaitForState();
          }
          prepCmd.setCoreName(leader.getName());
          prepCmd.setLeaderName(leader.getName());
          prepCmd.setCoresCollection(collection);
          prepCmd.setBasePath(leader.getBaseUrl());
          prepCmd.setCheckIsLeader(true);
          try {
            CoreAdminResponse result = prepCmd.process(client);

            log.debug("check leader result {}", result);

            boolean success = "0".equals(result.getResponse().get("STATUS"));

            if (!success) {
              log.debug("leader found is not valid {}", leader);
              if (leaderVerifyTimeout.hasTimedOut()) {
                log.debug("timeout out while checking if found leader is valid {}", leader);
                throw new TimeoutException(
                    "No registered leader was found " + "collection: " + collection + " slice: " + shard + " saw state=" + watchedCollectionStates.get(collection) + " with live_nodes=" + liveNodes);
              }
              Thread.sleep(250);
              continue;
            }

            return leader;
          } catch (RejectedExecutionException | AlreadyClosedException e) {
            log.warn("Rejected or already closed, bailing {} {}, {}", leader.getId(), leader.getName(), e.getClass().getSimpleName());
            throw e;
          } catch (Exception e) {
            log.info("failed checking for leader {}, {} {}", leader.getId(), leader.getName(), e.getMessage());
            Thread.sleep(250);
            continue;
          }

        } else {
          return leader;
        }
      }
    } finally {
//      if (checkValidLeader) {
//        unregisterCore(collection, null);
//      }
      if (closeHttpClient) {
        IOUtils.closeQuietly(client);
      }
    }

    throw new TimeoutException(
        "No registered leader was found and verified" + "collection: " + collection + " slice: " + shard + " see state=" + watchedCollectionStates.get(collection) + " with live_nodes=" + liveNodes);

  }

  private boolean higherLevelClosed() {
    return zkClient.getHigherLevelIsClosed() != null && zkClient.getHigherLevelIsClosed().isClosed();
  }

  /**
   * Get path where shard leader properties live in zookeeper.
   */
  public static String getShardLeadersPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/'
        + SHARD_LEADERS_ZKNODE + (shardId != null ? ('/' + shardId)
        : "") + "/leader";
  }

  /**
   * Get path where shard leader elections ephemeral nodes are.
   */
  public static String getShardLeadersElectPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/'
        + LEADER_ELECT_ZKNODE + (shardId != null ? ('/' + shardId + '/' + ELECTION_NODE)
        : "");
  }


  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter, Replica.State mustMatchStateFilter2) {
    //TODO: We don't need all these getReplicaProps method overloading. Also, it's odd that the default is to return replicas of type TLOG and NRT only
    Set<Replica.State> matchFilters = new HashSet<>(2);
    matchFilters.add(mustMatchStateFilter);
    matchFilters.add(mustMatchStateFilter2);
    return getReplicaProps(collection, shardId, thisCoreNodeName, matchFilters, EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT));
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Collection<Replica.State> matchStateFilters, final EnumSet<Replica.Type> acceptReplicaType) {
    assert thisCoreNodeName != null;

    ClusterState.CollectionRef docCollectionRef = allCollections.get(collection);
    if (docCollectionRef == null) {
      return null;
    }
    final DocCollection docCollection;
    try {
      docCollection = docCollectionRef.get().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) return null;
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    if (docCollection == null) return null;
    if (docCollection.getSlicesMap() == null) {
      return null;
    }
    Map<String, Slice> slices = docCollection.getSlicesMap();
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      return null;
    }

    Map<String, Replica> shardMap = replicas.getReplicasMap();
    List<Replica> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String, Replica> entry : shardMap.entrySet().stream().filter((e) -> acceptReplicaType.contains(e.getValue().getType())).collect(Collectors.toList())) {
      Replica nodeProps = entry.getValue();

      String coreNodeName = entry.getValue().getName();

      if (liveNodes.contains(nodeProps.getNodeName()) && !coreNodeName.equals(thisCoreNodeName)) {
        if (matchStateFilters == null || matchStateFilters.isEmpty() || matchStateFilters.contains(nodeProps.getState())) {
          nodes.add(nodeProps);
        }
      }
    }
    if (nodes.size() == 0) {
      // no replicas
      return null;
    }

    return nodes;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * Get a cluster property
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @param key          the property to read
   * @param defaultValue a default value to use if no such property exists
   * @param <T>          the type of the property
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Same as the above but allows a full json path as a list of parts
   *
   * @param keyPath      path to the property example ["collectionDefauls", "numShards"]
   * @param defaultValue a default value to use if no such property exists
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings({"unchecked"})
  public <T> T getClusterProperty(List<String> keyPath, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, keyPath);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Get all cluster properties for this cluster
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @return a Map of cluster properties
   */
  public Map<String, Object> getClusterProperties() {
    return Collections.unmodifiableMap(clusterProperties);
  }

  private final ClusterPropsWatcher clusterPropertiesWatcher = new ClusterPropsWatcher(ZkStateReader.CLUSTER_PROPS);

  @SuppressWarnings("unchecked")
  private void loadClusterProperties() {
    try {
        try {
          byte[] data = zkClient.getData(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, new Stat(), true);
          this.clusterProperties = ClusterProperties.convertCollectionDefaultsToNestedFormat((Map<String, Object>) Utils.fromJSON(data));
          log.debug("Loaded cluster properties: {}", this.clusterProperties);
          updateUrlScheme();
          clusterPropertiesListeners.forEach((it) -> notifications.submit(()-> it.onChange(getClusterProperties())));
        } catch (KeeperException.NoNodeException e) {
          this.clusterProperties = Collections.emptyMap();
          updateUrlScheme();
          if (log.isDebugEnabled()) {
            log.debug("Loaded empty cluster properties");
          }
        }
    } catch (KeeperException e) {
      log.error("Error reading cluster properties from zookeeper", SolrZkClient.checkInterrupted(e));
    } catch (InterruptedException e) {
      log.info("interrupted");
    }

  }

  /** Refresh the cached {@link #urlScheme} from the current {@link #clusterProperties} (default "http"). */
  private void updateUrlScheme() {
    Object scheme = clusterProperties.get(URL_SCHEME);
    urlScheme = (scheme instanceof String && !((String) scheme).isEmpty()) ? (String) scheme : "http";
  }

  /**
   * Get collection properties for a given collection. If the collection is watched, simply return it from the cache,
   * otherwise fetch it directly from zookeeper. This is a convenience for {@code getCollectionProperties(collection,0)}
   *
   * @param collection the collection for which properties are desired
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection) {
    return getCollectionProperties(collection, 0);
  }

  /**
   * Get and cache collection properties for a given collection. If the collection is watched, or still cached
   * simply return it from the cache, otherwise fetch it directly from zookeeper and retain the value for at
   * least cacheForMillis milliseconds. Cached properties are watched in zookeeper and updated automatically.
   * This version of {@code getCollectionProperties} should be used when properties need to be consulted
   * frequently in the absence of an active {@link CollectionPropsWatcher}.
   *
   * @param collection     The collection for which properties are desired
   * @param cacheForMillis The minimum number of milliseconds to maintain a cache for the specified collection's
   *                       properties. Setting a {@code CollectionPropsWatcher} will override this value and retain
   *                       the cache for the life of the watcher. A lack of changes in zookeeper may allow the
   *                       caching to remain for a greater duration up to the cycle time of {@link CacheCleaner}.
   *                       Passing zero for this value will explicitly remove the cached copy if and only if it is
   *                       due to expire and no watch exists. Any positive value will extend the expiration time
   *                       if required.
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection, long cacheForMillis) {
    PropsWatcher watcher = null;
    if (cacheForMillis > 0) {
      watcher = collectionPropsWatchers.compute(collection, (c, w) -> w == null ? new PropsWatcher(zkClient, notifications, node, c, cacheForMillis, watchedCollectionProps,
          collectionPropsObservers, collectionPropsWatchers) : w.renew(cacheForMillis));
    }
    PropsWatcher.VersionedCollectionProps vprops = watchedCollectionProps.get(collection);
    boolean haveUnexpiredProps = vprops != null && vprops.cacheUntilNs > System.nanoTime();
    long untilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(cacheForMillis, TimeUnit.MILLISECONDS);
    Map<String,String> properties;
    if (haveUnexpiredProps || (vprops != null && !zkClient.isConnected())) {
      properties = vprops.props;
      vprops.cacheUntilNs = Math.max(vprops.cacheUntilNs, untilNs);
    } else {
      try {
        PropsWatcher.VersionedCollectionProps vcp = fetchCollectionProperties(zkClient, collection, watcher);
        properties = vcp.props;
        if (cacheForMillis > 0) {
          vcp.cacheUntilNs = untilNs;
          watchedCollectionProps.put(collection, vcp);
        } else {
          // we're synchronized on watchedCollectionProps and we can only get here if we have found an expired
          // vprops above, so it is safe to remove the cached value and let the GC free up some mem a bit sooner.
          if (!collectionPropsObservers.containsKey(collection)) {
            watchedCollectionProps.remove(collection);
          }
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading collection properties", SolrZkClient.checkInterrupted(e));
      }
    }
    return properties;
  }

  /**
   * Returns the content of /security.json from ZooKeeper as a Map
   * If the files doesn't exist, it returns null.
   */
  @SuppressWarnings({"unchecked"})
  public ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ConfigData(EMPTY_MAP, -1);
      return new ConfigData(securityData.data, securityData.version);
    }
    try {
      Stat stat = new Stat();
      if (zkClient.exists(SOLR_SECURITY_CONF_PATH, true)) {
        final byte[] data = zkClient.getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, stat, true);
        return data != null && data.length > 0 ?
            new ConfigData((Map<String, Object>) Utils.fromJSON(data), stat.getVersion()) :
            null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    }
    return null;
  }

  /**
   * Returns the baseURL corresponding to a given node's nodeName --
   * NOTE: does not (currently) imply that the nodeName (or resulting
   * baseURL) exists in the cluster.
   *
   * @lucene.experimental
   */
  @Override
  public String getBaseUrlForNodeName(final String nodeName) {
    return Utils.getBaseUrlForNodeName(nodeName, getClusterProperty(URL_SCHEME, "http"));
  }

  /**
   * Watches collection properties
   */
  public final static class PropsWatcher implements Watcher, Closeable {

    private final SolrZkClient zkClient;

    private final String coll;
    private final Map<String,CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers;
    private final ExecutorService notifications;
    private final String node;
    private final Map<String,PropsWatcher> collectionPropsWatchers;
    private long watchUntilNs;

    /**
     * Collection properties being actively watched
     */
    private final Map<String, VersionedCollectionProps> watchedCollectionProps;

    PropsWatcher(SolrZkClient zkClient, ExecutorService notifications, String node, String coll, long forMillis, Map<String,
        VersionedCollectionProps> watchedCollectionProps, Map<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers, Map<String, PropsWatcher> collectionPropsWatchers ) {
      this.coll = coll;
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
      this.collectionPropsObservers = collectionPropsObservers;
      this.zkClient = zkClient;
      this.watchedCollectionProps = watchedCollectionProps;
      this.notifications = notifications;
      this.node = node;
      this.collectionPropsWatchers = collectionPropsWatchers;
    }

    public PropsWatcher renew(long forMillis) {
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
      return this;
    }

    @Override
    public void process(WatchedEvent event) {
      boolean expired = System.nanoTime() > watchUntilNs;
      if (!collectionPropsObservers.containsKey(coll) && expired) {
        // No one can be notified of the change, we can ignore it and "unset" the watch
        log.debug("Ignoring property change for collection {}", coll);
        return;
      }

      log.info("A collection property change: [{}] for collection [{}] has occurred - updating...",
          event, coll);

      refreshAndWatch(true);
    }

    @Override
    public void close() throws IOException {
      String znodePath = getCollectionPropsPath(coll);

      try {
        zkClient.removeWatches(znodePath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }

    public Map<String,VersionedCollectionProps> getWatchedCollectionProps() {
      return watchedCollectionProps;
    }

    private void notifyPropsWatchers(String collection, Map<String, String> collectionProperties) {
      try {
        notifications.submit(new PropsNotification(collection, node, collectionProperties,  collectionPropsObservers, watchedCollectionProps));
      } catch (RejectedExecutionException e) {
        log.debug("RejectedExecutionException {}", collection, e);
      }
    }

    /**
     * Refresh collection properties from ZK and leave a watch for future changes. Updates the properties in
     * watchedCollectionProps with the results of the refresh. Optionally notifies watchers
     */
    void refreshAndWatch(boolean notifyWatchers) {
      try {

        VersionedCollectionProps vcp = fetchCollectionProperties(zkClient, coll, this);
        Map<String,String> properties = vcp.props;
        VersionedCollectionProps existingVcp = watchedCollectionProps.get(coll);
        if (existingVcp == null ||                   // never called before, record what we found
            vcp.zkVersion > existingVcp.zkVersion || // newer info we should update
            vcp.zkVersion == -1) {                   // node was deleted start over
          watchedCollectionProps.put(coll, vcp);
          if (notifyWatchers) {
            notifyPropsWatchers(coll, properties);
          }
          if (vcp.zkVersion == -1 && existingVcp != null) { // Collection DELETE detected

            // We should not be caching a collection that has been deleted.
            watchedCollectionProps.remove(coll);

            // core ref counting not relevant here, don't need canRemove(), we just sent
            // a notification of an empty set of properties, no reason to watch what doesn't exist.
            collectionPropsObservers.remove(coll);

            // This is the one time we know it's safe to throw this out. We just failed to set the watch
            // due to an NoNodeException, so it isn't held by ZK and can't re-set itself due to an update.
            IOUtils.closeQuietly(collectionPropsWatchers.remove(coll));
          }
        }

      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("Lost collection property watcher for {} due to ZK error", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred refreshing property watcher", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Lost collection property watcher for {} due to the thread being interrupted", coll, e);
      }
    }

    private static class VersionedCollectionProps {
      int zkVersion;
      Map<String, String> props;
      long cacheUntilNs = 0;

      VersionedCollectionProps(int zkVersion, Map<String, String> props) {
        this.zkVersion = zkVersion;
        this.props = props;
      }
    }
  }

  /**
   * Watches /collections children .
   */
  // MRM TODO: persistent watch
//  class CollectionsChildWatcher implements Closeable {
//
//    private volatile boolean checkAgain = false;
//    private volatile boolean running;
//    private final ReentrantLock refreshLock = new ReentrantLock();
//
//    public void processEvent(WatchedEvent event) {
//      if (closed || zkClient.isClosed() || event.getType() == Event.EventType.None) {
//        return;
//      }
//
//      if (node != null) {
//        MDCLoggingContext.setNode(node);
//      }
//
//      if (log.isDebugEnabled()) log.debug("A collections change: [{}], has occurred - updating...", event);
//
//      if (running) {
//        checkAgain = true;
//      } else {
//        running = true;
//        ParWork.getRootSharedExecutor().submit(() -> {
//          try {
//            do {
//              try {
//                refresh();
//              } catch (Exception e) {
//                log.error("An error has occurred", e);
//                return;
//              }
//
//              if (!checkAgain) {
//                running = false;
//                break;
//              }
//              checkAgain = false;
//
//            } while (true);
//
//          } catch (Exception e) {
//            log.error("exception submitting queue task", e);
//          }
//
//        });
//      }
//
//    }
//
//    public void refresh() {
//      refreshLock.lock();
//      try {
//        refreshCollectionList();
//
//    //    notifyStateUpdated(null, "collection child watcher");
//      } catch (AlreadyClosedException e) {
//
//      } catch (KeeperException e) {
//        log.error("A ZK error has occurred refreshing CollectionsChildWatcher", e);
//        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred refreshing CollectionsChildWatcher", e);
//      } catch (InterruptedException e) {
//        // Restore the interrupted status
//        Thread.currentThread().interrupt();
//        log.warn("Interrupted", e);
//      } finally {
//        refreshLock.unlock();
//      }
//    }
//
//
//    @Override
//    public void close() throws IOException {
//
//    }
//  }

  /**
   * Watches the live_nodes and syncs changes.
   */
  class LiveNodeWatcher implements Watcher, Closeable {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || !EventType.NodeChildrenChanged.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      if (log.isDebugEnabled()) {
        log.debug("A live node change: [{}], has occurred - updating... (previous live nodes size: [{}])", event, liveNodes.size());
      }
      refresh();
    }

    public void refresh() {
      try {
        refreshLiveNodes();
      } catch (KeeperException.SessionExpiredException e) {
        // okay
      } catch (Exception e) {
        log.error("A ZK error has occurred refreshing CollectionsChildWatcher", e);
      }
    }

    public void createWatch() {
      if (closed) {
        return;
      }
      try {
        zkClient.addWatch(LIVE_NODES_ZKNODE, this, AddWatchMode.PERSISTENT, true, false);
      } catch (Exception e) {
        log.warn("Exception creating watch", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Exception creating watch", e);
      }
    }

    public void removeWatch() {
      try {
        zkClient.removeWatches(LIVE_NODES_ZKNODE, this, WatcherType.Any,true);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.warn("Exception removing watch", e);
      }
    }

    @Override
    public void close() throws IOException {
      removeWatch();
    }
  }

  public CompletableFuture<DocCollection> getCollectionLive(String coll) {
    log.debug("getCollectionLive {}", coll);
    docCollLiveUpdateRequests.mark();
    try {
      return zkStateReaderQueue.fetchCollectionState(coll);
    } catch (Exception e) {
      log.error("Exception getting fetching collection state: [{}]", coll, e);
      return CompletableFuture.failedFuture(new SolrException(ErrorCode.SERVER_ERROR, "Exception getting fetching collection state: " + coll, e));
    }
  }

  public Map<String,ClusterState.CollectionRef> getCollectionRefs() {
    Map<String,ClusterState.CollectionRef> result = new LinkedHashMap<>(lazyCollectionStates.size() + watchedCollectionStates.size());
    result.putAll(lazyCollectionStates);
    watchedCollectionStates.forEach((c, docCollection) -> {
      try {
        result.put(c, new ClusterState.CollectionRef(docCollection));
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    });
    return result;
  }

  public static String getCollectionPathRoot(String coll) {
    return COLLECTIONS_ZKNODE + '/' + coll;
  }

  public static String getCollectionPath(String coll) {
    return getCollectionPathRoot(coll) + STATE_JSON;
  }

  public static String getCollectionSCNPath(String coll) {
    return getCollectionPathRoot(coll) + '/' + STRUCTURE_CHANGE_NOTIFIER;
  }

  /**
   * Notify this reader that a local Core is a member of a collection, and so that collection
   * state should be watched.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * The number of cores per-collection is tracked, and adding multiple cores from the same
   * collection does not increase the number of watches.
   *
   * @param collection the collection that the core is a member of
   * @see ZkStateReader#unregisterCore(String, String)
   */
  public void registerCore(String collection, String coreName) {

    if (log.isDebugEnabled()) log.debug("register core for collection {} {}", collection, coreName);
    if (collection == null) {
      throw new IllegalArgumentException(COLLECTION_CANNOT_BE_NULL);
    }

    if (coreName == null || registeredCores.add(coreName)) {

      collectionWatches.compute(collection, (k, v) -> {
        if (v == null) {
          v = new CollectionWatch<>(collection);
        }

        v.coreRefCount.incrementAndGet();
        return v;
      });
      // This node now hosts a core for the collection — arm its scoped state-plane watch (finding #4).
      registerStatePlaneWatch(collection);
    }
  }

  public boolean watched(String collection) {
    return collectionWatches.containsKey(collection);
  }

  /**
   * Notify this reader that a local core that is a member of a collection has been closed.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * If no cores are registered for a collection, and there are no {@link org.apache.solr.common.cloud.CollectionStateWatcher}s
   * for that collection either, the collection watch will be removed.
   *
   * @param collection the collection that the core belongs to
   * @param coreName the name of the core
   */
  public void unregisterCore(String collection, String coreName) {
    if (collection == null) {
      throw new IllegalArgumentException(COLLECTION_CANNOT_BE_NULL);
    }

    if (coreName == null || registeredCores.remove(coreName)) {

      boolean[] removed = {false};
      collectionWatches.compute(collection, (k, v) -> {

        if (v == null) return v;
        if (v.coreRefCount.get() > 0) v.coreRefCount.decrementAndGet();
        if (v.canBeRemoved(watchedCollectionStates.size())) {
          log.debug("no longer watch collection {}", collection);
          LazyCollectionRef docRef = new LazyCollectionRef(collection);
          lazyCollectionStates.put(collection, docRef);
          removed[0] = true;
          return null;
        }
        return v;
      });
      if (removed[0]) {
        releaseLocalInterest(collection);
      }
    }
  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   * <em>or</em> the set of live nodes changes.
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   *
   * <p>
   * This is method is just syntactic sugar for registering both a {@link DocCollectionWatcher} and
   * a {@link LiveNodesListener}.  Callers that only care about one or the other (but not both) are
   * encouraged to use the more specific methods register methods as it may reduce the number of
   * ZooKeeper watchers needed, and reduce the amount of network/cpu used.
   * </p>
   *
   * @see #registerDocCollectionWatcher
   * @see #registerLiveNodesListener
   */
  public void registerCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher stateWatcher) {
    registerCollectionStateWatcher(collection, stateWatcher, null);
  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   * <em>or</em> the set of live nodes changes.
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   *
   * <p>
   * This is method is just syntactic sugar for registering both a {@link DocCollectionWatcher} and
   * a {@link LiveNodesListener}.  Callers that only care about one or the other (but not both) are
   * encouraged to use the more specific methods register methods as it may reduce the number of
   * ZooKeeper watchers needed, and reduce the amount of network/cpu used.
   * </p>
   *
   * @see #registerDocCollectionWatcher
   * @see #registerLiveNodesListener
   */
  public void registerCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher stateWatcher, SolrCountDownLatch latch) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper = new DocCollectionAndLiveNodesWatcherWrapper(collection, stateWatcher, latch);

    try {
      // notifyOnRegister=false: this method does its own on-register notify below (via stateWatcher)
      // and owns the combined doc-watcher + live-nodes-listener cleanup through
      // removeCollectionStateWatcher, so letting registerDocCollectionWatcher also notify would
      // both double-fire the delegate and risk leaking the live-nodes listener on auto-remove.
      registerDocCollectionWatcher(collection, wrapper, false);
    } catch (InterruptedException e) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, e);
    }
    registerLiveNodesListener(wrapper);

    DocCollection state = watchedCollectionStates.get(collection);

    if (state != null && stateWatcher.onStateChanged(liveNodes, state)) {
      removeCollectionStateWatcher(collection, stateWatcher);
    }
  }

  /**
   * Register a DocCollectionWatcher to be called when the state of a collection changes
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   */
  public void registerDocCollectionWatcher(String collection, DocCollectionWatcher docCollectionWatcher) throws InterruptedException {
    registerDocCollectionWatcher(collection, docCollectionWatcher, true);
  }

  /**
   * @param notifyOnRegister when true, the watcher is invoked synchronously with the currently
   *     known state if one is already cached locally. The {@link #registerCollectionStateWatcher}
   *     path passes false because it performs its own on-register notification (via the wrapped
   *     {@link org.apache.solr.common.cloud.CollectionStateWatcher}) and owns the combined
   *     doc-watcher + live-nodes-listener cleanup; double-notifying it would fire the delegate twice.
   */
  private void registerDocCollectionWatcher(String collection, DocCollectionWatcher docCollectionWatcher, boolean notifyOnRegister) throws InterruptedException {
    log.debug("registerDocCollectionWatcher {}", collection);

    if (collection == null) {
      throw new IllegalArgumentException(COLLECTION_CANNOT_BE_NULL);
    }

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>(collection);
      }
      v.stateWatchers.add(docCollectionWatcher);
      v.coreRefCount.incrementAndGet();
      return v;
    });
    // Arm the scoped state-plane watch BEFORE the catch-up fetch below so no leaf event between the
    // fetch and the watch can be lost (finding #4).
    registerStatePlaneWatch(collection);

    DocCollection state = watchedCollectionStates.get(collection);

    if (state == null) {
      zkStateReaderQueue.fetchStateUpdates(collection, false);
    } else if (notifyOnRegister) {
      // The state is already cached locally; the watcher contract requires that a freshly
      // registered watcher observes the current state without waiting for the next change event.
      if (docCollectionWatcher.onStateChanged(state)) {
        removeDocCollectionWatcher(collection, docCollectionWatcher);
      }
    }
  }

  public DocCollection getCollection(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection : " + collection);
    return coll;
  }

  public DocCollection getCollectionOrNull(String collection) {
    ClusterState.CollectionRef coll = allCollections.get(collection);
    if (coll == null) return null;
    try {
      return coll.get().get();
    } catch (ExecutionException e) {
      // A lazy ref resolves state.json directly from ZK. During a reconnect/refresh window the
      // node can be transiently absent. The contract of this method is "or null", so treat
      // NoNode as a non-existent collection rather than surfacing a spurious 500.
      // Mirrors the already-hardened ClusterState.getCollectionsMap / forEachCollection.
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        return null;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

//  public ClusterState.CollectionRef getCollectionRef(String collection) {
//    return allCollections.get(collection);
//  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * <p>
   * This implementation utilizes {@link org.apache.solr.common.cloud.CollectionStateWatcher} internally.
   * Callers that don't care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate}
   * instead
   * </p>
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   * @see #registerCollectionStateWatcher
   *
   *
   */
  public void waitForState(final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {
    if (closed) throw new AlreadyClosedException();
    // section waitforstate
    MDCLoggingContext.setNode(node);
    log.debug("wait for state for {}", collection);
    TimeOut timeout = new TimeOut(wait, unit, TimeSource.NANO_TIME);

    DocCollection state = watchedCollectionStates.get(collection);

    if (predicate.matches(getLiveNodes(), watchedCollectionStates.get(collection))) {
      return;
    }

    final SolrCountDownLatch latch = new SolrCountDownLatch(1);
    AtomicReference<DocCollection> docCollection = new AtomicReference<>();
    org.apache.solr.common.cloud.CollectionStateWatcher watcher = new PredicateMatcher(predicate, latch, docCollection).invoke();
    registerCollectionStateWatcher(collection, watcher, latch);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException("Timeout waiting to see state for collection=" + collection + " :" + docCollection.get());

    } finally {
      removeCollectionStateWatcher(collection, watcher);
    }
  }

  private static class SolrCountDownLatch extends CountDownLatch {
    private volatile String closed;

    /**
     * Constructs a {@code CountDownLatch} initialized with the given count.
     *
     * @param count the number of times {@link #countDown} must be invoked
     *              before threads can pass through {@link #await}
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public SolrCountDownLatch(int count) {
      super(count);
    }

    public boolean await(long timeout, TimeUnit unit)
        throws InterruptedException {
      boolean val = super.await(timeout, unit);
      if (closed != null) {
        throw new AlreadyClosedException(closed);
      }
      return val;
    }

    public void close(String reason) throws IOException {
      closed = reason;
      countDown();
    }
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas) throws TimeoutException {
    waitForActiveCollection(collection, wait, unit, shards, totalReplicas, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas, boolean exact) throws TimeoutException {
    waitForActiveCollection(collection, wait, unit, false, shards, totalReplicas, exact, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, boolean justLeaders,  int shards, int totalReplicas, boolean exact, boolean checkValidLeaders)
      throws TimeoutException {
    waitForActiveCollection(null, collection, wait, unit, justLeaders, shards, totalReplicas, exact, checkValidLeaders);
  }

  public void waitForActiveCollection(Http2SolrClient client, String collection, long wait, TimeUnit unit, boolean justLeaders,  int shards, int totalReplicas, boolean exact, boolean checkValidLeaders)
      throws TimeoutException {
    log.debug("waitForActiveCollection: {} interesting [{}] watched [{}] lazy [{}] total [{}] lazy={}", collection, collectionWatches.keySet().size(), watchedCollectionStates.size(), lazyCollectionStates.keySet().size(),
        allCollections.size(), lazyCollectionStates.containsKey(collection));

    boolean closeHttpClient = false;
    if (checkValidLeaders && client == null) {
      client = new Http2SolrClient.Builder().markInternalRequest().build();
      closeHttpClient = true;
    }
//    if (checkValidLeaders) {
//      registerCore(collection, null);
//    }
    try {
      AtomicReference<Object> state = new AtomicReference<>();
      AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
      Set<Replica> returnLeaders = ConcurrentHashMap.newKeySet();
      TimeOut leaderVerifyTimeout = new TimeOut(wait, unit, TimeSource.NANO_TIME);
      while (!closed) {
        returnLeaders.clear();
        CollectionStatePredicate predicate = expectedShardsAndActiveReplicas(justLeaders, shards, totalReplicas, exact, returnLeaders);
        state.set(null);
        liveNodesLastSeen.set(null);
        try {
          waitForState(collection, wait, unit, (n, c) -> {
            state.set(c == null ? "(null)" : c);
            liveNodesLastSeen.set(n);

            return predicate.matches(n, c);
          });
          if (!checkValidLeaders) {
            return;
          }
        } catch (TimeoutException e) {
          if (log.isDebugEnabled()) {
            log.debug("found in collectionWatches={} watchedStateCollections={} lazyStateCollections={}", collectionWatches.get(collection), watchedCollectionStates.get(collection), lazyCollectionStates.get(collection));
          }
          throw new TimeoutException(
              "Failed while waiting (" + wait + ' ' + unit + ") for active collection\n" + e.getMessage() + " \nShards:" + shards + " Replicas:" + totalReplicas
                  + "\nLive Nodes: " + liveNodesLastSeen.get() + "\nLast available state: " + state.get() + " inWatches=" + collectionWatches
                  .containsKey(collection) + " inWatchedStates=" + watchedCollectionStates
                  .containsKey(collection) + " inLazy=" + lazyCollectionStates.containsKey(collection));
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException("", e);
        }

        if (checkValidLeaders) {

          Replica failedLeader = checkLeaders(returnLeaders, client);

          if (failedLeader != null) {
            log.info("Failed confirming all shards have valid leaders, failed={}", failedLeader);
          } else {
            log.info("done checking valid leaders on active collection success");
            return;
          }
          if (leaderVerifyTimeout.hasTimedOut()) {
            throw new SolrException(ErrorCode.SERVER_ERROR,
                "Could not verify leaders for collection: " + collection + " failedLeader=" + failedLeader + " leaders=" + returnLeaders + " saw state=" + state
                    .get() + " with live_nodes=" + liveNodes);
          }

        }

        return;
      }
    } finally {
//      if (checkValidLeaders) {
//        unregisterCore(collection, null);
//      }
      if (closeHttpClient) {
        IOUtils.closeQuietly(client);
      }
    }

  }

  private Replica checkLeaders(Set<Replica> leaders, Http2SolrClient client) {

    for (Replica leader : leaders) {
      if (isLocalLeader != null && leader.getNodeName().equals(node)) {
        if (isLocalLeader.isLocalLeader(leader.getName())) {
          return null;
        }
      }

      CoreAdminRequest.WaitForState prepCmd = new CoreAdminRequest.WaitForState();
      prepCmd.setCoreName(leader.getName());
      prepCmd.setLeaderName(leader.getName());
      prepCmd.setCoresCollection(leader.getCollection());
      prepCmd.setCheckIsLeader(true);
      prepCmd.setBasePath(leader.getBaseUrl());

      try {
        CoreAdminResponse result = prepCmd.process(client);

        log.debug("check leader result {}", result);

        boolean success = "0".equals(result.getResponse().get("STATUS"));

        if (!success) {
          Thread.sleep(500);
          return leader;
        } else {
          return null;
        }
      } catch (RejectedExecutionException | AlreadyClosedException e) {
        log.warn("Rejected or already closed, bailing {} {}", leader.getName(), e.getClass().getSimpleName());
        throw e;
      } catch (Exception e) {
        log.debug("failed checking for leader {} {}", leader.getName(), e.getMessage());
        try {
          Thread.sleep(250);
        } catch (InterruptedException interruptedException) {
          ParWork.propagateInterrupt(interruptedException);
        }
        return leader;
      }

    }

    return null;
  }

  /**
   * Block until a LiveNodesStatePredicate returns true, or the wait times out
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * @param wait      how long to wait
   * @param unit      the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   */
  public void waitForLiveNodes(long wait, TimeUnit unit, LiveNodesPredicate predicate)
      throws InterruptedException, TimeoutException {

    if (predicate.matches(liveNodes)) {
      return;
    }

    final CountDownLatch latch = new CountDownLatch(1);

    LiveNodesListener listener = (o, n) -> {
      boolean matches = predicate.matches(n);
      if (matches)
        latch.countDown();
      return matches;
    };

    registerLiveNodesListener(listener);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException("Timeout waiting for live nodes, currently they are: " + liveNodes);

    } finally {
      removeLiveNodesListener(listener);
    }
  }


  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerCollectionStateWatcher
   */
  public void removeCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher watcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper
        = new DocCollectionAndLiveNodesWatcherWrapper(collection, watcher, null);

    removeLiveNodesListener(wrapper);
    removeDocCollectionWatcher(collection, wrapper);
  }

  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerDocCollectionWatcher
   */
  public void removeDocCollectionWatcher(String collection, DocCollectionWatcher watcher) {

    if (collection == null) {
      throw new IllegalArgumentException(COLLECTION_CANNOT_BE_NULL);
    }

    boolean[] removed = {false};
    collectionWatches.compute(collection, (k, v) -> {

      if (v == null) return v;

      log.debug("remove doc collection watcher");
      // registerDocCollectionWatcher adds the watcher AND increments coreRefCount exactly once, so
      // decrement ONLY when this watcher was actually present. A double-remove of the same watcher
      // (e.g. registerCollectionStateWatcher auto-removed it because the predicate already matched,
      // then waitForState's finally removes it again) would otherwise over-decrement coreRefCount past
      // a live core's registration, driving canBeRemoved true and tearing down a watch still in use
      // (review finding H6).
      if (v.stateWatchers.remove(watcher) && v.coreRefCount.get() > 0) {
        v.coreRefCount.decrementAndGet();
      }
      if (v.canBeRemoved(watchedCollectionStates.size())) {
        log.debug("no longer watch collection {}", collection);
        // Only downgrade to a lazy ref for a collection we actually resolved real state for. A
        // timed-out waitForState on a NON-EXISTENT collection name (e.g. a stale request routed to a
        // deleted core via SolrCall.getRemoteCoreUrl) would otherwise leave behind a permanent
        // LazyCollectionRef that can never resolve. That doomed ref lingers in getCollectionRefs() and
        // makes the next overseer's ZkStateWriter.init fail with NoNode (root cause of the
        // TestTlogReplica.testKillLeader "Expect new leader" flake). Don't manufacture it for a phantom.
        if (watchedCollectionStates.containsKey(collection)) {
          LazyCollectionRef docRef = new LazyCollectionRef(collection);
          lazyCollectionStates.put(collection, docRef);
        }
        removed[0] = true;
        return null;
      }
      return v;
    });
    if (removed[0]) {
      releaseLocalInterest(collection);
    }

  }

  /* package-private for testing */
  Set<DocCollectionWatcher> getStateWatchers(String collection) {
    if (collection == null) {
      throw new IllegalArgumentException(COLLECTION_CANNOT_BE_NULL);
    }

    CollectionWatch<DocCollectionWatcher> watchers = collectionWatches.get(collection);

    return watchers == null ? Collections.emptySet() : watchers.stateWatchers;
  }

  // returns true if the state has changed
  void updateWatchedCollection(String coll, ClusterState.CollectionRef newState) {
    // section updateWatchedCollection
    log.debug("updateWatchedCollection collection={}, newState={}", coll, newState);
    try {
      if (newState == null) {
        if (log.isDebugEnabled()) log.debug("State is null [{}]", coll);

        return;
      }

      AtomicBoolean update = new AtomicBoolean(false);

      DocCollection newDocState = newState.get(false).get();
      if (newDocState == null) {
        if (log.isDebugEnabled()) log.debug("State is null from a non null collecton ref, collection={}", coll);
        return;
      }

      DocCollection state = watchedCollectionStates.compute(coll, (k, v) -> {

        if (v == null) {
          log.debug("[{}] updateWatchedCollection existing state is null, replace and notify", coll);
          update.set(true);
          lazyCollectionStates.remove(coll);

          return newDocState;
        }

        if (StatePlaneReader.shouldReplaceWatched(newDocState, v)) {

          log.debug("[{}] updateWatchedCollection existing state is {}, replace", v, coll);
          update.set(true);
          lazyCollectionStates.remove(coll);
          // Preserve whichever StateUpdates is newer. The incoming newDocState may already carry
          // freshly-applied state updates (the full-fetch path chains getAndProcessStateUpdates before
          // calling us); unconditionally overwriting with the previous watched state's updates clobbers
          // them and can reset the applied version to a stale/-1 value. During restore's rapid structure
          // churn that makes leader/active state oscillate and never stabilize, so replica registration
          // (which waits to see the leader active) times out and the new replicas never recover.
          //
          // carryForwardStateUpdates also carries the per-shard delta-plane (epoch, seq) cursors
          // when the previous watched state is ahead, so an older state.json read cannot regress
          // newer delta-plane state.
          StatePlaneReader.carryForwardStateUpdates(newDocState, v);
          return newDocState;
        }

        log.debug("[{}] updateWatchedCollection is old, existing state is {}, do not replace", v, coll);
        return v;
      });

      log.debug("done updating watched collections {} interesting [{}] watched [{}] lazy [{}] total [{}]", coll, collectionWatches.keySet().size(),
          watchedCollectionStates.size(), lazyCollectionStates.keySet().size(), allCollections.size());

      CollectionWatch<DocCollectionWatcher> watchers = collectionWatches.get(coll);
      if (watchers != null) {
        watchers.stateWatchers.forEach(watcher -> CompletableFuture.runAsync(() -> {
          log.debug("Notify DocCollectionWatcher {} {}", watcher, state);
          try {
            if (watcher.onStateChanged(state)) {
              removeDocCollectionWatcher(coll, watcher);
            }
          } catch (Exception exception) {
            log.warn("Error on calling state watcher", exception);
          }
        }));
      }

      notifyCloudCollectionsListeners();

    } catch (Exception e) {
      log.error("[{}] failed updating doc state", coll, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void registerCollectionPropsWatcher(final String collection, CollectionPropsWatcher propsWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionPropsObservers.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>(collection);
        watchSet.set(true);
      }
      v.propStateWatchers.add(propsWatcher);
      return v;
    });

    if (watchSet.get()) {
      collectionPropsWatchers.computeIfAbsent(collection, s -> new PropsWatcher(zkClient, notifications, node, collection, 0,
          watchedCollectionProps, collectionPropsObservers, collectionPropsWatchers)).refreshAndWatch(false);
    }
  }

  public void setCollectionRemovedListener(CollectionRemoved listener) {
    this.collectionRemovedListener = listener;
  }

  public static class ConfigData {
    public Map<String, Object> data;
    public int version;

    public ConfigData() {
    }

    public ConfigData(Map<String, Object> data, int version) {
      this.data = data;
      this.version = version;

    }
  }

//  private void notifyStateWatchers(String collection, DocCollection collectionState) {
//    if (log.isDebugEnabled()) {
//      log.debug("Notify state watchers [{}] {}", collectionWatches.keySet(), collectionState == null ? "(null)" : collectionState.getName());
//    }
//
//    try {
//      notifications.submit(new Notification(collection, collectionState, collectionWatches));
//    } catch (RejectedExecutionException e) {
//      log.error("Couldn't run collection notifications for {}", collection, e);
//    }
//  }

  //
  //  Aliases related
  //

  /**
   * Access to the {@link Aliases}.
   */
  public final AliasesManager aliasesManager = new AliasesManager();

  /**
   * Get an immutable copy of the present state of the aliases. References to this object should not be retained
   * in any context where it will be important to know if aliases have changed.
   *
   * @return The current aliases, Aliases.EMPTY if not solr cloud, or no aliases have existed yet. Never returns null.
   */
  public Aliases getAliases() {
    return aliasesManager.getAliases();
  }

  // called by createClusterStateWatchersAndUpdate()
  private void refreshAliases(AliasesManager watcher) throws KeeperException, InterruptedException {
  //  notifyStateUpdated(null, "refreshAliases");
    zkClient.exists(ALIASES, watcher);
    aliasesManager.update();
  }

  /**
   * A class to manage the aliases instance, including watching for changes.
   * There should only ever be one instance of this class
   * per instance of ZkStateReader. Normally it will not be useful to create a new instance since
   * this watcher automatically re-registers itself every time it is updated.
   */
  public class AliasesManager implements Watcher { // the holder is a Zk watcher
    // note: as of this writing, this class if very generic. Is it useful to use for other ZK managed things?
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicReference<Aliases> aliases = new AtomicReference<>(Aliases.EMPTY);

    public Aliases getAliases() {
      return aliases.get();
    }

    /**
     * Writes an updated {@link Aliases} to zk.
     * It will retry if there are races with other modifications, giving up after 30 seconds with a SolrException.
     * The caller should understand it's possible the aliases has further changed if it examines it.
     */
    public void applyModificationAndExportToZk(UnaryOperator<Aliases> op) {
      // This method was previously entirely commented out, making it a silent no-op: alias
      // create/modify/delete never persisted to ZK. Restored so alias operations actually take effect.
      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      // note: triesLeft tuning is based on ConcurrentCreateRoutedAliasTest
      for (int triesLeft = 5; triesLeft > 0; triesLeft--) {
        // we could synchronize on "this" but there doesn't seem to be a point; we have a retry loop.
        Aliases curAliases = getAliases();
        Aliases modAliases = op.apply(curAliases);
        final byte[] modAliasesJson = modAliases.toJSON();
        if (curAliases == modAliases) {
          log.debug("Current aliases has the desired modification; no further ZK interaction needed.");
          return;
        }

        try {
          try {
            final Stat stat = getZkClient().setData(ALIASES, modAliasesJson, curAliases.getZNodeVersion(), true);
            setIfNewer(Aliases.fromJSON(modAliasesJson, stat.getVersion()));
            return;
          } catch (KeeperException.BadVersionException e) {
            log.debug("{}", e, e);
            log.warn("Couldn't save aliases due to race with another modification; will update and retry until timeout");
            // considered a backoff here, but we really do want to compete strongly since the normal case is
            // that we will do one update and succeed. This is left as a hot loop for limited tries intentionally.
            // More failures than that here probably indicate a bug or a very strange high write frequency usage for
            // aliases.json, timeouts mean zk is being very slow to respond, or this node is being crushed
            // by other processing and just can't find any cpu cycles at all.
            update();
            if (deadlineNanos < System.nanoTime()) {
              throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out trying to update aliases! " +
                  "Either zookeeper or this node may be overloaded.");
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        } catch (KeeperException e) {
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        }
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Too many successive version failures trying to update aliases");
    }

    /**
     * Ensures the internal aliases is up to date. If there is a change, return true.
     *
     * @return true if an update was performed
     */
    public boolean update() throws KeeperException, InterruptedException {
      log.debug("Checking ZK for most up to date Aliases {}", ALIASES);
      // Call sync() first to ensure the subsequent read (getData) is up to date.
      // MRM TODO: review
      // getKeeper() can be transiently null during a reconnect window; the sync() is only a
      // freshness optimization, so skip it rather than NPE out of this KeeperException-only
      // retry loop (the getData below + outer retry still guarantee correctness).
      ZooKeeper keeper = zkClient.getConnectionManager().getKeeper();
      if (keeper != null) {
        keeper.sync(ALIASES, null, null);
      }
      Stat stat = new Stat();
      final byte[] data = zkClient.getData(ALIASES, null, stat, true);
      return setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
    }

    // ZK Watcher interface
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || EventType.None.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      try {
        log.debug("Aliases: updating");

        // re-register the watch
        Stat stat = new Stat();
        final byte[] data = zkClient.getData(ALIASES, this, stat, true);
        // note: it'd be nice to avoid possibly needlessly parsing if we don't update aliases but not a big deal
        setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
      } catch (NoNodeException e) {
        // /aliases.json will not always exist
      } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
        // note: aliases.json is required to be present
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred on Alias update", e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    /**
     * Update the internal aliases reference with a new one, provided that its ZK version has increased.
     *
     * @param newAliases the potentially newer version of Aliases
     * @return true if aliases have been updated to a new version, false otherwise
     */
    private boolean setIfNewer(Aliases newAliases) {
      while (true) {
        Aliases currentAliases = aliases.get();
        int cmp = Integer.compare(currentAliases.getZNodeVersion(), newAliases.getZNodeVersion());
        if (cmp < 0) {
          log.debug("Aliases: cmp={}, new definition is: {}", cmp, newAliases);
          boolean success = aliases.compareAndSet(currentAliases, newAliases);
          if (!success) {
            continue;
          }
          return true;
        } else {
          log.debug("Aliases: cmp={}, not overwriting ZK version.", cmp);
          assert cmp != 0 || Arrays.equals(currentAliases.toJSON(), newAliases.toJSON()) : aliases + " != " + newAliases;
          return false;
        }
      }
    }
  }

  private static class PropsNotification implements Runnable {

    private final String collection;
    private final Map<String, String> collectionProperties;
    private final List<CollectionPropsWatcher> watchers = new ArrayList<>();
    private final String node;
    private final Map<String,CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers;
    private final Map<String,PropsWatcher.VersionedCollectionProps> watchedCollectionProps;

    private PropsNotification(String collection, String node, Map<String, String> collectionProperties,
        Map<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers, Map<String,PropsWatcher.VersionedCollectionProps> watchedCollectionProps) {
      this.collection = collection;
      this.node = node;
      this.collectionProperties = collectionProperties;
      this.collectionPropsObservers = collectionPropsObservers;
      this.watchedCollectionProps = watchedCollectionProps;
      // guarantee delivery of notification regardless of what happens to collectionPropsObservers
      // while we wait our turn in the executor by capturing the list on creation.
      collectionPropsObservers.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        watchers.addAll(v.propStateWatchers);
        return v;
      });
    }

    @Override
    public void run() {
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      for (CollectionPropsWatcher watcher : watchers) {
        if (watcher.onStateChanged(collectionProperties)) {
          removeCollectionPropsWatcher(collection, watcher);
        }
      }
    }
    public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
      collectionPropsObservers.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        v.propStateWatchers.remove(watcher);
        if (v.canBeRemoved(-1)) {
          // don't want this to happen in middle of other blocks that might add it back.
          watchedCollectionProps.remove(collection);

          return null;
        }
        return v;
      });
    }


  }


  private class CacheCleaner implements Runnable {
    public void run() {
      while (true) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Executor shutdown will send us an interrupt
          break;
        }
        watchedCollectionProps.entrySet().removeIf(entry ->
            entry.getValue().cacheUntilNs < System.nanoTime() && !collectionPropsObservers.containsKey(entry.getKey()));
      }
    }
  }

  /**
   * Helper class that acts as both a {@link DocCollectionWatcher} and a {@link LiveNodesListener}
   * while wraping and delegating to a {@link org.apache.solr.common.cloud.CollectionStateWatcher}
   */
  private final class DocCollectionAndLiveNodesWatcherWrapper implements DocCollectionWatcher, Closeable, LiveNodesListener {
    private final String collectionName;
    private final org.apache.solr.common.cloud.CollectionStateWatcher delegate;
    private final SolrCountDownLatch latch;

    public int hashCode() {
      return collectionName.hashCode() * delegate.hashCode();
    }

    public boolean equals(Object other) {
      if (other instanceof DocCollectionAndLiveNodesWatcherWrapper) {
        DocCollectionAndLiveNodesWatcherWrapper that
            = (DocCollectionAndLiveNodesWatcherWrapper) other;
        return this.collectionName.equals(that.collectionName)
            && this.delegate.equals(that.delegate);
      }
      return false;
    }

    public DocCollectionAndLiveNodesWatcherWrapper(final String collectionName, final CollectionStateWatcher delegate, SolrCountDownLatch latch) {
      this.collectionName = collectionName;
      this.delegate = delegate;
      this.latch = latch;
    }

    @Override
    public boolean onStateChanged(DocCollection collectionState) {
      return delegate.onStateChanged(ZkStateReader.this.liveNodes,
          collectionState);
    }

    @Override
    public boolean onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
      DocCollection collection = watchedCollectionStates.get(collectionName);

      return delegate.onStateChanged(newLiveNodes, collection);
    }

    @Override public void close() throws IOException {
      latch.close("ZkStateReader closed");
    }
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas) {
    return expectedShardsAndActiveReplicas(expectedShards, expectedReplicas, false);
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas, boolean exact) {
    return expectedShardsAndActiveReplicas(false, expectedShards, expectedReplicas, exact, null);
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(boolean justLeaders, int expectedShards, int expectedReplicas, boolean exact, Set<Replica> returnLeaders) {
    return (liveNodes, collectionState) -> {

      if (collectionState == null) return false;
      Collection<Slice> activeSlices = collectionState.getActiveSlices();

      if (log.isDebugEnabled()) {
        log.debug("active slices expected={} {} {} allSlices={}", expectedShards, activeSlices.size(), activeSlices, collectionState.getSlices());
      }

      if (!exact) {
        if (activeSlices.size() < expectedShards) {
          return false;
        }
      } else {
        if (activeSlices.size() != expectedShards) {
          return false;
        }
      }

      if (expectedReplicas == 0 && !exact) {
        log.debug("0 replicas expected and found, return");
        return true;
      }

      int activeReplicas = 0;
      for (Slice slice : activeSlices) {
        Replica leader = slice.getLeader(liveNodes);
        log.debug("slice is {} and leader is {}", slice.getName(), leader);
        if (leader == null) {
          log.debug("no leader for slice returning slice={}", slice);
          return false;
        }
        if (!leader.isActive()) {
          log.debug("leader is not active for slice={}", slice);
          return false;
        }
        if (returnLeaders != null) returnLeaders.add(leader);

        for (Replica replica : slice) {
          if (replica.isActive() && liveNodes.contains(replica.getNodeName())) {
            activeReplicas++;
          }
        }

        log.debug("slice is {} and active replicas is {}, expected {} liveNodes={}", slice.getName(), activeReplicas, expectedReplicas, liveNodes);
      }

      if (justLeaders) {
        return true;
      }
      if (!exact) {
        return activeReplicas >= expectedReplicas;
      } else {
        return activeReplicas == expectedReplicas;
      }
    };
  }

  /* Checks both shard replcia consistency and against the control shard.
   * The test will be failed if differences are found.
   */
  public void checkShardConsistency(String collection) throws Exception {
    checkShardConsistency(collection, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(String collection, boolean verbose)
      throws Exception {

    Set<String> theShards = getCollection(collection).getSlicesMap().keySet();
    String failMessage = null;
    for (Object shard : theShards) {
      String shardFailMessage = checkShardConsistency(collection, (String) shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      System.err.println(failMessage);
      // MRM TODO: fail test if from tests

      throw new AssertionError(failMessage);
    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is
   * consistent.
   */
  protected String checkShardConsistency(String collection, String shard, boolean expectFailure, boolean verbose)
      throws Exception {


    long num;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("\nCheck consistency of shard: " + shard);
    if (verbose) System.err.println("__________________________\n");

    DocCollection coll = getCollection(collection);

    Slice replicas = coll.getSlice(shard);

    Replica lastReplica = null;
    for (Replica replica : replicas) {
      boolean live = false;
      String nodeName = replica.getNodeName();
      if (isNodeLive(nodeName)) {
        live = true;
      }
      boolean active = replica.getState() == Replica.State.ACTIVE;
      if (active && live) {
        //if (verbose) System.err.println("client" + cnt++);
        if (verbose) System.err.println("Replica: " + replica);
        try (SolrClient client = getHttpClient(replica.getCoreUrl())) {
          try {
            SolrParams query = params("q", "*:*", "rows", "0", "distrib", "false", "tests",
                "checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
            num = client.query(query).getResults().getNumFound();
          } catch (SolrException | SolrServerException e) {
            if (verbose) System.err.println("error contacting client: " + e.getMessage() + '\n');
            continue;
          }

          if (verbose) System.err.println(" Live:" + live);
          if (verbose) System.err.println(" Count:" + num + '\n');

          if (lastNum > -1 && lastNum != num && failMessage == null) {
            failMessage =
                shard + " is not consistent.  Got " + lastNum + " from " + (lastReplica != null ? lastReplica.getCoreUrl() : "(none)") + " (previous client)" + " and got " + num + " from "
                    + replica.getCoreUrl();

            if ((!expectFailure || verbose) && lastReplica != null) {
              System.err.println("######" + failMessage);
              SolrQuery query = new SolrQuery("*:*");
              query.set("distrib", false);
              query.set("fl", "id,_version_");
              query.set("rows", "100000");
              query.set("sort", "id asc");
              query.set("tests", "checkShardConsistency/showDiff");

              try (SolrClient lastClient = getHttpClient(lastReplica.getCoreUrl())) {
                SolrDocumentList lst1 = lastClient.query(query).getResults();
                SolrDocumentList lst2 = client.query(query).getResults();

                CloudInspectUtil.showDiff(lst1, lst2, lastReplica.getCoreUrl(), replica.getCoreUrl());
              }
            }

          }
          lastNum = num;
          lastReplica = replica;
        }
      }
    }
    return failMessage;

  }

  /**
   * Generates the correct SolrParams from an even list of strings.
   * A string in an even position will represent the name of a parameter, while the following string
   * at position (i+1) will be the assigned value.
   *
   * @param params an even list of strings
   * @return the ModifiableSolrParams generated from the given list of strings.
   */
  public static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    int sz = params.length;
    for (int i=0; i<sz; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  public static Http2SolrClient getHttpClient(String baseUrl) {
    return new Http2SolrClient.Builder(baseUrl)
        .idleTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
  }

  private class CloudCollectionsListenerConsumer implements Consumer<CloudCollectionsListener> {
    private final Set<String> oldCollections;
    private final Set<String> newCollections;

    public CloudCollectionsListenerConsumer(Set<String> oldCollections, Set<String> newCollections) {
      this.oldCollections = oldCollections;
      this.newCollections = newCollections;
    }

    @Override
    public void accept(CloudCollectionsListener listener) {
      if (log.isDebugEnabled()) log.debug("fire listeners {}", listener);
      notifications.submit(new ListenerOnChange(listener));
    }

    private class ListenerOnChange implements Runnable {
      private final CloudCollectionsListener listener;

      public ListenerOnChange(CloudCollectionsListener listener) {
        this.listener = listener;
      }

      @Override
      public void run() {
        log.debug("notify {}", newCollections);
        listener.onChange(oldCollections, newCollections);
      }
    }
  }

  private class ClusterPropsWatcher implements Watcher, Closeable {

    private final String path;

    ClusterPropsWatcher(String path) {
      this.path = path;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (closed || EventType.None.equals(event.getType())) {
        return;
      }
      if (closed) return;
      loadClusterProperties();
    }

    @Override
    public void close() throws IOException {
      try {
        if (!zkClient.isClosed()) {
          zkClient.removeWatches(path, this, Watcher.WatcherType.Any, true);
        }
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  private class PredicateMatcher {
    private final CollectionStatePredicate predicate;
    private final CountDownLatch latch;
    private final AtomicReference<DocCollection> docCollection;

    public PredicateMatcher(CollectionStatePredicate predicate, CountDownLatch latch, AtomicReference<DocCollection> docCollection) {
      this.predicate = predicate;
      this.latch = latch;
      this.docCollection = docCollection;
    }

    public org.apache.solr.common.cloud.CollectionStateWatcher invoke() {
      return (n, c) -> {
        // if (isClosed()) return true;
        docCollection.set(c);
        boolean matches = predicate.matches(getLiveNodes(), c);
        if (matches)
          latch.countDown();

        return matches;
      };
    }
  }
}
