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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySortedSet;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateReader implements SolrCloseable {
  public static final int STATE_UPDATE_DELAY =
      Integer.getInteger(
          "solr.OverseerStateUpdateDelay", 2000); // delay between cloud state updates
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";
  public static final String CORE_NODE_NAME_PROP = "core_node_name";
  public static final String STATE_PROP = "state";
  // if this flag equals to false and the replica does not exist in cluster state, set state op
  // become no op (default is true)
  public static final String FORCE_SET_STATE_PROP = Replica.ReplicaStateProps.FORCE_SET_STATE;

  /** SolrCore name. */
  public static final String CORE_NAME_PROP = "core";

  public static final String COLLECTION_PROP = "collection";
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

  // TODO: Deprecate and remove support for roles.json in an upcoming release.
  /**
   * The following, node_roles and roles.json are for assigning roles to nodes. The node_roles is
   * the preferred way (using -Dsolr.node.roles param), and roles.json is used by legacy ADDROLE API
   * command.
   */
  public static final String NODE_ROLES = "/node_roles";

  public static final String ROLES = "/roles.json";

  public static final String ALIASES = "/aliases.json";

  /**
   * This ZooKeeper file is no longer used starting with Solr 9 but keeping the name around to check
   * if it is still present and non-empty (in case of upgrade from previous Solr version). It used
   * to contain collection state for all collections in the cluster.
   */
  public static final String UNSUPPORTED_CLUSTER_STATE = "/clusterstate.json";

  // This zookeeper file was allowed until Solr 10
  public static final String UNSUPPORTED_SOLR_XML = "/solr.xml";

  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String COLLECTION_PROPS_ZKNODE = "collectionprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";
  public static final String SOLR_PKGS_PATH = "/packages.json";

  public static final String DEFAULT_SHARD_PREFERENCES = "defaultShardPreferences";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_CORES_PER_NODE = "maxCoresPerNode";
  public static final String PULL_REPLICAS = "pullReplicas";
  public static final String NRT_REPLICAS = "nrtReplicas";
  public static final String TLOG_REPLICAS = "tlogReplicas";
  public static final String READ_ONLY = "readOnly";

  public static final String CONFIGS_ZKNODE = "/configs";
  public static final String CONFIGNAME_PROP = "configName";

  public static final String URL_SCHEME = "urlScheme";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTPS_PORT_PROP = "solr.jetty.https.port";

  private static final String SOLR_ENVIRONMENT = "environment";

  public static final String REPLICA_TYPE = CollectionAdminParams.REPLICA_TYPE;

  public static final String CONTAINER_PLUGINS = "plugin";

  public static final String PLACEMENT_PLUGIN = "placement-plugin";
  private final CollectionPropertiesZkStateReader collectionPropertiesZkStateReader;

  /** A view of the current state of all collections. */
  protected volatile ClusterState clusterState;

  private static final int GET_LEADER_RETRY_DEFAULT_TIMEOUT =
      Integer.parseInt(System.getProperty("zkReaderGetLeaderRetryTimeoutMs", "4000"));

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";

  /** "Interesting" but not actively watched Collections. */
  private final ConcurrentHashMap<String, LazyCollectionRef> lazyCollectionStates =
      new ConcurrentHashMap<>();

  private volatile SortedSet<String> liveNodes = emptySortedSet();

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();

  /**
   * Collections with active watches. The {@link StatefulCollectionWatch} inside for each collection
   * might also contain the latest DocCollection (state) observed
   */
  private DocCollectionWatches collectionWatches = new DocCollectionWatches();

  private Set<CloudCollectionsListener> cloudCollectionsListeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService notifications = ExecutorUtil.newMDCAwareCachedThreadPool("watches");

  private Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();

  private Set<ClusterPropertiesListener> clusterPropertiesListeners = ConcurrentHashMap.newKeySet();

  private static final long LAZY_CACHE_TIME =
      TimeUnit.NANOSECONDS.convert(STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  /**
   * Gets the ZkStateReader inside a ZK based SolrClient.
   *
   * @throws IllegalArgumentException if solrClient isn't ZK based.
   */
  public static ZkStateReader from(CloudSolrClient solrClient) {
    try {
      var provider = (ZkClientClusterStateProvider) solrClient.getClusterStateProvider();
      return provider.getZkStateReader();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("client must be ZK based", e);
    }
  }

  protected static class CollectionWatch<T> {

    int coreRefCount = 0;
    Set<T> stateWatchers = ConcurrentHashMap.newKeySet();

    public boolean canBeRemoved() {
      return coreRefCount + stateWatchers.size() == 0;
    }
  }

  /**
   * A ConcurrentHashMap of active watcher by collection name
   *
   * <p>Each watcher DocCollectionWatch also contains the latest DocCollection (state) observed
   */
  private static class DocCollectionWatches {
    private final ConcurrentHashMap<String, StatefulCollectionWatch>
        statefulWatchesByCollectionName = new ConcurrentHashMap<>();

    /**
     * Gets the DocCollection (state) of the collection which the corresponding watch last observed
     *
     * @param collection the collection name to get DocCollection on
     * @return The last observed DocCollection(state). if null, that means there's no such
     *     collection.
     */
    private DocCollection getDocCollection(String collection) {
      StatefulCollectionWatch watch = statefulWatchesByCollectionName.get(collection);
      return watch != null ? watch.currentState : null;
    }

    /**
     * Gets the active collections (collections that exist) being watched
     *
     * @return an immutable set of active collection names
     */
    private Set<String> activeCollections() {
      return statefulWatchesByCollectionName.entrySet().stream()
          .filter(
              (Entry<String, StatefulCollectionWatch> entry) ->
                  entry.getValue().currentState != null)
          .map(Entry::getKey)
          .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Gets the count of active collections (collections that exist) being watched
     *
     * @return the count of active collections
     */
    private long activeCollectionCount() {
      return statefulWatchesByCollectionName.entrySet().stream()
          .filter(
              (Entry<String, StatefulCollectionWatch> entry) ->
                  entry.getValue().currentState != null)
          .count();
    }

    /**
     * Gets a Set of watched collection names. The returned value is thread-safe and unmodifiable.
     *
     * @return Set of watched collection names
     */
    private Set<String> watchedCollections() {
      return Collections.unmodifiableSet(statefulWatchesByCollectionName.keySet());
    }

    private Set<Entry<String, StatefulCollectionWatch>> watchedCollectionEntries() {
      return Collections.unmodifiableSet(statefulWatchesByCollectionName.entrySet());
    }

    /**
     * Updates the latest observed DocCollection (state) of the {@link StatefulCollectionWatch} if
     * the collection is being watched
     *
     * @param collection the collection name
     * @param newState the new DocCollection (state) observed
     * @return whether the state has changed for the watched collection
     */
    private boolean updateDocCollection(String collection, DocCollection newState) {
      AtomicBoolean stateHasChanged = new AtomicBoolean(false);
      statefulWatchesByCollectionName.computeIfPresent(
          collection,
          (col, watch) -> {
            DocCollection oldState = watch.currentState;
            if (oldState == null && newState == null) {
              // OK, the collection not yet exist in ZK or already deleted
            } else if (oldState == null) {
              if (log.isDebugEnabled()) {
                log.debug("Add data for [{}] ver [{}]", collection, newState.getZNodeVersion());
              }
              watch.currentState = newState;
            } else if (newState == null) {
              log.debug("Removing cached collection state for [{}]", collection);
              watch.currentState = null;
            } else { // both new and old states are non-null
              int oldCVersion =
                  oldState.getPerReplicaStates() == null
                      ? -1
                      : oldState.getPerReplicaStates().cversion;
              int newCVersion =
                  newState.getPerReplicaStates() == null
                      ? -1
                      : newState.getPerReplicaStates().cversion;
              if (oldState.getZNodeVersion() < newState.getZNodeVersion()
                  || oldCVersion < newCVersion) {
                watch.currentState = newState;
                if (log.isDebugEnabled()) {
                  log.debug(
                      "Updating data for [{}] from [{}] to [{}]",
                      collection,
                      oldState.getZNodeVersion(),
                      newState.getZNodeVersion());
                }
              }
            }
            stateHasChanged.set(!Objects.equals(oldState, watch.currentState));
            return watch;
          });

      return stateHasChanged.get();
    }

    /**
     * Computes the new StatefulCollectionWatch by the supplied remappingFunction.
     *
     * @param collectionName collection name
     * @param remappingFunction remaps the StatefulCollectionWatch. If this returns null, the
     *     associated StatefulCollectionWatch will be removed; otherwise, the returned value will be
     *     assigned to such collection
     * @return the new StatefulCollectionWatch associated with the collection
     * @see ConcurrentHashMap#compute(Object, BiFunction)
     */
    private StatefulCollectionWatch compute(
        String collectionName,
        BiFunction<String, StatefulCollectionWatch, StatefulCollectionWatch> remappingFunction) {
      return statefulWatchesByCollectionName.compute(collectionName, remappingFunction);
    }
  }

  private static class StatefulCollectionWatch extends CollectionWatch<DocCollectionWatcher> {
    private DocCollection currentState;

    /**
     * The {@link StateWatcher} that is associated with this {@link StatefulCollectionWatch}. It is
     * necessary to track this because of the way {@link StateWatcher} instances expire
     * asynchronously: once registered with ZooKeeper, a {@link StateWatcher} cannot be removed, and
     * its {@link StateWatcher#process(WatchedEvent)} method will be invoked upon node update.
     * Because it is not possible to synchronously remove the {@link StateWatcher} as part of a
     * transaction with {@link ZkStateReader#collectionWatches}, we keep track of a unique {@link
     * StateWatcher} here, so that all other {@link StateWatcher}s may properly expire in a deferred
     * way.
     */
    private volatile StateWatcher associatedWatcher;

    private StatefulCollectionWatch(StateWatcher associatedWatcher) {
      this.associatedWatcher = associatedWatcher;
    }
  }

  public static final Set<String> KNOWN_CLUSTER_PROPS =
      Set.of(
          URL_SCHEME,
          CoreAdminParams.BACKUP_LOCATION,
          DEFAULT_SHARD_PREFERENCES,
          MAX_CORES_PER_NODE,
          SOLR_ENVIRONMENT,
          CollectionAdminParams.DEFAULTS,
          CONTAINER_PLUGINS,
          PLACEMENT_PLUGIN);

  private final SolrZkClient zkClient;

  private final boolean closeClient;

  private volatile boolean closed = false;

  private Set<CountDownLatch> waitLatches = ConcurrentHashMap.newKeySet();
  private final SecurityNodeWatcher securityNodeWatcher;

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    this.zkClient = zkClient;
    this.closeClient = false;
    this.securityNodeWatcher = new SecurityNodeWatcher(this, securityNodeListener);
    collectionPropertiesZkStateReader = new CollectionPropertiesZkStateReader(this);
    assert ObjectReleaseTracker.track(this);
  }

  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, true);
  }

  public ZkStateReader(
      String zkServerAddress,
      int zkClientTimeout,
      int zkClientConnectTimeout,
      boolean canUseZkACLs) {
    this.zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServerAddress)
            .withTimeout(zkClientTimeout, TimeUnit.MILLISECONDS)
            .withConnTimeOut(zkClientConnectTimeout, TimeUnit.MILLISECONDS)
            .withUseDefaultCredsAndACLs(canUseZkACLs)
            .build();
    this.zkClient
        .getCuratorFramework()
        .getConnectionStateListenable()
        .addListener(
            (OnReconnect)
                () -> {
                  // on reconnect, reload cloud info
                  try {
                    this.createClusterStateWatchersAndUpdate();
                  } catch (InterruptedException e) {
                    // Restore the interrupted status
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted", e);
                  } catch (Throwable e) {
                    log.error("An error has occurred while updating the cluster state", e);
                  }
                });
    this.closeClient = true;
    this.securityNodeWatcher = null;
    collectionPropertiesZkStateReader = new CollectionPropertiesZkStateReader(this);
    assert ObjectReleaseTracker.track(this);
  }

  /**
   * Forcibly refresh cluster state from ZK. Do this only to avoid race conditions because it's
   * expensive.
   *
   * <p>It is cheaper to call {@link #forceUpdateCollection(String)} on a single collection if you
   * must.
   *
   * @lucene.internal
   */
  public void forciblyRefreshAllClusterStateSlow() throws KeeperException, InterruptedException {
    synchronized (getUpdateLock()) {
      if (clusterState == null) {
        // Never initialized, just run normal initialization.
        createClusterStateWatchersAndUpdate();
        return;
      }
      // No need to set watchers because we should already have watchers registered for everything.
      refreshCollectionList(null);
      refreshLiveNodes(null);

      Set<String> updatedCollections = new HashSet<>();

      // Iterate through the actively watched collections. Take note that the returned watched
      // collections might change during the iteration, but it should not throw exception as
      // it's thread-safe.
      // If such set is modified elsewhere during the iteration, the code logic should still
      // handle such missing/extra collection w/o issues.
      for (String coll : collectionWatches.watchedCollections()) {
        DocCollection newState = fetchCollectionState(coll, null);
        if (collectionWatches.updateDocCollection(coll, newState)) {
          updatedCollections.add(coll);
        }
      }
      constructState(updatedCollections);
    }
  }

  /**
   * Forcibly refresh a collection's internal state from ZK. Try to avoid having to resort to this
   * when a better design is possible.
   */
  // TODO shouldn't we call ZooKeeper.sync() at the right places to prevent reading a stale value?
  // We do so for aliases.
  public void forceUpdateCollection(String collection)
      throws KeeperException, InterruptedException {

    synchronized (getUpdateLock()) {
      if (clusterState == null) {
        log.warn("ClusterState watchers have not been initialized");
        return;
      }

      ClusterState.CollectionRef ref = clusterState.getCollectionRef(collection);
      if (ref == null) {
        // We either don't know anything about this collection (maybe it's new?).
        // see if it just got created.
        LazyCollectionRef tryLazyCollection = new LazyCollectionRef(collection);
        if (tryLazyCollection.get() != null) {
          // What do you know, it exists!
          log.debug("Adding lazily-loaded reference for collection {}", collection);
          lazyCollectionStates.putIfAbsent(collection, tryLazyCollection);
          constructState(Collections.singleton(collection));
        }
      } else if (ref.isLazilyLoaded()) {
        log.debug("Refreshing lazily-loaded state for collection {}", collection);
        if (ref.get() != null) {
          return;
        }
      } else if (collectionWatches.watchedCollections().contains(collection)) {
        // Exists as a watched collection, force a refresh.
        log.debug("Forcing refresh of watched collection state for {}", collection);
        DocCollection newState = fetchCollectionState(collection, null);
        if (collectionWatches.updateDocCollection(collection, newState)) {
          constructState(Collections.singleton(collection));
        }
      } else {
        log.error("Collection {} is not lazy nor watched!", collection);
      }
    }
  }

  /** Refresh the set of live nodes. */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes(null);
  }

  public Integer compareStateVersions(String coll, int version) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() < version) {
      if (log.isDebugEnabled()) {
        log.debug("Server older than client {}<{}", collection.getZNodeVersion(), version);
      }
      DocCollection nu = getCollectionLive(coll);
      if (nu == null) return -1;
      if (nu.getZNodeVersion() > collection.getZNodeVersion()) {
        if (collectionWatches.updateDocCollection(coll, nu)) {
          synchronized (getUpdateLock()) {
            constructState(Collections.singleton(coll));
          }
        }
        collection = nu;
      }
    }

    if (collection.getZNodeVersion() == version) {
      return null;
    }

    if (log.isDebugEnabled()) {
      log.debug("Wrong version from client [{}]!=[{}]", version, collection.getZNodeVersion());
    }

    return collection.getZNodeVersion();
  }

  @SuppressWarnings({"unchecked"})
  public synchronized void createClusterStateWatchersAndUpdate()
      throws KeeperException, InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes

    log.debug("Updating cluster state from ZooKeeper... ");

    try {
      // on reconnect of SolrZkClient force refresh and re-add watches.
      loadClusterProperties();
      refreshLiveNodes(new LiveNodeWatcher());
      refreshCollections();
      refreshCollectionList(new CollectionsChildWatcher());
      refreshAliases(aliasesManager);

      if (securityNodeWatcher != null) {
        securityNodeWatcher.register();
      }
      collectionPropertiesZkStateReader.refreshCollectionProperties();
    } catch (KeeperException.NoNodeException nne) {
      String noNodePath = nne.getPath();
      if (noNodePath.length() > zkClient.getCuratorFramework().getNamespace().length()) {
        noNodePath =
            noNodePath.substring(zkClient.getCuratorFramework().getNamespace().length() + 1);
      }
      throw new SolrException(
          ErrorCode.SERVICE_UNAVAILABLE,
          "Cannot connect to cluster at "
              + zkClient.getZkServerAddress()
              + ": cluster not found/not ready."
              + " Expected node '"
              + noNodePath
              + "' does not exist.");
    }
  }

  /**
   * Construct the total state view from all sources. Must hold {@link #getUpdateLock()} before
   * calling this.
   *
   * @param changedCollections collections that have changed since the last call, and that should
   *     fire notifications
   */
  private void constructState(Set<String> changedCollections) {

    Set<String> liveNodes = this.liveNodes; // volatile read

    Map<String, ClusterState.CollectionRef> result = new LinkedHashMap<>();

    // Add collections
    for (Entry<String, StatefulCollectionWatch> entry :
        collectionWatches.watchedCollectionEntries()) {
      if (entry.getValue().currentState != null) {
        // if the doc is null for the collection watch, then it should not be inserted into the
        // state
        result.put(entry.getKey(), new ClusterState.CollectionRef(entry.getValue().currentState));
      }
    }

    // Finally, add any lazy collections that aren't already accounted for.
    for (Map.Entry<String, LazyCollectionRef> entry : lazyCollectionStates.entrySet()) {
      result.putIfAbsent(entry.getKey(), entry.getValue());
    }

    this.clusterState = new ClusterState(result, liveNodes);

    if (log.isDebugEnabled()) {
      log.debug(
          "clusterStateSet: interesting [{}] watched [{}] lazy [{}] total [{}]",
          collectionWatches.watchedCollections().size(),
          collectionWatches.activeCollectionCount(),
          lazyCollectionStates.keySet().size(),
          clusterState.size());
    }

    if (log.isTraceEnabled()) {
      log.trace(
          "clusterStateSet: interesting [{}] watched [{}] lazy [{}] total [{}]",
          collectionWatches.watchedCollections(),
          collectionWatches.activeCollections(),
          lazyCollectionStates.keySet(),
          clusterState.collectionStream().toList());
    }

    notifyCloudCollectionsListeners();

    for (String collection : changedCollections) {
      notifyStateWatchers(collection, clusterState.getCollectionOrNull(collection));
    }
  }

  /** Refresh collections. */
  private void refreshCollections() {
    for (Entry<String, StatefulCollectionWatch> e : collectionWatches.watchedCollectionEntries()) {
      StateWatcher newStateWatcher = new StateWatcher(e.getKey());
      e.getValue().associatedWatcher = newStateWatcher;
      newStateWatcher.refreshAndWatch();
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionListLock = new Object();

  /** Search for any lazy-loadable collections. */
  private void refreshCollectionList(Watcher watcher) throws KeeperException, InterruptedException {
    synchronized (refreshCollectionListLock) {
      List<String> children = null;
      try {
        children = zkClient.getChildren(COLLECTIONS_ZKNODE, watcher, true);
      } catch (KeeperException.NoNodeException e) {
        log.warn("Error fetching collection names: ", e);
        // fall through
      }
      if (children == null || children.isEmpty()) {
        lazyCollectionStates.clear();
        return;
      }

      // Don't lock getUpdateLock() here, we don't need it and it would cause deadlock.
      // Don't mess with watchedCollections, they should self-manage.

      // First, drop any children that disappeared.
      this.lazyCollectionStates.keySet().retainAll(new HashSet<>(children)); // Set avoids O(N^2)
      for (String coll : children) {
        // We will create an eager collection for any interesting collections, so don't add to lazy.
        if (!collectionWatches.watchedCollections().contains(coll)) {
          // Double check contains just to avoid allocating an object.
          LazyCollectionRef existing = lazyCollectionStates.get(coll);
          if (existing == null) {
            lazyCollectionStates.putIfAbsent(coll, new LazyCollectionRef(coll));
          }
        }
      }
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionsSetLock = new Object();
  // Ensures that only the latest getChildren fetch gets applied.
  private final AtomicReference<Set<String>> lastFetchedCollectionSet = new AtomicReference<>();

  /**
   * Register a CloudCollectionsListener to be called when the set of collections within a cloud
   * changes.
   */
  public void registerCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.add(cloudCollectionsListener);
    notifyNewCloudCollectionsListener(cloudCollectionsListener);
  }

  /** Remove a registered CloudCollectionsListener. */
  public void removeCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.remove(cloudCollectionsListener);
  }

  private void notifyNewCloudCollectionsListener(CloudCollectionsListener listener) {
    listener.onChange(Collections.emptySet(), lastFetchedCollectionSet.get());
  }

  private void notifyCloudCollectionsListeners() {
    notifyCloudCollectionsListeners(false);
  }

  private void notifyCloudCollectionsListeners(boolean notifyIfSame) {
    synchronized (refreshCollectionsSetLock) {
      final Set<String> newCollections = getCurrentCollections();
      final Set<String> oldCollections = lastFetchedCollectionSet.getAndSet(newCollections);
      if (!newCollections.equals(oldCollections) || notifyIfSame) {
        cloudCollectionsListeners.forEach(
            listener -> listener.onChange(oldCollections, newCollections));
      }
    }
  }

  public Set<String> getCurrentCollections() {
    Set<String> collections = new HashSet<>();
    collections.addAll(collectionWatches.activeCollections());
    collections.addAll(lazyCollectionStates.keySet());
    return collections;
  }

  private class LazyCollectionRef extends ClusterState.CollectionRef {
    private final String collName;
    private volatile long lastUpdateTime;
    private DocCollection cachedDocCollection;

    public LazyCollectionRef(String collName) {
      super(null);
      this.collName = collName;
      this.lastUpdateTime = -1;
    }

    @Override
    public synchronized DocCollection get(boolean allowCached) {
      gets.incrementAndGet();
      if (!allowCached
          || lastUpdateTime < 0
          || System.nanoTime() - lastUpdateTime > LAZY_CACHE_TIME) {
        boolean shouldFetch = true;
        if (cachedDocCollection != null) {
          Stat freshStats = null;
          try {
            freshStats = zkClient.exists(DocCollection.getCollectionPath(collName), null, true);
          } catch (Exception e) {
          }
          if (freshStats != null
              && !cachedDocCollection.isModified(
                  freshStats.getVersion(), freshStats.getCversion())) {
            shouldFetch = false;
          }
        }
        if (shouldFetch) {
          cachedDocCollection = getCollectionLive(collName);
          lastUpdateTime = System.nanoTime();
        }
      }
      return cachedDocCollection;
    }

    @Override
    public boolean isLazilyLoaded() {
      return true;
    }

    @Override
    public String toString() {
      return "LazyCollectionRef(" + collName + ")";
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshLiveNodesLock = new Object();
  // Ensures that only the latest getChildren fetch gets applied.
  private final AtomicReference<SortedSet<String>> lastFetchedLiveNodes = new AtomicReference<>();

  /** Refresh live_nodes. */
  private void refreshLiveNodes(Watcher watcher) throws KeeperException, InterruptedException {
    synchronized (refreshLiveNodesLock) {
      SortedSet<String> newLiveNodes;
      try {
        List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, watcher, true);
        newLiveNodes = new TreeSet<>(nodeList);
      } catch (KeeperException.NoNodeException | AlreadyClosedException e) {
        newLiveNodes = emptySortedSet();
      }
      lastFetchedLiveNodes.set(newLiveNodes);
    }

    // Can't lock getUpdateLock() until we release the other, it would cause deadlock.
    SortedSet<String> oldLiveNodes, newLiveNodes;
    synchronized (getUpdateLock()) {
      newLiveNodes = lastFetchedLiveNodes.getAndSet(null);
      if (newLiveNodes == null) {
        // Someone else won the race to apply the last update, just exit.
        return;
      }

      oldLiveNodes = this.liveNodes;
      this.liveNodes = newLiveNodes;
      if (clusterState != null) {
        clusterState.setLiveNodes(newLiveNodes);
      }
    }
    if (oldLiveNodes.size() != newLiveNodes.size()) {
      if (log.isInfoEnabled()) {
        log.info(
            "Updated live nodes from ZooKeeper... ({}) -> ({})",
            oldLiveNodes.size(),
            newLiveNodes.size());
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
    }
    if (!oldLiveNodes.equals(newLiveNodes)) { // fire listeners
      liveNodesListeners.forEach(
          listener -> {
            if (listener.onChange(new TreeSet<>(oldLiveNodes), new TreeSet<>(newLiveNodes))) {
              removeLiveNodesListener(listener);
            }
          });
    }
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
    if (listener.onChange(
        new TreeSet<>(getClusterState().getLiveNodes()),
        new TreeSet<>(getClusterState().getLiveNodes()))) {
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
    return clusterState;
  }

  public Object getUpdateLock() {
    return this;
  }

  public SolrZkClient getZKClient() {
    return zkClient;
  }

  @Override
  public void close() {
    this.closed = true;

    notifications.shutdownNow();

    waitLatches.parallelStream()
        .forEach(
            c -> {
              c.countDown();
            });

    ExecutorUtil.shutdownAndAwaitTermination(notifications);
    collectionPropertiesZkStateReader.close();
    if (closeClient) {
      zkClient.close();
    }
    assert ObjectReleaseTracker.release(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  public String getLeaderUrl(String collection, String shard, int timeout)
      throws InterruptedException {
    Replica replica = getLeaderRetry(collection, shard, timeout);
    if (replica == null || replica.getBaseUrl() == null) {
      return null;
    }
    return replica.getCoreUrl();
  }

  public Replica getLeader(Set<String> liveNodes, DocCollection docCollection, String shard) {
    Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
    if (replica != null && liveNodes.contains(replica.getNodeName())) {
      return replica;
    }
    return null;
  }

  public Replica getLeader(String collection, String shard) {
    if (clusterState != null) {
      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
      if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
        return replica;
      }
    }
    return null;
  }

  public boolean isNodeLive(String node) {
    return liveNodes.contains(node);
  }

  /** Get shard leader properties, with retry if none exist. */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException {
    return getLeaderRetry(collection, shard, GET_LEADER_RETRY_DEFAULT_TIMEOUT);
  }

  /** Get shard leader properties, with retry if none exist. */
  public Replica getLeaderRetry(String collection, String shard, int timeout)
      throws InterruptedException {
    AtomicReference<Replica> leader = new AtomicReference<>();
    try {
      waitForState(
          collection,
          timeout,
          TimeUnit.MILLISECONDS,
          (n, c) -> {
            if (c == null) return false;
            Replica l = getLeader(n, c, shard);
            if (l != null) {
              log.debug("leader found for {}/{} to be {}", collection, shard, l);
              leader.set(l);
              return true;
            }
            return false;
          });
    } catch (TimeoutException e) {
      throw new SolrException(
          ErrorCode.SERVICE_UNAVAILABLE,
          "No registered leader was found after waiting for "
              + timeout
              + "ms "
              + ", collection: "
              + collection
              + " slice: "
              + shard
              + " saw state="
              + clusterState.getCollectionOrNull(collection)
              + " with live_nodes="
              + clusterState.getLiveNodes());
    }
    return leader.get();
  }

  /** Get path where shard leader properties live in zookeeper. */
  public static String getShardLeadersPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE
        + "/"
        + collection
        + "/"
        + SHARD_LEADERS_ZKNODE
        + (shardId != null ? ("/" + shardId) : "")
        + "/leader";
  }

  /** Get path where shard leader elections ephemeral nodes are. */
  public static String getShardLeadersElectPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE
        + "/"
        + collection
        + "/"
        + LEADER_ELECT_ZKNODE
        + (shardId != null ? ("/" + shardId + "/" + ELECTION_NODE) : "");
  }

  public List<ZkCoreNodeProps> getReplicaProps(
      String collection, String shardId, String thisCoreNodeName) {
    // TODO: It's odd that the default is to return replicas of type TLOG and NRT only
    return getReplicaProps(
        collection,
        shardId,
        thisCoreNodeName,
        null,
        null,
        EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT));
  }

  public List<ZkCoreNodeProps> getReplicaProps(
      String collection,
      String shardId,
      String thisCoreNodeName,
      Replica.State mustMatchStateFilter,
      Replica.State mustNotMatchStateFilter,
      final EnumSet<Replica.Type> acceptReplicaType) {
    assert thisCoreNodeName != null;
    ClusterState clusterState = this.clusterState;
    if (clusterState == null) {
      return null;
    }
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlicesMap() == null) {
      throw new ZooKeeperException(
          ErrorCode.BAD_REQUEST, "Could not find collection in zk: " + collection);
    }

    Map<String, Slice> slices = docCollection.getSlicesMap();
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      throw new ZooKeeperException(
          ErrorCode.BAD_REQUEST, "Could not find shardId in zk: " + shardId);
    }

    Map<String, Replica> shardMap = replicas.getReplicasMap();
    List<ZkCoreNodeProps> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String, Replica> entry :
        shardMap.entrySet().stream()
            .filter((e) -> acceptReplicaType.contains(e.getValue().getType()))
            .collect(Collectors.toList())) {
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());

      String coreNodeName = entry.getValue().getName();

      if (clusterState.liveNodesContain(nodeProps.getNodeName())
          && !coreNodeName.equals(thisCoreNodeName)) {
        if (mustMatchStateFilter == null
            || mustMatchStateFilter == Replica.State.getState(nodeProps.getState())) {
          if (mustNotMatchStateFilter == null
              || mustNotMatchStateFilter != Replica.State.getState(nodeProps.getState())) {
            nodes.add(nodeProps);
          }
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
   *
   * <p>N.B. Cluster properties are updated via ZK watchers, and so may not necessarily be
   * completely up-to-date. If you need to get the latest version, then use a {@link
   * ClusterProperties} instance.
   *
   * @param key the property to read
   * @param defaultValue a default value to use if no such property exists
   * @param <T> the type of the property
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, key);
    if (value == null) return defaultValue;
    return value;
  }

  /**
   * Same as the above but allows a full json path as a list of parts
   *
   * @param keyPath path to the property example ["collectionDefauls", "numShards"]
   * @param defaultValue a default value to use if no such property exists
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings({"unchecked"})
  public <T> T getClusterProperty(List<String> keyPath, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, keyPath);
    if (value == null) return defaultValue;
    return value;
  }

  /**
   * Get all cluster properties for this cluster
   *
   * <p>N.B. Cluster properties are updated via ZK watchers, and so may not necessarily be
   * completely up-to-date. If you need to get the latest version, then use a {@link
   * ClusterProperties} instance.
   *
   * @return a Map of cluster properties
   */
  public Map<String, Object> getClusterProperties() {
    return Collections.unmodifiableMap(clusterProperties);
  }

  private final Watcher clusterPropertiesWatcher =
      event -> {
        // session events are not change events, and do not remove the watcher
        if (Watcher.Event.EventType.None.equals(event.getType())) {
          return;
        }
        loadClusterProperties();
      };

  @SuppressWarnings("unchecked")
  private void loadClusterProperties() {
    try {
      while (true) {
        try {
          byte[] data =
              zkClient.getData(
                  ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, new Stat(), true);
          this.clusterProperties = (Map<String, Object>) Utils.fromJSON(data);
          log.debug("Loaded cluster properties: {}", this.clusterProperties);

          for (ClusterPropertiesListener listener : clusterPropertiesListeners) {
            listener.onChange(getClusterProperties());
          }
          return;
        } catch (KeeperException.NoNodeException e) {
          this.clusterProperties = Collections.emptyMap();
          log.debug("Loaded empty cluster properties");
          // set an exists watch, and if the node has been created since the last call,
          // read the data again
          if (zkClient.exists(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, true) == null)
            return;
        }
      }
    } catch (KeeperException | InterruptedException e) {
      log.error(
          "Error reading cluster properties from zookeeper", SolrZkClient.checkInterrupted(e));
    }
  }

  /** Get properties for a specific collection */
  public Map<String, String> getCollectionProperties(final String collection) {
    return collectionPropertiesZkStateReader.getCollectionProperties(collection, 0);
  }

  /** Get and cache collection properties for a given collection */
  public Map<String, String> getCollectionProperties(final String collection, long cacheForMillis) {
    return collectionPropertiesZkStateReader.getCollectionProperties(collection, cacheForMillis);
  }

  static String getCollectionPropsPath(final String collection) {
    return CollectionPropertiesZkStateReader.getCollectionPropsPath(collection);
  }

  /**
   * Returns the content of /security.json from ZooKeeper as a Map If the files doesn't exist, it
   * returns null.
   */
  public ConfigData getSecurityProps(boolean getFresh) {
    if (securityNodeWatcher == null) return new ConfigData(emptyMap(), -1);
    return securityNodeWatcher.getSecurityProps(getFresh);
  }

  /**
   * Returns the baseURL corresponding to a given node's nodeName -- NOTE: does not (currently)
   * imply that the nodeName (or resulting baseURL) exists in the cluster.
   *
   * @param nodeName name of the node
   * @return url that looks like {@code https://localhost:8983/solr}
   */
  public String getBaseUrlForNodeName(final String nodeName) {
    String urlScheme = getClusterProperty(URL_SCHEME, "http");
    return Utils.getBaseUrlForNodeName(nodeName, urlScheme, false);
  }

  /**
   * Returns the V2 baseURL corresponding to a given node's nodeName -- NOTE: does not (currently)
   * imply that the nodeName (or resulting baseURL) exists in the cluster.
   *
   * @param nodeName name of the node
   * @return url that looks like {@code https://localhost:8983/api}
   */
  public String getBaseUrlV2ForNodeName(final String nodeName) {
    String urlScheme = getClusterProperty(URL_SCHEME, "http");
    return Utils.getBaseUrlForNodeName(nodeName, urlScheme, true);
  }

  /** Watches a single collection's state.json. */
  class StateWatcher implements Watcher {
    private final String coll;
    private final String collectionPath;

    StateWatcher(String coll) {
      this.coll = coll;
      collectionPath = DocCollection.getCollectionPath(coll);
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }

      StatefulCollectionWatch scw = collectionWatches.statefulWatchesByCollectionName.get(coll);
      if (scw == null || scw.associatedWatcher != this) {
        // Collection no longer interesting, or we have been replaced by a different watcher.
        log.debug("Uninteresting collection {}", coll);
        return;
      }

      Set<String> liveNodes = ZkStateReader.this.liveNodes;
      if (log.isInfoEnabled()) {
        log.info(
            "A cluster state change: [{}] for collection [{}] has occurred - updating... (live nodes size: [{}])",
            event,
            coll,
            liveNodes.size());
      }

      refreshAndWatch(event.getType());
    }

    public void refreshAndWatch() {
      refreshAndWatch(null);
    }

    /**
     * Refresh collection state from ZK and leave a watch for future changes. As a side effect,
     * updates {@link #clusterState} and collection ref within {@link #collectionWatches} with the
     * results of the refresh.
     */
    public void refreshAndWatch(EventType eventType) {
      try {
        if (eventType == null || eventType == EventType.NodeChildrenChanged) {
          refreshAndWatchChildren();
          if (eventType == EventType.NodeChildrenChanged) {
            // only per-replica states modified. return
            return;
          }
        }

        DocCollection newState = fetchCollectionState(coll, this);
        collectionWatches.updateDocCollection(coll, newState);
        synchronized (getUpdateLock()) {
          constructState(Collections.singleton(coll));
        }

      } catch (KeeperException.SessionExpiredException
          | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
      } catch (KeeperException e) {
        log.error("Unwatched collection: [{}]", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Unwatched collection: [{}]", coll, e);
      }
    }

    private void refreshAndWatchChildren() throws KeeperException, InterruptedException {
      Stat stat = new Stat();
      List<String> replicaStates = null;
      try {
        replicaStates = zkClient.getChildren(collectionPath, this, stat, true);
        PerReplicaStates newStates =
            new PerReplicaStates(collectionPath, stat.getCversion(), replicaStates);
        DocCollection oldState = collectionWatches.getDocCollection(coll);
        final DocCollection newState =
            oldState != null
                ? oldState.setPerReplicaStates(newStates)
                : fetchCollectionState(coll, null);
        collectionWatches.updateDocCollection(coll, newState);
        synchronized (getUpdateLock()) {
          constructState(Collections.singleton(coll));
        }
        if (log.isDebugEnabled()) {
          log.debug(
              "updated per-replica states changed for: {}, ver: {} , new vals: {}",
              coll,
              stat.getCversion(),
              replicaStates);
        }

      } catch (NoNodeException e) {
        log.info("{} is deleted, stop watching children", collectionPath);
      }
    }
  }

  /** Watches /collections children . */
  class CollectionsChildWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      if (ZkStateReader.this.closed) {
        return;
      }

      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      log.debug("A collections change: [{}], has occurred - updating...", event);
      refreshAndWatch();
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }

    /** Must hold {@link #getUpdateLock()} before calling this method. */
    public void refreshAndWatch() {
      try {
        refreshCollectionList(this);
      } catch (KeeperException.SessionExpiredException
          | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }
  }

  /** Watches the live_nodes and syncs changes. */
  class LiveNodeWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "A live node change: [{}], has occurred - updating... (live nodes size: [{}])",
            event,
            liveNodes.size());
      }
      refreshAndWatch();
    }

    public void refreshAndWatch() {
      try {
        refreshLiveNodes(this);
      } catch (KeeperException.SessionExpiredException
          | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }
  }

  public DocCollection getCollectionLive(String coll) {
    try {
      return fetchCollectionState(coll, null);
    } catch (KeeperException e) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "Could not load collection from ZK: " + coll, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "Could not load collection from ZK: " + coll, e);
    }
  }

  private DocCollection fetchCollectionState(String coll, Watcher watcher)
      throws KeeperException, InterruptedException {
    String collectionPath = DocCollection.getCollectionPath(coll);
    while (true) {
      try {
        Stat stat = new Stat();
        byte[] data = zkClient.getData(collectionPath, watcher, stat, true);

        // This factory method can detect a missing configName and supply it by reading it from the
        // old ZK location.
        // TODO in Solr 10 remove that factory method
        ClusterState state =
            ZkClientClusterStateProvider.createFromJsonSupportingLegacyConfigName(
                stat.getVersion(),
                data,
                Collections.emptySet(),
                coll,
                zkClient,
                Instant.ofEpochMilli(stat.getCtime()));

        return state.getCollectionOrNull(coll);
      } catch (KeeperException.NoNodeException e) {
        if (watcher != null) {
          // Leave an exists watch in place in case a state.json is created later.
          Stat exists = zkClient.exists(collectionPath, watcher, true);
          if (exists != null) {
            // Rare race condition, we tried to fetch the data and couldn't find it, then we found
            // it exists. Loop and try again.
            continue;
          }
        }
        return null;
      } catch (PerReplicaStatesOps.PrsZkNodeNotFoundException e) {
        assert CommonTestInjection.injectBreakpoint(
            ZkStateReader.class.getName() + "/exercised", e);
        // could be a race condition that state.json and PRS entries are deleted between the
        // state.json fetch and PRS entry fetch
        Stat exists = zkClient.exists(collectionPath, watcher, true);
        if (exists == null) {
          log.info(
              "PRS entry for collection {} not found in ZK. It was probably deleted between state.json read and PRS entry read.",
              coll);

          return null;
        } else {
          throw e; // unexpected, PRS node not found but the collection state.json still exists
        }
      }
    }
  }

  @Deprecated // see DocCollection
  public static String getCollectionPathRoot(String coll) {
    return DocCollection.getCollectionPathRoot(coll);
  }

  @Deprecated // see DocCollection
  public static String getCollectionPath(String coll) {
    return DocCollection.getCollectionPath(coll);
  }

  /**
   * Notify this reader that a local Core is a member of a collection, and so that collection state
   * should be watched.
   *
   * <p>Not a public API. This method should only be called from ZkController.
   *
   * <p>The number of cores per-collection is tracked, and adding multiple cores from the same
   * collection does not increase the number of watches.
   *
   * @param collection the collection that the core is a member of
   * @see ZkStateReader#unregisterCore(String)
   */
  public void registerCore(String collection) {
    AtomicReference<StateWatcher> newWatcherRef = new AtomicReference<>();
    collectionWatches.compute(
        collection,
        (k, v) -> {
          if (v == null) {
            StateWatcher stateWatcher = new StateWatcher(collection);
            newWatcherRef.set(stateWatcher);
            v = new StatefulCollectionWatch(stateWatcher);
          }
          v.coreRefCount++;
          return v;
        });
    StateWatcher newWatcher = newWatcherRef.get();
    if (newWatcher != null) {
      newWatcher.refreshAndWatch();
    }
  }

  /**
   * Notify this reader that a local core that is a member of a collection has been closed.
   *
   * <p>Not a public API. This method should only be called from ZkController.
   *
   * <p>If no cores are registered for a collection, and there are no {@link
   * CollectionStateWatcher}s for that collection either, the collection watch will be removed.
   *
   * @param collection the collection that the core belongs to
   */
  public void unregisterCore(String collection) {
    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(
        collection,
        (k, v) -> {
          if (v == null) return null;
          if (v.coreRefCount > 0) v.coreRefCount--;
          if (v.canBeRemoved()) {
            lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
            reconstructState.set(true);
            return null;
          }
          return v;
        });
    if (reconstructState.get()) {
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }
  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   * <em>or</em> the set of live nodes changes.
   *
   * <p>The Watcher will automatically be removed when it's <code>onStateChanged</code> returns
   * <code>true</code>
   *
   * <p>This is method is just syntactic sugar for registering both a {@link DocCollectionWatcher}
   * and a {@link LiveNodesListener}. Callers that only care about one or the other (but not both)
   * are encouraged to use the more specific methods register methods as it may reduce the number of
   * ZooKeeper watchers needed, and reduce the amount of network/cpu used.
   *
   * @param collection the collection to watch
   * @param stateWatcher a watcher that will be called when the state changes
   * @see #registerDocCollectionWatcher
   * @see #registerLiveNodesListener
   */
  public void registerCollectionStateWatcher(
      String collection, CollectionStateWatcher stateWatcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper =
        new DocCollectionAndLiveNodesWatcherWrapper(collection, stateWatcher);

    registerDocCollectionWatcher(collection, wrapper);
    registerLiveNodesListener(wrapper);

    DocCollection state = clusterState.getCollectionOrNull(collection);
    if (stateWatcher.onStateChanged(liveNodes, state) == true) {
      removeCollectionStateWatcher(collection, stateWatcher);
    }
  }

  /**
   * Register a DocCollectionWatcher to be called when the cluster state for a collection changes.
   *
   * <p>The Watcher will automatically be removed when it's <code>onStateChanged</code> returns
   * <code>true</code>
   */
  public void registerDocCollectionWatcher(
      String collection, DocCollectionWatcher docCollectionWatcher) {
    AtomicReference<StateWatcher> newWatcherRef = new AtomicReference<>();
    collectionWatches.compute(
        collection,
        (k, v) -> {
          if (v == null) {
            StateWatcher stateWatcher = new StateWatcher(collection);
            newWatcherRef.set(stateWatcher);
            v = new StatefulCollectionWatch(stateWatcher);
          }
          v.stateWatchers.add(docCollectionWatcher);
          return v;
        });

    StateWatcher newWatcher = newWatcherRef.get();
    if (newWatcher != null) {
      newWatcher.refreshAndWatch();
    }

    DocCollection state = clusterState.getCollectionOrNull(collection);
    if (docCollectionWatcher.onStateChanged(state) == true) {
      removeDocCollectionWatcher(collection, docCollectionWatcher);
    }
  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * <p>Note that the predicate may be called again even after it has returned true, so implementors
   * should avoid changing state within the predicate call itself.
   *
   * <p>This implementation utilizes {@link CollectionStateWatcher} internally. Callers that don't
   * care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate} instead
   *
   * @param collection the collection to watch
   * @param wait how long to wait
   * @param unit the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   * @see #waitForState(String, long, TimeUnit, Predicate)
   * @see #registerCollectionStateWatcher
   */
  public void waitForState(
      final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {

    if (closed) {
      throw new AlreadyClosedException();
    }

    // Check predicate against known clusterState before trying to add watchers
    if (clusterState != null) {
      Set<String> liveNodes = clusterState.getLiveNodes();
      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      if (liveNodes != null && docCollection != null) {
        if (predicate.matches(liveNodes, docCollection)) {
          log.debug("Found {} directly in clusterState", predicate);
          return;
        }
      }
    }

    final CountDownLatch latch = new CountDownLatch(1);
    waitLatches.add(latch);
    final AtomicReference<DocCollection> docCollection = new AtomicReference<>();
    final AtomicReference<SolrException> thrownException = new AtomicReference<>();
    CollectionStateWatcher watcher =
        (n, c) -> {
          docCollection.set(c);
          boolean matches = false;
          try {
            matches = predicate.matches(n, c);
            if (matches) {
              latch.countDown();
              thrownException.set(null);
            }
          } catch (SolrException e) {
            if (thrownException.getAndSet(e) != null) {
              // Return if we have seen an exception twice
              latch.countDown();
              matches = true;
            }
          }

          return matches;
        };

    try {
      registerCollectionStateWatcher(collection, watcher);
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit)) {
        throw new TimeoutException(
            "Timeout waiting to see state for collection="
                + collection
                + " :"
                + docCollection.get());
      } else if (thrownException.get() != null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Error occurred while checking state",
            thrownException.get());
      }
    } finally {
      removeCollectionStateWatcher(collection, watcher);
      waitLatches.remove(latch);
    }
  }

  /**
   * Block until a Predicate returns true, or the wait times out
   *
   * <p>Note that the predicate may be called again even after it has returned true, so implementors
   * should avoid changing state within the predicate call itself. The predicate may also be called
   * concurrently when multiple state changes are seen in rapid succession.
   *
   * @param collection the collection to watch
   * @param wait how long to wait
   * @param unit the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @return the state of the doc collection after the predicate succeeds
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   */
  public DocCollection waitForState(
      final String collection, long wait, TimeUnit unit, Predicate<DocCollection> predicate)
      throws InterruptedException, TimeoutException {
    if (log.isDebugEnabled()) {
      log.debug("Waiting up to {}ms for state {}", unit.toMillis(wait), predicate);
    }
    if (closed) {
      throw new AlreadyClosedException();
    }

    // Check predicate against known clusterState before trying to add watchers
    if (clusterState != null) {
      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      if (docCollection != null) {
        if (predicate.test(docCollection)) {
          log.debug("Found {} directly in clusterState", predicate);
          return docCollection;
        }
      }
    }

    final CountDownLatch latch = new CountDownLatch(1);
    waitLatches.add(latch);
    AtomicReference<DocCollection> docCollectionReference = new AtomicReference<>();
    DocCollectionWatcher watcher =
        (c) -> {
          docCollectionReference.set(c);
          boolean matches = predicate.test(c);
          if (matches) latch.countDown();

          return matches;
        };

    try {
      registerDocCollectionWatcher(collection, watcher);
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException(
            "Timeout waiting to see state for collection="
                + collection
                + " :"
                + docCollectionReference.get());
      return docCollectionReference.get();
    } finally {
      removeDocCollectionWatcher(collection, watcher);
      waitLatches.remove(latch);
      if (log.isDebugEnabled()) {
        log.debug("Completed wait for {}", predicate);
      }
    }
  }

  /**
   * Block until a LiveNodesStatePredicate returns true, or the wait times out
   *
   * <p>Note that the predicate may be called again even after it has returned true, so implementors
   * should avoid changing state within the predicate call itself.
   *
   * @param wait how long to wait
   * @param unit the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   */
  public void waitForLiveNodes(long wait, TimeUnit unit, LiveNodesPredicate predicate)
      throws InterruptedException, TimeoutException {

    if (closed) {
      throw new AlreadyClosedException();
    }

    final CountDownLatch latch = new CountDownLatch(1);
    waitLatches.add(latch);

    LiveNodesListener listener =
        (o, n) -> {
          boolean matches = predicate.matches(o, n);
          if (matches) latch.countDown();
          return matches;
        };

    registerLiveNodesListener(listener);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException(
            "Timeout waiting for live nodes, currently they are: "
                + getClusterState().getLiveNodes());

    } finally {
      removeLiveNodesListener(listener);
      waitLatches.remove(latch);
    }
  }

  /**
   * Remove a watcher from a collection's watch list.
   *
   * <p>This allows Zookeeper watches to be removed if there is no interest in the collection.
   *
   * @param collection the collection
   * @param watcher the watcher
   * @see #registerCollectionStateWatcher
   */
  public void removeCollectionStateWatcher(String collection, CollectionStateWatcher watcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper =
        new DocCollectionAndLiveNodesWatcherWrapper(collection, watcher);

    removeDocCollectionWatcher(collection, wrapper);
    removeLiveNodesListener(wrapper);
  }

  /**
   * Remove a watcher from a collection's watch list.
   *
   * <p>This allows Zookeeper watches to be removed if there is no interest in the collection.
   *
   * @param collection the collection
   * @param watcher the watcher
   * @see #registerDocCollectionWatcher
   */
  public void removeDocCollectionWatcher(String collection, DocCollectionWatcher watcher) {
    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(
        collection,
        (k, v) -> {
          if (v == null) return null;
          v.stateWatchers.remove(watcher);
          if (v.canBeRemoved()) {
            lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
            reconstructState.set(true);
            assert CommonTestInjection.injectDelay(); // To unit test race condition
            return null;
          }
          return v;
        });
    if (reconstructState.get()) {
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }
  }

  /* package-private for testing */
  Set<DocCollectionWatcher> getStateWatchers(String collection) {
    final Set<DocCollectionWatcher> watchers = new HashSet<>();
    collectionWatches.compute(
        collection,
        (k, v) -> {
          if (v != null) {
            watchers.addAll(v.stateWatchers);
          }
          return v;
        });
    return watchers;
  }

  /* package-private for testing*/
  Map<String, StatefulCollectionWatch> getCollectionWatches() {
    return Collections.unmodifiableMap(collectionWatches.statefulWatchesByCollectionName);
  }

  public void registerCollectionPropsWatcher(
      final String collection, CollectionPropsWatcher propsWatcher) {

    collectionPropertiesZkStateReader.registerCollectionPropsWatcher(collection, propsWatcher);
  }

  public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
    collectionPropertiesZkStateReader.removeCollectionPropsWatcher(collection, watcher);
  }

  public static class ConfigData {
    public Map<String, Object> data;
    public int version;

    public ConfigData() {}

    public ConfigData(Map<String, Object> data, int version) {
      this.data = data;
      this.version = version;
    }
  }

  private void notifyStateWatchers(String collection, DocCollection collectionState) {
    if (this.closed) {
      return;
    }
    try {
      notifications.execute(new Notification(collection, collectionState));
    } catch (RejectedExecutionException e) {
      if (closed == false) {
        log.error("Couldn't run collection notifications for {}", collection, e);
      }
    }
  }

  private class Notification implements Runnable {

    final String collection;
    final DocCollection collectionState;

    private Notification(String collection, DocCollection collectionState) {
      this.collection = collection;
      this.collectionState = collectionState;
    }

    @Override
    public void run() {
      List<DocCollectionWatcher> watchers = new ArrayList<>();
      collectionWatches.compute(
          collection,
          (k, v) -> {
            if (v == null) return null;
            watchers.addAll(v.stateWatchers);
            return v;
          });
      for (DocCollectionWatcher watcher : watchers) {
        try {
          if (watcher.onStateChanged(collectionState)) {
            removeDocCollectionWatcher(collection, watcher);
          }
        } catch (Exception exception) {
          log.warn("Error on calling watcher", exception);
        }
      }
    }
  }

  //
  //  Aliases related
  //

  /** Access to the {@link Aliases}. */
  public final AliasesManager aliasesManager = new AliasesManager();

  /**
   * Get an immutable copy of the present state of the aliases. References to this object should not
   * be retained in any context where it will be important to know if aliases have changed.
   *
   * @return The current aliases, Aliases.EMPTY if not solr cloud, or no aliases have existed yet.
   *     Never returns null.
   */
  public Aliases getAliases() {
    return aliasesManager.getAliases();
  }

  public AliasesManager getAliasesManager() {
    return aliasesManager;
  }

  // called by createClusterStateWatchersAndUpdate()
  private void refreshAliases(AliasesManager watcher) throws KeeperException, InterruptedException {
    synchronized (getUpdateLock()) {
      constructState(Collections.emptySet());
      zkClient.exists(ALIASES, watcher, true);
    }
    aliasesManager.update();
  }

  /**
   * A class to manage the aliases instance, including watching for changes. There should only ever
   * be one instance of this class per instance of ZkStateReader. Normally it will not be useful to
   * create a new instance since this watcher automatically re-registers itself every time it is
   * updated.
   */
  public class AliasesManager implements Watcher { // the holder is a Zk watcher
    // note: as of this writing, this class if very generic. Is it useful to use for other ZK
    // managed things?
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private volatile Aliases aliases = Aliases.EMPTY;

    public Aliases getAliases() {
      return aliases; // volatile read
    }

    /**
     * Writes an updated {@link Aliases} to zk. It will retry if there are races with other
     * modifications, giving up after 30 seconds with a SolrException. The caller should understand
     * it's possible the aliases has further changed if it examines it.
     */
    public void applyModificationAndExportToZk(UnaryOperator<Aliases> op) {
      // The current aliases hasn't been update()'ed yet -- which is impossible?  Any way just
      // update it first.
      if (aliases.getZNodeVersion() == -1) {
        try {
          boolean updated = update();
          assert updated;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        } catch (KeeperException e) {
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        }
      }

      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      // note: triesLeft tuning is based on ConcurrentCreateRoutedAliasTest
      for (int triesLeft = 30; triesLeft > 0; triesLeft--) {
        // we could synchronize on "this" but there doesn't seem to be a point; we have a retry
        // loop.
        Aliases curAliases = getAliases();
        Aliases modAliases = op.apply(curAliases);
        final byte[] modAliasesJson = modAliases.toJSON();
        if (curAliases == modAliases) {
          log.debug(
              "Current aliases has the desired modification; no further ZK interaction needed.");
          return;
        }

        try {
          try {
            final Stat stat =
                getZkClient().setData(ALIASES, modAliasesJson, curAliases.getZNodeVersion(), true);
            setIfNewer(new SolrZkClient.NodeData(stat, modAliasesJson));
            return;
          } catch (KeeperException.BadVersionException e) {
            log.debug("{}", e, e);
            log.warn(
                "Couldn't save aliases due to race with another modification; will update and retry until timeout");
            // considered a backoff here, but we really do want to compete strongly since the normal
            // case is that we will do one update and succeed. This is left as a hot loop for
            // limited tries intentionally. More failures than that here probably indicate a bug or
            // a very strange high write frequency usage for aliases.json, timeouts mean zk is being
            // very slow to respond, or this node is being crushed by other processing and just
            // can't find any cpu cycles at all.
            update();
            if (deadlineNanos < System.nanoTime()) {
              throw new SolrException(
                  ErrorCode.SERVER_ERROR,
                  "Timed out trying to update aliases! "
                      + "Either zookeeper or this node may be overloaded.");
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        } catch (KeeperException e) {
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        }
      }
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Too many successive version failures trying to update aliases");
    }

    /**
     * Ensures the internal aliases is up-to-date. If there is a change, return true.
     *
     * @return true if an update was performed
     */
    public boolean update() throws KeeperException, InterruptedException {
      if (log.isDebugEnabled()) {
        log.debug("Checking ZK for most up to date Aliases {}", ALIASES);
      }
      // Call sync() first to ensure the subsequent read (getData) is up-to-date.
      zkClient.runWithCorrectThrows(
          "syncing aliases", () -> zkClient.getCuratorFramework().sync().forPath(ALIASES));
      return setIfNewer(zkClient.getNode(ALIASES, null, true));
    }

    // ZK Watcher interface
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      try {
        log.debug("Aliases: updating");

        // re-register the watch
        setIfNewer(zkClient.getNode(ALIASES, this, true));
      } catch (NoNodeException e) {
        // /aliases.json will not always exist
      } catch (KeeperException.ConnectionLossException
          | KeeperException.SessionExpiredException e) {
        // note: aliases.json is required to be present
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: ", e);
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    /**
     * Update the internal aliases reference with a new one, provided that its ZK version has
     * increased.
     *
     * @param n the node data
     * @return true if aliases have been updated to a new version, false otherwise
     */
    private boolean setIfNewer(SolrZkClient.NodeData n) {
      assert n.stat.getVersion() >= 0;
      synchronized (this) {
        int cmp = Integer.compare(aliases.getZNodeVersion(), n.stat.getVersion());
        if (cmp < 0) {
          if (log.isDebugEnabled()) {
            log.debug(
                "Aliases: cmp={}, new definition is: {}",
                cmp,
                Aliases.fromJSON(n.data, n.stat.getVersion()));
          }
          aliases = Aliases.fromJSON(n.data, n.stat.getVersion());
          this.notifyAll();
          return true;
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Aliases: cmp={}, not overwriting ZK version.", cmp);
          }
          assert cmp != 0 || Arrays.equals(aliases.toJSON(), n.data)
              : aliases + " != " + Aliases.fromJSON(n.data, n.stat.getVersion());
          return false;
        }
      }
    }
  }

  /**
   * Helper class that acts as both a {@link DocCollectionWatcher} and a {@link LiveNodesListener}
   * while wraping and delegating to a {@link CollectionStateWatcher}
   */
  private final class DocCollectionAndLiveNodesWatcherWrapper
      implements DocCollectionWatcher, LiveNodesListener {
    private final String collectionName;
    private final CollectionStateWatcher delegate;

    @Override
    public int hashCode() {
      return collectionName.hashCode() * delegate.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof DocCollectionAndLiveNodesWatcherWrapper that) {
        return this.collectionName.equals(that.collectionName)
            && this.delegate.equals(that.delegate);
      }
      return false;
    }

    public DocCollectionAndLiveNodesWatcherWrapper(
        final String collectionName, final CollectionStateWatcher delegate) {
      this.collectionName = collectionName;
      this.delegate = delegate;
    }

    @Override
    public boolean onStateChanged(DocCollection collectionState) {
      final boolean result = delegate.onStateChanged(ZkStateReader.this.liveNodes, collectionState);
      if (result) {
        // it might be a while before live nodes changes, so proactively remove ourselves
        removeLiveNodesListener(this);
      }
      return result;
    }

    @Override
    public boolean onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
      final DocCollection collection =
          ZkStateReader.this.clusterState.getCollectionOrNull(collectionName);
      final boolean result = delegate.onStateChanged(newLiveNodes, collection);
      if (result) {
        // it might be a while before collection changes, so proactively remove ourselves
        removeDocCollectionWatcher(collectionName, this);
      }
      return result;
    }
  }

  public DocCollection getCollection(String collection) {
    return clusterState == null ? null : clusterState.getCollectionOrNull(collection);
  }
}
