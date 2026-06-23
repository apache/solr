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
package org.apache.solr.cloud;

import com.codahale.metrics.Counter;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.LockListener;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DefaultZkACLProvider;
import org.apache.solr.common.cloud.DefaultZkCredentialsProvider;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.NodesSysPropsCacher;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrLifcycleListener;
import org.apache.solr.update.UpdateLog;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTIONS_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.packagemanager.PackageUtils.getMapper;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * Handle ZooKeeper interactions.
 * <p>
 * notes: loads everything on init, creates what's not there - further updates
 * are prompted with Watches.
 * <p>
 * TODO: exceptions during close on attempts to update cloud state
 */
public class ZkController implements Closeable, Runnable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static Counter coreRegisteredInZkEvent = Metrics.MARKS_METRICS.counter("core_registeredzk");
  static Counter coreEnteredLeaderElection = Metrics.MARKS_METRICS.counter("core_enteredleaderelection");
  static Counter coreWaitingForLeader = Metrics.MARKS_METRICS.counter("core_waitingforleader");

  public static final String CLUSTER_SHUTDOWN = "/cluster/shutdown";

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final ZkACLProvider zkACLProvider;

  private boolean closeZkClient = false;

  private volatile StatePublisher statePublisher;

  private volatile ZkDistributedQueue overseerJobQueue;
  private volatile OverseerTaskQueue overseerCollectionQueue;
  private volatile OverseerTaskQueue overseerConfigSetQueue;

  private volatile DistributedMap overseerRunningMap;
  private volatile DistributedMap overseerCompletedMap;
  private volatile DistributedMap overseerFailureMap;
  private volatile DistributedMap asyncIdsMap;

  public final static String COLLECTION_PARAM_PREFIX = "collection.";
  public final static String CONFIGNAME_PROP = "configName";
  private volatile boolean isShutdownCalled;
  private volatile boolean dcCalled;
  private volatile boolean started;


  public final ExecutorService zkRegisterExecutor = ParWork.getExecutorService("zkRegisterExecutor", Integer.MAX_VALUE, true);

  @Override
  public void run() {
    disconnect(true);
    log.info("Continuing to Solr shutdown");
  }

  public boolean isDcCalled() {
    return dcCalled;
  }

  public void removeShardLeaderElector(String name) {
    LeaderElector elector = leaderElectors.remove(name);
    IOUtils.closeQuietly(elector);
  }

  public LeaderElector getLeaderElector(String name) {
    return leaderElectors.get(name);
  }
  private static final byte[] emptyJson = Utils.toJSON(Collections.emptyMap());

  private final Map<String, LeaderElector> leaderElectors = new ConcurrentHashMap<>(32);

//  private final Map<ContextKey, ElectionContext> electionContexts = new ConcurrentHashMap<>(16) {
//    @Override
//    public ElectionContext put(ContextKey key, ElectionContext value) {
//      if (ZkController.this.isClosed || cc.isShutDown()) {
//        throw new AlreadyClosedException();
//      }
//      return super.put(key, value);
//    }
//  };

//  private final Map<ContextKey, ElectionContext> overseerContexts = new ConcurrentHashMap<>() {
//    @Override
//    public ElectionContext put(ContextKey key, ElectionContext value) {
//      if (ZkController.this.isClosed || cc.isShutDown()) {
//        throw new AlreadyClosedException();
//      }
//      return super.put(key, value);
//    }
//  };

  private final SolrZkClient zkClient;
  public volatile ZkStateReader zkStateReader;
  private volatile SolrCloudManager cloudManager;
  private volatile CloudHttp2SolrClient cloudSolrClient;

  private final String zkServerAddress;          // example: 127.0.0.1:54062/solr

  private final int localHostPort;      // example: 54065
  private final String hostName;           // example: 127.0.0.1
  private final String nodeName;           // example: 127.0.0.1:54065_solr
  private volatile String baseURL;            // example: http://127.0.0.1:54065/solr

  private final CloudConfig cloudConfig;
  private volatile NodesSysPropsCacher sysPropsCacher;

  protected volatile LeaderElector overseerElector;

  private final Map<String, ReplicateFromLeader> replicateFromLeaders = new ConcurrentHashMap<>(32);
  private final Map<String, ZkCollectionTerms> collectionToTerms = new ConcurrentHashMap<>(32);

  //private final ReentrantLock collectionToTermsLock = new ReentrantLock(true);

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private final CoreContainer cc;

  protected volatile Overseer overseer;

  private final int leaderVoteWait;
  private final int leaderConflictResolveWait;

  private final int clientTimeout;

  private volatile boolean isClosed;

  private final ReentrantLock initLock = new ReentrantLock();

  private final ReentrantLock cloudManagerLock = new ReentrantLock();

  private final Map<String, Throwable> replicasMetTragicEvent = new ConcurrentHashMap<>(16);

  @Deprecated
  // keeps track of replicas that have been asked to recover by leaders running on this node
  private final Map<String, String> replicasInLeaderInitiatedRecovery = new HashMap<>();

  // keeps track of a list of objects that need to know a new ZooKeeper session was created after expiration occurred
  // ref is held as a HashSet since we clone the set before notifying to avoid synchronizing too long
  private final Set<OnReconnect> reconnectListeners = ConcurrentHashMap.newKeySet();

  private static class MyLockListener implements LockListener {
    private final CountDownLatch lockWaitLatch;

    public MyLockListener(CountDownLatch lockWaitLatch) {
      this.lockWaitLatch = lockWaitLatch;
    }

    @Override
    public void lockAcquired() {
      lockWaitLatch.countDown();
    }

    @Override
    public void lockReleased() {

    }
  }

  public static class RegisterCoreAsync implements Callable<Object> {

    private final ZkController zkController;
    final CoreDescriptor descriptor;
    final boolean afterExpiration;

    public RegisterCoreAsync(ZkController zkController, CoreDescriptor descriptor, boolean afterExpiration) {
      this.descriptor = descriptor;
      this.afterExpiration = afterExpiration;
      this.zkController = zkController;
    }

    public Object call() throws Exception {
      MDCLoggingContext.setCoreName(descriptor.getName());
      try {
        log.info("Registering core with ZK {} afterExpiration? {}", descriptor.getName(), afterExpiration);

        if (zkController.isDcCalled() || zkController.getCoreContainer().isShutDown() || (afterExpiration && !descriptor.getCloudDescriptor().hasRegistered())) {
          return null;
        }

        try {
          zkController.register(descriptor.getName(), descriptor, afterExpiration);
        } catch (AlreadyClosedException e) {
         // log.warn("Error registering core name={} afterExpireation={}", descriptor.getName(), afterExpiration, e);
        } catch (Exception e) {
          log.error("Error registering core name={} afterExpireation={}", descriptor.getName(), afterExpiration, e);
        }

        return descriptor;
      } finally {
        MDCLoggingContext.clear();
      }
    }
  }



  // notifies registered listeners after the ZK reconnect in the background
  private static class OnReconnectNotifyAsync implements Callable<Object> {

    private final OnReconnect listener;

    OnReconnectNotifyAsync(OnReconnect listener) {
      this.listener = listener;
    }

    @Override
    public Object call() throws Exception {
      listener.command();
      return null;
    }
  }


  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig) throws InterruptedException, IOException, TimeoutException {
    this(cc, new SolrZkClient(), cloudConfig);
    this.closeZkClient = true;
  }

  /**
   * @param cc Core container associated with this controller. cannot be null.
   * @param cloudConfig configuration for this controller. TODO: possibly redundant with CoreContainer
   */
  public ZkController(final CoreContainer cc, SolrZkClient zkClient, CloudConfig cloudConfig)
      throws InterruptedException, TimeoutException, IOException {
    assert new CloseTracker() != null;
    if (cc == null) log.error("null corecontainer");
    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    try {
      this.cc = cc;
      this.cloudConfig = cloudConfig;
      this.zkClient = zkClient;

      // be forgiving and strip this off leading/trailing slashes
      // this allows us to support users specifying hostContext="/" in
      // solr.xml to indicate the root context, instead of hostContext=""
      // which means the default of "solr"
      String localHostContext = trimLeadingAndTrailingSlashes(cloudConfig.getSolrHostContext());

      this.zkServerAddress = zkClient.getZkServerAddress();
      this.localHostPort = cloudConfig.getSolrHostPort();
      if (log.isDebugEnabled()) log.debug("normalize hostname {}", cloudConfig.getHost());
      this.hostName = normalizeHostName(cloudConfig.getHost());
      if (log.isDebugEnabled()) log.debug("generate node name");
      this.nodeName = generateNodeName(this.hostName, Integer.toString(this.localHostPort), localHostContext);
      log.info("node name={}", nodeName);
      MDCLoggingContext.setNode(nodeName);


      this.leaderVoteWait = cloudConfig.getLeaderVoteWait();
      this.leaderConflictResolveWait = cloudConfig.getLeaderConflictResolveWait();

      this.clientTimeout = cloudConfig.getZkClientTimeout();

      String zkACLProviderClass = cloudConfig.getZkACLProviderClass();

      if (zkACLProviderClass != null && !StringUtils.isBlank(zkACLProviderClass)) {
        zkACLProvider = cc.getResourceLoader().newInstance(zkACLProviderClass, ZkACLProvider.class);
      } else {
        zkACLProvider = new DefaultZkACLProvider();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception during ZkController init", e);
      throw e;
    }

    assert ObjectReleaseTracker.getInstance().track(this);
  }

  public void start() {
    if (started) throw new IllegalStateException("Already started");

    started = true;

    try {
      if (zkClient.exists( ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName)) {
        removeEphemeralLiveNode();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
    }

    boolean isRegistered = SolrLifcycleListener.isRegistered(this);
    if (!isRegistered) {
      SolrLifcycleListener.registerShutdown(this);
    }

    String zkCredentialsProviderClass = cloudConfig.getZkCredentialsProviderClass();
    if (zkCredentialsProviderClass != null && !StringUtils.isBlank(zkCredentialsProviderClass)) {
      zkClient.getConnectionManager().setZkCredentialsToAddAutomatically(cc.getResourceLoader().newInstance(zkCredentialsProviderClass, ZkCredentialsProvider.class));
    } else {
      zkClient.getConnectionManager().setZkCredentialsToAddAutomatically(new DefaultZkCredentialsProvider());
    }
    addOnReconnectListener(getConfigDirListener());

    zkClient.setAclProvider(zkACLProvider);
    zkClient.getConnectionManager().setOnReconnect(new OnReconnect() {

      @Override
      public void command() {
        // protect against startup and quick expiration reconnect attempts
        initLock.lock();
        try {
          if (cc.isShutDown() || isClosed() || isShutdownCalled) {
            log.info("skipping zk reconnect logic due to shutdown");
            return;
          }
          // init() assigns statePublisher/zkStateReader/overseerElector under initLock, but this
          // OnReconnect listener is registered in start() BEFORE init() runs. A session event in the
          // start()->init() window (or a partially-failed init) would otherwise NPE in the async task
          // below, which dereferences all three. We hold initLock here, so once they are set, init()
          // has completed assigning them. Skip reconnect handling until then.
          if (statePublisher == null || zkStateReader == null || overseerElector == null) {
            log.info("skipping zk reconnect logic, ZkController not fully initialized yet");
            return;
          }
          ParWork.getRootSharedExecutor().submit(() -> {
            log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
            try {

              removeEphemeralLiveNode();

              statePublisher.clearStatCache();

              // recreate our watchers first so that they exist even on any problems below
              zkStateReader.createClusterStateWatchersAndUpdate();

              // this is troublesome - we dont want to kill anything the old
              // leader accepted
              // though I guess sync will likely get those updates back? But
              // only if
              // he is involved in the sync, and he certainly may not be
              // ExecutorUtil.shutdownAndAwaitTermination(cc.getCmdDistribExecutor());
              // we need to create all of our lost watches

              // seems we dont need to do this again...
              // Overseer.createClientNodes(zkClient, getNodeName());

              // start the overseer first as following code may need it's processing

              // Route through rejoinOverseerElection (not overseerElector.retryElection directly) so the
              // overseer.isCloseAndDone()/isClosed/isShutdownCalled/null-context guards apply: a reconnect
              // must not re-win overseer election around an already closed-and-done Overseer that would then
              // process nothing.
              rejoinOverseerElection(false);

              Collection<CoreDescriptor> descriptors = getCoreContainer().getCoreDescriptors();
              // re register all descriptors

              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it
                  // was
                  // a leader that was expired - as well as what to do about
                  // leaders/overseers
                  // with connection loss
                  try {
                    // unload solrcores that have been 'failed over'
                    // throwErrorIfReplicaReplaced(descriptor);

                    zkRegisterExecutor.submit(new RegisterCoreAsync(ZkController.this, descriptor, true));

                  } catch (Exception e) {
                    SolrException.log(log, "Error registering SolrCore", e);
                  }
                }
              }

              // notify any other objects that need to know when the session was re-connected

              // the OnReconnect operation can be expensive per listener, so do that async in the background
              try (ParWork work = new ParWork(this)) {
                reconnectListeners.forEach(listener -> {
                  try {
                    work.collect(new OnReconnectNotifyAsync(listener));
                  } catch (Exception exc) {
                    // not much we can do here other than warn in the log
                    log.warn("Error when notifying OnReconnect listener {} after session re-connected.", listener, exc);
                  }
                });
              }

              createEphemeralLiveNode();

            } catch (AlreadyClosedException e) {
              log.info("Already closed");
            } catch (Exception e) {
              SolrException.log(log, "", e);
            }
          });
        } finally {
          initLock.unlock();
        }
      }

      @Override
      public String getName() {
        return "ZkController";
      }
    });

    zkClient.setDisconnectListener(() -> {
      try (ParWork worker = new ParWork("disconnected", true)) {
        worker.collect(ZkController.this.overseerElector);
        worker.collect(ZkController.this.overseer);
        worker.collect(leaderElectors.values());
        leaderElectors.clear();
        // I don't think so...
//        worker.collect("clearZkCollectionTerms", () -> {
//          clearZkCollectionTerms();
//        });
      }

    });
    init();
  }

  private ElectionContext getOverseerContext() {
    return new OverseerElectionContext(overseerElector, localHostPort, zkClient, overseer);
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  public NodesSysPropsCacher getSysPropsCacher() {
    return sysPropsCacher;
  }

  public void disconnect(boolean publishDown) {
    log.info("disconnect");
    this.dcCalled = true;

    try {
      removeEphemeralLiveNode();
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
    }

    for (LeaderElector elector : leaderElectors.values()) {
      elector.disableRemoveWatches();
    }

    try (ParWork closer = new ParWork(this, true)) {
      closer.collect("replicateFromLeaders", replicateFromLeaders);
      closer.collect(leaderElectors);
    }
//
//    if (publishDown) {
//      try {
//        publishNodeAs(getNodeName(), OverseerAction.DOWNNODE);
//      } catch (Exception e) {
//        log.warn("Problem publish node as DOWN", e);
//      }
//    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing ZkController");
    //assert closeTracker.close();

    this.isShutdownCalled = true;

    this.isClosed = true;

    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(sysPropsCacher);
      closer.collect(cloudManager);
      closer.collect(cloudSolrClient);

      collectionToTerms.forEach((s, zkCollectionTerms) -> {
        closer.collect(zkCollectionTerms);
      });

    } finally {
      if (statePublisher != null) {
        statePublisher.close();
      }

      IOUtils.closeQuietly(statePublisher);
      IOUtils.closeQuietly(overseerElector);
      if (overseer != null) {
        try {
          overseer.closeAndDone();
        } catch (Exception e) {
          log.warn("Exception closing Overseer", e);
        }
      }
      if (zkStateReader != null) {
        zkStateReader.disableCloseLock();
      }
      IOUtils.closeQuietly(zkStateReader);

      if (closeZkClient && zkClient != null) {
        zkClient.disableCloseLock();
        IOUtils.closeQuietly(zkClient);
      }

      SolrLifcycleListener.removeShutdown(this);

      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

  /**
   * Best effort to give up the leadership of a shard in a core after hitting a tragic exception
   * @param cd The current core descriptor
   * @param tragicException The tragic exception from the {@code IndexWriter}
   */
  public void giveupLeadership(CoreDescriptor cd, Throwable tragicException) {
    assert tragicException != null;
    assert cd != null;
    DocCollection dc = getClusterState().getCollectionOrNull(cd.getCollectionName());
    if (dc == null) return;

    Slice shard = dc.getSlice(cd.getCloudDescriptor().getShardId());
    if (shard == null) return;

    // if this replica is not a leader, it will be put in recovery state by the leader
    if (shard.getReplica(cd.getName()) != shard.getLeader(zkStateReader.getLiveNodes())) return;

    int numActiveReplicas = shard.getReplicas(
        rep -> rep.getState() == Replica.State.ACTIVE
            && rep.getType() != Type.PULL
            && zkStateReader.getLiveNodes().contains(rep.getNodeName())
    ).size();

    // at least the leader still be able to search, we should give up leadership if other replicas can take over
    if (numActiveReplicas >= 2) {
      String key = cd.getCollectionName() + ":" + cd.getName();
      //TODO better handling the case when delete replica was failed
      if (replicasMetTragicEvent.putIfAbsent(key, tragicException) == null) {
        log.warn("Leader {} met tragic exception, give up its leadership", key, tragicException);
        try {
          // by using Overseer to remove and add replica back, we can do the task in an async/robust manner
          Object2ObjectMap<String,Object> props = new Object2ObjectLinkedOpenHashMap<>(16, 0.5f);
          props.put(Overseer.QUEUE_OPERATION, "deletereplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(REPLICA_PROP, cd.getName());
          overseerCollectionQueue.offer(Utils.toJSON(new ZkNodeProps(props)), false);

          props.clear();
          props.put(Overseer.QUEUE_OPERATION, "addreplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().name().toUpperCase(Locale.ROOT));
          props.put(CoreAdminParams.NODE, nodeName);
          overseerCollectionQueue.offer(Utils.toJSON(new ZkNodeProps(props)), false);
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          // Exceptions are not bubbled up. giveupLeadership is best effort, and is only called in case of some other
          // unrecoverable error happened
          log.error("Met exception on give up leadership for {}", key, e);
          replicasMetTragicEvent.remove(key);
        }
      }
    }
  }


  /**
   * Returns true if config file exists
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return zkStateReader.getClusterState();
  }

  public SolrCloudManager getSolrCloudManager() {
    if (cloudManager != null) {
      return cloudManager;
    }
    cloudManagerLock.lock();
    try {
      if (cloudManager != null) {
        return cloudManager;
      }
      cloudSolrClient = new CloudHttp2SolrClient.Builder(zkStateReader)
          .withHttpClient(cc.getUpdateShardHandler().getTheSharedHttpClient())
          .build();
      cloudSolrClient.connect();
      cloudManager = new SolrClientCloudManager(
          new ZkDistributedQueueFactory(zkClient),
          cloudSolrClient,
          cc.getObjectCache(), cc.getUpdateShardHandler().getTheSharedHttpClient());
      cloudManager.getClusterStateProvider().connect();
    } finally {
      cloudManagerLock.unlock();
    }
    return cloudManager;
  }

  public CloudHttp2SolrClient getCloudSolrClient() {
    return  cloudSolrClient;
  }

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  public static String normalizeHostName(String host) {

    if (host == null || host.length() == 0) {
      String hostaddress;
      try {
        hostaddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        hostaddress = "127.0.0.1"; // cannot resolve system hostname, fall through
      }
      // Re-get the IP again for "127.0.0.1", the other case we trust the hosts
      // file is right.
      if ("127.0.0.1".equals(hostaddress)) {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
          netInterfaces = NetworkInterface.getNetworkInterfaces();
          while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
              InetAddress ip = ips.nextElement();
              if (ip.isSiteLocalAddress()) {
                hostaddress = ip.getHostAddress();
              }
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          SolrException.log(log,
              "Error while looking for a better host name than 127.0.0.1", e);
        }
      }
      host = hostaddress;
    } else {
      if (log.isDebugEnabled()) log.debug("remove host scheme");
      if (URLUtil.hasScheme(host)) {
        host = URLUtil.removeScheme(host);
      }
    }
    if (log.isDebugEnabled()) log.debug("return host {}", host);
    return host;
  }

  public String getHostName() {
    return hostName;
  }

  public int getHostPort() {
    return localHostPort;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * @return zookeeper server address
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  boolean isClosed() {
    return isClosed;
  }

  boolean isShutdownCalled() {
    return isShutdownCalled;
  }

  /**
   * Create the zknodes necessary for a cluster to operate
   *
   * @param zkClient a SolrZkClient
   * @throws KeeperException      if there is a Zookeeper error
   * @throws InterruptedException on interrupt
   */
  public static void createClusterZkNodes(SolrZkClient zkClient)
      throws KeeperException, InterruptedException, IOException {
    log.info("Creating cluster zk nodes");
    // we want to have a full zk layout at the start
    // this is especially important so that we don't miss creating
    // any watchers with ZkStateReader on startup

    Map<String,byte[]> paths = new TreeMap();

    paths.put(ZkStateReader.LIVE_NODES_ZKNODE, null);
    paths.put(ZkStateReader.CONFIGS_ZKNODE, null);
    paths.put(ZkStateReader.ALIASES, emptyJson);

    paths.put("/overseer", null);
    paths.put(Overseer.OVERSEER_ELECT, null);
    paths.put(Overseer.OVERSEER_ELECT + "/leader", null);
    paths.put(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null);

    paths.put(Overseer.OVERSEER_QUEUE, null);
    paths.put(Overseer.OVERSEER_COLLECTION_QUEUE_WORK, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_RUNNING, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_COMPLETED, null);

    paths.put(Overseer.OVERSEER_COLLECTION_MAP_FAILURE, null);
    paths.put(Overseer.OVERSEER_ASYNC_IDS, null);

//
    //   operations.add(zkClient.createPathOp(ZkStateReader.CLUSTER_PROPS, emptyJson));
    paths.put(ZkStateReader.SOLR_PKGS_PATH, getMapper().writeValueAsString(Collections.emptyMap()).getBytes("UTF-8"));
    paths.put(PackageUtils.REPOSITORIES_ZK_PATH, "[]".getBytes(StandardCharsets.UTF_8));

    paths.put(ZkStateReader.ROLES, emptyJson);

    paths.put("/clusterstate.json", emptyJson);


    paths.put(COLLECTIONS_ZKNODE, null);
    
    // create-only: never overwrite existing seed nodes (e.g. /aliases.json, /roles.json, /clusterstate.json)
    // on a re-init of an already-bootstrapped cluster — otherwise the empty seed data would wipe them.
    zkClient.mkdirs(paths, false);

    try {
      zkClient.mkdir(ZkStateReader.SOLR_SECURITY_CONF_PATH, emptyJson);
    } catch (KeeperException.NodeExistsException e) {
      // okay, can be prepopulated
    }
    try {
      zkClient.mkdir(ZkStateReader.CLUSTER_PROPS, emptyJson);
    } catch (KeeperException.NodeExistsException e) {
      // okay, can be prepopulated
    }

    if (!Boolean.getBoolean("solr.suppressDefaultConfigBootstrap")) {
      bootstrapDefaultConfigSet(zkClient);
    } else {
      log.info("Supressing upload of default config set");
    }

    if (log.isDebugEnabled()) log.debug("Creating final {} node", "/cluster/init");
    try {
      zkClient.mkdir( "/cluster_init_done");
    } catch (KeeperException.NodeExistsException e) {
      // another node already completed cluster bootstrap; the marker is a one-time signal so this is fine
    }

  }

  private static void bootstrapDefaultConfigSet(SolrZkClient zkClient) throws KeeperException, InterruptedException, IOException {
    if (!zkClient.exists("/configs/_default")) {
      String configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn("The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset {} {}"
            , "intended to be the default. Current 'solr.default.confdir' value:"
            , System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        ZkMaintenanceUtils.upConfig(zkClient, Paths.get(configDirPath), ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
      }
    }
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from.
   * First tries the sysprop "solr.default.confdir". If not found, tries to find
   * the _default dir relative to the sysprop "solr.install.dir".
   * Returns null if not found anywhere.
   *
   * @lucene.internal
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  public static String getDefaultConfigDirPath() {
    String configDirPath = null;
    String serverSubPath = "solr" + File.separator +
        "configsets" + File.separator + "_default" +
        File.separator + "conf";
    String subPath = File.separator + "server" + File.separator + serverSubPath;
    String defaultConfigSet = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    log.info("{} set to {}", SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, defaultConfigSet);
    if (defaultConfigSet != null) {
      configDirPath = new File(defaultConfigSet).getAbsolutePath();
    }
    return configDirPath;
  }

  private void init() {

    try {

    try {
      zkClient.mkdir("/cluster_init");
      log.info("make cluster nodes");
      createClusterZkNodes(zkClient);
    } catch (KeeperException.NodeExistsException e) {

    } catch (IOException e) {
      log.error("Error in create cluster zk nodes", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    // Protect against immediate zk expiration and reconnect race
    initLock.lock();
    try {
      zkStateReader = new ZkStateReader(zkClient, () -> {
        if (cc != null) cc.securityNodeChanged();
      });
      zkStateReader.enableCloseLock();
      zkStateReader.setNode(nodeName);
      zkStateReader.setLeaderChecker(name -> {
        LeaderElector elector = leaderElectors.get(name);
        if (elector != null && elector.isLeader()) {
          return true;
        }
        return false;
      });
      zkStateReader.setCollectionRemovedListener(this::removeCollectionTerms);
      this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

      zkStateReader.createClusterStateWatchersAndUpdate();

      this.overseer = new Overseer(cc.getUpdateShardHandler(), CommonParams.CORES_HANDLER_PATH, this, cloudConfig);

      try {
        this.overseerRunningMap = Overseer.getRunningMap(zkClient);
        this.overseerCompletedMap = Overseer.getCompletedMap(zkClient);
        this.overseerFailureMap = Overseer.getFailureMap(zkClient);
        this.asyncIdsMap = Overseer.getAsyncIdsMap(zkClient);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      this.overseerJobQueue = overseer.getStateUpdateQueue();
      this.overseerCollectionQueue = Overseer.getCollectionQueue(zkClient);
      this.overseerConfigSetQueue = Overseer.getConfigSetQueue(zkClient);

      statePublisher = new StatePublisher(zkStateReader, cc);
      statePublisher.start();

      this.sysPropsCacher = new NodesSysPropsCacher(getSolrCloudManager().getNodeStateProvider(), nodeName, zkStateReader);
      overseerElector = new LeaderElector(this);
      //try (ParWork worker = new ParWork(this, false, true)) {
      // start the overseer first as following code may need it's processing
      // worker.collect("startOverseer", () -> {
      ElectionContext context = getOverseerContext();
      if (log.isDebugEnabled()) log.debug("Overseer setting up context {}", context.replica.getInternalId());
      overseerElector.setup(context);

      log.info("Overseer joining election {}", context.replica.getNodeName());
      overseerElector.joinElection(false);
    } finally {
      initLock.unlock();
    }

    } catch (Exception e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

  }

//  private synchronized void shutdown() {
//    if (this.isShutdownCalled) return;
//    this.isShutdownCalled = true;
//
//    log.info("Cluster shutdown initiated");
//
//    URL url;
//    try {
//      url = new URL(getHostName() + ":" + getHostPort() + "/shutdown?token=" + "solrrocks");
//    } catch (MalformedURLException e) {
//      SolrException.log(log, e);
//      return;
//    }
//    try {
//      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//      connection.setRequestMethod("POST");
//      connection.getResponseCode();
//      log.info("Shutting down " + url + ": " + connection.getResponseCode() + " " + connection.getResponseMessage());
//    } catch (SocketException e) {
//      SolrException.log(log, e);
//      // Okay - the server is not running
//    } catch (IOException e) {
//      SolrException.log(log, e);
//      return;
//    }
//  }

  public void publishDownStates() throws KeeperException {
    publishNodeAs(nodeName, OverseerAction.DOWNNODE);
  }

  /**
   * Validates if the chroot exists in zk (or if it is successfully created).
   * Optionally, if create is set to true this method will create the path in
   * case it doesn't exist
   *
   * @return true if the path exists or is created false if the path doesn't
   * exist and 'create' = false
   */
  public static boolean checkChrootPath(String zkHost, boolean create)
      throws KeeperException, InterruptedException {
    // Restored: when zkHost carries a chroot (e.g. host:port/foo/bar), the chroot znode must be created
    // BEFORE any config/collection upload, otherwise the first relative create (e.g. /configs) fails with
    // NoNode because its parent chroot does not exist (ZkCLITest.testBootstrapWithChroot). Stubbing this to
    // `return true` skipped that ordering. Connect a temporary chroot-less client to create the chroot.
    if (!SolrZkClient.containsChroot(zkHost)) {
      return true;
    }
    log.trace("zkHost includes chroot");
    String chrootPath = zkHost.substring(zkHost.indexOf("/"), zkHost.length());

    SolrZkClient tmpClient = new SolrZkClient(zkHost.substring(0, zkHost.indexOf("/")), 60000, 30000);
    tmpClient.start();
    try {
      boolean exists = tmpClient.exists(chrootPath);
      if (!exists && create) {
        tmpClient.makePath(chrootPath, false, true);
        exists = true;
      }
      return exists;
    } finally {
      tmpClient.close();
    }
  }

  public boolean isConnected() {
    return zkClient.isConnected();
  }

  public void createEphemeralLiveNode() {
    String nodeName = this.nodeName;
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;

    log.info("Create our ephemeral live node {}", ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName);

    createLiveNodeImpl(nodePath);
  }

  private void createLiveNodeImpl(String nodePath) {
    try {
      try {
        zkClient.create(nodePath, (byte[]) null, CreateMode.EPHEMERAL, true);
      } catch (KeeperException.NodeExistsException e) {
        // A stale ephemeral from a prior (now-expired) session may not be reaped by the ZK server yet
        // when the new session recreates the live node on reconnect (common right after expiry). The
        // node is always our own nodeName path, so delete the stale node and recreate it under the
        // current session rather than aborting the reconnect (which would leave this node permanently
        // absent from live_nodes for the new session).
        log.warn("Our ephemeral live node already exists, removing the stale node and recreating {}", nodePath);
        try {
          zkClient.delete(nodePath, -1, true, false);
        } catch (KeeperException.NoNodeException nne) {
          // already gone - fine
        }
        zkClient.create(nodePath, (byte[]) null, CreateMode.EPHEMERAL, true);
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeEphemeralLiveNode() {
    if (zkClient.isAlive()) {
      log.info("Removing our ephemeral live node");
      String nodeName = this.nodeName;
      String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
      try {
        zkClient.delete(nodePath, -1, true, false);
      } catch (NoNodeException | SessionExpiredException e) {
        // okay
      } catch (Exception e) {
        log.warn("Could not remove ephemeral live node {}", nodePath, e);
      }
    }
  }

  public String getNodeName() {
    return nodeName;
  }

  /**
   * Returns true if the path exists
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path);
  }

  public void register(String coreName, final CoreDescriptor desc) throws Exception {
      register(coreName, desc, false);
  }

  public boolean isOverseerLeader() {
    return overseerElector != null && overseerElector.isLeader();
  }

  public static volatile Predicate<CoreDescriptor> testing_beforeRegisterInZk;

  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  private void register(String coreName, final CoreDescriptor desc, boolean afterExpiration) {
    if (getCoreContainer().isShutDown() || dcCalled) {
      throw new AlreadyClosedException();
    }

    if (testing_beforeRegisterInZk != null) {
      boolean didTrigger = testing_beforeRegisterInZk.test(desc);
      if (log.isDebugEnabled()) {
        log.debug("{} pre-zk hook", (didTrigger ? "Ran" : "Skipped"));
      }
    }

    MDCLoggingContext.setCoreName(desc.getName());
    try {
      final String baseUrl = baseURL;
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();

      if (log.isDebugEnabled()) {
        log.debug("Register replica - core={} id={} address={} collection={} shard={} type={}", coreName, desc.getCoreProperty("id", null), baseUrl, collection,
            shardId, cloudDesc.getReplicaType());

        log.debug("Register terms for replica {}", coreName);
      }
      try (SolrCore core = cc.getCore(desc.getName())) {
        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

        // we will call register again after zk expiration and on reload
        if (!afterExpiration && !core.isReloaded() && ulog != null) {
          // disable recovery in case shard is in construction state (for shard splits)
          DocCollection coll = getClusterState().getCollectionOrNull(collection);
          Slice slice = null;
          if (coll != null) {
            slice = coll.getSlice(shardId);
          }
          if ((slice == null || slice.getState() != Slice.State.CONSTRUCTION)) {
            Future<UpdateLog.RecoveryInfo> recoveryFuture = core.getUpdateHandler().getUpdateLog().recoverFromLog();
            if (recoveryFuture != null) {
              // MRM TODO: if we publish active early (like we will) log replay will not be done
              log.info("Replaying tlog for {} during startup... NOTE: This can take a while.", core);
              recoveryFuture.get(); // NOTE: this could potentially block for
              // minutes or more!
              // TODO: public as recovering in the mean time?
              // TODO: in the future we could do peersync in parallel with recoverFromLog
            } else {
              if (log.isDebugEnabled()) {
                log.debug("No LogReplay needed for core={} baseURL={}", core.getName(), baseUrl);
              }
            }
          }
        }
      }

      // If we're a preferred leader, insert ourselves at the head of the queue
      boolean joinAtHead = false; //replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false);

      // A replica of an INACTIVE slice (a shard-split parent whose data has already migrated to its
      // sub-shards) is retired: it must not join leader election or publish ACTIVE. On a node restart
      // its core still loads, but it should register as DOWN rather than re-elect itself leader and
      // re-advertise the retired shard as active. Otherwise the parent's replica is counted as an
      // active replica forever (ShardSplitTest.testSplitStaticIndexReplication restarts the parent
      // leader node and then waits for activeClusterShape(2,2) -- only the two sub-shard replicas
      // should be active).
      {
        DocCollection regColl = getClusterState().getCollectionOrNull(collection);
        Slice regSlice = regColl != null ? regColl.getSlice(shardId) : null;
        if (regSlice != null && regSlice.getState() == Slice.State.INACTIVE) {
          log.info("Slice {} of {} is INACTIVE; registering replica {} as DOWN without leader election", shardId, collection, coreName);
          publish(desc, Replica.State.DOWN);
          desc.getCloudDescriptor().setHasRegistered(true);
          return;
        }
      }

      if (cloudDesc.getReplicaType() != Type.PULL) {
        //getCollectionTerms(collection).register(cloudDesc.getShardId(), coreName);
        // MRM TODO: review joinAtHead
        coreEnteredLeaderElection.inc();
        joinElection(desc, joinAtHead, afterExpiration);
      }

      log.debug("no leader found, register for notification {}", coreName);
      var foundLeader = new AtomicBoolean();
      zkStateReader.waitForState(collection, Integer.getInteger("solr.waitForLeaderInZkRegSec", 60), TimeUnit.SECONDS, (n, c) -> {
        if (log.isDebugEnabled()) {
          log.debug("wait for leader notified collection={}", c == null ? "(null)" : c);
        }

        if (c == null) return false;
        Slice slice = c.getSlice(shardId);
        if (slice == null) return false;
        Replica fleader = slice.getLeader(n);

        if (fleader != null && fleader.getState() == Replica.State.ACTIVE) {
          boolean success = foundLeader.compareAndSet(false, true);
          if (success) {
            if (log.isDebugEnabled()) {
              log.debug("Found ACTIVE leader for slice={} leader={}", slice.getName(), fleader);
            }
            zkRegisterExecutor.submit(() -> finishRegistration(afterExpiration, coreName, desc, fleader));
          }
          return true;
        }

        return false;
      });

    } catch (AlreadyClosedException e) {
      log.warn("Won't register with ZooKeeper, already shutting down core={}", desc.getName());
      throw e;
    } catch (Exception e) {
      log.error("Error registering SolrCore with Zookeeper core={}", desc, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore with Zookeeper", e);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  /**
   * Block (bounded) until the core has a registered searcher, so we never advertise a replica as
   * ACTIVE before its searcher is open. The recovery path guarantees this via
   * {@code openNewSearcherAndUpdateCommitPoint()}; the two direct publish-ACTIVE paths in
   * {@link #finishRegistration} (the skip-autorecovery branch and the in-sync gate) must guarantee
   * it too — otherwise a query can be routed to a just-registered replica whose searcher is not yet
   * open (TestCloudSearcherWarming asserts exactly this; it also matches upstream, where the first
   * searcher is registered during core load before ACTIVE is published). With {@code
   * useColdSearcher=false} the core opens and registers its first searcher during load, so this wait
   * terminates quickly in practice; the timeout is only a safety net.
   */
  private void awaitRegisteredSearcher(SolrCore core) {
    if (core.hasRegisteredSearcher()) {
      return;
    }
    TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    try {
      while (!core.hasRegisteredSearcher()) {
        if (core.isClosing() || getCoreContainer().isShutDown() || isClosed || timeout.hasTimedOut()) {
          return;
        }
        timeout.sleep(50);
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
    }
  }

  public void finishRegistration(boolean afterExpiration, String coreName, final CoreDescriptor desc, Replica leader) {

    MDCLoggingContext.setCoreName(desc.getName());
    log.debug("finishZkRegister replica={} core={}", coreName, leader);

    if (getCoreContainer().isShutDown() || dcCalled) {
      throw new AlreadyClosedException();
    }

    try {
      boolean isLeader = leader.getName().equals(coreName);

      final String baseUrl = baseURL;

      final var cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();

      log.debug("We are {} and leader is {} isLeader={}", coreName, leader.getName(), isLeader);

      //assert !(isLeader && replica.getType() == Type.PULL) : "Pull replica became leader!";


      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null || core.isClosing() || getCoreContainer().isShutDown()) {
          throw new AlreadyClosedException();
        }

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        boolean isTlogReplicaAndNotLeader = cloudDesc.getReplicaType() == Replica.Type.TLOG && !isLeader;
        if (isTlogReplicaAndNotLeader) {
          String commitVersion = ReplicateFromLeader.getCommitVersion(core);
          if (commitVersion != null) {
            ulog.copyOverOldUpdates(Long.parseLong(commitVersion));
          }
        }
        // we will call register again after zk expiration


        if (cloudDesc.getReplicaType() != Type.PULL) {
          ZkShardTerms shardTerms = getShardTerms(collection, cloudDesc.getShardId());
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          if (log.isDebugEnabled()) log.debug("add shard terms listener for {}", coreName);
          shardTerms.registerTerm(coreName);
          // Do NOT setTermEqualsToLeader here (SOLR-9504): registration must not mark a possibly
          // stale/empty replica as up-to-date with the leader. Terms only reach the leader's value
          // through actually catching up (recovery completion → publish(ACTIVE) → doneRecovering).
          shardTerms.addListener(desc.getName(), new RecoveringCoreTermWatcher(core.getCoreDescriptor(), getCoreContainer()));
        }

        if (cloudDesc.getReplicaType() != Type.PULL && !isLeader) {
          // When solrcloud.skip.autorecovery is set (a test-only Boolean.getBoolean property, always
          // false in production), recovery is short-circuited inside the RecoveryTask AFTER BUFFERING
          // has already been published — which would leave the replica stuck in BUFFERING forever and
          // never reach ACTIVE. Upstream's checkRecovery returns false in this case and register()
          // publishes ACTIVE directly. Restore that here so skip-autorecovery tests can see replicas
          // become active. Gated on the flag, so production behavior (full recovery) is unchanged.
          if (Boolean.getBoolean("solrcloud.skip.autorecovery")) {
            log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery; publishing ACTIVE directly for {}", coreName);
            awaitRegisteredSearcher(core);
            publish(desc, Replica.State.ACTIVE);
          } else {
            ZkShardTerms shardTerms = getShardTerms(collection, shardId);
            // If we are already in sync with the leader — our shard term equals the shard's highest
            // term and we carry no recovering marker (canBecomeLeader) — there is nothing to recover,
            // so publish ACTIVE directly instead of running a (potentially long, prep-recovery)
            // recovery. Upstream short-circuits the same case via PeerSync; the fork otherwise always
            // sent a prep-recovery op even for a fresh, in-sync replica (which made it needlessly slow
            // and, under TestInjection.prepRecoveryOpPauseForever, stuck in BUFFERING on registration).
            // Data-safe ONLY for a freshly-created replica (core.isNewCore() — a runtime CoreAdmin
            // CREATE / addReplica, which starts from an empty index). For such a replica term==max
            // with no recovering marker genuinely means in sync: any replica that later missed an
            // update has its term pushed strictly below the leader's — on a failed forward
            // (replicasShouldBeInLowerTerms) or while it is skipped as DOWN/not-live
            // (skippedCoreNodeNames -> ensureTermsIsHigher).
            //
            // A cold-loaded replica (node restart: newCore==false, loaded from an existing on-disk
            // index) must NOT trust a persisted term==max. The term invariant has a hole under a hard
            // kill: an NRT replica can ACK a forwarded update into its not-yet-fsynced tlog buffer — so
            // the leader sees success and never lowers the replica's term — then lose that buffer on
            // kill -9. It restarts with term==max but a stale index, would take this short-circuit, and
            // (being NRT, with no ReplicateFromLeader poller) go ACTIVE and never converge
            // (ChaosMonkeyNothingIsSafeWithPullReplicasTest: a replica ACTIVE with 0 docs). So on a cold
            // load we always recover (DOWN -> RECOVERING -> ACTIVE), matching upstream Solr's
            // recover-on-restart behavior; a genuinely in-sync replica's PeerSync finds nothing and
            // completes fast. A behind or mid-recovery replica still recovers below regardless.
            if (shardTerms != null && shardTerms.canBecomeLeader(coreName) && !isTlogReplicaAndNotLeader
                && core.isNewCore()) {
              log.info("Replica {} is in sync with the leader (highest term, not recovering); publishing ACTIVE without recovery", coreName);
              // Mark as recovered-without-full-recovery so a LATER recovery uses current recent
              // versions, not the empty at-startup snapshot (else PeerSync gets "no frame of
              // reference" and falls back to a full index replication).
              core.getUpdateHandler().getSolrCoreState().recoveredWithoutFullRecovery();
              awaitRegisteredSearcher(core);
              publish(desc, Replica.State.ACTIVE);
              // A TLOG (non-leader) replica does NOT index locally — it stays current by continuously
              // replicating the leader's commits via ReplicateFromLeader. The recovery path
              // (RecoveryStrategy) starts that poller on completion, but this in-sync short-circuit
              // skips recovery, so we must start it here too. Without this the replica goes ACTIVE but
              // never polls the leader and never sees any subsequently-indexed doc (TestTlogReplica
              // testAddDocs/testKillLeader: "Replica … not up to date"). The else-if below only runs
              // when this whole (!PULL && !isLeader) block is skipped, so it never covers TLOG here.
              if (isTlogReplicaAndNotLeader) {
                startReplicationFromLeader(coreName, true);
              }
            } else {
              // This replica is behind the leader and must recover. Publish DOWN *before* starting
              // recovery so cluster state does not keep advertising a stale ACTIVE from a prior
              // lifetime (e.g. after a restart, where finishRegistration runs again but the replica
              // never re-published its state). Without this, expectedShardsAndActiveReplicas /
              // waitForState(ACTIVE) can return while a full-copy recovery is still installing the
              // index, and a real-time-get races the index swap (the doc isn't visible yet). Upstream
              // Solr publishes DOWN at registration for exactly this reason. Recovery then drives
              // DOWN -> BUFFERING -> ACTIVE; only on completion (index in sync) do we go ACTIVE again.
              publish(desc, Replica.State.DOWN);
              checkRecovery(core, cc, leader);
            }
          }
        } else if (isTlogReplicaAndNotLeader) {
          startReplicationFromLeader(coreName, true);
        }

        if (cloudDesc.getReplicaType() == Type.PULL) {
          startReplicationFromLeader(coreName, false);
          // A PULL replica has no shard term and never runs RecoveryStrategy; it stays current purely
          // through ReplicateFromLeader's background poll. The fork relies on that poll listener to
          // publish ACTIVE, but the listener only fires on a *subsequent non-empty* fetch — so a PULL
          // replica that does its initial full-copy fetch and then sits idle (e.g. a freshly created
          // sub-shard replica after SPLITSHARD, with no further indexing) is never published ACTIVE and
          // stays DOWN forever (ShardSplitTest.testSplitMixedReplicaTypes: sub-shards never complete,
          // parent stays ACTIVE). Upstream Solr publishes ACTIVE for a (non-recovering) PULL replica at
          // registration; do the same here. The poller keeps it up to date thereafter.
          awaitRegisteredSearcher(core);
          publish(desc, Replica.State.ACTIVE);
        }
      }

      log.debug("SolrCore Registered, core{} baseUrl={} collection={}, shard={}", coreName, baseUrl, collection, shardId);

      desc.getCloudDescriptor().setHasRegistered(true);

      coreRegisteredInZkEvent.inc();

    } catch (AlreadyClosedException e) {
      log.warn("Won't register with ZooKeeper, already shutting down core={}", desc.getName());
      throw e;
    } catch (Exception e) {
      log.error("Error registering SolrCore with Zookeeper core={}", desc, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore with Zookeeper", e);
    } finally {
      if (dcCalled || isClosed) {
        IOUtils.closeQuietly(leaderElectors.get(coreName));
      }
      MDCLoggingContext.clear();
    }
  }

  public void startReplicationFromLeader(String coreName, boolean switchTransactionLog) throws InterruptedException {
    if (isClosed) throw new AlreadyClosedException();
    log.info("{} starting background replication from leader", coreName);

    stopReplicationFromLeader(coreName);

    if (dcCalled || isClosed || cc.isShutDown()) {
      return;
    }
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.computeIfAbsent(coreName, s -> new ReplicateFromLeader(cc, coreName));

    replicateFromLeader.startReplication(switchTransactionLog);
  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      IOUtils.closeQuietly(replicateFromLeader);
    }
  }

  private void joinElection(CoreDescriptor cd, boolean joinAtHead, boolean afterExpiration) {
    log.debug("joinElection {}", cd.getName());
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();

    String shardId = cd.getCloudDescriptor().getShardId();

    Object2ObjectMap<String, Object> props = new Object2ObjectLinkedOpenHashMap<>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.NODE_NAME_PROP, nodeName);
    props.put(CORE_NAME_PROP, cd.getName());

    String id = cd.getCoreProperty("id", "-1");
    if (id.equals("-1")) {
      log.error("no id found in core props for {}", cd.getName());
      throw new IllegalArgumentException("no id found in core props for " + cd.getName());
    }

    props.put("id", Integer.parseInt(id));

    String collId = cd.getCoreProperty("collId", "-1");
    if (collId.equals("-1")) {
      log.error("no collId found in core props for {}", cd.getName());
      throw new IllegalArgumentException("no collId found in core props for " + cd.getName());
    }

    Replica replica = new Replica(cd.getName(), props, collection, Integer.parseInt(collId), cd.getCloudDescriptor().getShardId());
    LeaderElector leaderElector;

    if (dcCalled || isClosed) {
      throw new AlreadyClosedException();
    }

    leaderElector = new LeaderElector(this);
    LeaderElector prevLeaderElector = leaderElectors.put(replica.getName(), leaderElector);

    IOUtils.closeQuietly(prevLeaderElector);

    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, replica, this, cc, cd);

    leaderElector.setup(context);
    log.debug("Joining election ...");
    leaderElector.joinElection( false, joinAtHead);
  }


  /**
   * Returns whether or not a recovery was started
   */
  private static void checkRecovery(SolrCore core, CoreContainer cc, Replica leader) {
    log.debug("Core needs to recover:{}", core.getName());
    core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor(), "ZkRegistration", leader);
  }


  public String getBaseUrl() {
    return baseURL;
  }

  public void publish(final CoreDescriptor cd, final Replica.State state) throws Exception {
    publish(cd, state, true);
  }

  /**
   * Publish core state to overseer.
   */
  public void publish(final CoreDescriptor cd, final Replica.State state, boolean updateLastState) throws Exception {
    // Guard: statePublisher is only created in init() (after start()). A publish racing a
    // never-started or torn-down ZkController would NPE on submitState below; treat it as closed.
    if (statePublisher == null) {
      throw new AlreadyClosedException();
    }

    String collection = cd.getCloudDescriptor().getCollectionName();
    String shardId = cd.getCloudDescriptor().getShardId();
    Object2ObjectMap<String,Object> props = new Object2ObjectLinkedOpenHashMap<>(16, 0.5f);
    MDCLoggingContext.setCoreName(cd.getName());
    try {
      // System.out.println(Thread.currentThread().getStackTrace()[3]);
      Integer numShards = cd.getCloudDescriptor().getNumShards();
      if (numShards == null) { // XXX sys prop hack
        log.debug("numShards not found on descriptor - reading it from system property");
      }

      props.put(Overseer.QUEUE_OPERATION, "state");
      props.put(ZkStateReader.STATE_PROP, state);
      props.put("id",  cd.getCoreProperty("collId", "-1") + "-" + cd.getCoreProperty("id", "-1"));
      //  props.put(ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles());
      props.put(CORE_NAME_PROP, cd.getName());
      //  props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
      //  props.put(ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId());
      props.put(ZkStateReader.COLLECTION_PROP, collection);
      props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().toString());
//      try {
//        if (core.getDirectoryFactory().isSharedStorage()) {
//          // MRM TODO: currently doesn't publish anywhere
//          if (core.getDirectoryFactory().isSharedStorage()) {
//            props.put(ZkStateReader.SHARED_STORAGE_PROP, "true");
//            props.put("dataDir", core.getDataDir());
//            UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
//            if (ulog != null) {
//              props.put("ulogDir", ulog.getLogDir());
//            }
//          }
//        }
//      } catch (SolrCoreInitializationException ex) {
//        // The core had failed to initialize (in a previous request, not this one), hence nothing to do here.
//        if (log.isInfoEnabled()) {
//          log.info("The core '{}' had failed to initialize before.", cd.getName());
//        }
//      }

      // pull replicas are excluded because their terms are not considered
      if ((state == Replica.State.RECOVERING || state == Replica.State.BUFFERING) && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        // state is used by client, state of replica can change from RECOVERING to DOWN without needed to finish recovery
        // by calling this we will know that a replica actually finished recovery or not
        ZkShardTerms shardTerms = getShardTerms(collection, shardId);
        // startRecovering raises our term to max but leaves the "<core>_recovering" marker in
        // place — the marker is what keeps a still-recovering (possibly empty) replica from
        // winning a leader election. It is removed only when recovery completes
        // (publish(ACTIVE) → doneRecovering). Never call setTermEqualsToLeader here.
        shardTerms.startRecovering(cd.getName());
      }
      if (state == Replica.State.ACTIVE && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        ZkShardTerms shardTerms = getShardTerms(collection, shardId);
        if (shardTerms.isRecovering(cd.getName())) {
          // Recovery actually completed (the "<core>_recovering" marker set by startRecovering when we
          // published BUFFERING is still present). We replicated/PeerSynced from the authoritative leader,
          // so set our term equal to the leader's CURRENT max and clear the marker. doneRecovering would
          // only clear the marker and leave the term at the value startRecovering snapshotted when
          // recovery BEGAN; if the leader advanced while we recovered (it bumps its term on every update
          // that skipped/failed to a behind replica) we would otherwise stay permanently below the
          // leader's term and keep being skipped by its update fan-out, never converging
          // (ReplicationFactorTest: rf=1 even after every replica heals).
          shardTerms.setTermEqualsToLeader(cd.getName());
        } else {
          // No recovery marker: this is an in-sync replica (re)publishing ACTIVE. Do NOT raise its term.
          // An out-of-sync replica that missed updates while partitioned has a lower term and no marker;
          // bumping it to the leader's term here would wrongly let it win a later election without having
          // recovered the missing data (TestCloudConsistency.testOutOfSyncReplicasCannotBecomeLeader,
          // SOLR-9504). doneRecovering is a no-op when no marker is present.
          shardTerms.doneRecovering(cd.getName());
        }
      }

      ZkNodeProps m = new ZkNodeProps(props);

      if (updateLastState) {
        cd.getCloudDescriptor().setLastPublished(state);
      }
      statePublisher.submitState(m);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public void publish(ZkNodeProps message) {
    // See publish(CoreDescriptor, State, boolean): statePublisher exists only after init().
    if (statePublisher == null) {
      throw new AlreadyClosedException();
    }
    statePublisher.submitState(message);
  }

  public ZkShardTerms getShardTerms(String collection, String shardId) {
    ZkCollectionTerms ct = getCollectionTerms(collection);
    return ct.getShard(shardId);
  }

  public ZkShardTerms getShardTermsOrNull(String collection, String shardId) {
    ZkCollectionTerms ct = getCollectionTerms(collection);
    if (ct == null) return null;
    return ct.getShardOrNull(shardId);
  }

  public void removeCollectionTerms(String collection) {
    ZkCollectionTerms collectionTerms = collectionToTerms.remove(collection);
    IOUtils.closeQuietly(collectionTerms);
  }

  public ZkCollectionTerms getCollectionTerms(String collection) {
    return collectionToTerms.computeIfAbsent(collection, ct -> new ZkCollectionTerms(collection, zkClient));
  }

  public void clearZkCollectionTerms() {
    collectionToTerms.values().forEach(ZkCollectionTerms::close);
  }

  public void unregister(String coreName, String collection, String shardId, boolean removeTerms) throws KeeperException, InterruptedException {
    log.info("Unregister core from zookeeper {}", coreName);
    try {

      removeShardLeaderElector(coreName);

      if (removeTerms) {
        ZkCollectionTerms ct = collectionToTerms.get(collection);
        if (ct != null) {
          ct.remove(shardId, coreName);
        }
      }

      replicasMetTragicEvent.remove(collection + ":" + coreName);

    } finally {
      try {
        zkStateReader.unregisterCore(collection, coreName);
      } finally {
        if (statePublisher != null) {
          statePublisher.clearStatCache(coreName);
        }
      }
    }
    //    if (Strings.isNullOrEmpty(collection)) {
    //      log.error("No collection was specified.");
    //      assert false : "No collection was specified [" + collection + "]";
    //      return;
    //    }
    //    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    //    Replica replica = (docCollection == null) ? null : docCollection.getReplica(coreName);

  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  public static void linkConfSet(SolrZkClient zkClient, String collection, String confSetName) throws KeeperException, InterruptedException {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Load collection config from:{}", path);
    byte[] data;
    try {
      data = zkClient.getData(path, null, null);
    } catch (NoNodeException e) {
      // if there is no node, we will try and create it
      // first try to make in case we are pre configuring
      ZkNodeProps props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
      try {

        zkClient.makePath(path, Utils.toJSON(props),
            CreateMode.PERSISTENT, null, true);
      } catch (KeeperException e2) {
        // it's okay if the node already exists
        if (e2.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
        // if we fail creating, setdata
        // TODO: we should consider using version
        zkClient.setData(path, Utils.toJSON(props), true);
      }
      return;
    }
    // we found existing data, let's update it
    ZkNodeProps props;
    if (data != null) {
      props = ZkNodeProps.load(data);
      Object2ObjectMap<String, Object> newProps = new Object2ObjectLinkedOpenHashMap<>(16, 0.5f);
      newProps.putAll(props.getProperties());
      newProps.put(CONFIGNAME_PROP, confSetName);
      props = new ZkNodeProps(newProps);
    } else {
      props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
    }

    // TODO: we should consider using version
    zkClient.setData(path, Utils.toJSON(props), true);

  }

  /**
   * If in SolrCloud mode, upload config sets for each SolrCore in solr.xml.
   */
  public static void bootstrapConf(SolrZkClient zkClient, CoreContainer cc) throws IOException, KeeperException {

    ZkConfigManager configManager = new ZkConfigManager(zkClient);

    //List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);

    if (log.isInfoEnabled()) {
      log.info("bootstrapping config for {} cores into ZooKeeper using solr.xml from {}", cds.size(), cc.getSolrHome());
    }

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName))
        confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory {} with name {} for solrCore {}", udir, confName, coreName);
      configManager.uploadConfigDir(udir, confName);
    }
  }

  public ZkDistributedQueue getOverseerJobQueue() {
    return overseerJobQueue;
  }

  public OverseerTaskQueue getOverseerCollectionQueue() {
    return overseerCollectionQueue;
  }

  public OverseerTaskQueue getOverseerConfigSetQueue() {
    return overseerConfigSetQueue;
  }

  public DistributedMap getOverseerRunningMap() {
    return overseerRunningMap;
  }

  public DistributedMap getOverseerCompletedMap() {
    return overseerCompletedMap;
  }

  public DistributedMap getOverseerFailureMap() {
    return overseerFailureMap;
  }

  /**
   * When an operation needs to be performed in an asynchronous mode, the asyncId needs
   * to be claimed by calling this method to make sure it's not duplicate (hasn't been
   * claimed by other request). If this method returns true, the asyncId in the parameter
   * has been reserved for the operation, meaning that no other thread/operation can claim
   * it. If for whatever reason, the operation is not scheduled, the asuncId needs to be
   * cleared using {@link #clearAsyncId(String)}.
   * If this method returns false, no reservation has been made, and this asyncId can't
   * be used, since it's being used by another operation (currently or in the past)
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the reservation succeeds.
   *         False if this ID is already in use.
   */
  public boolean claimAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.putIfAbsent(asyncId, EMPTY_BYTE_ARRAY);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Clears an asyncId previously claimed by calling {@link #claimAsyncId(String)}
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the asyncId existed and was cleared.
   *         False if the asyncId didn't exist before.
   */
  public boolean clearAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.remove(asyncId);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new RuntimeException(e);
    }
  }

  public void clearStatePublisher() {
    this.statePublisher.clearStatCache();
  }

  public void clearCachedState(String coreName) {
    this.statePublisher.clearStatCache(coreName);
  }

  public int getClientTimeout() {
    return clientTimeout;
  }

  public Overseer getOverseer() {
    return overseer;
  }

  /**
   * Returns the nodeName that should be used based on the specified properties.
   *
   * @param hostName    - must not be null or the empty string
   * @param hostPort    - must consist only of digits, must not be null or the empty string
   * @param hostContext - should not begin or end with a slash (leading/trailin slashes will be ignored), must not be null, may be the empty string to denote the root context
   * @lucene.experimental
   * @see ZkStateReader#getBaseUrlForNodeName
   */
  public static String generateNodeName(final String hostName,
                                 final String hostPort,
                                 final String hostContext) {
    return hostName + ':' + hostPort + '_' +
        URLEncoder.encode(trimLeadingAndTrailingSlashes(hostContext), StandardCharsets.UTF_8);
  }

  public static String generateNodeName(final String url) {
    return URLEncoder.encode(trimLeadingAndTrailingSlashes(url), StandardCharsets.UTF_8);
  }

  /**
   * Utility method for trimming and leading and/or trailing slashes from
   * its input.  May return the empty string.  May return null if and only
   * if the input is null.
   */
  public static String trimLeadingAndTrailingSlashes(final String in) {
    if (null == in) return in;

    String out = in;
    if (!out.isEmpty() && out.charAt(0) == '/') {
      out = out.substring(1);
    }
    if (!out.isEmpty() && out.charAt(out.length() - 1) == '/') {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  public void rejoinOverseerElection(boolean joinAtHead) {
    boolean closeAndDone;
    try {
      closeAndDone = overseer.isCloseAndDone();
    } catch (NullPointerException e) {
      // okay
      closeAndDone = true;
    }
    ElectionContext context = overseerElector.getContext();
    if (overseerElector == null || isClosed || isShutdownCalled || closeAndDone || context == null) {
      return;
    }
    try {
      overseerElector.retryElection(joinAtHead);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void rejoinShardLeaderElection(SolrParams params) {

    String coreName = params.get(CORE_NAME_PROP);

    try {
      log.info("Rejoin the shard leader election.");
      LeaderElector elect =  leaderElectors.get(coreName);
      if (elect != null) {
        elect.retryElection(params.getBool(REJOIN_AT_HEAD_PROP, false));
      }
//      try (SolrCore core = getCoreContainer().getCore(coreName)) {
//        core.getSolrCoreState().doRecovery(core);
//      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }
  }

  public CoreContainer getCoreContainer() {
    return cc;
  }

  public void throwErrorIfReplicaReplaced(CoreDescriptor desc) {
    ClusterState clusterState = zkStateReader.getClusterState();
    if (clusterState != null && desc  != null) {
      CloudUtil.checkSharedFSFailoverReplaced(cc, desc);
    }
  }

  /**
   * Add a listener to be notified once there is a new session created after a ZooKeeper session expiration occurs;
   * in most cases, listeners will be components that have watchers that need to be re-created.
   */
  public void addOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
       reconnectListeners.add(listener);
       if (log.isDebugEnabled()) log.debug("Added new OnReconnect listener {}", listener);
    }
  }

  /**
   * Removed a previously registered OnReconnect listener, such as when a core is removed or reloaded.
   */
  public void removeOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
      boolean wasRemoved = reconnectListeners.remove(listener);

      if (wasRemoved) {
        if (log.isDebugEnabled()) log.debug("Removed OnReconnect listener {}", listener);
      } else {
        log.warn("Was asked to remove OnReconnect listener {}, but remove operation " +
                "did not find it in the list of registered listeners."
            , listener);
      }
    }
  }

  Set<OnReconnect> getCurrentOnReconnectListeners() {
    return Collections.unmodifiableSet(reconnectListeners);
  }

  /**
   * Persists a config file to ZooKeeper using optimistic concurrency.
   *
   * @return true on success
   */
  public static int persistConfigResourceToZooKeeper(ZkSolrResourceLoader zkLoader, int znodeVersion,
                                                         String resourceName, byte[] content,
                                                         boolean createIfNotExists) {
    int latestVersion = znodeVersion;
    final SolrZkClient zkClient = zkLoader.getZkClient();
    final String resourceLocation = zkLoader.getConfigSetZkPath() + "/" + resourceName;
    String errMsg = "Failed to persist resource at {0} - old {1}";
    try {
      try {
        Stat stat = zkClient.setData(resourceLocation, content, znodeVersion, true);
        latestVersion = stat.getVersion();// if the set succeeded , it should have incremented the version by one always
        log.info("Persisted config data to node {} ", resourceLocation);
        touchConfDir(zkLoader);
      } catch (NoNodeException e) {
        if (createIfNotExists) {
          try {
            zkClient.create(resourceLocation, content, CreateMode.PERSISTENT, true);
            latestVersion = 0;//just created so version must be zero
            touchConfDir(zkLoader);
          } catch (KeeperException.NodeExistsException nee) {
            try {
              Stat stat = zkClient.exists(resourceLocation, null);

              log.info("failed to set data version in zk is {} and expected version is {} ", stat.getVersion(), znodeVersion);

            } catch (Exception e1) {
              ParWork.propagateInterrupt(e1);
              log.warn("could not get stat");
            }

            if (log.isInfoEnabled()) {
              log.info(StrUtils.formatString(errMsg, resourceLocation, znodeVersion));
            }
            throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
          }
        }
      }

    } catch (KeeperException.BadVersionException bve) {
      Stat stat = null;
      try {
        stat = zkClient.exists(resourceLocation, null);
      } catch (Exception e1) {
        ParWork.propagateInterrupt(e1);
        log.warn("could not get stat");
      }

      log.info("failed to set data version in zk is {} and expected version is {} ", stat == null ? "(null)" : stat.getVersion(), znodeVersion);
      if (log.isInfoEnabled()) {
        log.info(StrUtils.formatString("%s zkVersion= %d %s %d", errMsg, resourceLocation, znodeVersion));
      }
      throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
    } catch (ResourceModifiedInZkException e) {
      throw e;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      final String msg = "Error persisting resource at " + resourceLocation;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
    return latestVersion;
  }

  public static void touchConfDir(ZkSolrResourceLoader zkLoader) {
    SolrZkClient zkClient = zkLoader.getZkClient();
    try {
      zkClient.setData(zkLoader.getConfigSetZkPath(), new byte[]{0}, true);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      final String msg = "Error 'touching' conf location " + zkLoader.getConfigSetZkPath();
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);

    }
  }

  public static class ResourceModifiedInZkException extends SolrException {
    public ResourceModifiedInZkException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  private void unregisterConfListener(String confDir, Runnable listener) {
    AtomicBoolean removeWatch = new AtomicBoolean(false);

    ConfListeners confListener = confDirectoryListeners.computeIfPresent(confDir, (c, confListeners) -> {
      if (confListeners.confDirListeners.remove(listener)) {
        if (log.isDebugEnabled()) log.debug("removed listener for config directory [{}]", confDir);
        if (confListeners.confDirListeners.isEmpty()) {
          // no more listeners for this confDir, remove it from the map
          if (log.isDebugEnabled()) log.debug("No more listeners for config directory [{}]", confDir);
          removeWatch.set(true);
          return null;
        }
      }
      return confListeners;
    });



    if (removeWatch.get() && confListener != null) {
      try {
        zkClient.removeWatches(confDir, confListener.watcher, Watcher.WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  /**
   * This will give a callback to the listener whenever a child is modified in the
   * conf directory. It is the responsibility of the listener to check if the individual
   * item of interest has been modified.  When the last core which was interested in
   * this conf directory is gone the listeners will be removed automatically.
   */
  public void registerConfListenerForCore(final String confDir, SolrCore core, final Runnable listener) {
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }

    final ConfListeners confDirListeners = getConfDirListeners(confDir);
    confDirListeners.confDirListeners.add(listener);
    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        unregisterConfListener(confDir, listener);
      }

      @Override
      public void postClose(SolrCore core) {
      }
    });
  }

  private static class ConfListeners {

    private Set<Runnable> confDirListeners;
    private final Watcher watcher;

    ConfListeners( Set<Runnable> confDirListeners, Watcher watcher) {
      this.confDirListeners = confDirListeners;
      this.watcher = watcher;
    }
  }

  private ConfListeners getConfDirListeners(final String confDir) {
    if (isClosed || dcCalled) {
      throw new AlreadyClosedException();
    }
    AtomicReference<ConfDirWatcher> cdw = new AtomicReference<>();
    ConfListeners listeners = confDirectoryListeners.computeIfAbsent(confDir, c -> {
      ConfDirWatcher watcher = new ConfDirWatcher(confDir, cc, confDirectoryListeners);
      cdw.set(watcher);
      return new ConfListeners(ConcurrentHashMap.newKeySet(), watcher);
    });

    ConfDirWatcher watcher = cdw.get();
    if (watcher != null) {
      setConfWatcher(confDir, watcher, null, cc, confDirectoryListeners, cc.getZkController().zkClient);
    }

   return listeners;

  }

  private final Map<String, ConfListeners> confDirectoryListeners = new NonBlockingHashMap<>();

  private static class ConfDirWatcher implements Watcher {
    private final String zkDir;
    private final CoreContainer cc;
    private final SolrZkClient zkClient;
    private final Map<String, ConfListeners> confDirectoryListeners;

    private ConfDirWatcher(String dir, CoreContainer cc, Map<String, ConfListeners> confDirectoryListeners) {
      this.zkDir = dir;
      this.cc = cc;
      this.zkClient = cc.getZkController().getZkClient();
      this.confDirectoryListeners = confDirectoryListeners;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (cc.getZkController().isClosed() || cc.isShutDown() || cc.getZkController().isDcCalled()) {
        return;
      }
      Stat stat = null;
      try {
        stat = zkClient.exists(zkDir, null);
      } catch (KeeperException e) {
        log.info(e.getMessage(), e);
      } catch (InterruptedException e) {
        log.info("WatcherImpl Interrupted");
        return;
      }

      fireEventListeners(zkDir, confDirectoryListeners, cc);
    }
  }

  private static boolean fireEventListeners(String zkDir, Map<String, ConfListeners> confDirectoryListeners, CoreContainer cc) {
    if (cc.isShutDown()) {
      return false;
    }

    // if this is not among directories to be watched then don't set the watcher anymore
    if (!confDirectoryListeners.containsKey(zkDir)) {
      if (log.isDebugEnabled()) log.debug("Watcher on {} is removed ", zkDir);
      return false;
    }

    final Set<Runnable> listeners = confDirectoryListeners.get(zkDir).confDirListeners;
    if (listeners != null) {
      if (cc.isShutDown() || cc.getZkController().dcCalled) {
        return false;
      }
      listeners.forEach(runnable -> ParWork.getRootSharedExecutor().submit(runnable));
    }
    return true;
  }

  private static void setConfWatcher(String zkDir, Watcher watcher, Stat stat, CoreContainer cc, Map<String, ConfListeners> confDirectoryListeners, SolrZkClient zkClient) {
    try {
      zkClient.addWatch(zkDir, watcher, AddWatchMode.PERSISTENT);
      Stat newStat = zkClient.exists(zkDir, null);
      if (stat != null && newStat.getVersion() > stat.getVersion()) {
        //a race condition where a we missed an event fired
        //so fire the event listeners
        fireEventListeners(zkDir, confDirectoryListeners, cc);
      }
    } catch (KeeperException e) {
      log.error("failed to set watcher for conf dir {} ", zkDir);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("failed to set watcher for conf dir {} ", zkDir);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public OnReconnect getConfigDirListener() {
    return new ZkControllerOnReconnect(confDirectoryListeners, cc);
  }

  /**
   * Thrown during pre register process if the replica is not present in clusterstate
   */
  public static class NotInClusterStateException extends SolrException {
    public NotInClusterStateException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  /**
   * Best effort to set DOWN state for all replicas on node.
   *
   * @param nodeName to operate on
   */
  public void publishNodeAs(String nodeName, OverseerAction state) throws KeeperException {
    log.info("Publish node={} as {}", nodeName, state);

    if (overseer == null) {
      log.warn("Could not publish node as down, no overseer was started yet");
      return;
    }

    ZkNodeProps m = new ZkNodeProps(StatePublisher.OPERATION, state.toLower(),
        ZkStateReader.NODE_NAME_PROP, nodeName);
    try {
      statePublisher.submitState(m);
    } catch (AlreadyClosedException e) {
      ParWork.propagateInterrupt("Not publishing node as " + state + " because a resource required to do so is already closed.", null, true);
      return;
    }
//    Collection<SolrCore> cores = cc.getCores();
//    for (SolrCore core : cores) {
//      CoreDescriptor desc = core.getCoreDescriptor();
//      String collection = desc.getCollectionName();
//      try {
//        zkStateReader.waitForState(collection, 3, TimeUnit.SECONDS, (n,c) -> {
//          if (c != null) {
//            List<Replica> replicas = c.getReplicas();
//            for (Replica replica : replicas) {
//              if (replica.getNodeName().equals(getNodeName())) {
//                if (!replica.getState().equals(Replica.State.DOWN)) {
//                  log.info("Found state {} {}", replica.getState(), replica.getNodeName());
//                  return false;
//                }
//              }
//            }
//          }
//          return true;
//        });
//      } catch (InterruptedException e) {
//        ParWork.propegateInterrupt(e);
//        return;
//      } catch (TimeoutException e) {
//        log.error("Timeout", e);
//      }
//    }
  }

  private static class ZkControllerOnReconnect implements OnReconnect {

    private final Map<String, ConfListeners> confDirectoryListeners;
    private final CoreContainer cc;

    ZkControllerOnReconnect(Map<String, ConfListeners> confDirectoryListeners, CoreContainer cc) {
      this.confDirectoryListeners = confDirectoryListeners;
      this.cc = cc;
    }
    
    @Override
    public void command() {
        confDirectoryListeners.forEach((s, runnables) -> {
          setConfWatcher(s, new ConfDirWatcher(s, cc, confDirectoryListeners), null, cc, confDirectoryListeners, cc.getZkController().getZkClient());
          fireEventListeners(s, confDirectoryListeners, cc);
        });
    }

    @Override
    public String getName() {
      return null;
    }
  }
}
