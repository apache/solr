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
package org.apache.solr.zero.process;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.client.ZeroStoreClientFactory;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.exception.ZeroLockException;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.util.DeduplicatingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is the main entry point to the ZERO implementation from existing SolrCloud classes. */
public class ZeroStoreManager {

  /**
   * If more than this number of DISTINCT cores need to be enqueued for async pull, the enqueue
   * might block. We likely do not want the enqueue to block (currently enqueue is done from {@link
   * org.apache.solr.servlet.HttpSolrCall#init()} so it would block a Jetty thread), so keep this
   * number high enough. They day is far though before a single SolrCloud node is able to support
   * that number of cores. That constant is likely more than an order of magnitude too big.
   */
  private static final int CORE_TO_PULL_LIST_MAX_SIZE = 100000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreContainer coreContainer;
  private ZeroMetadataController zeroMetadataController;
  private MetadataCacheManager metadataCacheManager;
  private ZeroStoreClient zeroStoreClient;

  /** A generic delete processor that is used only to delete individual files */
  private DeleteProcessor corePusherFileDeleteProcessor;

  private final DeduplicatingList<String, CorePuller> corePullTaskQueue;

  /**
   * This executor feeds off the {@link #corePullTaskQueue} {@link DeduplicatingList}, disguising as
   * a bare bones {@link BlockingQueue} by using {@link CorePullerBlockingQueue}. This executor is
   * used in a very specific way: nothing is ever submitted to it, it only consumes the queue, with
   * entries added directly into the queue. This imposes constraints on the worker threads of the
   * executor.
   *
   * <p>The reason is that we want to maintain the queue of cores to pull as a {@link
   * DeduplicatingList} of {@link CorePuller} in order to effectively be able to deduplicate
   * inserts. A {@link ThreadPoolExecutor} only accepts queues of {@link Runnable}, so we can't
   * easily do sumbits (unless we submitted {@link CorePuller} instances already wrapped within a
   * {@link Runnable}, but then the deduplication code would become ugly).
   */
  private final ExecutorUtil.MDCAwareThreadPoolExecutor asyncCorePullExecutor;

  private DeleteProcessor overseerDeleteProcessor;
  private CorePullerFactory corePullerFactory;
  private final ZeroConfig config;

  public ZeroStoreManager(CoreContainer coreContainer, ZeroConfig config) {
    this.coreContainer = coreContainer;

    zeroMetadataController =
        new ZeroMetadataController(coreContainer.getZkController().getSolrCloudManager());
    metadataCacheManager = new MetadataCacheManager(config, coreContainer);

    // The default CorePuller creation below passes null for the pullEndNotification which
    // means no pull end notifications. A different factory is installed by tests when a
    // notification is needed
    corePullerFactory =
        (solrCore,
            zeroStoreClient,
            metadataCacheManager,
            metadataController,
            maxFailedCorePullAttempts) ->
            new CorePuller(
                solrCore,
                zeroStoreClient,
                metadataCacheManager,
                metadataController,
                maxFailedCorePullAttempts,
                null);

    this.zeroStoreClient =
        ZeroStoreClientFactory.newInstance(
            coreContainer.getNodeConfig(), coreContainer.getMetricManager());

    corePusherFileDeleteProcessor =
        new DeleteProcessor(
            "PusherFileDeleter",
            zeroStoreClient,
            config.getAlmostMaxDeleterQueueSize(),
            config.getDeleterThreadPoolSize(),
            config.getMaxDeleteAttempts(),
            config.getDeleteSleepMsFailedAttempt(),
            config.getDeleteDelayMs());

    // Non-Overseer nodes will initiate a delete processor but the underlying pool will sit idle
    // until the node is elected and tasks are added. The overhead should be small.
    overseerDeleteProcessor =
        new DeleteProcessor(
            "OverseerDeleteProcessor",
            zeroStoreClient,
            config.getAlmostMaxDeleterQueueSize(),
            config.getDeleterThreadPoolSize(),
            config.getMaxDeleteAttempts(),
            config.getDeleteSleepMsFailedAttempt(),
            config.getDeleteDelayMs());

    this.config = config;

    // Queue containing cores to pull from the Zero store
    this.corePullTaskQueue = CorePuller.getDeduplicatingList(CORE_TO_PULL_LIST_MAX_SIZE);
    // The executor executing the code pulling these cores. Actual files are pulled using the
    // file pull executor defined in ZeroStoreClient.
    //
    // Tasks are added directly into the queue, not via the executor. We therefore set a fixed
    // thread pool size (i.e. core size = max size) to not let threads terminate but have them
    // wait for tasks to arrive in the queue.
    // TODO All pushing and pulling executors likely belong in the same place
    asyncCorePullExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            config.getNumCorePullerThreads(),
            config.getNumCorePullerThreads(),
            0L,
            TimeUnit.MILLISECONDS,
            new CorePullerBlockingQueue(corePullTaskQueue, this),
            new SolrNamedThreadFactory("asyncCorePullExecutor"));
    // We also need to pre-start all threads because nothing will start them for us (there are no
    // direct submits to this ThreadPoolExecutor).
    asyncCorePullExecutor.prestartAllCoreThreads();

    if (log.isInfoEnabled()) {
      log.info("asyncCorePullExecutor initialized");
    }
  }

  public ZeroStoreClient getZeroStoreClient() {
    return zeroStoreClient;
  }

  /** For testing purposes only. Current Zero store client is closed before replacing it. */
  @VisibleForTesting
  public void replaceZeroStoreClient(ZeroStoreClient zeroStoreClient) {
    if (this.zeroStoreClient != null) {
      this.zeroStoreClient.shutdown();
    }
    this.zeroStoreClient = zeroStoreClient;
  }

  @VisibleForTesting
  public void replaceDeleteProcessors(
      DeleteProcessor deleteProcessor, DeleteProcessor overseerDeleteProcessor) {
    if (this.corePusherFileDeleteProcessor != null) {
      this.corePusherFileDeleteProcessor.close();
    }
    this.corePusherFileDeleteProcessor = deleteProcessor;

    if (this.overseerDeleteProcessor != null) {
      this.overseerDeleteProcessor.close();
    }
    this.overseerDeleteProcessor = overseerDeleteProcessor;
  }

  public ZeroMetadataController getZeroMetadataController() {
    return zeroMetadataController;
  }

  public MetadataCacheManager getMetadataCacheManager() {
    return metadataCacheManager;
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  public ZeroConfig getConfig() {
    return config;
  }

  /**
   * Check and evict any existing entry for Zero store metadata in the MetadataCacheManager. Zero
   * store metadata should be treated the way CoreDescriptors in SolrCores are, and removed from
   * this cache when the descriptors are removed from SolrCores.
   */
  public void evictCoreZeroMetadata(CoreDescriptor cd) {
    if (cd != null
        && cd.getCloudDescriptor().getReplicaType().equals(Replica.Type.ZERO)
        && metadataCacheManager.removeCoreMetadata(cd.getName())) {
      String collectionName = cd.getCollectionName();
      String shardId = cd.getCloudDescriptor().getShardId();
      if (log.isInfoEnabled()) {
        log.info(
            "Evicted core {} for collection {} and shard {} from ZERO core concurrency cache",
            cd.getName(),
            collectionName,
            shardId);
      }
    }
  }

  public CorePuller newCorePuller(SolrCore solrCore) {
    return corePullerFactory.createPuller(
        solrCore,
        zeroStoreClient,
        metadataCacheManager,
        zeroMetadataController,
        config.getMaxFailedCorePullAttempts());
  }

  public CorePusher newCorePusher(SolrCore solrCore) {
    return new CorePusher(
        solrCore,
        zeroStoreClient,
        corePusherFileDeleteProcessor,
        metadataCacheManager,
        zeroMetadataController);
  }

  /**
   * This method is called at the end of an indexing batch. It pushes new content of the local core
   * present in the Zero store. When called after the first indexing batch for a core, it pushes all
   * files of the core (i.e. this could be the first write of that shard to the Zero store).
   */
  public CorePusherExecutionInfo pushCoreToZeroStore(SolrCore core) {
    CorePusher corePusher = newCorePusher(core);
    return corePusher.endToEndPushCoreToZeroStore();
  }

  public void shutdown() {
    stopBackgroundCorePulling();

    if (corePusherFileDeleteProcessor != null) {
      corePusherFileDeleteProcessor.close();
      if (log.isInfoEnabled()) {
        log.info("DeleteProcessor {} has shutdown", corePusherFileDeleteProcessor.getName());
      }
    }
    if (overseerDeleteProcessor != null) {
      overseerDeleteProcessor.close();
      if (log.isInfoEnabled()) {
        log.info("Overseer DeleteProcessor {} has shutdown", overseerDeleteProcessor.getName());
      }
    }

    if (zeroStoreClient != null) {
      zeroStoreClient.shutdown();
      zeroStoreClient = null;
    }
  }

  @VisibleForTesting
  void replaceMetadataCacheManager(MetadataCacheManager metadataCacheManager) {
    this.metadataCacheManager = metadataCacheManager;
  }

  @VisibleForTesting
  void replaceCorePullerFactory(CorePullerFactory corePullerFactory) {
    this.corePullerFactory = corePullerFactory;
  }

  @VisibleForTesting
  void replaceZeroShardMetadataController(ZeroMetadataController metadataController) {
    this.zeroMetadataController = metadataController;
  }

  /**
   * This is used for optimizations and skipping pull in some cases. Unclear if the cost of the
   * optimization (running this method) is not higher than the potential savings.
   */
  public boolean isLeader(SolrCore core) {
    try {
      if (!coreContainer.isZooKeeperAware()) {
        // not solr cloud
        return false;
      }

      CoreDescriptor coreDescriptor = coreContainer.getCoreDescriptor(core.getName());
      if (coreDescriptor == null) {
        // core descriptor does not exist
        return false;
      }

      CloudDescriptor cd = coreDescriptor.getCloudDescriptor();
      if (cd == null || cd.getReplicaType() != Replica.Type.ZERO) {
        // not a ZERO replica
        return false;
      }

      ZkController zkController = coreContainer.getZkController();
      Replica leaderReplica =
          zkController.getZkStateReader().getLeaderRetry(cd.getCollectionName(), cd.getShardId());
      // not a leader replica
      return leaderReplica != null && cd.getCoreNodeName().equals(leaderReplica.getName());
    } catch (Exception ex) {
      log.warn(
          String.format(
              Locale.ROOT,
              "Could not establish if current replica is leader for the given core, collection=%s shard=%s core=%s",
              core.getCoreDescriptor().getCollectionName(),
              core.getCoreDescriptor().getCloudDescriptor().getShardId(),
              core.getName()),
          ex);
      // we will proceed further as we are not a leader
    }
    return false;
  }

  @VisibleForTesting
  @FunctionalInterface
  public interface CorePullerFactory {
    CorePuller createPuller(
        SolrCore solrCore,
        ZeroStoreClient zeroStoreClient,
        MetadataCacheManager metadataCacheManager,
        ZeroMetadataController metadataController,
        int maxFailedCorePullAttempts);
  }

  /**
   * Returns the list of core properties that are needed to create a core corresponding to provided
   * {@code replica} of the {@code collection}. These cores are ZERO cores found in ZooKeeper that
   * do not have a local presence on the disk at startup. See {@link
   * ZeroStoreManager#discoverAdditionalCoreDescriptorsForZeroReplicas}
   */
  public Map<String, String> getZeroCoreProperties(DocCollection collection, Replica replica) {
    // "numShards" is a property that is found in core descriptors. But it is only set on the
    // cores created at collection creation time. It is not part of cores created by addition of
    // replicas/shards or shard splits. Once set, it is not even kept in sync with latest number
    // of shards. Therefore, we do not put it in any of missing cores we create.

    Map<String, String> params = new HashMap<>();
    params.put(CoreDescriptor.CORE_COLLECTION, collection.getName());
    params.put(CoreDescriptor.CORE_NODE_NAME, replica.getName());
    params.put(
        CoreDescriptor.CORE_SHARD,
        collection.getShardId(replica.getNodeName(), replica.getCoreName()));
    params.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.ZERO.name());
    params.put(CollectionAdminParams.COLL_CONF, collection.getConfigName());
    return params;
  }

  /** True if the core has ever synced with the Zero store, otherwise, false. */
  public boolean hasCoreEverSyncedWithZeroStore(SolrCore core) {
    MetadataCacheManager.MetadataCacheEntry coreMetadata =
        metadataCacheManager.getOrCreateCoreMetadata(core.getName());
    // If the cached value has moved beyond the initialization value then we must have pushed
    // (performed indexing as a leader) or pulled (including the no-op pull that happens when the
    // shard is at ZeroShardMetadataController.METADATA_NODE_DEFAULT_VALUE )
    if (!metadataCacheManager.hasInitialCacheSuffixValue(coreMetadata)) return true;

    // If local cache doesn't know about any updates, check in ZK
    ZeroMetadataVersion shardMetadataVersion =
        zeroMetadataController.readMetadataValue(getCollectionName(core), getShardName(core));
    return zeroMetadataController.hasDefaultNodeSuffix(shardMetadataVersion);
  }

  /** If the core has never synced with the Zero store then fail the query. */
  public void ensureZeroCoreFreshness(SolrCore core) {
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    if (cloudDescriptor != null && cloudDescriptor.getReplicaType() == Replica.Type.ZERO) {
      boolean hasCoreEverSynced = hasCoreEverSyncedWithZeroStore(core);
      if (!hasCoreEverSynced) {
        // The message in this exception is verified by some tests. Change here, change there...
        throw new SolrException(
            SolrException.ErrorCode.SERVICE_UNAVAILABLE,
            core.getName()
                + " is not fresh enough because it has never synced with the Zero store.");
      }
    }
  }

  public CorePullStatus pullCoreFromZeroStore(SolrCore core) throws SolrException {
    try {
      CorePuller corePuller = newCorePuller(core);
      return corePuller.pullCoreWithRetries(
          isLeader(core),
          ZeroCoreIndexingBatchProcessor.SECONDS_TO_WAIT_PULL_LOCK,
          getConfig().getCorePullRetryDelay());
    } catch (ZeroException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
    }
  }

  public void setIndexingBatchReceived(SolrCore core) {
    getMetadataCacheManager()
        .recordState(core, MetadataCacheManager.ZeroCoreStage.INDEXING_BATCH_RECEIVED);
  }

  public void setIndexingStarted(SolrCore core) {
    getMetadataCacheManager()
        .recordState(core, MetadataCacheManager.ZeroCoreStage.LOCAL_INDEXING_STARTED);
  }

  public void setIndexingFinished(SolrCore core) {
    getMetadataCacheManager()
        .recordState(core, MetadataCacheManager.ZeroCoreStage.INDEXING_BATCH_FINISHED);
  }

  public ZeroAccessLocks getZeroAccessLocks(String coreName) {
    return getMetadataCacheManager().getOrCreateCoreMetadata(coreName).getZeroAccessLocks();
  }

  /**
   * Acquires the indexing lock that must be held during the split process using the {@link
   * ZeroCoreIndexingBatchProcessor#SECONDS_TO_WAIT_PULL_LOCK} timeout.
   *
   * <p>For indexing, the same lock is acquired in {@link
   * ZeroCoreIndexingBatchProcessor#startIndexingBatch} using the actual indexing timeout.
   */
  public AutoCloseable acquireIndexingLockForSplit(SolrCore core) throws SolrException {
    try {
      // TODO Maybe use ZeroCoreIndexingBatchProcessor.SECONDS_TO_WAIT_INDEXING_LOCK instead?
      return metadataCacheManager
          .getOrCreateCoreMetadata(core.getName())
          .getZeroAccessLocks()
          .acquireIndexingLock(ZeroCoreIndexingBatchProcessor.SECONDS_TO_WAIT_PULL_LOCK);
    } catch (ZeroLockException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          String.format(
              Locale.ROOT,
              "Failed to acquire indexing lock within %s seconds. "
                  + "collection=%s shard=%s core=%s",
              ZeroCoreIndexingBatchProcessor.SECONDS_TO_WAIT_PULL_LOCK,
              getCollectionName(core),
              getShardName(core),
              core.getName()));
    } catch (InterruptedException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          String.format(
              Locale.ROOT,
              "Thread interrupted while trying to acquire pull read lock."
                  + " collection=%s shard=%s core=%s",
              getCollectionName(core),
              getShardName(core),
              core.getName()));
    }
  }

  public void initialCorePushToZeroStore(SolrCore core) throws InterruptedException {
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collectionName = cloudDesc.getCollectionName();
    String shardName = cloudDesc.getShardId();
    String coreName = core.getName();

    if (log.isInfoEnabled()) {
      log.info(
          "Attempting to push to Zero store for collection={} shard={} core={}",
          collectionName,
          shardName,
          coreName);
    }

    CorePusher pusher = newCorePusher(core);

    pusher.initialCorePushToZeroStore();

    if (log.isInfoEnabled()) {
      log.info(
          "Successfully pushed to Zero store for collection={} shard={} core={}",
          collectionName,
          shardName,
          coreName);
    }
  }

  /**
   * Enqueues the core (wrapped in a {@link CorePuller}) into the deduplicating list to be
   * eventually pulled from Zero store.
   *
   * <p>Eventually, the {@link #asyncCorePullExecutor} will call {@link
   * CorePuller#lockAndPullCore(boolean, long)}. The actual execution will happen in {@link
   * CorePullerBlockingQueue.RunnableAsyncCorePull#run()}, built in {@link
   * CorePullerBlockingQueue#take()}.
   *
   * <p>In case of pull errors (even obviously transient ones) there are no retries. Given the
   * enqueue is triggered by queries, a subsequent query will trigger a new pull request. Note that
   * upon completion, the {@link CorePuller#pullEndNotification} is called by calling {@link
   * CorePuller#notifyPullEnd(CorePullStatus)}.
   *
   * <p>TODO more work needed because we shouldn't try to pull too often
   */
  public void enqueueCorePullFromZeroStore(SolrCore core) throws Exception {
    CorePuller puller = newCorePuller(core);
    corePullTaskQueue.addDeduplicated(puller, false);
  }

  @VisibleForTesting
  void stopBackgroundCorePulling() {
    // Stopping the executor will stop the async pulls.
    // Tests do not need to restart the pulls, we're ok.
    ExecutorUtil.shutdownAndAwaitTermination(asyncCorePullExecutor);
  }

  public void deleteShard(String collectionName, String shardName) throws SolrException {
    zeroMetadataController.cleanUpMetadataNodes(collectionName, shardName);

    CompletableFuture<DeleterTask.Result> deleteFuture =
        overseerDeleteProcessor.deleteShard(collectionName, shardName, false);

    DeleterTask.Result result = null;
    Throwable t = null;
    try {
      // TODO: Find a reasonable timeout value
      result = deleteFuture.get(60, TimeUnit.SECONDS);
    } catch (Exception ex) {
      t = ex;
    }
    if (t != null || !result.isSuccess()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not complete deleting shard"
              + shardName
              + " from Zero store belonging to collection "
              + collectionName
              + ". Files belonging to this shard may be orphaned.",
          t);
    }
  }

  public void deleteCollection(String collectionName) throws SolrException {
    // deletes all files belonging to this collection
    CompletableFuture<DeleterTask.Result> deleteFuture =
        overseerDeleteProcessor.deleteCollection(collectionName, false);

    DeleterTask.Result result = null;
    Throwable t = null;
    try {
      // TODO: Find a reasonable timeout value
      result = deleteFuture.get(60, TimeUnit.SECONDS);
    } catch (Exception ex) {
      t = ex;
    }
    if (t != null || !result.isSuccess()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not complete deleting collection "
              + collectionName
              + " from Zero store, files belonging to this collection"
              + " may be orphaned.",
          t);
    }
  }

  private String getCollectionName(SolrCore core) {
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    return cloudDesc.getCollectionName();
  }

  private String getShardName(SolrCore core) {
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    return cloudDesc.getShardId();
  }

  /**
   * This method goes over all the {@link org.apache.solr.common.cloud.Replica.Type#ZERO} replicas
   * that belong to this node and ensures that there are local core descriptors corresponding to all
   * of them. If missing, it will create them.
   *
   * <p>If we encounter any unknown error during discovery we will log a warning and ignore the
   * error i.e. we prefer loading core container over missing cores.
   *
   * @param locallyDiscoveredCoreDescriptors list of the core descriptors that already exist locally
   * @return list of the core descriptors of {@link org.apache.solr.common.cloud.Replica.Type#ZERO}
   *     replicas that were missing locally
   */
  public List<CoreDescriptor> discoverAdditionalCoreDescriptorsForZeroReplicas(
      List<CoreDescriptor> locallyDiscoveredCoreDescriptors, CoreContainer coreContainer) {
    List<CoreDescriptor> additionalCoreDescriptors = new ArrayList<>();
    try {
      Set<String> localCoreDescriptorSet =
          CollectionUtil.newHashSet(locallyDiscoveredCoreDescriptors.size());
      for (CoreDescriptor cd : locallyDiscoveredCoreDescriptors) {
        localCoreDescriptorSet.add(cd.getName());
      }

      ClusterState clusterState = coreContainer.getZkController().getClusterState();
      for (DocCollection collection : clusterState.getCollectionsMap().values()) {
        try {
          if (!collection.isZeroIndex()) {
            // skip non-Zero collections
            continue;
          }

          // TODO: if shard activation (including post split) can guarantee core existence locally
          // we can skip inactive shards
          // go over collection's replicas belonging to this node
          List<Replica> nodeReplicas =
              collection.getReplicas(coreContainer.getZkController().getNodeName());
          if (nodeReplicas != null) {
            for (Replica replica : nodeReplicas) {
              try {
                if (!localCoreDescriptorSet.contains(replica.getCoreName())) {
                  String coreName = replica.getCoreName();
                  // no corresponding core descriptor present locally
                  if (log.isInfoEnabled()) {
                    log.info(
                        "Found a replica with missing core descriptor, collection={} replica={} core={}",
                        collection.getName(),
                        replica.getName(),
                        coreName);
                  }

                  Map<String, String> coreProperties = getZeroCoreProperties(collection, replica);

                  // create the missing core descriptor
                  CoreDescriptor cd =
                      new CoreDescriptor(
                          coreName,
                          coreContainer.getCoreRootDirectory().resolve(coreName),
                          coreProperties,
                          coreContainer.getContainerProperties(),
                          coreContainer.getZkController());
                  // this will create the core.properties file on disk
                  coreContainer.getCoresLocator().create(coreContainer, cd);
                  // add to list of additional core descriptors
                  additionalCoreDescriptors.add(cd);

                  // also add to local core descriptor set, so if we encounter it again (ideally
                  // we should not) we might not recreate it
                  localCoreDescriptorSet.add(coreName);
                }
              } catch (Exception ex) {
                log.warn(
                    String.format(
                        Locale.ROOT,
                        "Failed to create missing core descriptor for a replica, collection=%s replica=%s core=%s",
                        collection.getName(),
                        replica != null ? replica.getName() : "",
                        replica != null ? replica.getCoreName() : ""),
                    ex);
              }
            }
          }
        } catch (Exception ex) {
          log.warn(
              String.format(
                  Locale.ROOT,
                  "Failed to discover additional core descriptors for a Zero collection from zookeeper, collection=%s",
                  collection != null ? collection.getName() : ""),
              ex);
        }
      }
    } catch (Exception ex) {
      log.warn(
          "Failed to discover additional core descriptors for Zero collections from zookeeper.",
          ex);
    }

    return additionalCoreDescriptors;
  }
}
