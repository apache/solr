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

package org.apache.solr.zero.metadata;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.zero.process.CorePuller;
import org.apache.solr.zero.process.ZeroAccessLocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains a cache of Zero store shard metadata for local cores of ZERO replicas. For
 * each local core, an instance of {@link MetadataCacheEntry} is stored. The data comes in part from
 * the Zero store, in part from ZooKeeper.
 */
public class MetadataCacheManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Value for a newly initialized MetadataCacheEntry instance. Not expected to make it to ZK,
   * unlike real UUID's or the default value {@link
   * ZeroMetadataController#METADATA_NODE_DEFAULT_VALUE} written at the very beginning of the ZERO
   * shard's life cycle
   */
  private static final String METADATA_SUFFIX_CACHE_INITIAL_VALUE = "iNiTiAl";

  private final ZeroConfig config;
  private final CoreContainer coreContainer;

  /**
   * This map maintains for a local core information about Zero store shard metadata, corresponding
   * metadataSuffix and version, as well as the locks for Zero store operations done on this node
   */
  private final ConcurrentHashMap<String, MetadataCacheEntry> coresMetadata;

  /**
   * Counters for the failed core pull attempts. Key is the core name, value is the counter which
   * also includes the last attempt time.
   */
  private final ConcurrentHashMap<String, PullAttempts> coresPullAttempts;

  /** Used by tests to provide their own {@link PullAttempts} factory. */
  private final Supplier<PullAttempts> pullAttemptsFactory;

  public MetadataCacheManager(ZeroConfig config, CoreContainer coreContainer) {
    this(config, coreContainer, PullAttempts::new);
  }

  public MetadataCacheManager(
      ZeroConfig config, CoreContainer coreContainer, Supplier<PullAttempts> pullAttemptsFactory) {
    this.config = config;
    this.coreContainer = coreContainer;
    this.pullAttemptsFactory = pullAttemptsFactory;
    coresMetadata = buildMetadataCache();
    coresPullAttempts = new ConcurrentHashMap<>();
  }

  /**
   * Logs the current {@link ZeroCoreStage} a core is at. Increments the pull attempts ({@link
   * ZeroCoreStage#PULL_STARTED}) to abort pulling if there were more than {@link
   * ZeroConfig#getMaxFailedCorePullAttempts()} unsuccessful pulls (without the corresponding {@link
   * ZeroCoreStage#PULL_SUCCEEDED}).
   */
  public void recordState(SolrCore core, ZeroCoreStage stage) {
    String coreName = getCoreName(core);
    String collectionName = getCollectionName(core);
    String shardName = getShardName(core);
    log.info(
        "RecordZeroCoreStage: collection={} shard={} core={} stage={}",
        collectionName,
        shardName,
        coreName,
        stage);
    switch (stage) {
      case PULL_STARTED:
        coresPullAttempts.compute(
            coreName,
            (__, attempts) -> {
              if (attempts == null) {
                attempts = pullAttemptsFactory.get();
              }
              return attempts.checkAndIncrement(coreName, config);
            });
        break;
      case PULL_SUCCEEDED:
        coresPullAttempts.remove(coreName);
        break;
      case PULL_FAILED:
        coresPullAttempts.computeIfPresent(coreName, (__, attempts) -> attempts.onFailedAttempt());
        break;
      case PULL_FAILED_WITH_CORRUPTION:
        // We only want to retry once to pull a corrupted core.
        coresPullAttempts.compute(
            coreName,
            (__, attempts) -> {
              if (attempts == null) {
                attempts = pullAttemptsFactory.get();
              }
              if (!attempts.onFailedAttempt().exhaustAllButOne(config)) {
                // Done with attempts to pull the corrupted core. Set the replica state to
                // RECOVERY_FAILED
                // to prevent updates and queries on it, and to alert.
                setReplicaStateToRecoveryFailed(collectionName, shardName, coreName);
              }
              return attempts;
            });
        break;
      default:
        break;
    }
  }

  /**
   * Sets the replica state to RECOVERY_FAILED for a corrupted core that cannot be pulled from the
   * Zero store. This state will prevent any update or query on the replica, and it will be
   * monitored with a metric.
   */
  private void setReplicaStateToRecoveryFailed(
      String collectionName, String shardName, String coreName) {
    try (SolrCore core = coreContainer.getCore(coreName)) {
      try {
        coreContainer
            .getZkController()
            .publish(core.getCoreDescriptor(), Replica.State.RECOVERY_FAILED);
      } catch (Exception e) {
        log.error(
            "Cannot set replica state to {} for core {} shard {} collection {}",
            Replica.State.RECOVERY_FAILED,
            coreName,
            shardName,
            collectionName);
      }
    }
  }

  /**
   * Updates the {@link MetadataCacheEntry} for the core with passed in {@link ZeroMetadataVersion},
   * {@link ZeroStoreShardMetadata} and {@code cacheLikelyUpToDate}
   */
  public void updateCoreMetadata(
      String coreName,
      ZeroMetadataVersion shardMetadataVersion,
      ZeroStoreShardMetadata zeroStoreShardMetadata,
      boolean cacheLikelyUpToDate) {
    MetadataCacheEntry currentMetadata = getOrCreateCoreMetadata(coreName);
    MetadataCacheEntry updatedMetadata =
        currentMetadata.updatedOf(
            shardMetadataVersion, zeroStoreShardMetadata, cacheLikelyUpToDate);
    updateCoreMetadata(coreName, currentMetadata, updatedMetadata);
  }

  /** Updates {@link MetadataCacheEntry} for the core with passed in {@code cacheLikelyUpToDate} */
  public void updateCoreMetadata(String coreName, boolean cacheLikelyUpToDate) {
    MetadataCacheEntry currentMetadata = getOrCreateCoreMetadata(coreName);
    MetadataCacheEntry updatedMetadata = currentMetadata.updatedOf(cacheLikelyUpToDate);
    updateCoreMetadata(coreName, currentMetadata, updatedMetadata);
  }

  /**
   * Evicts an entry from the {@link #coresMetadata} if one exists for the given core name.
   *
   * @return {@code true} if an entry was actually removed
   */
  public boolean removeCoreMetadata(String coreName) {
    coresPullAttempts.remove(coreName);
    return coresMetadata.remove(coreName) != null;
  }

  private void updateCoreMetadata(
      String coreName, MetadataCacheEntry currentMetadata, MetadataCacheEntry updatedMetadata) {
    if (currentMetadata.getZeroAccessLocks().canUpdateCoreMetadata()) {
      if (log.isInfoEnabled()) {
        log.info(
            "updateCoreMetadata: core={}  current={} updated={}",
            coreName,
            currentMetadata,
            updatedMetadata);
      }
      coresMetadata.put(coreName, updatedMetadata);
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error updating the MetadataCacheEntry because the proper "
              + "locks are not held by the current thread. "
              + currentMetadata.locksHeldToString()
              + " core="
              + coreName);
    }
  }

  /**
   * Returns the cached {@link MetadataCacheEntry} representing Zero store metadata of the core or
   * creates a new entry for the core if none exists.
   */
  public MetadataCacheEntry getOrCreateCoreMetadata(String coreName) {
    return coresMetadata.computeIfAbsent(coreName, k -> new MetadataCacheEntry());
  }

  protected ConcurrentHashMap<String, MetadataCacheEntry> buildMetadataCache() {
    return new ConcurrentHashMap<>();
  }

  public boolean hasInitialCacheSuffixValue(MetadataCacheEntry coreMetadata) {
    return MetadataCacheManager.METADATA_SUFFIX_CACHE_INITIAL_VALUE.equals(
        coreMetadata.getMetadataVersion().getMetadataSuffix());
  }

  protected String getCoreName(SolrCore solrCore) {
    return solrCore.getName();
  }

  protected String getShardName(SolrCore solrCore) {
    return solrCore.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  protected String getCollectionName(SolrCore solrCore) {
    return solrCore.getCoreDescriptor().getCollectionName();
  }

  /**
   * This represents metadata that needs to be cached for a core of a Zero collection {@link
   * DocCollection#isZeroIndex()} so that it can be properly synchronized for concurrent indexing,
   * pushes and pulls.
   */
  public static class MetadataCacheEntry {
    /**
     * Value originating from a ZooKeeper node used to handle conditionally and safely update the
     * shard.metadata file written to the Zero store.
     */
    private final ZeroMetadataVersion metadataVersion;

    /**
     * {@link ZeroStoreShardMetadata} representing the state corresponding to {@link
     * #metadataVersion}
     */
    private final ZeroStoreShardMetadata zeroStoreShardMetadata;

    /**
     * Whether cache likely up to date with {@link ZeroMetadataVersion} of the shard. In steady
     * state this is set for leader cores when they push and pull {@link
     * CorePuller#pullCoreWithRetries(boolean, long, long)} since followers cannot index. In
     * presence of this flag we can skip consulting zookeeper before processing an indexing batch
     * (if the flag proved wrong, indexing will fail and the cached state will get updated).
     */
    private final boolean cacheLikelyUpToDate;

    /**
     * The locks only need to be initialized once. When the metadata changes (and a new instance of
     * {@link MetadataCacheEntry} created), the locks from the previous one must be reused.
     */
    private final ZeroAccessLocks locks;

    /** Constructor for initial insertion of an entry into the cache. */
    public MetadataCacheEntry() {
      // Metadata suffix that will never be actually used in the Zero store
      // Start with version 0 since when we use this entry to update the ZooKeeper node, it's
      // the first update after that node got created.
      this.metadataVersion = new ZeroMetadataVersion(METADATA_SUFFIX_CACHE_INITIAL_VALUE, 0);
      this.zeroStoreShardMetadata = new ZeroStoreShardMetadata();
      this.cacheLikelyUpToDate = false;
      this.locks = new ZeroAccessLocks();
    }

    /**
     * Used by {@link #updatedOf(boolean)} and {@link #updatedOf(ZeroMetadataVersion,
     * ZeroStoreShardMetadata, boolean)} in this class. Visible outside the clas only for tests.
     */
    @VisibleForTesting
    public MetadataCacheEntry(
        ZeroMetadataVersion metadataVersion,
        ZeroStoreShardMetadata zeroStoreShardMetadata,
        boolean cacheLikelyUpToDate,
        ZeroAccessLocks locks) {
      this.metadataVersion = metadataVersion;
      this.zeroStoreShardMetadata = zeroStoreShardMetadata;
      this.cacheLikelyUpToDate = cacheLikelyUpToDate;
      this.locks = locks;
    }

    public ZeroMetadataVersion getMetadataVersion() {
      return metadataVersion;
    }

    public ZeroStoreShardMetadata getZeroShardMetadata() {
      return zeroStoreShardMetadata;
    }

    public boolean isCacheLikelyUpToDate() {
      return cacheLikelyUpToDate;
    }

    public ZeroAccessLocks getZeroAccessLocks() {
      return locks;
    }

    private MetadataCacheEntry updatedOf(
        ZeroMetadataVersion metadataVersion,
        ZeroStoreShardMetadata zeroStoreShardMetadata,
        boolean cacheLikelyUpToDate) {
      return new MetadataCacheEntry(
          metadataVersion, zeroStoreShardMetadata, cacheLikelyUpToDate, locks);
    }

    private MetadataCacheEntry updatedOf(boolean cacheLikelyUpToDate) {
      return new MetadataCacheEntry(
          metadataVersion, zeroStoreShardMetadata, cacheLikelyUpToDate, locks);
    }

    public String locksHeldToString() {
      return String.format(
          Locale.ROOT, "%s cacheLikelyUpToDate=%s", metadataVersion, cacheLikelyUpToDate);
    }
  }

  /**
   * Various stages a core of a Zero collection {@link DocCollection#isZeroIndex()} might go through
   * during indexing and querying.
   */
  public enum ZeroCoreStage {
    /** Necessary locks have been acquired and we have started to pull from the Zero store. */
    PULL_STARTED,
    /** Pull has ended and succeeded, and we are about to release the necessary locks. */
    PULL_SUCCEEDED,
    /** Pull has ended but failed, and we are about to release the necessary locks. */
    PULL_FAILED,
    /** Pull has ended but failed because of a corrupted core index. */
    PULL_FAILED_WITH_CORRUPTION,
    /** We have received an indexing batch but necessary locks have not been acquired yet. */
    INDEXING_BATCH_RECEIVED,
    /**
     * We have passed the Zero store pull stage (if applicable) and are in sync with Zero store. Now
     * we will proceed with local indexing.
     */
    LOCAL_INDEXING_STARTED,
    /** Local indexing finished but has not been pushed to Zero store. */
    LOCAL_INDEXING_FINISHED,
    /** Necessary locks have been acquired and push to Zero store has started. */
    PUSH_STARTED,
    /** Files have been pushed to Zero store. */
    FILE_PUSHED,
    /** Zookeeper has been successfully updated with new metadata. */
    ZK_UPDATE_FINISHED,
    /** Local cache {@link #coresMetadata} has been successfully updated with new metadata. */
    LOCAL_CACHE_UPDATE_FINISHED,
    /**
     * Push (either successful or failed) has ended and we are about to release the necessary locks.
     */
    PUSH_FINISHED,
    /**
     * Indexing batch (either successful or failed) has ended and we are about to release the
     * necessary locks.
     */
    INDEXING_BATCH_FINISHED
  }

  /** Records the number of failed pull attempts and the last attempt time. */
  @VisibleForTesting
  public static class PullAttempts {
    private int count;
    private boolean pullAllowed;
    private long lastAttemptTimeNs;

    /**
     * Checks the pull attempts count and either increments it, or throws a {@link SolrException} if
     * the pull is not allowed until some delay elapses since the previous attempt. Sets
     * lastAttemptTimeNs to the current time if the attempt is allowed.
     */
    synchronized PullAttempts checkAndIncrement(String coreName, ZeroConfig config) {
      long timeNs = nanoTime();
      long retryInNs = computeRemainingTimeNs(timeNs, config);
      if (retryInNs > 0) {
        throw new SolrException(
            SolrException.ErrorCode.INVALID_STATE,
            coreName
                + " will not be pulled from the Zero store; canceling pull attempt due to "
                + count
                + " failed attempts, next attempt allowed in "
                + TimeUnit.NANOSECONDS.toSeconds(retryInNs)
                + " s");
      }
      count++;
      pullAllowed = true;
      // Resetting lastAttemptTimeNs here is not required, just in case onFailedAttempt() is not
      // called.
      lastAttemptTimeNs = timeNs;
      return this;
    }

    synchronized PullAttempts onFailedAttempt() {
      // Only reset lastAttemptTimeNs if the pull was allowed and failed.
      if (pullAllowed) {
        lastAttemptTimeNs = nanoTime();
        pullAllowed = false;
      }
      return this;
    }

    private long computeRemainingTimeNs(long timeNs, ZeroConfig config) {
      long remainingTimeNs;
      if (count == 0) {
        remainingTimeNs = 0;
      } else {
        long delayMs =
            count >= config.getMaxFailedCorePullAttempts()
                ?
                // All pull attempts are exhausted. Still retry once periodically.
                config.getAllAttemptsExhaustedRetryDelay()
                :
                // Allow the next pull attempt only if the last attempt was long enough ago.
                count * config.getCorePullAttemptDelay();
        remainingTimeNs = TimeUnit.MILLISECONDS.toNanos(delayMs) - (timeNs - lastAttemptTimeNs);
      }
      return remainingTimeNs;
    }

    synchronized boolean exhaustAllButOne(ZeroConfig config) {
      if (count >= config.getMaxFailedCorePullAttempts()) {
        return false;
      }
      count = config.getMaxFailedCorePullAttempts() - 1;
      return true;
    }

    @VisibleForTesting
    protected long nanoTime() {
      return System.nanoTime();
    }
  }
}
