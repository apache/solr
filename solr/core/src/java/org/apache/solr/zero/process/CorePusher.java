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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.exception.ZeroLockException;
import org.apache.solr.zero.metadata.LocalCoreMetadata;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.MetadataCacheManager.MetadataCacheEntry;
import org.apache.solr.zero.metadata.MetadataCacheManager.ZeroCoreStage;
import org.apache.solr.zero.metadata.MetadataComparisonResult;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.apache.solr.zero.util.FileTransferCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes synchronous pushes of core updates to the Zero store.
 *
 * <p>Pushes will be triggered at the end of an indexing batch when a shard's index data has changed
 * locally and needs to be persisted in the Zero store, or when a core is created post split or
 * restore.
 *
 * <p>Pushes to the Zero store do not happen during replica creation (including when part of
 * collection creation) but only after the first indexing batch.
 *
 * <p>TODO This is most certainly an issue when a collection is created but not indexed right away,
 * or when a collection is created with multiple shards and some of them are not indexed (possibly
 * never indexed).
 */
public class CorePusher {

  /**
   * There should be no contention when acquiring these locks so a small timeout should be
   * reasonable
   */
  private static final int LOCK_ACQUIRE_TIMEOUT = 5;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  ZeroStoreClient zeroStoreClient;
  DeleteProcessor deleteProcessor;
  MetadataCacheManager metadataCacheManager;
  ZeroMetadataController metadataController;
  SolrCore solrCore;

  public CorePusher(
      SolrCore solrCore,
      ZeroStoreClient zeroStoreClient,
      DeleteProcessor deleteProcessor,
      MetadataCacheManager metadataCacheManager,
      ZeroMetadataController metadataController) {
    this.zeroStoreClient = zeroStoreClient;
    this.deleteProcessor = deleteProcessor;
    this.metadataCacheManager = metadataCacheManager;
    this.metadataController = metadataController;
    this.solrCore = solrCore;
  }

  /**
   * Pushes the local core updates to the Zero store and logs whether the push succeeded or failed.
   * This is the end to end push method, comparing the local and distant states of the core (local
   * core vs "distant" shard stored in Zero store), writing all required files to the Zero store and
   * finally updating the metadata in ZooKeeper to refer to the new shard.metadata file.
   */
  @SuppressWarnings("try")
  public CorePusherExecutionInfo endToEndPushCoreToZeroStore() throws SolrException {
    try {
      String collectionName = getCollectionName();
      String shardName = getShardName();

      MDCLoggingContext.setCore(solrCore);
      if (log.isInfoEnabled()) {
        log.info("Initiating push {}", getTaskDescription());
      }

      Instant startTime = Instant.now();
      try (AutoCloseable ignore =
          metadataCacheManager
              .getOrCreateCoreMetadata(solrCore.getName())
              .getZeroAccessLocks()
              .acquirePushLock(LOCK_ACQUIRE_TIMEOUT)) {
        long lockAcquisitionTime = startTime.until(Instant.now(), ChronoUnit.MILLIS);
        try {
          metadataCacheManager.recordState(solrCore, ZeroCoreStage.PUSH_STARTED);

          IndexCommit latestCommit = solrCore.getDeletionPolicy().getLatestCommit();
          if (latestCommit == null) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR, "Core " + getCoreName() + " has no available commit point");
          }

          MetadataCacheEntry coreMetadata =
              metadataCacheManager.getOrCreateCoreMetadata(solrCore.getName());
          long zeroGeneration = coreMetadata.getZeroShardMetadata().getGeneration();
          if (latestCommit.getGeneration() == zeroGeneration) {
            // Everything up to latest commit point has already been pushed
            // This can happen if another indexing batch comes in and acquires the push lock first
            // and ends up pushing segments produced by this indexing batch.
            // TODO could it be another node pushed a different segment of same generation?
            if (log.isInfoEnabled())
              log.info(
                  "Nothing to push, pushLockTime={}: {}",
                  lockAcquisitionTime,
                  getTaskDescription());
            long totalTimeMs = startTime.until(Instant.now(), ChronoUnit.MILLIS);
            return CorePusherExecutionInfo.localAlreadyUpToDate(
                zeroGeneration, lockAcquisitionTime, totalTimeMs);
          }

          // Compute the differences (if any) between the local shard index data and shard index
          // data on the Zero store. The names of new files to push to the Zero store will include
          // a suffix that is generated here and passed to the comparison method. Even though not
          // required, all files pushed together will have the same suffix (but different file
          // names). This helps debug when trying to understand when a file appeared.
          // Reserving the commit point, so it can be saved while pushing files to the Zero store.
          // We don't need to compute a directory hash for the push scenario as we only need to
          // verify local index does not change during pull.
          LocalCoreMetadata localCoreMetadata = new LocalCoreMetadata(solrCore);
          localCoreMetadata.readMetadata(true, false);
          MetadataComparisonResult metadataComparisonResult =
              metadataController.diffMetadataForPush(
                  localCoreMetadata,
                  coreMetadata.getZeroShardMetadata(),
                  ZeroStoreShardMetadata.generateMetadataSuffix());
          if (metadataComparisonResult.getFilesToPush().isEmpty()) {
            log.warn(
                "Why there is nothing to push even when there is a newer commit point since last push pushLockTime={}: {}",
                lockAcquisitionTime,
                getTaskDescription());
            long totalTimeMs = startTime.until(Instant.now(), ChronoUnit.MILLIS);
            return CorePusherExecutionInfo.oddEmptyDiffCommitPoint(
                zeroGeneration,
                Collections.emptySet(),
                metadataComparisonResult.getFilesToDelete(),
                lockAcquisitionTime,
                totalTimeMs);
          }

          // begin the push process
          Instant pushStartTime = Instant.now();

          ZeroStoreShardMetadata zeroStoreShardMetadata =
              pushFilesToZeroStore(metadataComparisonResult);
          metadataCacheManager.recordState(
              solrCore, MetadataCacheManager.ZeroCoreStage.FILE_PUSHED);
          long pushTimeMs = pushStartTime.until(Instant.now(), ChronoUnit.MILLIS);
          // at this point we've pushed the new metadata file with the newMetadataSuffix and now
          // need to write to zookeeper
          Instant metadataUpdateStartTime = Instant.now();
          ZeroMetadataVersion newShardMetadataVersion = null;
          try {
            newShardMetadataVersion =
                metadataController.updateMetadataValueWithVersion(
                    collectionName,
                    shardName,
                    metadataComparisonResult.getMetadataSuffix(),
                    coreMetadata.getMetadataVersion().getVersion());
          } catch (Exception ex) {
            // conditional update of zookeeper failed, reset "cache likely up to date"
            // regardless of the exception type.
            // We observe that we can hit Keeper$ConnectionLoss while the suffix is actually updated
            // That will make sure before processing next indexing batch, we sync with zookeeper and
            // pull from the Zero store.
            metadataCacheManager.updateCoreMetadata(solrCore.getName(), false);
            throw ex;
          }
          metadataCacheManager.recordState(
              solrCore, MetadataCacheManager.ZeroCoreStage.ZK_UPDATE_FINISHED);
          long metadataUpdateTimeMs =
              metadataUpdateStartTime.until(Instant.now(), ChronoUnit.MILLIS);

          assert metadataComparisonResult
              .getMetadataSuffix()
              .equals(newShardMetadataVersion.getMetadataSuffix());
          // after successful update to zookeeper, update cached core metadata with new info and
          // consider cache likely up to date wrt the Zero store
          metadataCacheManager.updateCoreMetadata(
              solrCore.getName(), newShardMetadataVersion, zeroStoreShardMetadata, true);
          metadataCacheManager.recordState(
              solrCore, MetadataCacheManager.ZeroCoreStage.LOCAL_CACHE_UPDATE_FINISHED);
          if (log.isInfoEnabled())
            log.info(
                "Successfully pushed to the Zero store, pushLockTime={}: {}",
                lockAcquisitionTime,
                getTaskDescription());
          long totalTimeMs = startTime.until(Instant.now(), ChronoUnit.MILLIS);
          return new CorePusherExecutionInfo(
              true,
              zeroGeneration,
              metadataComparisonResult.getMetadataSuffix(),
              metadataComparisonResult.getFilesToPush(),
              metadataComparisonResult.getFilesToDelete(),
              lockAcquisitionTime,
              pushTimeMs,
              metadataUpdateTimeMs,
              totalTimeMs);
        } finally {
          metadataCacheManager.recordState(solrCore, ZeroCoreStage.PUSH_FINISHED);
        }
      }
      // TODO - make error handling a little nicer?
    } catch (InterruptedException e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "CorePusher was interrupted while pushing to the Zero store", e);
    } catch (SolrException e) {
      Throwable t = e.getCause();
      if (t instanceof BadVersionException) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "CorePusher failed to push because the node "
                + "version doesn't match, requestedVersion="
                + ((BadVersionException) t).getRequested(),
            t);
      }
      throw e;
    } catch (Exception ex) {
      // wrap every thrown exception in a solr exception
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error trying to push to the Zero store " + getTaskDescription(),
          ex);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  /**
   * This method calls the {@link #endToEndPushCoreToZeroStore()} after having acquired the indexing
   * lock. It verifies that this is the first push of that shard to the Zero store.
   *
   * <p>Acquiring the indexing lock is slightly vain... This code works because it is called from a
   * restore or post shard split, there are no competing cores elsewhere in the cluster that could
   * push content to the Zero store while this core is trying to do its push.
   *
   * <p>TODO need to clarify the lifecycle of the MetadataCacheManager vs ZK stored metadataSuffix
   */
  @SuppressWarnings("try")
  public void initialCorePushToZeroStore() throws InterruptedException {
    String collectionName = getCollectionName();
    String shardName = getShardName();

    // we'll do a timed attempt to acquire the read lock, but at this point in time the sub shard
    // should not be active and indexing so we shouldn't wait long
    try (ZeroAccessLocks.NoThrowAutoCloseable ignore =
        metadataCacheManager
            .getOrCreateCoreMetadata(solrCore.getName())
            .getZeroAccessLocks()
            .acquireIndexingLock(LOCK_ACQUIRE_TIMEOUT)) {
      ZeroMetadataVersion shardMetadataVersion =
          metadataController.readMetadataValue(collectionName, shardName);
      // TODO if split failed and retried, will it use a different core name? Or fail forever?
      if (!metadataController.hasDefaultNodeSuffix(shardMetadataVersion)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "New shard has zk information that is not default. "
                + "The suffix value is "
                + shardMetadataVersion.getMetadataSuffix()
                + getTaskDescription());
      }
      // sync local cache with zk's default information i.e. equivalent of no-op pull
      // this syncing is necessary for the zk conditional update to succeed at the end of core
      // push
      try (ZeroAccessLocks.NoThrowAutoCloseable ignore2 =
          metadataCacheManager
              .getOrCreateCoreMetadata(solrCore.getName())
              .getZeroAccessLocks()
              .acquirePushLock(LOCK_ACQUIRE_TIMEOUT)) {
        // Push yet to happen so metadata cache not assumed up to date
        metadataCacheManager.updateCoreMetadata(
            solrCore.getName(), shardMetadataVersion, new ZeroStoreShardMetadata(), false);
        endToEndPushCoreToZeroStore();
      }
    } catch (ZeroLockException e) {

      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Push lock: " + e.getMessage() + " " + getTaskDescription());
    }
  }

  /**
   * Writes to the Zero store all the files that should be written to it, including the contents of
   * {@link ZeroStoreShardMetadata} as shard.metadata (with a suffix). This method does not update
   * ZooKeeper with the new metadataSuffix. Until this happens, the writes to Zero store are not
   * taken into account.
   */
  @VisibleForTesting
  protected ZeroStoreShardMetadata pushFilesToZeroStore(
      MetadataComparisonResult metadataComparisonResult) throws Exception {

    String collectionName = getCollectionName();
    String shardName = getShardName();

    MetadataCacheEntry coreMetadata =
        metadataCacheManager.getOrCreateCoreMetadata(solrCore.getName());
    ZeroStoreShardMetadata shardMetadata = coreMetadata.getZeroShardMetadata();
    Instant startTime = Instant.now();

    FileTransferCounter counter = new FileTransferCounter();
    boolean isSuccessful = false;

    try {
      MDCLoggingContext.setCore(solrCore);

      // Creating the new ZeroStoreShardMetadata as a modified clone of the existing one
      ZeroStoreShardMetadata newShardMetadata =
          new ZeroStoreShardMetadata(
              shardMetadata.getZeroFiles(),
              shardMetadata.getZeroFilesToDelete(),
              metadataComparisonResult.getLocalGeneration());

      // Submit for delete only Zero store files previously marked for deletion, not the ones that
      // the current update marks for deletion. That's why the enqueue for delete is done before
      // adding the just deleted files to newShardMetadata. If the current update fails (in the
      // ZooKeeper update phase for example), the just deleted files remain required.
      enqueueForHardDelete(newShardMetadata);

      // Files that are part of the current commit point on the Zero store but no longer needed
      // after this push succeeds are not deleted (see above why) but moved from the set of files
      // being part of the shard to the set of files that need to be deleted later.
      for (ZeroFile.WithLocal bf : metadataComparisonResult.getFilesToDelete()) {
        newShardMetadata.removeFile(bf);
        ZeroFile.ToDelete zftd = new ZeroFile.ToDelete(bf, Instant.now());
        newShardMetadata.addFileToDelete(zftd);
      }

      // add the old shard.metadata file to delete
      if (!metadataController.hasDefaultNodeSuffix(coreMetadata)) {
        ZeroFile.ToDelete zftd =
            metadataController.newShardMetadataZeroFileToDelete(
                collectionName, shardName, coreMetadata.getMetadataVersion().getMetadataSuffix());
        newShardMetadata.addFileToDelete(zftd);
      }

      // When we build the LocalCoreMetadata we requested to reserve the commit point for some short
      // duration. Assumption is it took less than this duration to get here (no blocking
      // operations). Now we actually save the commit point for the (potentially long) time it takes
      // to push all files to the Zero store.
      solrCore.getDeletionPolicy().saveCommitPoint(metadataComparisonResult.getLocalGeneration());

      Directory coreIndexDir = CorePuller.getDirectory(solrCore, solrCore.getIndexDir());
      try {
        // parallel pushes of segment files and the new shard.metadata file into the Zero store
        CompletableFuture<?>[] futures =
            new CompletableFuture<?>[metadataComparisonResult.getFilesToPush().size() + 1];

        int i = 0;
        // Push all segment files
        for (ZeroFile.WithLocal zf : metadataComparisonResult.getFilesToPush()) {
          // Sanity check that we're talking about the same file (just sanity, Solr doesn't update
          // files so should never be different)
          assert zf.getFileSize() == coreIndexDir.fileLength(zf.getSolrFileName());

          futures[i++] = zeroStoreClient.pushFileAsync(coreIndexDir, zf);
          newShardMetadata.addFile(zf);
          counter.incrementActualFilesTransferred();
          counter.incrementBytesTransferred(zf.getFileSize());
        }
        // push the new shard.metadata in parallel with other files
        // As opposed to segment files, for shard.metadata not capturing file size and checksum.
        // This is because there is nothing to compare them with, as opposed to normal segment files
        // for which this information is captured... in the shard.metadata file :-O
        futures[i] =
            zeroStoreClient.pushShardMetadataAsync(
                metadataController.newShardMetadataZeroFile(
                    getCollectionName(),
                    getShardName(),
                    metadataComparisonResult.getMetadataSuffix()),
                newShardMetadata);
        counter.incrementActualFilesTransferred();
        String shardMetadataFileContent = newShardMetadata.toJson();
        counter.incrementBytesTransferred(shardMetadataFileContent.length());

        CompletableFuture.allOf(futures).get();
      } finally {
        solrCore.getDirectoryFactory().release(coreIndexDir);
        solrCore
            .getDeletionPolicy()
            .releaseCommitPoint(metadataComparisonResult.getLocalGeneration());
      }

      isSuccessful = true;
      return newShardMetadata;
    } finally {
      MDCLoggingContext.clear();

      counter.setExpectedFilesTransferred(metadataComparisonResult.getFilesToPush().size());

      if (log.isInfoEnabled()) {
        log.info(
            "PUSH runTime={} bytesTransferred={} expectedFilesAffected={} actualFilesAffected={} isSuccessful={} "
                + "localGeneration={} zeroGeneration={}",
            startTime.until(Instant.now(), ChronoUnit.MILLIS),
            counter.getBytesTransferred(),
            counter.getExpectedFilesTransferred(),
            counter.getActualFilesTransferred(),
            isSuccessful,
            metadataComparisonResult.getLocalGeneration(),
            coreMetadata.getZeroShardMetadata().getGeneration());
      }
    }
  }

  @VisibleForTesting
  public void enqueueForHardDelete(ZeroStoreShardMetadata shardMetadata) {
    Set<ZeroFile.ToDelete> filesToDelete =
        shardMetadata.getZeroFilesToDelete().stream()
            .filter(this::okForHardDelete)
            .collect(Collectors.toSet());
    if (enqueueForDelete(filesToDelete)) {
      shardMetadata.removeFilesFromDeleted(filesToDelete);
    }
  }

  /**
   * @return true if the files were enqueued for deletion successfully
   */
  @VisibleForTesting
  protected boolean enqueueForDelete(Set<ZeroFile.ToDelete> zeroFilesToDelete) {
    if (zeroFilesToDelete == null || zeroFilesToDelete.isEmpty()) {
      return false;
    }
    Set<ZeroFile> zeroFiles = new HashSet<>(zeroFilesToDelete);

    if (zeroFiles.isEmpty()) return true;
    try {
      deleteProcessor.deleteFiles(zeroFiles, true);
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  private String getTaskDescription() {
    return String.format(
        Locale.ROOT,
        "(col=%s,shard=%s,name=%s,thread=%s)",
        getCollectionName(),
        getShardName(),
        getCoreName(),
        Thread.currentThread().getName());
  }

  /**
   * Returns true if a deleted Zero store file (i.e. a file marked for delete but not deleted yet)
   * can be hard deleted now.
   */
  @VisibleForTesting
  protected boolean okForHardDelete(ZeroFile.ToDelete file) {
    // For now we only check how long ago the file was marked for delete.
    return file.getDeletedAt().until(Instant.now(), ChronoUnit.MILLIS)
        >= deleteProcessor.getDeleteDelayMs();
  }

  protected String getCoreName() {
    return solrCore.getName();
  }

  protected String getShardName() {
    return solrCore.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  protected String getCollectionName() {
    return solrCore.getCoreDescriptor().getCollectionName();
  }
}
