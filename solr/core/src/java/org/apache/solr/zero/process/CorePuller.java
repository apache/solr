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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.exception.CorruptedCoreException;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.exception.ZeroLockException;
import org.apache.solr.zero.metadata.LocalCoreMetadata;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.MetadataComparisonResult;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.apache.solr.zero.util.DeduplicatingList;
import org.apache.solr.zero.util.FileTransferCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Code for pulling updates for a specific core from the Zero store. */
public class CorePuller implements DeduplicatingList.Deduplicatable<String> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZeroStoreClient zeroStoreClient;
  private final SolrCore solrCore;
  private final MetadataCacheManager metadataCacheManager;
  private final ZeroMetadataController metadataController;
  private final Instant queuedTime;
  private final AtomicInteger currentPullAttempt;
  private final int maxPullAttempts;
  private final Instant lastPullAttemptTime;

  /**
   * A callback method for end pull notification for async pulls (queries). Will be a no-op in
   * production but some tests do set it. Note no notification will be received when the pull throws
   * an exception rather than return a failure status code, which is the case for most though not
   * all errors.
   */
  private final BiConsumer<CorePuller, CorePullStatus> pullEndNotification;

  CorePuller(
      SolrCore solrCore,
      ZeroStoreClient zeroStoreClient,
      MetadataCacheManager metadataCacheManager,
      ZeroMetadataController metadataController,
      Instant queuedTime,
      int currentPullAttempt,
      int maxPullAttempts,
      Instant lastPullAttemptTime,
      BiConsumer<CorePuller, CorePullStatus> pullEndNotification) {
    this.zeroStoreClient = zeroStoreClient;
    this.metadataCacheManager = metadataCacheManager;
    this.metadataController = metadataController;
    this.solrCore = solrCore;
    this.queuedTime = queuedTime;
    this.currentPullAttempt = new AtomicInteger(currentPullAttempt);
    this.lastPullAttemptTime = lastPullAttemptTime;
    this.maxPullAttempts = maxPullAttempts;
    this.pullEndNotification = pullEndNotification != null ? pullEndNotification : (u, v) -> {};
  }

  public CorePuller(
      SolrCore solrCore,
      ZeroStoreClient zeroStoreClient,
      MetadataCacheManager metadataCacheManager,
      ZeroMetadataController metadataController,
      int maxPullAttempts,
      BiConsumer<CorePuller, CorePullStatus> pullEndNotification) {
    this(
        solrCore,
        zeroStoreClient,
        metadataCacheManager,
        metadataController,
        Instant.now(),
        0,
        maxPullAttempts,
        Instant.EPOCH,
        pullEndNotification);
  }

  /** Needed for the {@link CorePuller} to be used in a {@link DeduplicatingList}. */
  @Override
  public String getDedupeKey() {
    return this.getCoreName();
  }

  public static DeduplicatingList<String, CorePuller> getDeduplicatingList(int maxCapacity) {
    return new DeduplicatingList<>(maxCapacity, new CorePuller.CorePullerMerger());
  }

  public void incCurrentPullAttempts() {
    currentPullAttempt.incrementAndGet();
  }

  public int getCurrentPullAttempt() {
    return currentPullAttempt.get();
  }

  public int getMaxPullAttempts() {
    return maxPullAttempts;
  }

  private CorePuller newUpdatedPuller(
      Instant queuedTime, int attempts, int maxAttempts, Instant lastAttemptTime) {

    return new CorePuller(
        solrCore,
        zeroStoreClient,
        metadataCacheManager,
        metadataController,
        queuedTime,
        attempts,
        maxAttempts,
        lastAttemptTime,
        pullEndNotification);
  }

  /**
   * This method is only used in this class for now because the "re-enqueue with delay"
   * implementation is imperfect. Longer term, such a re-enqueue should be handled outside this
   * class.
   */
  public Instant getLastPullAttemptTime() {
    return lastPullAttemptTime;
  }

  public Instant getQueuedTime() {
    return this.queuedTime;
  }

  /**
   * Manages multiple retries calling {@link #lockAndPullCore} to pull a core from Zero store when
   * transient failures are encountered.
   */
  @SuppressWarnings("try")
  public CorePullStatus pullCoreWithRetries(
      boolean isLeaderPulling, long secondsToWaitPullLock, long retryDelay) throws ZeroException {
    MDCLoggingContext.setCore(getCore());
    try {
      if (skipPullIfShardAlreadySynced(isLeaderPulling)) return CorePullStatus.NOT_NEEDED;

      // Initialization needed if we don't pass in the loop even once (tests...). Then
      // CorePullStatus.FAILURE
      // being a transient error, the log will be correct that all retries exhausted.
      CorePullStatus pullStatus = CorePullStatus.FAILURE;

      while (getCurrentPullAttempt() < maxPullAttempts) {
        // Sleep a bit if we're retrying, to let the previous error disappear naturally
        try {
          sleepIfNeeded(retryDelay);
        } catch (InterruptedException ie) {
          // No retry if the wait is interrupted (no timeout here)
          Thread.currentThread().interrupt();
          String message = "Wait done before core pull got interrupted, likely shutdown?";
          log.error(message, ie);
          throw new ZeroException(message);
        }
        // Do the actual pull
        pullStatus = lockAndPullCore(isLeaderPulling, secondsToWaitPullLock);
        // Check the return code and throw exceptions for most errors
        incCurrentPullAttempts();
        if (pullStatus.isSuccess()) {
          if (log.isInfoEnabled()) {
            log.info(
                String.format(
                    Locale.ROOT,
                    "Pulling core for collection %s shard %s succeeded. Last status=%s attempts=%s",
                    getCollectionName(),
                    getShardName(),
                    pullStatus,
                    getCurrentPullAttempt()));
          }
          notifyPullEnd(pullStatus);
          return pullStatus;
        } else if (!pullStatus.isTransientError()) {
          break;
        }
        if (log.isInfoEnabled()) {
          log.info(
              String.format(
                  Locale.ROOT,
                  "Pulling core for collection %s shard %s failed with transient error. Retrying. Last status=%s attempts=%s",
                  getCollectionName(),
                  getShardName(),
                  pullStatus,
                  getCurrentPullAttempt()));
        }
      }
      // Give up if unrecoverable or attempts exhausted
      if (log.isWarnEnabled()) {
        log.warn(
            String.format(
                Locale.ROOT,
                "Pulling core for collection %s shard %s failed %s. Giving up. Last status=%s attempts=%s",
                getCollectionName(),
                getShardName(),
                pullStatus.isTransientError()
                    ? "after max attempts reached"
                    : "with non transient error",
                pullStatus,
                getCurrentPullAttempt()));
      }
      throw new ZeroException(pullStatus.name());
    } finally {
      MDCLoggingContext.clear();
    }
  }

  @VisibleForTesting
  void notifyPullEnd(CorePullStatus pullStatus) {
    // Tests count down on a latch to observe the end of the pull
    pullEndNotification.accept(this, pullStatus);
  }

  /**
   * Acquires the required locks and calls {@link #pullCoreFilesFromZeroStore(boolean)} to do the
   * actual pulling work. Also checks a few conditions to skip the pull completely when not needed.
   * No pull completion notification ({@link #notifyPullEnd(CorePullStatus)}) done from this method
   * because if there are retries, we'd only notify after multiple failures (or a success).
   */
  @SuppressWarnings("try")
  public CorePullStatus lockAndPullCore(boolean isLeaderPulling, long secondsToWaitPullLock) {
    if (skipPullIfShardAlreadySynced(isLeaderPulling)) return CorePullStatus.NOT_NEEDED;
    try (ZeroAccessLocks.NoThrowAutoCloseable ignore =
        metadataCacheManager
            .getOrCreateCoreMetadata(solrCore.getName())
            .getZeroAccessLocks()
            .acquirePullLock(secondsToWaitPullLock)) {
      if (skipPullIfShardHasDefaultSuffix(isLeaderPulling)) {
        return CorePullStatus.NOT_NEEDED;
      }
      return pullCoreFilesFromZeroStore(isLeaderPulling);
    } catch (ZeroLockException ex) {
      log.warn("Failed to acquire lock for pull", ex);
      return CorePullStatus.FAILED_TO_ACQUIRE_LOCK;
    } catch (ZeroException ex) {
      log.warn("Failure pulling core", ex);
      return CorePullStatus.FAILURE;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.warn("Pull interrupted", ex);
      return CorePullStatus.INTERRUPTED;
    }
  }

  private String getTaskDescription() {
    return String.format(
        Locale.ROOT,
        "(col=%s,shard=%s,name=%s,attempt=%d,thread=%s)",
        getCollectionName(),
        getShardName(),
        getCoreName(),
        getCurrentPullAttempt() + 1,
        Thread.currentThread().getName());
  }

  /**
   * This method is used to quickly ends core pulling if shard has never been updated since creation
   * Requires read lock to be taken otherwise fails
   */
  private boolean skipPullIfShardHasDefaultSuffix(boolean isLeaderPulling) throws ZeroException {
    ZeroMetadataVersion shardMetadataVersion = getShardMetadataVersion();
    if (metadataController.hasDefaultNodeSuffix(shardMetadataVersion)) {
      ZeroStoreShardMetadata emptyZeroStoreShardMetadata = new ZeroStoreShardMetadata();
      metadataCacheManager.updateCoreMetadata(
          solrCore.getName(), shardMetadataVersion, emptyZeroStoreShardMetadata, isLeaderPulling);
      return true;
    } else {
      return false;
    }
  }

  /**
   * This method is used to quickly end core pulling if server side and remote shard have the same
   * version
   */
  private boolean skipPullIfShardAlreadySynced(boolean isLeaderPulling) {
    MetadataCacheManager.MetadataCacheEntry coreMetadata =
        metadataCacheManager.getOrCreateCoreMetadata(solrCore.getName());
    ZeroMetadataVersion shardMetadataVersion = getShardMetadataVersion();
    // If leader is pulling, compare metadata to cache only if cache assumed up to date
    return (coreMetadata.isCacheLikelyUpToDate() || !isLeaderPulling)
        && coreMetadata.getMetadataVersion().equals(shardMetadataVersion);
  }

  /**
   * Method that sleeps for a while depending on the number of pull attempts already done (in order
   * not to retry pulling in short sequence and likely hit the same transient errors)
   *
   * @param retryDelay milliseconds to let elapse since previous pull attempt, by sleeping if needed
   */
  private void sleepIfNeeded(long retryDelay) throws InterruptedException {
    if (getCurrentPullAttempt() != 0) {
      long sleepTime =
          retryDelay - getLastPullAttemptTime().until(Instant.now(), ChronoUnit.MILLIS);
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
    }
  }

  /**
   * Pulls required files representing the core update from the Zero store and returns the success
   * or failure status. Pull lock ({@link ZeroAccessLocks#acquirePullLock(long)}) must be held.
   */
  private CorePullStatus pullCoreFilesFromZeroStore(boolean isLeaderPulling) throws ZeroException {
    if (log.isInfoEnabled()) {
      log.info("Initiating core pull {}", getTaskDescription());
    }

    ZeroMetadataVersion shardMetadataVersion = getShardMetadataVersion();

    MetadataCacheManager.ZeroCoreStage stage = null;
    ZeroStoreShardMetadata zeroMetadata;
    CorePullStatus corePullStatus = CorePullStatus.FAILURE;

    try {
      metadataCacheManager.recordState(solrCore, MetadataCacheManager.ZeroCoreStage.PULL_STARTED);

      // Get Zero store shard metadata
      ZeroFile.WithLocal localZeroFile =
          metadataController.newShardMetadataZeroFile(
              getCollectionName(), getShardName(), shardMetadataVersion.getMetadataSuffix());
      zeroMetadata = zeroStoreClient.pullShardMetadata(localZeroFile);

      // Handle callback
      if (zeroMetadata == null) {
        return CorePullStatus.ZERO_METADATA_MISSING;
      }

      // Diff local metadata against Zero store metadata. Given we're doing a pull, don't
      // need to reserve the commit point.
      // We do need to compute a directory hash to verify after pulling or before switching index
      // dirs that no local changes occurred concurrently
      LocalCoreMetadata localCoreMetadata = new LocalCoreMetadata(solrCore);
      localCoreMetadata.readMetadata(false, true);
      MetadataComparisonResult metadataComparisonResult =
          metadataController.diffMetadataforPull(localCoreMetadata, zeroMetadata);

      // If there is nothing to pull, we should report SUCCESS_EQUIVALENT and do nothing.
      if (!metadataComparisonResult.getFilesToPull().isEmpty()) {
        pullUpdateFromZero(metadataComparisonResult, true);
        corePullStatus = CorePullStatus.SUCCESS;
      } else if (getOpenTimeForSearcher().isBefore(getLastPullAttemptTime())) {
        // If the searcher's opened ts is less than the last time this task ran, we assume it failed
        // to open successfully
        log.warn(
            "Looks like a searcher was never opened after the last pull from Zero store, doing that now. {} ",
            getTaskDescription());
        openSearcher(true);
        corePullStatus = CorePullStatus.SUCCESS;
      } else {
        log.warn(
            "There are no files to pull and we don't need to re-open a new searcher, even though we do not match the version in zk? {}",
            getTaskDescription());
        corePullStatus = CorePullStatus.NOT_NEEDED;
      }

      metadataCacheManager.updateCoreMetadata(
          solrCore.getName(), shardMetadataVersion, zeroMetadata, isLeaderPulling);

    } catch (InterruptedException e) {
      corePullStatus = CorePullStatus.INTERRUPTED;
      stage = MetadataCacheManager.ZeroCoreStage.PULL_FAILED;
      throwZeroException(e, "Failed attempt to pull core");
    } catch (CorruptedCoreException e) {
      corePullStatus = CorePullStatus.FAILURE;
      stage = MetadataCacheManager.ZeroCoreStage.PULL_FAILED_WITH_CORRUPTION;
      throwZeroException(e, "Failed attempt to pull core");
    } catch (Exception e) {
      corePullStatus = CorePullStatus.FAILURE;
      throwZeroException(e, "Failed attempt to pull core");
    } finally {
      if (stage == null) {
        stage =
            corePullStatus.isSuccess()
                ? MetadataCacheManager.ZeroCoreStage.PULL_SUCCEEDED
                : MetadataCacheManager.ZeroCoreStage.PULL_FAILED;
      }
      metadataCacheManager.recordState(solrCore, stage);
    }
    return corePullStatus;
  }

  /**
   * We're doing here what replication does in {@link
   * org.apache.solr.handler.IndexFetcher#fetchLatestIndex(boolean, boolean)}.
   *
   * <p>This method will work in 2 cases:
   *
   * <ol>
   *   <li>Local core needs to fetch an update from Zero store
   *   <li>Local core did not exist (was created empty before calling this method) and is fetched
   *       from Zero store
   * </ol>
   *
   * @param waitForSearcher {@code true} if this call should wait until the index searcher is
   *     created (so that any query after the return from this method sees the new pulled content)
   *     or {@code false} if we request a new index searcher to be eventually created but do not
   *     wait for it to be created (a query following the return from this call might see the old
   *     core content).
   * @throws CorruptedCoreException If the local core index cannot be opened after pulling it from
   *     the Zero store.
   * @throws Exception We currently do not treat differently errors such as Zero store temporarily
   *     not available or network issues. We therefore consider all exceptions thrown by this method
   *     as a sign that it is durably not possible to pull the core from the Zero Store. TODO This
   *     has to be revisited at some point
   */
  void pullUpdateFromZero(
      MetadataComparisonResult metadataComparisonResult, boolean waitForSearcher) throws Exception {
    Instant startTime = Instant.now();
    boolean isSuccessful = false;
    FileTransferCounter counter = new FileTransferCounter();
    try {
      // if there is a conflict between local and Zero store contents we will move the core to a new
      // index directory
      boolean coreSwitchedToNewIndexDir = false;
      // Create temp directory (within the core local folder).
      // If we are moving index to a new directory because of conflict then this will be that new
      // directory.
      // Even if we are not moving to a newer directory we will first download files from the Zero
      // store into this temp directory.
      // Then we will move files from temp directory to index directory. This is to avoid leaving
      // a download half done in case of failure as well as to limit the time during which
      // we close then reopen the index writer to take into account the new files. In theory
      // nothing should be changing the local directory as we pull files from the Zero store,
      // but let's be defensive (we're checking further down that local dir hasn't changed
      // in the meantime).
      String tempIndexDirName = "index.pull." + System.nanoTime();
      String tempIndexDirPath = solrCore.getDataDir() + tempIndexDirName;
      Directory tempIndexDir = getDirectory(solrCore, tempIndexDirPath);
      try {
        String indexDirPath = solrCore.getIndexDir();
        Collection<ZeroFile.WithLocal> filesToDownload;
        if (metadataComparisonResult.isLocalConflictingWithZero()) {
          // This is an optimization to not download everything from Zero store if possible
          Directory indexDir = getDirectory(solrCore, indexDirPath);
          try {
            filesToDownload =
                initializeNewIndexDirWithLocallyAvailableFiles(
                    indexDir, tempIndexDir, metadataComparisonResult.getFilesToPull());
          } finally {
            solrCore.getDirectoryFactory().release(indexDir);
          }
        } else {
          filesToDownload = metadataComparisonResult.getFilesToPull();
        }

        pullZeroFiles(tempIndexDir, filesToDownload, counter);

        Directory indexDir = getDirectory(solrCore, indexDirPath);
        try {
          if (!metadataComparisonResult.isLocalConflictingWithZero()) {
            // TODO should we call solrCore.closeSearcher() here? IndexFetcher.fetchLatestIndex()
            //  does call it.
            // Close the index writer to stop changes to this core
            solrCore.getUpdateHandler().getSolrCoreState().closeIndexWriter(solrCore, true);
          }

          Exception thrownException = null;
          try {
            // Make sure Solr core directory content hasn't changed since we decided what we want
            // to pull from the Zero store
            if (!metadataComparisonResult.isSameDirectoryContent(indexDir)) {
              // Maybe return something less aggressive than throwing an exception? TBD once we
              // end up calling this method :)
              throw new Exception(
                  "Local Directory content "
                      + indexDirPath
                      + " has changed since core pull from Zero store started. Aborting pull.");
            }

            if (metadataComparisonResult.isLocalConflictingWithZero()) {
              // point index to the new directory.
              solrCore.modifyIndexProps(tempIndexDirName);
              coreSwitchedToNewIndexDir = true;
            } else {
              moveFilesFromTempToIndexDir(tempIndexDir, indexDir, metadataComparisonResult);
            }
          } catch (Exception e) {
            // Do not mask the exception in the finally block below.
            thrownException = e;
            throw e;
          } finally {
            try {
              openIndexWriter(
                  metadataComparisonResult.isLocalConflictingWithZero(), coreSwitchedToNewIndexDir);
            } catch (Exception e) {
              // We are not able to open an index writer on files we just wrote locally.
              // Consider the index is corrupted and wipe it, so it will be pulled again.
              wipeCorruptedIndex(indexDir);
              wipeCorruptedIndex(tempIndexDir);
              throw new CorruptedCoreException(
                  "Core " + getCoreName() + " is corrupted locally",
                  thrownException == null ? e : thrownException);
            }
          }
        } finally {
          try {
            if (coreSwitchedToNewIndexDir) {
              solrCore.getDirectoryFactory().doneWithDirectory(indexDir);
              solrCore.getDirectoryFactory().remove(indexDir);
            }
          } catch (Exception e) {
            log.warn("Cannot remove previous index directory {}", indexDir, e);
          } finally {
            solrCore.getDirectoryFactory().release(indexDir);
          }
        }
      } finally {
        try {
          if (!coreSwitchedToNewIndexDir) {
            solrCore.getDirectoryFactory().doneWithDirectory(tempIndexDir);
            solrCore.getDirectoryFactory().remove(tempIndexDir);
          }
        } catch (Exception e) {
          log.warn("Cannot remove temp directory {}", tempIndexDirPath, e);
        } finally {
          solrCore.getDirectoryFactory().release(tempIndexDir);
        }
      }
      try {
        openSearcher(waitForSearcher);
      } catch (Exception e) {
        // We are not able to open an index searcher on files we just wrote locally.
        // Consider the index is corrupted and wipe it, so it will be pulled again.
        Directory indexDir = getDirectory(solrCore, solrCore.getIndexDir());
        try {
          wipeCorruptedIndex(indexDir);
        } finally {
          solrCore.getDirectoryFactory().release(indexDir);
        }
        throw new CorruptedCoreException("Core " + getCoreName() + " is corrupted locally", e);
      }
      isSuccessful = true;
    } finally {
      if (log.isInfoEnabled()) {
        log.info(
            "PULL runTime={} bytesTransferred={} expectedFilesAffected={} actualFilesAffected={} isSuccessful={} "
                + "localGeneration={} zeroGeneration={}",
            startTime.until(Instant.now(), ChronoUnit.MILLIS),
            counter.getBytesTransferred(),
            counter.getExpectedFilesTransferred(),
            counter.getActualFilesTransferred(),
            isSuccessful,
            metadataComparisonResult.getLocalGeneration(),
            metadataComparisonResult.getDistantGeneration());
      }
    }
  }

  public static Directory getDirectory(SolrCore core, String path) throws IOException {
    return core.getDirectoryFactory()
        .get(path, DirectoryFactory.DirContext.DEFAULT, core.getSolrConfig().indexConfig.lockType);
  }

  @VisibleForTesting
  protected void pullZeroFiles(
      Directory tempIndexDir,
      Collection<ZeroFile.WithLocal> filesToDownload,
      FileTransferCounter counter)
      throws InterruptedException, ExecutionException {
    counter.setExpectedFilesTransferred(filesToDownload.size());

    CompletableFuture<?>[] futures = new CompletableFuture<?>[filesToDownload.size()];
    int i = 0;
    for (ZeroFile.WithLocal bf : filesToDownload) {
      futures[i++] = zeroStoreClient.pullFileAsync(tempIndexDir, bf);

      counter.incrementActualFilesTransferred();
      counter.incrementBytesTransferred(bf.getFileSize());
    }

    CompletableFuture.allOf(futures).get();
  }

  private Collection<ZeroFile.WithLocal> initializeNewIndexDirWithLocallyAvailableFiles(
      Directory indexDir, Directory newIndexDir, Collection<ZeroFile.WithLocal> filesToPull) {
    Collection<ZeroFile.WithLocal> filesToDownload = new HashSet<>();
    for (ZeroFile.WithLocal zeroFile : filesToPull) {
      assert zeroFile.isChecksumPresent();
      try {
        try (final IndexInput indexInput =
            indexDir.openInput(zeroFile.getSolrFileName(), IOContext.READONCE)) {
          if (indexInput.length() == zeroFile.getFileSize()
              && CodecUtil.retrieveChecksum(indexInput) == zeroFile.getChecksum()) {
            copyFileToDirectory(indexDir, zeroFile.getSolrFileName(), newIndexDir);
          } else {
            filesToDownload.add(zeroFile);
          }
        }
      } catch (Exception ex) {
        // Either file does not exist locally or copy failed, we will download from Zero store
        filesToDownload.add(zeroFile);
      }
    }
    return filesToDownload;
  }

  protected ZeroMetadataVersion getShardMetadataVersion() {
    return metadataController.readMetadataValue(getCollectionName(), getShardName());
  }

  @VisibleForTesting
  protected void openIndexWriter(boolean createNewIndexDir, boolean coreSwitchedToNewIndexDir)
      throws Exception {
    if (!createNewIndexDir) {
      // The closed index writer must be opened back
      // (in the finally block as stated in the javadoc of SolrCoreState.closeIndexWriter).
      solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(solrCore);
    } else if (coreSwitchedToNewIndexDir) {
      solrCore.getUpdateHandler().newIndexWriter(true);
    }
  }

  void openSearcher(boolean waitForSearcher) throws Exception {
    if (waitForSearcher) {
      // Open and register a new searcher, we don't need it but we wait for it to be open.
      @SuppressWarnings("unchecked")
      final Future<Void>[] waitSearcher = (Future<Void>[]) Array.newInstance(Future.class, 1);
      solrCore.getSearcher(true, false, waitSearcher, true);
      if (waitSearcher[0] == null) {
        throw new Exception(
            "Can't wait for index searcher to be created. Future queries might misbehave for core="
                + solrCore.getName());
      } else {
        waitSearcher[0].get();
      }
    } else {
      // Open and register a new searcher, but don't wait and we don't need it either.
      solrCore.getSearcher(true, false, null, true);
    }
  }

  /** Wipe the provided index directory. */
  private void wipeCorruptedIndex(Directory dir) {
    try {
      // Delete the files individually instead of calling Directory.remove().
      // The latter is not done immediately and may result in the files not being
      // removed before the next pull attempt.
      for (String file : dir.listAll()) {
        dir.deleteFile(file);
      }
    } catch (Exception e) {
      log.error("Cannot wipe the corrupted index in directory {}", dir, e);
    }
  }

  /** Copies {@code fileName} from {@code fromDir} to {@code toDir} */
  private void copyFileToDirectory(Directory fromDir, String fileName, Directory toDir)
      throws IOException {
    // TODO: Consider optimizing with org.apache.lucene.store.HardlinkCopyDirectoryWrapper
    toDir.copyFrom(fromDir, fileName, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
  }

  /** Moves {@code fileName} from {@code fromDir} to {@code toDir} */
  private void moveFileToDirectory(Directory fromDir, String fileName, Directory toDir)
      throws IOException {
    // We don't need to keep the original files so we move them over.
    // TODO: Consider optimizing with org.apache.lucene.store.HardlinkCopyDirectoryWrapper
    solrCore
        .getDirectoryFactory()
        .move(fromDir, toDir, fileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
  }

  private void moveFilesFromTempToIndexDir(
      Directory tmpIndexDir, Directory dir, MetadataComparisonResult metadataComparisonResult)
      throws IOException {
    if (metadataComparisonResult.getFilesToPull().isEmpty()) {
      return;
    }

    // Copy all files into the Solr directory
    // Move the segments_N file last once all other are ok.
    String segmentsN = null;
    for (ZeroFile.WithLocal bf : metadataComparisonResult.getFilesToPull()) {
      if (metadataController.isSegmentsNFilename(bf)) {
        assert segmentsN == null;
        segmentsN = bf.getSolrFileName();
      } else {
        // Copy all non segments_N files
        moveFileToDirectory(tmpIndexDir, bf.getSolrFileName(), dir);
      }
    }
    assert segmentsN != null;
    // Copy segments_N file. From this point on the local core might be accessed and is up to date
    // with Zero store content
    moveFileToDirectory(tmpIndexDir, segmentsN, dir);
  }

  /**
   * @return timestamp in millis when the core's searcher was opened, NOT wall-clock time but nano
   *     time, so that it can be compared against the last run timestamp of this job.
   */
  private Instant getOpenTimeForSearcher() throws IOException {
    return solrCore == null
        ? Instant.EPOCH
        : Instant.ofEpochMilli(
            TimeUnit.MILLISECONDS.convert(
                solrCore.withSearcher(SolrIndexSearcher::getOpenTimeStamp).getTime(),
                TimeUnit.NANOSECONDS));
  }

  public String getCoreName() {
    return solrCore.getName();
  }

  public String getShardName() {
    return solrCore.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  public String getCollectionName() {
    return solrCore.getCoreDescriptor().getCollectionName();
  }

  private void throwZeroException(Exception ex, String format, Object... args)
      throws ZeroException {
    throw new ZeroException(
        String.format(Locale.ROOT, format, args) + ": " + getTaskDescription(), ex);
  }

  private void throwZeroException(String format, Object... args) throws ZeroException {
    throw new ZeroException(String.format(Locale.ROOT, format, args) + ": " + getTaskDescription());
  }

  public SolrCore getCore() {
    return solrCore;
  }

  /** Needed for the {@link CorePuller} to be used in a {@link DeduplicatingList}. */
  static class CorePullerMerger implements DeduplicatingList.Merger<String, CorePuller> {
    /**
     * Given two tasks (that have not yet started executing!) that target the same shard (and would
     * basically do the same things were they both executed), returns a merged task that can replace
     * both and that retains the oldest enqueue time and the smallest number of attempts, so we
     * don't "lose" retries because of the merge yet we correctly report that tasks might have been
     * waiting for execution for a long while.
     *
     * @return a merged {@link CorePuller} that can replace the two tasks passed as parameters.
     */
    @Override
    public CorePuller merge(CorePuller puller1, CorePuller puller2) {
      // We allow more opportunities to try as the core is changed again by Solr...
      int mergedAttempts =
          Math.min(puller1.getCurrentPullAttempt(), puller2.getCurrentPullAttempt());
      int mergedMaxAttempts = Math.max(puller1.getMaxPullAttempts(), puller2.getMaxPullAttempts());

      // ...and base the delay computation on the time of last attempt.
      Instant mergedLastAttemptsTime =
          puller1.getLastPullAttemptTime().isBefore(puller2.getLastPullAttemptTime())
              ? puller2.getLastPullAttemptTime()
              : puller1.getLastPullAttemptTime();

      Instant minQueueTime =
          puller1.getQueuedTime().isBefore(puller2.getQueuedTime())
              ? puller1.getQueuedTime()
              : puller2.getQueuedTime();
      // We merge the tasks.
      return puller1.newUpdatedPuller(
          minQueueTime, mergedAttempts, mergedMaxAttempts, mergedLastAttemptsTime);
    }
  }
}
