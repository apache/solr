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
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.zero.exception.ZeroLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the features required for a ZERO core (replica) to do indexing. It brings
 * it up to date (if needed) before indexing starts and pushes the updated core after a successful
 * commit.
 */
public class ZeroCoreIndexingBatchProcessor implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Time indexing thread needs to wait to try acquiring pull lock before checking if someone else
   * has already done the pull.
   */
  public static final int SECONDS_TO_WAIT_PULL_LOCK = 5;

  /**
   * If an indexing thread is waiting on the indexing lock, then it has not performed local indexing
   * yet. If it is waiting, that means another thread is currently pulling index files for the core.
   * Other threads can be indexing concurrently so the longest wait time is proportional to the time
   * it takes to hydrate (load) a full core from Zero store. This can take time on saturated
   * clusters.
   */
  public static final int SECONDS_TO_WAIT_INDEXING_LOCK = 120;

  private final SolrCore core;
  private final String collectionName;
  private final String shardName;
  private IndexingBatchState state;
  private final ZeroStoreManager zeroStoreManager;
  private final ZeroAccessLocks locks;

  private ZeroAccessLocks.NoThrowAutoCloseable coreIndexingLock;
  private final SolrQueryResponse response;

  public ZeroCoreIndexingBatchProcessor(
      SolrCore core, ClusterState clusterState, SolrQueryResponse response) {
    this.core = core;
    this.response = response;
    CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
    collectionName = cloudDescriptor.getCollectionName();
    shardName = cloudDescriptor.getShardId();

    CoreContainer coreContainer = core.getCoreContainer();
    zeroStoreManager = coreContainer.getZeroStoreManager();
    locks = zeroStoreManager.getZeroAccessLocks(core.getName());

    DocCollection collection = clusterState.getCollection(collectionName);
    if (!collection.isZeroIndex()) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, collectionName + " is not a Zero collection.");
    }

    Slice shard = collection.getSlicesMap().get(shardName);
    if (shard == null) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Indexing batch received for an unknown shard,"
              + " collection="
              + collectionName
              + " shard="
              + shardName
              + " core="
              + core.getName());
    }

    if (!Slice.State.ACTIVE.equals(shard.getState())) {
      // This happens when we buffer updates for a sub shard.
      // ZERO replica should eventually stop supporting buffered updates and then this should
      // become a real exception
      log.warn(
          "Processing an indexing batch for a non-active shard, collection={} shard={} core={}",
          collectionName,
          shardName,
          core.getName());

      // TODO it would have been better to have instead the code below, but then ZERO
      //  replica split tests fail. Something to look into.
      // In ZeroStoreUpdateProcessor.rejectIfConstructing() an indexing batch would be rejected
      // if a shard splits, so seeing activity here might be due to post split actions?

      //      // ZERO replica do not support buffered updates (not until transaction log is updated
      //      // to go to Zero store, as the Solr nodes are considered stateless)
      //      throw new SolrException(
      //          ErrorCode.SERVER_ERROR,
      //          "Processing an indexing batch for a non-active shard, collection="
      //              + collectionName
      //              + " shard="
      //              + shardName
      //              + " core="
      //              + core.getName());
    }

    state = IndexingBatchState.NOT_STARTED;
  }

  /**
   * Should be called whenever a document is about to be added/deleted from the ZERO core. If it is
   * the first doc of the core, this method will mark the start of an indexing batch and bring a
   * stale ZERO core upto date by pulling from the Zero store.
   */
  public void addOrDeleteGoingToBeIndexedLocally() {
    // Following logic is built on the assumption that one particular instance of this processor
    // will solely be consumed by a single thread. And all the documents of indexing batch will be
    // processed by this one instance.
    try {
      MDCLoggingContext.setCore(core);

      if (IndexingBatchState.NOT_STARTED.equals(state)) {
        startIndexingBatch();
      } else if (IndexingBatchState.STARTED.equals(state)) {
        // do nothing, we only use this method to start an indexing batch once
      } else if (IndexingBatchState.COMMITTED.equals(state)) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Why are we adding/deleting a doc through an already committed indexing batch?"
                + " collection="
                + collectionName
                + " shard="
                + shardName
                + " core="
                + core.getName());
      } else {
        throwUnknownStateError();
      }
    } finally {
      MDCLoggingContext.clear();
    }
  }

  @VisibleForTesting
  protected void startIndexingBatch() {
    // Following pull logic should only run once before the first add/delete of an indexing batch is
    // processed by this processor
    assert IndexingBatchState.NOT_STARTED.equals(state);

    if (coreIndexingLock != null) {
      String err = "Indexing thread already has an indexing lock but indexing has not yet started!";
      if (locks.getIndexingHoldCount() != 0) {
        err += " The current thread also holds a lock on it when it shouldn't!";
      }
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          err
              + " collection="
              + collectionName
              + " shard="
              + shardName
              + " core="
              + core.getName());
    }

    try {
      zeroStoreManager.setIndexingBatchReceived(core);
      state = IndexingBatchState.STARTED;
      CorePullStatus pullStatus = zeroStoreManager.pullCoreFromZeroStore(core);
      response.addToLog("pull.done", pullStatus == CorePullStatus.SUCCESS);
      coreIndexingLock = locks.acquireIndexingLock(getIndexingLockTimeout());
      zeroStoreManager.setIndexingStarted(core);
    } catch (ZeroLockException ex) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Indexing thread timed out trying to acquire the pull read lock in "
              + getIndexingLockTimeout()
              + " seconds"
              + " collection="
              + collectionName
              + " shard="
              + shardName
              + " core="
              + core.getName());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Indexing thread interrupted while trying to acquire pull read lock."
              + " collection="
              + collectionName
              + " shard="
              + shardName
              + " core="
              + core.getName(),
          ex);
    }
  }

  /**
   * Should be called after the ZERO core has successfully hard committed locally. This method will
   * push the updated core to the Zero store. If there was no local add/delete of a document for
   * this processor then the push will be skipped.
   */
  public void hardCommitCompletedLocally() {
    finishIndexingBatch();
  }

  protected void finishIndexingBatch() {
    String coreName = core.getName();
    if (IndexingBatchState.NOT_STARTED.equals(state)) {
      // This is an isolated commit since we've not seen any added/deleted doc
      // ZERO replicas currently require (and force) hard commits after each batch, isolated commits
      // are therefore ignored
      state = IndexingBatchState.COMMITTED;
      if (log.isInfoEnabled()) {
        log.info(
            "Isolated commit encountered for a ZERO replica, ignoring writing to Zero store."
                + " collection={} shard={} core={}",
            collectionName,
            shardName,
            coreName);
      }
    } else if (IndexingBatchState.STARTED.equals(state)) {
      if (locks.getIndexingHoldCount() != 1) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Indexing lock hold count is not 1 (is "
                + locks.getIndexingHoldCount()
                + ") collection="
                + collectionName
                + " shard="
                + shardName
                + " core="
                + coreName);
      }

      zeroStoreManager.setIndexingFinished(core);
      state = IndexingBatchState.COMMITTED;
      CorePusherExecutionInfo pushExecutionInfo = zeroStoreManager.pushCoreToZeroStore(core);
      addToLog(pushExecutionInfo);

    } else if (IndexingBatchState.COMMITTED.equals(state)) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Why are we committing an already committed indexing batch?"
              + " collection="
              + collectionName
              + " shard="
              + shardName
              + " core="
              + coreName);
    } else {
      throwUnknownStateError();
    }
  }

  private void addToLog(CorePusherExecutionInfo pushExecutionInfo) {
    response.addToLog("push.done", pushExecutionInfo.hasPushed());
    response.addToLog("push.zeroGeneration", pushExecutionInfo.getZeroGeneration());
    response.addToLog("push.metadataSuffix", pushExecutionInfo.getMetadataSuffix());
    response.addToLog("push.lockWaiTimeMs", pushExecutionInfo.getPushLockWaitTimeMs());
    response.addToLog("push.actualPushTimeMs", pushExecutionInfo.getActualPushTimeMs());
    response.addToLog("push.metadataUpdateTimeMs", pushExecutionInfo.getMetadataUpdateTimeMs());
    response.addToLog("push.totalTimeMs", pushExecutionInfo.getTotalTimeMs());
    response.addToLog("push.numFiles", pushExecutionInfo.getNumFilesPushed());
    response.addToLog("push.sizeBytes", pushExecutionInfo.getSizeBytesPushed());
    response.addToLog("push.numFilesToDelete", pushExecutionInfo.getNumFilesToDelete());
  }

  private void throwUnknownStateError() {
    throw new SolrException(
        ErrorCode.SERVER_ERROR,
        "Programmer's error, unknown IndexingBatchState"
            + state
            + " collection="
            + collectionName
            + " shard="
            + shardName
            + " core="
            + core.getName());
  }

  @VisibleForTesting
  protected int getIndexingLockTimeout() {
    return SECONDS_TO_WAIT_INDEXING_LOCK;
  }

  @Override
  public void close() {
    if (!IndexingBatchState.NOT_STARTED.equals(state)) {
      try {
        zeroStoreManager.setIndexingFinished(core);
      } catch (Exception ex) {
        log.error("Error recording the finish of a ZERO core indexing batch", ex);
      }
    }
    // TODO would be nice to decide in a more deterministic way if the lock has to be released
    if (locks.getIndexingHoldCount() > 0) {
      // release read lock
      coreIndexingLock.close();
      if (locks.getIndexingHoldCount() != 0) {
        log.error(
            "Indexing lock of a ZERO core was unlocked but this thread still holds it: {}",
            locks.getIndexingHoldCount());
      }
    }
  }

  private enum IndexingBatchState {
    NOT_STARTED,
    STARTED,
    COMMITTED
  }
}
