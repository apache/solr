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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.PeerSyncWithLeader;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class may change in future and customisations are not supported between versions in terms of
 * API or back compat behaviour.
 *
 * @lucene.experimental
 */
public class RecoveryStrategy implements Runnable, Closeable {

  public static class Builder implements NamedListInitializedPlugin {
    private NamedList<?> args;

    @Override
    public void init(NamedList<?> args) {
      this.args = args;
    }

    // this should only be used from SolrCoreState
    public RecoveryStrategy create(
        CoreContainer cc, CoreDescriptor cd, RecoveryStrategy.RecoveryListener recoveryListener) {
      final RecoveryStrategy recoveryStrategy = newRecoveryStrategy(cc, cd, recoveryListener);
      SolrPluginUtils.invokeSetters(recoveryStrategy, args);
      return recoveryStrategy;
    }

    protected RecoveryStrategy newRecoveryStrategy(
        CoreContainer cc, CoreDescriptor cd, RecoveryStrategy.RecoveryListener recoveryListener) {
      return new RecoveryStrategy(cc, cd, recoveryListener);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private int waitForUpdatesWithStaleStatePauseMilliSeconds =
      Integer.getInteger("solr.cloud.wait-for-updates-with-stale-state-pause", 2500);
  private int maxRetries = 500;
  private int startingRecoveryDelayMilliSeconds = 2000;
  private ReplicationHandler replicationHandlerDoingFetch;

  public static interface RecoveryListener {
    public void recovered();

    public void failed();
  }

  private volatile boolean close = false;

  private RecoveryListener recoveryListener;
  private ZkController zkController;
  private String baseUrl;
  private String coreZkNodeName;
  private ZkStateReader zkStateReader;
  private volatile String coreName;
  private int retries;
  private boolean recoveringAfterStartup;
  private CoreContainer cc;
  private volatile FutureTask<NamedList<Object>> prevSendPreRecoveryHttpUriRequest;
  private final Replica.Type replicaType;

  private CoreDescriptor coreDescriptor;

  protected RecoveryStrategy(
      CoreContainer cc, CoreDescriptor cd, RecoveryListener recoveryListener) {
    this.cc = cc;
    this.coreName = cd.getName();
    this.recoveryListener = recoveryListener;
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();
    coreZkNodeName = cd.getCloudDescriptor().getCoreNodeName();
    replicaType = cd.getCloudDescriptor().getReplicaType();
  }

  public final int getWaitForUpdatesWithStaleStatePauseMilliSeconds() {
    return waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  public final void setWaitForUpdatesWithStaleStatePauseMilliSeconds(
      int waitForUpdatesWithStaleStatePauseMilliSeconds) {
    this.waitForUpdatesWithStaleStatePauseMilliSeconds =
        waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  public final int getMaxRetries() {
    return maxRetries;
  }

  public final void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public final int getStartingRecoveryDelayMilliSeconds() {
    return startingRecoveryDelayMilliSeconds;
  }

  public final void setStartingRecoveryDelayMilliSeconds(int startingRecoveryDelayMilliSeconds) {
    this.startingRecoveryDelayMilliSeconds = startingRecoveryDelayMilliSeconds;
  }

  public final boolean getRecoveringAfterStartup() {
    return recoveringAfterStartup;
  }

  public final void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  private Http2SolrClient.Builder recoverySolrClientBuilder(String baseUrl, String leaderCoreName) {
    final UpdateShardHandlerConfig cfg = cc.getConfig().getUpdateShardHandlerConfig();
    return new Http2SolrClient.Builder(baseUrl)
        .withDefaultCollection(leaderCoreName)
        .withHttpClient(cc.getUpdateShardHandler().getRecoveryOnlyHttpClient());
  }

  // make sure any threads stop retrying
  @Override
  public final void close() {
    close = true;
    cancelPrepRecoveryCmd();
    log.warn("Stopping recovery for core=[{}] coreNodeName=[{}]", coreName, coreZkNodeName);
    abortIndexFetchingIfNecessary(replicationHandlerDoingFetch);
  }

  private void abortIndexFetchingIfNecessary(ReplicationHandler fetcher) {
    // a 'null' ReplicationHandler indicates that no full-recovery/index-fetching is ongoing to
    // abort.
    if (fetcher != null) {
      fetcher.abortFetch();
    }
  }

  private final void recoveryFailed(final ZkController zkController, final CoreDescriptor cd)
      throws Exception {
    log.error("Recovery failed - I give up.");
    try {
      zkController.publish(cd, Replica.State.RECOVERY_FAILED);
    } finally {
      close();
      recoveryListener.failed();
    }
  }

  /**
   * This method may change in future and customisations are not supported between versions in terms
   * of API or back compat behaviour.
   *
   * @lucene.experimental
   */
  protected String getReplicateLeaderUrl(ZkNodeProps leaderprops) {
    return new ZkCoreNodeProps(leaderprops).getCoreUrl();
  }

  private void replicate(String nodeName, SolrCore core, ZkNodeProps leaderprops)
      throws SolrServerException, IOException {

    final String leaderBaseUrl = URLUtil.extractBaseUrl(getReplicateLeaderUrl(leaderprops));
    final String leaderCore = URLUtil.extractCoreFromCoreUrl(getReplicateLeaderUrl(leaderprops));

    log.info("Attempting to replicate from core [{}] on node [{}].", leaderCore, leaderBaseUrl);

    // send commit if replica could be a leader
    if (replicaType.leaderEligible) {
      commitOnLeader(leaderBaseUrl, leaderCore);
    }

    // use rep handler directly, so we can do this sync rather than async
    SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
    ReplicationHandler replicationHandler = (ReplicationHandler) handler;

    if (replicationHandler == null) {
      throw new SolrException(
          ErrorCode.SERVICE_UNAVAILABLE,
          "Skipping recovery, no " + ReplicationHandler.PATH + " handler found");
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(ReplicationHandler.LEADER_URL, URLUtil.buildCoreUrl(leaderBaseUrl, leaderCore));
    solrParams.set(
        ReplicationHandler.SKIP_COMMIT_ON_LEADER_VERSION_ZERO, replicaType == Replica.Type.TLOG);

    if (isClosed()) return; // we check closed on return
    try {
      // Stash the RH so the fetch can be aborted if RecoveryStrategy is closed mid-fetch
      replicationHandlerDoingFetch = replicationHandler;
      boolean success = replicationHandler.doFetch(solrParams, false).getSuccessful();
      if (!success) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replication for recovery failed.");
      }
    } finally {
      replicationHandlerDoingFetch = null;
    }

    // solrcloud_debug
    if (log.isDebugEnabled()) {
      try {
        RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
        SolrIndexSearcher searcher = searchHolder.get();
        Directory dir =
            core.getDirectoryFactory().get(core.getIndexDir(), DirContext.META_DATA, null);
        try {
          final IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
          if (log.isDebugEnabled()) {
            log.debug(
                "{} replicated {} from core {} on node {} gen: {} data: {} index: {} newIndex: {} files: {}",
                core.getCoreContainer().getZkController().getNodeName(),
                searcher.count(new MatchAllDocsQuery()),
                leaderCore,
                leaderBaseUrl,
                (null == commit ? "null" : commit.getGeneration()),
                core.getDataDir(),
                core.getIndexDir(),
                core.getNewIndexDir(),
                Arrays.asList(dir.listAll()));
          }
        } finally {
          core.getDirectoryFactory().release(dir);
          searchHolder.decref();
        }
      } catch (Exception e) {
        log.debug("Error in solrcloud_debug block", e);
      }
    }
  }

  private void commitOnLeader(String leaderBaseUrl, String coreName)
      throws SolrServerException, IOException {
    try (SolrClient client = recoverySolrClientBuilder(leaderBaseUrl, coreName).build()) {
      UpdateRequest ureq = new UpdateRequest();
      // ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      // ureq.getParams().set(UpdateParams.OPEN_SEARCHER, onlyLeaderIndexes);
      // Why do we need to open searcher if "onlyLeaderIndexes"?
      ureq.getParams().set(UpdateParams.OPEN_SEARCHER, false);
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true).process(client);
    }
  }

  @Override
  public final void run() {

    // set request info for logging
    try (SolrCore core = cc.getCore(coreName)) {

      if (core == null) {
        log.error("SolrCore not found - cannot recover: {}", coreName);
        return;
      }

      log.info("Starting recovery process. recoveringAfterStartup={}", recoveringAfterStartup);

      final RTimer timer = new RTimer();
      try {
        doRecovery(core);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("interrupted", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (Exception e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }

      if (log.isInfoEnabled()) {
        log.info(
            "Finished recovery process. recoveringAfterStartup={} msTimeTaken={}",
            recoveringAfterStartup,
            timer.getTime());
      }
    }
  }

  public final void doRecovery(SolrCore core) throws Exception {
    // we can lose our core descriptor, so store it now
    this.coreDescriptor = core.getCoreDescriptor();

    if (this.coreDescriptor.getCloudDescriptor().getReplicaType().requireTransactionLog) {
      doSyncOrReplicateRecovery(core);
    } else {
      doReplicateOnlyRecovery(core);
    }
  }

  private void doReplicateOnlyRecovery(SolrCore core) {
    final RTimer timer = new RTimer();
    boolean successfulRecovery = false;

    // if (core.getUpdateHandler().getUpdateLog() != null) {
    // log.error("'replicate-only' recovery strategy should only be used if no update
    // logs are present, but
    // this core has one: "
    // + core.getUpdateHandler().getUpdateLog());
    // return;
    // }
    // don't use interruption or it will close channels though
    while (!successfulRecovery && !Thread.currentThread().isInterrupted() && !isClosed()) {
      try {
        CloudDescriptor cloudDesc = this.coreDescriptor.getCloudDescriptor();
        Replica leader =
            zkStateReader.getLeaderRetry(cloudDesc.getCollectionName(), cloudDesc.getShardId());
        final String leaderUrl = leader.getCoreUrl();
        final String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);

        // TODO: We can probably delete most of this code if we say this strategy can only be used
        // for pull replicas
        boolean isLeader = ourUrl.equals(leaderUrl);
        if (isLeader && !cloudDesc.isLeader()) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Cloud state still says we are leader.");
        }
        if (cloudDesc.isLeader()) {
          assert cloudDesc.getReplicaType().leaderEligible;
          // we are now the leader - no one else must have been suitable
          log.warn("We have not yet recovered - but we are now the leader!");
          log.info("Finished recovery process.");
          zkController.publish(this.coreDescriptor, Replica.State.ACTIVE);
          return;
        }

        if (log.isInfoEnabled()) {
          log.info(
              "Publishing state of core [{}] as recovering, leader is [{}] and I am [{}]",
              core.getName(),
              leaderUrl,
              ourUrl);
        }
        zkController.publish(this.coreDescriptor, Replica.State.RECOVERING);

        if (isClosed()) {
          if (log.isInfoEnabled()) {
            log.info("Recovery for core {} has been closed", core.getName());
          }
          break;
        }
        log.info("Starting Replication Recovery.");

        try {
          if (replicaType.replicateFromLeader) {
            log.info("Stopping background replicate from leader process");
            zkController.stopReplicationFromLeader(coreName);
          }
          replicate(zkController.getNodeName(), core, leader);

          if (isClosed()) {
            if (log.isInfoEnabled()) {
              log.info("Recovery for core {} has been closed", core.getName());
            }
            break;
          }

          log.info("Replication Recovery was successful.");
          successfulRecovery = true;
        } catch (Exception e) {
          log.error("Error while trying to recover", e);
        }

      } catch (Exception e) {
        log.error("Error while trying to recover. core={}", coreName, e);
      } finally {
        if (successfulRecovery) {
          if (replicaType.replicateFromLeader) {
            log.info("Restarting background replicate from leader process");
            zkController.startReplicationFromLeader(coreName, false);
          }
          log.info("Registering as Active after recovery.");
          try {
            zkController.publish(this.coreDescriptor, Replica.State.ACTIVE);
          } catch (Exception e) {
            log.error("Could not publish as ACTIVE after successful recovery", e);
            successfulRecovery = false;
          }

          if (successfulRecovery) {
            close = true;
            recoveryListener.recovered();
          }
        }
      }

      if (!successfulRecovery) {
        if (waitBetweenRecoveries(core.getName())) break;
      }
    }
    // We skip core.seedVersionBuckets(); We don't have a transaction log
    if (log.isInfoEnabled()) {
      log.info(
          "Finished recovery process, successful=[{}] msTimeTaken={}",
          successfulRecovery,
          timer.getTime());
    }
  }

  /**
   * @return true if we have reached max attempts or should stop recovering for some other reason
   */
  private boolean waitBetweenRecoveries(String coreName) {
    // lets pause for a moment and we need to try again...
    // TODO: we don't want to retry for some problems?
    // Or do a fall off retry...
    try {
      if (isClosed()) {
        log.info("Recovery for core {} has been closed", coreName);
        return true;
      }

      log.error("Recovery failed - trying again... ({})", retries);

      retries++;
      if (retries >= maxRetries) {
        log.error("Recovery failed - max retries exceeded ({}).", retries);
        try {
          recoveryFailed(zkController, this.coreDescriptor);
        } catch (Exception e) {
          log.error("Could not publish that recovery failed", e);
        }
        return true;
      }
    } catch (Exception e) {
      log.error("An error has occurred during recovery", e);
    }

    try {
      // Wait an exponential interval between retries, start at 4 seconds and work up to a minute.
      // Meanwhile we will check in 2s sub-intervals to see if we've been closed
      // Maximum loop count is 30 because we never want to wait longer than a minute (2s * 30 = 1m)
      long loopCount = retries < 5 ? Math.round(Math.pow(2, retries)) : 30;
      if (log.isInfoEnabled()) {
        log.info(
            "Wait [{}] seconds before trying to recover again (attempt={})",
            TimeUnit.MILLISECONDS.toSeconds(loopCount * startingRecoveryDelayMilliSeconds),
            retries);
      }
      for (int i = 0; i < loopCount; i++) {
        if (isClosed()) {
          log.info("Recovery for core {} has been closed", coreName);
          break; // check if someone closed us
        }
        Thread.sleep(startingRecoveryDelayMilliSeconds);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Recovery was interrupted.", e);
      close = true;
    }
    return false;
  }

  // TODO: perhaps make this grab a new core each time through the loop to handle core reloads?
  public final void doSyncOrReplicateRecovery(SolrCore core) throws Exception {
    final RTimer timer = new RTimer();
    boolean successfulRecovery = false;

    UpdateLog ulog;
    ulog = core.getUpdateHandler().getUpdateLog();
    if (ulog == null) {
      log.error("No UpdateLog found - cannot recover.");
      recoveryFailed(zkController, this.coreDescriptor);
      return;
    }

    // we temporary ignore peersync for tlog replicas
    boolean firstTime = replicaType != Replica.Type.TLOG;

    List<Long> recentVersions;
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      recentVersions = recentUpdates.getVersions(ulog.getNumRecordsToKeep());
    } catch (Exception e) {
      log.error("Corrupt tlog - ignoring.", e);
      recentVersions = new ArrayList<>(0);
    }

    List<Long> startingVersions = ulog.getStartingVersions();

    if (startingVersions != null && recoveringAfterStartup) {
      try {
        int oldIdx = 0; // index of the start of the old list in the current list
        long firstStartingVersion = startingVersions.size() > 0 ? startingVersions.get(0) : 0;

        for (; oldIdx < recentVersions.size(); oldIdx++) {
          if (recentVersions.get(oldIdx) == firstStartingVersion) break;
        }

        if (oldIdx > 0) {
          log.info("Found new versions added after startup: num=[{}]", oldIdx);
          if (log.isInfoEnabled()) {
            log.info(
                "currentVersions size={} range=[{} to {}]",
                recentVersions.size(),
                recentVersions.get(0),
                recentVersions.get(recentVersions.size() - 1));
          }
        }

        if (startingVersions.isEmpty()) {
          log.info("startupVersions is empty");
        } else {
          if (log.isInfoEnabled()) {
            log.info(
                "startupVersions size={} range=[{} to {}]",
                startingVersions.size(),
                startingVersions.get(0),
                startingVersions.get(startingVersions.size() - 1));
          }
        }
      } catch (Exception e) {
        log.error("Error getting recent versions.", e);
        recentVersions = new ArrayList<>(0);
      }
    }

    if (recoveringAfterStartup) {
      // if we're recovering after startup (i.e. we have been down), then we need to know what the
      // last versions were when we went down. We may have received updates since then.
      recentVersions = startingVersions;
      try {
        if (ulog.existOldBufferLog()) {
          // this means we were previously doing a full index replication that probably didn't
          // complete and buffering updates in the meantime.
          log.info(
              "Looks like a previous replication recovery did not complete - skipping peer sync.");
          firstTime = false; // skip peersync
        }
      } catch (Exception e) {
        log.error("Error trying to get ulog starting operation.", e);
        firstTime = false; // skip peersync
      }
    }

    if (replicaType.replicateFromLeader) {
      zkController.stopReplicationFromLeader(coreName);
    }

    final String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
    Future<RecoveryInfo> replayFuture = null;
    // don't use interruption or it will close channels though
    while (!successfulRecovery && !Thread.currentThread().isInterrupted() && !isClosed()) {
      try {
        CloudDescriptor cloudDesc = this.coreDescriptor.getCloudDescriptor();
        final Replica leader = pingLeader(ourUrl, this.coreDescriptor, true);
        if (isClosed()) {
          log.info("RecoveryStrategy has been closed");
          break;
        }

        boolean isLeader = leader.getCoreUrl().equals(ourUrl);
        if (isLeader && !cloudDesc.isLeader()) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Cloud state still says we are leader.");
        }
        if (cloudDesc.isLeader()) {
          // we are now the leader - no one else must have been suitable
          log.warn("We have not yet recovered - but we are now the leader!");
          log.info("Finished recovery process.");
          zkController.publish(this.coreDescriptor, Replica.State.ACTIVE);
          return;
        }

        log.info("Begin buffering updates. core=[{}]", coreName);
        // recalling buffer updates will drop the old buffer tlog
        ulog.bufferUpdates();

        if (log.isInfoEnabled()) {
          log.info(
              "Publishing state of core [{}] as recovering, leader is [{}] and I am [{}]",
              core.getName(),
              leader.getCoreUrl(),
              ourUrl);
        }
        zkController.publish(this.coreDescriptor, Replica.State.RECOVERING);

        final Slice slice =
            zkStateReader
                .getClusterState()
                .getCollection(cloudDesc.getCollectionName())
                .getSlice(cloudDesc.getShardId());

        cancelPrepRecoveryCmd();

        if (isClosed()) {
          log.info("RecoveryStrategy has been closed");
          break;
        }

        sendPrepRecoveryCmd(leader.getBaseUrl(), leader.getCoreName(), slice);

        if (isClosed()) {
          log.info("RecoveryStrategy has been closed");
          break;
        }

        // we wait a bit so that any updates on the leader
        // that started before they saw recovering state
        // are sure to have finished (see SOLR-7141 for
        // discussion around current value)
        // TODO since SOLR-11216, we probably won't need this
        try {
          Thread.sleep(waitForUpdatesWithStaleStatePauseMilliSeconds);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        // first thing we just try to sync
        if (firstTime) {
          firstTime = false; // only try sync the first time through the loop
          if (log.isInfoEnabled()) {
            log.info(
                "Attempting to PeerSync from [{}] - recoveringAfterStartup=[{}]",
                leader.getCoreUrl(),
                recoveringAfterStartup);
          }
          // System.out.println("Attempting to PeerSync from " + leaderUrl
          // + " i am:" + zkController.getNodeName());
          boolean syncSuccess;
          try (PeerSyncWithLeader peerSyncWithLeader =
              new PeerSyncWithLeader(core, leader.getCoreUrl(), ulog.getNumRecordsToKeep())) {
            syncSuccess = peerSyncWithLeader.sync(recentVersions).isSuccess();
          }
          if (syncSuccess) {
            SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
            // force open a new searcher
            core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
            req.close();
            log.info("PeerSync stage of recovery was successful.");

            // solrcloud_debug
            cloudDebugLog(core, "synced");

            log.info("Replaying updates buffered during PeerSync.");
            replayFuture = replay(core);

            // sync success
            successfulRecovery = true;
            break;
          }

          log.info("PeerSync Recovery was not successful - trying replication.");
        }

        if (isClosed()) {
          log.info("RecoveryStrategy has been closed");
          break;
        }

        log.info("Starting Replication Recovery.");

        try {

          replicate(zkController.getNodeName(), core, leader);

          if (isClosed()) {
            log.info("RecoveryStrategy has been closed");
            break;
          }

          replayFuture = replay(core);

          if (isClosed()) {
            log.info("RecoveryStrategy has been closed");
            break;
          }

          log.info("Replication Recovery was successful.");
          successfulRecovery = true;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Recovery was interrupted", e);
          close = true;
        } catch (Exception e) {
          log.error("Error while trying to recover", e);
        }

      } catch (Exception e) {
        log.error("Error while trying to recover. core={}", coreName, e);
      } finally {
        if (successfulRecovery) {
          log.info("Registering as Active after recovery.");
          try {
            if (replicaType.replicateFromLeader) {
              zkController.startReplicationFromLeader(coreName, true);
            }
            zkController.publish(this.coreDescriptor, Replica.State.ACTIVE);
          } catch (Exception e) {
            log.error("Could not publish as ACTIVE after successful recovery", e);
            successfulRecovery = false;
          }

          if (successfulRecovery) {
            close = true;
            recoveryListener.recovered();
          }
        }
      }

      if (!successfulRecovery) {
        if (waitBetweenRecoveries(core.getName())) break;
      }
    }

    if (log.isInfoEnabled()) {
      log.info(
          "Finished recovery process, successful=[{}] msTimeTaken={}",
          successfulRecovery,
          timer.getTime());
    }
  }

  /**
   * Make sure we can connect to the shard leader as currently defined in ZK
   *
   * @param ourUrl if the leader url is the same as our url, we will skip trying to connect
   * @return the leader replica, or null if closed
   */
  private Replica pingLeader(String ourUrl, CoreDescriptor coreDesc, boolean mayPutReplicaAsDown)
      throws Exception {
    int numTried = 0;
    while (true) {
      CloudDescriptor cloudDesc = coreDesc.getCloudDescriptor();
      DocCollection docCollection =
          zkStateReader.getClusterState().getCollection(cloudDesc.getCollectionName());
      if (!isClosed()
          && mayPutReplicaAsDown
          && numTried == 1
          && docCollection.getReplica(coreDesc.getCloudDescriptor().getCoreNodeName()).getState()
              == Replica.State.ACTIVE) {
        // this operation may take a long time, by putting replica into DOWN state, client won't
        // query this replica
        zkController.publish(coreDesc, Replica.State.DOWN);
      }
      numTried++;

      if (isClosed()) {
        return null;
      }

      Replica leaderReplica = null;
      try {
        leaderReplica =
            zkStateReader.getLeaderRetry(cloudDesc.getCollectionName(), cloudDesc.getShardId());
      } catch (SolrException e) {
        // ignore
      }
      if (leaderReplica == null) {
        Thread.sleep(500);
        continue;
      }

      if (leaderReplica.getCoreUrl().equals(ourUrl)) {
        return leaderReplica;
      }

      try (SolrClient httpSolrClient =
          recoverySolrClientBuilder(leaderReplica.getBaseUrl(), leaderReplica.getCoreName())
              .build()) {
        httpSolrClient.ping();
        return leaderReplica;
      } catch (IOException e) {
        log.error("Failed to connect leader {} on recovery, try again", leaderReplica.getBaseUrl());
        Thread.sleep(500);
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          log.error(
              "Failed to connect leader {} on recovery, try again", leaderReplica.getBaseUrl());
          Thread.sleep(500);
        } else {
          return leaderReplica;
        }
      }
    }
  }

  public static Runnable testing_beforeReplayBufferingUpdates;

  private final Future<RecoveryInfo> replay(SolrCore core)
      throws InterruptedException, ExecutionException {
    if (testing_beforeReplayBufferingUpdates != null) {
      testing_beforeReplayBufferingUpdates.run();
    }
    if (replicaType == Replica.Type.TLOG) {
      // roll over all updates during buffering to new tlog, make RTG available
      SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
      core.getUpdateHandler()
          .getUpdateLog()
          .copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
      req.close();
      return null;
    }
    Future<RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().applyBufferedUpdates();
    if (future == null) {
      // no replay needed\
      log.info("No replay needed.");
    } else {
      log.info("Replaying buffered documents.");
      // wait for replay
      RecoveryInfo report = future.get();
      if (report.failed) {
        log.error("Replay failed");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      }
    }

    // the index may ahead of the tlog's caches after recovery, by calling this tlog's caches will
    // be purged
    core.getUpdateHandler().getUpdateLog().openRealtimeSearcher();

    // solrcloud_debug
    cloudDebugLog(core, "replayed");

    return future;
  }

  private final void cloudDebugLog(SolrCore core, String op) {
    if (!log.isDebugEnabled()) {
      return;
    }
    try {
      RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
      SolrIndexSearcher searcher = searchHolder.get();
      try {
        final int totalHits = searcher.count(new MatchAllDocsQuery());
        final String nodeName = core.getCoreContainer().getZkController().getNodeName();
        log.debug("[{}] {} [{} total hits]", nodeName, op, totalHits);
      } finally {
        searchHolder.decref();
      }
    } catch (Exception e) {
      log.debug("Error in solrcloud_debug block", e);
    }
  }

  public final boolean isClosed() {
    return close || cc.isShutDown();
  }

  private final void sendPrepRecoveryCmd(String leaderBaseUrl, String leaderCoreName, Slice slice)
      throws SolrServerException, IOException, InterruptedException, ExecutionException {
    WaitForState prepCmd = new WaitForState();
    prepCmd.setCoreName(leaderCoreName);
    prepCmd.setNodeName(zkController.getNodeName());
    prepCmd.setCoreNodeName(coreZkNodeName);
    prepCmd.setState(Replica.State.RECOVERING);
    prepCmd.setCheckLive(true);
    prepCmd.setOnlyIfLeader(true);
    final Slice.State state = slice.getState();
    if (state != Slice.State.CONSTRUCTION
        && state != Slice.State.RECOVERY
        && state != Slice.State.RECOVERY_FAILED) {
      prepCmd.setOnlyIfLeaderActive(true);
    }

    int conflictWaitMs = zkController.getLeaderConflictResolveWait();
    // timeout after 5 seconds more than the max timeout (conflictWait + 3 seconds) on the server
    // side
    int readTimeout =
        conflictWaitMs
            + Integer.parseInt(System.getProperty("prepRecoveryReadTimeoutExtraWait", "8000"));
    try (SolrClient client =
        recoverySolrClientBuilder(
                leaderBaseUrl,
                null) // leader core omitted since client only used for 'admin' request
            .withIdleTimeout(readTimeout, TimeUnit.MILLISECONDS)
            .build()) {
      prevSendPreRecoveryHttpUriRequest = new FutureTask<>(() -> client.request(prepCmd));
      log.info("Sending prep recovery command to [{}]; [{}]", leaderBaseUrl, prepCmd);
      prevSendPreRecoveryHttpUriRequest.run();
    }
  }

  private void cancelPrepRecoveryCmd() {
    Optional.ofNullable(prevSendPreRecoveryHttpUriRequest).ifPresent(req -> req.cancel(true));
  }
}
