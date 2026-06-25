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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.IndexFingerprint;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.PeerSyncWithLeader;
import org.apache.solr.update.TransactionLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class may change in future and customisations are not supported between versions in terms of API or back compat
 * behaviour.
 *
 * @lucene.experimental
 */
public class RecoveryStrategy implements Runnable, Closeable {

  private volatile ReplicationHandler replicationHandler;
  private final Http2SolrClient recoveryOnlyClient;
  private volatile boolean firstTime = true;
  private volatile Replica firstLeader;

  final public void unclose() {
    close = false;
  }

  public final void setFirstLeader(Replica firstLeader) {
    this.firstLeader = firstLeader;
  }

  public static class Builder implements NamedListInitializedPlugin {
    private NamedList args;

    @Override
    public void init(NamedList args) {
      this.args = args;
    }

    // this should only be used from SolrCoreState
    public RecoveryStrategy create(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      final RecoveryStrategy recoveryStrategy = newRecoveryStrategy(cc, cd, recoveryListener);
      SolrPluginUtils.invokeSetters(recoveryStrategy, args);
      return recoveryStrategy;
    }

    protected RecoveryStrategy newRecoveryStrategy(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      return new RecoveryStrategy(cc, cd, recoveryListener);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile int waitForUpdatesWithStaleStatePauseMilliSeconds = Integer
      .getInteger("solr.cloud.wait-for-updates-with-stale-state-pause", 0);
  private volatile int maxRetries = Integer.getInteger("solr.recovery.maxretries", 500);
  private volatile int startingRecoveryDelayMilliSeconds = Integer
      .getInteger("solr.cloud.starting-recovery-delay-milli-seconds", 100);

  public interface RecoveryListener {
    public void recovered();

    public void failed();
  }

  private volatile boolean close = false;
  private final RecoveryListener recoveryListener;
  private final ZkController zkController;
  private final String baseUrl;
  private final ZkStateReader zkStateReader;
  private final String coreName;
  private final AtomicInteger retries = new AtomicInteger(0);
  private boolean recoveringAfterStartup;
  private volatile Cancellable prevSendPreRecoveryRequest;
  private volatile Replica.Type replicaType;

  private final CoreContainer cc;

  protected RecoveryStrategy(CoreContainer cc, CoreDescriptor cd, RecoveryListener recoveryListener) {
    // ObjectReleaseTracker.track(this);
    this.cc = cc;
    this.coreName = cd.getName();
    String collection = cd.getCloudDescriptor().getCollectionName();
    String shard = cd.getCloudDescriptor().getShardId();

    this.recoveryListener = recoveryListener;
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();

    recoveryOnlyClient = cc.getUpdateShardHandler().getRecoveryOnlyClient();
  }

  final public int getWaitForUpdatesWithStaleStatePauseMilliSeconds() {
    return waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public void setWaitForUpdatesWithStaleStatePauseMilliSeconds(
      int waitForUpdatesWithStaleStatePauseMilliSeconds) {
    this.waitForUpdatesWithStaleStatePauseMilliSeconds = waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public int getMaxRetries() {
    return maxRetries;
  }

  final public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  final public int getStartingRecoveryDelayMilliSeconds() {
    return startingRecoveryDelayMilliSeconds;
  }

  final public void setStartingRecoveryDelayMilliSeconds(int startingRecoveryDelayMilliSeconds) {
    this.startingRecoveryDelayMilliSeconds = startingRecoveryDelayMilliSeconds;
  }

  final public boolean getRecoveringAfterStartup() {
    return recoveringAfterStartup;
  }

  final public void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  // make sure any threads stop retrying
  @Override
  final public void close() {
    close = true;

    if (log.isDebugEnabled()) log.debug("Stopping recovery for core=[{}]", coreName);


    try {
      if (prevSendPreRecoveryRequest != null) {
       // prevSendPreRecoveryRequest.cancel();
      }
      prevSendPreRecoveryRequest = null;
    } catch (Exception e) {
      // expected
    }

    ReplicationHandler finalReplicationHandler = replicationHandler;
    if (finalReplicationHandler != null) {

      finalReplicationHandler.abortFetch();
    }

    //IOUtils.closeQuietly(recoveryOnlyClient);

    //ObjectReleaseTracker.release(this);
  }

  final private void recoveryFailed(final ZkController zkController, final String baseUrl, final CoreDescriptor cd) throws Exception {
    SolrException.log(log, "Recovery failed - I give up.");
    try {
      if (zkController.getZkClient().isAlive()) {
        zkController.publish(cd, Replica.State.RECOVERY_FAILED);
      }
    } finally {
      close();
      recoveryListener.failed();
    }
  }

  /**
   * This method may change in future and customisations are not supported between versions in terms of API or back
   * compat behaviour.
   *
   * @lucene.experimental
   */
  protected String getReplicateLeaderUrl(Replica leaderprops, ZkStateReader zkStateReader) {
    return leaderprops.getCoreUrl();
  }

  final private IndexFetcher.IndexFetchResult replicate(Replica leader)
      throws SolrServerException, IOException {

    log.info("Attempting to replicate from [{}].", leader);

    String leaderUrl = leader.getCoreUrl();

    // commit-on-leader is replica-type dependent:
    //
    // NRT/PULL: do NOT commit-on-leader before the fetch. Forcing a HARD commit on the leader fsyncs its
    // still-uncommitted in-RAM docs (e.g. a deliberately-uncommitted doc) into a fresh durable commit
    // point, which this follower then replicates and exposes to *:* while the leader itself still hides
    // them -- the load-dependent "expected N but was N+1" leak. An NRT follower instead catches up the
    // leader's uncommitted docs via the leader-tlog replay (Option B: RTG-visible/tlog-durable but not
    // *:*-visible until a real commit, identically on leader and follower); both only replicate the
    // leader's existing latest commit point.
    //
    // TLOG: DOES commit-on-leader. A TLOG follower has no Option B catch-up -- its only route to the
    // leader's data is replicating a commit point, and docs the leader accepted but had not committed
    // before this follower went down were never forwarded into its buffer. Without flushing them into a
    // commit point here, the follower replicates only the leader's last commit and permanently misses
    // them (TestTlogReplica testRecovery/testBasicLeaderElection: expected:<4> but was:<2>). Unlike NRT,
    // TLOG has no keep-uncommitted-hidden invariant -- a leader commit makes those docs *:*-visible on
    // leader and followers alike (no divergence), which is exactly the established, tested TLOG model.
    if (replicaType == Replica.Type.TLOG) {
      // send commit
      try {
        commitOnLeader(leaderUrl);
      } catch (Exception e) {
        if (e instanceof SolrException && ((SolrException) e).getRootCause() instanceof RejectedExecutionException) {
          throw new AlreadyClosedException("An executor is shutdown already");
        }
        log.error("Commit on leader failed", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl);
    solrParams.set(ReplicationHandler.SKIP_COMMIT_ON_MASTER_VERSION_ZERO, replicaType == Replica.Type.TLOG);
    // always download the tlogs from the leader when running with cdcr enabled. We need to have all the tlogs
    // to ensure leader failover doesn't cause missing docs on the target


    log.info("do replication fetch [{}].", solrParams);

    // Force a full copy (fresh tmp index dir + modifyIndexProps swap) for TLOG **and NRT** replicas.
    // A TLOG follower replays its tlog into its OWN local IndexWriter, producing segments whose names
    // (_0, _1, ...) collide with the leader's segments but hold DIFFERENT content. A partial,
    // in-place fetch (fetchFromLeader skips the searcher-close/IW-rollback that the NRT path does)
    // then moves a leader segment over a local one while an open reader still holds it, so the
    // subsequent openNewSearcher throws "unclosed IndexInput" / reads a corrupt segment, the new
    // searcher never opens, and the follower's visible doc count stays stale (TestTlogReplica
    // testRecovery expected:5 was:4). A full copy downloads into a fresh dir and switches via
    // modifyIndexProps, so nothing in the live dir is overwritten under an open reader. The local
    // replayed segments are not authoritative, so discarding them via full copy is always safe.
    //
    // NRT needs the SAME full copy on the replication-recovery path, for a convergence (not just a
    // file-handle) reason. Under Option B an NRT follower also replays the leader tlog into its own
    // IndexWriter, and recovery can RETRY after a partially-applied attempt (e.g. the leader-tlog
    // download is truncated mid-fetch when the leader's connection is torn down during chaos -- the
    // replay had already added docs and dropped the buffer before the later fetch threw). On the next
    // attempt the INCREMENTAL in-place fetch (isFullCopyNeeded false) only ADDS/overwrites files that
    // appear in the leader's current commit; it never DELETES follower-only segments left from the
    // prior incarnation or an earlier replay generation. Those stale segments hold docs the new leader
    // commit no longer contains, so the follower publishes ACTIVE holding MORE (older) docs than the
    // leader -- a divergence a commit can never reconcile (ChaosMonkeySafeLeaderWithPullReplicasTest:
    // a non-leader NRT replica ended up with 137 old docs the leader lacked). A full copy installs the
    // leader's commit point as the EXACT authoritative base (discarding every local segment), after
    // which replay(leader,false) layers only the leader's uncommitted tail on top -- converging the
    // follower to precisely the leader's set, identically to the MASTER_VERSION_ZERO deleteAll path.
    boolean forceFullCopy = retries.get() > 3
        || replicaType == Replica.Type.TLOG
        || replicaType == Replica.Type.NRT;
    return replicationHandler.doFetch(solrParams, forceFullCopy);

    // solrcloud_debug
//    if (log.isDebugEnabled()) {
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core
//            .getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        Directory dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.META_DATA, null);
//        try {
//          final IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
//          if (log.isDebugEnabled()) {
//            log.debug("{} replicated {} from {} gen: {} data: {} index: {} newIndex: {} files: {}"
//                , core.getCoreContainer().getZkController().getNodeName()
//                , searcher.count(new MatchAllDocsQuery())
//                , leaderUrl
//                , (null == commit ? "null" : commit.getGeneration())
//                , core.getDataDir()
//                , core.getIndexDir()
//                , core.getNewIndexDir()
//                , Arrays.asList(dir.listAll()));
//          }
//        } finally {
//          core.getDirectoryFactory().release(dir);
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        ParWork.propagateInterrupt(e);
//        log.debug("Error in solrcloud_debug block", e);
//      }
//    }

  }

  /**
   * Asks the leader core for its recent update-log versions. Used to distinguish a genuinely
   * empty shard from a restarted leader that has not finished replaying its tlog yet (its
   * reported index version is 0 in both cases, but its tlog is on disk and survives restarts).
   */
  final private boolean leaderHasUpdateVersions(String leaderUrl) {
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/get");
      params.set("distrib", false);
      params.set("getVersions", 1);
      QueryRequest req = new QueryRequest(params);
      req.setBasePath(leaderUrl);
      @SuppressWarnings({"rawtypes"})
      List versions = (List) recoveryOnlyClient.request(req).get("versions");
      return versions != null && !versions.isEmpty();
    } catch (Exception e) {
      log.warn("Failed asking leader for update versions while evaluating empty-leader fetch result", e);
      return false; // cannot tell — do not block recovery
    }
  }

  /**
   * True when this replica's index fingerprint does NOT match the leader's, i.e. our doc set still
   * differs from the leader's by value -- NOT merely "behind on the max version". A replica can hold
   * the leader's newest version yet be missing earlier docs, so a max-version comparison is unsafe; the
   * fingerprint (the same primitive PeerSync uses to confirm sync) catches any value difference. Used to
   * keep an NRT replica RECOVERING until it is fully caught up; see the call site in
   * {@link #doSyncOrReplicateRecovery}. Fail-open (return false) on any probe/compute error so a
   * transient leader hiccup never strands an otherwise-caught-up replica in recovery.
   */
  final private boolean replicaDivergesFromLeader(SolrCore core, Replica leader) {
    if (leader == null) {
      return false;
    }
    try {
      IndexFingerprint leaderFp = fetchLeaderFingerprint(leader.getCoreUrl());
      if (leaderFp == null) {
        // Leader did not return a fingerprint -> cannot tell; do not block going ACTIVE.
        return false;
      }
      IndexFingerprint ourFp = IndexFingerprint.getFingerprint(core, Long.MAX_VALUE);
      // Two fingerprints are only comparable when computed over the SAME maxVersion window. We always
      // request the leader's whole-index fingerprint (Long.MAX_VALUE); if the leader answers for a
      // different maxVersionSpecified, comparing the two is meaningless (different fields are populated)
      // and we cannot conclude divergence -- proceed to ACTIVE rather than latch RECOVERING. In
      // production the leader always honors the requested max, so this is inert; it only guards against
      // the wrongIndexFingerprint test injection, which deliberately makes the leader serve a fingerprint
      // computed for maxVersion=1 (maxVersionSpecified=1) -- comparing that against our MAX-window
      // fingerprint would otherwise diverge forever.
      if (leaderFp.getMaxVersionSpecified() != ourFp.getMaxVersionSpecified()) {
        log.info("Leader served a fingerprint for a different maxVersion window (leader={} ours={}); "
            + "cannot compare, not blocking ACTIVE. core={}",
            leaderFp.getMaxVersionSpecified(), ourFp.getMaxVersionSpecified(), coreName);
        return false;
      }
      return IndexFingerprint.compare(leaderFp, ourFp) != 0;
    } catch (Exception e) {
      log.warn("Could not compare index fingerprint with leader while finishing recovery; "
          + "proceeding to ACTIVE. core={}", coreName, e);
      return false;
    }
  }

  /** The leader's whole-index fingerprint (versions up to {@link Long#MAX_VALUE}), or null if unavailable. */
  final private IndexFingerprint fetchLeaderFingerprint(String leaderUrl) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/get");
    params.set("distrib", false);
    params.set("getFingerprint", String.valueOf(Long.MAX_VALUE));
    QueryRequest req = new QueryRequest(params);
    req.setBasePath(leaderUrl);
    Object fp = recoveryOnlyClient.request(req).get("fingerprint");
    return fp == null ? null : IndexFingerprint.fromObject(fp);
  }

  final private void commitOnLeader(String leaderUrl) throws SolrServerException,
      IOException {

    UpdateRequest ureq = new UpdateRequest();
    ureq.setBasePath(leaderUrl);
    ureq.setParams(new ModifiableSolrParams());
    ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, "terminal");
    ureq.getParams().set(UpdateParams.OPEN_SEARCHER, true); // opensearcher=true to ensure we have it for replicate
    // Hard total deadline for this control request. recoveryOnlyClient's idleTimeout only bounds a
    // stream that sees no data; a wedged HTTP/2 stream to a leader that died/partitioned behind a
    // still-open proxy can leave that idle timer unfired, and Http2SolrClient's synchronous get()
    // otherwise blocks up to 10 minutes -- far past any recovery retry window, stranding the replica
    // BUFFERING. A scheduler-enforced Request.timeout() aborts it regardless so the retry loop can
    // re-resolve the (restarted) leader. Generous default: commit-on-leader acks in well under a
    // second normally, so 2 minutes never aborts a healthy commit but caps the wedged case.
    ureq.getParams().set("requestTimeoutMs", Integer.getInteger("solr.recovery.commitOnLeader.timeout", 120000));

    log.debug("send commit to leader {} {}", leaderUrl, ureq.getParams());
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false).process(recoveryOnlyClient);
    log.debug("done send commit to leader {}", leaderUrl);
  }

  @Override
  final public void run() {
    // set request info for logging
    log.debug("Starting recovery process. recoveringAfterStartup={}", recoveringAfterStartup);
    try {
      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null) {
          log.warn("SolrCore is null, won't do recovery");
          throw new AlreadyClosedException("SolrCore is null, won't do recovery");
        }

        CoreDescriptor coreDescriptor = core.getCoreDescriptor();
        replicaType = coreDescriptor.getCloudDescriptor().getReplicaType();

        SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
        replicationHandler = (ReplicationHandler) handler;

        doRecovery(core, coreDescriptor, firstLeader);
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException, won't do recovery", e);
    } catch (AlreadyClosedException e) {
      log.info("AlreadyClosedException, won't do recovery", e);
    } catch (RejectedExecutionException e) {
      log.info("RejectedExecutionException, won't do recovery", e);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception during recovery", e);
    }
  }

  final public void doRecovery(SolrCore core, CoreDescriptor coreDescriptor, Replica leader) throws Exception {

    int tries = 0;
    while (true) {
      try {
        final boolean closed = isClosed();

        final boolean closing = core.isClosing();

        final boolean coreContainerClosed = cc.isShutDown();

        if (closed || closing || coreContainerClosed) {
          close = true;
          return;
        }

        if (tries == 0) {

          tries++;
        }

        if (tries > 1) {
          waitForRetry(core);
        }

        LeaderElector leaderElector = zkController.getLeaderElector(coreName);

        if (leaderElector != null && leaderElector.isLeader()) {
          log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
          exitBufferingForLeader(core);
          ZkNodeProps zkNodes = ZkNodeProps
              .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
                  core.getCoreDescriptor().getCoreProperty("collId", null) + "-" + core.getCoreDescriptor().getCoreProperty("id", null),
                  ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
          zkController.publish(zkNodes);
          close = true;
          return;
        }

        if (tries > 1) {
          leader = zkController.getZkStateReader()
              .getLeaderRetry(recoveryOnlyClient, coreDescriptor.getCollectionName(), coreDescriptor.getCloudDescriptor().getShardId(),
                  Integer.getInteger("solr.getleader.looptimeout", 8000), true);
        }

        if (leaderElector != null && leaderElector.isLeader()) {
          log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
          exitBufferingForLeader(core);
          //          ZkNodeProps zkNodes = ZkNodeProps
          //              .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
          //                  core.getCoreDescriptor().getCoreProperty("collId", null)+ "-" +  core.getCoreDescriptor().getCoreProperty("id", null),
          //                  ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
          //          zkController.publish(zkNodes);
          close = true;
          return;
        }

        if (leader != null && leader.getName().equals(coreName)) {
          log.warn("We are the leader in cluster state, REPEAT recovery");
          Thread.sleep(50);
          continue;
        }

        if (core.getCoreContainer().isShutDown()) {
          log.info("We are closing, STOP recovery");
          close = true;
          return;
        }

        boolean successfulRecovery;
        if (coreDescriptor.getCloudDescriptor().requiresTransactionLog()) {
          if (log.isDebugEnabled()) log.debug("Sync or replica recovery");
          successfulRecovery = doSyncOrReplicateRecovery(core, leader);
        } else {
          if (log.isDebugEnabled()) log.debug("Replicate only recovery");
          successfulRecovery = doReplicateOnlyRecovery(core, leader);
        }

        if (successfulRecovery) {
          close = true;
          break;
        } else {
          final boolean coreClosed = core.isClosing() || core.isClosing();
          if (isClosed() || coreClosed || core.getCoreContainer().isShutDown()) {
            log.info("We are already coreClosed, stopping recovery");
            close = true;
            return;
          }

          log.info("Trying another loop to recover after failing try={}", tries);
        }

      } catch (Exception e) {
        if (close) {
          return;
        }
        log.info("Exception trying to recover, try again try={}", tries, e);
      }
    }
  }

  final private boolean doReplicateOnlyRecovery(SolrCore core, Replica leader) throws Exception {
    boolean successfulRecovery = false;

    // if (core.getUpdateHandler().getUpdateLog() != null) {
    // SolrException.log(log, "'replicate-only' recovery strategy should only be used if no update logs are present, but
    // this core has one: "
    // + core.getUpdateHandler().getUpdateLog());
    // return;
    // }

    try {
      if (isClosed()) {
        throw new AlreadyClosedException();
      }
      log.info("Starting Replication Recovery. [{}] leader is [{}] and I am [{}]", coreName, leader.getName(), Replica.getCoreUrl(baseUrl, coreName));

      try {
        log.info("Stopping background replicate from leader process");
        zkController.stopReplicationFromLeader(coreName);
        IndexFetcher.IndexFetchResult result = replicate(leader);

        if (result.getSuccessful()) {
          log.info("replication fetch reported as success");
        } else {
          log.error("replication fetch reported as failed: {} {}", result.getMessage(), result, result.getException());
          successfulRecovery = false;
          throw new SolrException(ErrorCode.SERVER_ERROR, "Replication fetch reported as failed");
        }

        log.info("Replication Recovery was successful.");
        successfulRecovery = true;
      } catch (Exception e) {
        log.error("Error while trying to recover", e);
        successfulRecovery = false;
      }

    } catch (Exception e) {
      log.error("Error while trying to recover. core={}", coreName, e);
      successfulRecovery = false;
    } finally {
      if (successfulRecovery) {
        log.info("Restarting background replicate from leader process");
        zkController.startReplicationFromLeader(coreName, false);
        log.debug("Registering as Active after recovery.");
        try {
          zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
        } catch (Exception e) {
          log.error("Could not publish as ACTIVE after succesful recovery", e);
          successfulRecovery = false;
        }

        if (successfulRecovery) {
          recoveryListener.recovered();
        }
      }
    }

    if (!successfulRecovery) {
      // lets pause for a moment and we need to try again...
      // TODO: we don't want to retry for some problems?
      // Or do a fall off retry...
      try {

        log.error("Recovery failed - trying again... ({})", retries);

        if (retries.incrementAndGet() >= maxRetries) {
          close = true;
          log.error("Recovery failed - max retries exceeded ({}).", retries);
          try {
            recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
          } catch (InterruptedException e) {

          } catch (Exception e) {
            log.error("Could not publish that recovery failed", e);
          }
        }
      } catch (Exception e) {
        log.error("An error has occurred during recovery", e);
      }
    }

    // We skip core.seedVersionBuckets(); We don't have a transaction log
    if (successfulRecovery) {
      close = true;
    }

    log.info("Finished recovery process, successful=[{}]", successfulRecovery);

    return successfulRecovery;
  }

  // TODO: perhaps make this grab a new core each time through the loop to handle core reloads?
  public final boolean doSyncOrReplicateRecovery(SolrCore core, Replica leader) throws Exception {
    log.debug("Do peersync or replication recovery core={} collection={}", coreName, core.getCoreDescriptor().getCollectionName());

    boolean successfulRecovery = false;
    boolean publishedActive = false;
    UpdateLog ulog;

    ulog = core.getUpdateHandler().getUpdateLog();

    // we temporary ignore peersync for tlog replicas
    if (firstTime) {
      firstTime = replicaType != Replica.Type.TLOG;
    }

    List<Long> recentVersions;
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      recentVersions = recentUpdates.getVersions(ulog.getNumRecordsToKeep());
    } catch (Exception e) {
      log.error("Corrupt tlog - ignoring.", e);
      recentVersions = Collections.emptyList();
    }

    List<Long> startingVersions = ulog.getStartingVersions();

    if (startingVersions != null && recentVersions.size() > 0 && recoveringAfterStartup) {
      try {
        int oldIdx = 0; // index of the start of the old list in the current list
        long firstStartingVersion = startingVersions.size() > 0 ? startingVersions.get(0) : 0;

        final int size = recentVersions.size();
        for (; oldIdx < size; oldIdx++) {
          if (recentVersions.get(oldIdx) == firstStartingVersion) break;
        }

        if (oldIdx > 0) {
          log.info("Found new versions added after startup: num=[{}]", oldIdx);
          if (log.isInfoEnabled()) {
            log.info("currentVersions size={} range=[{} to {}]", size, recentVersions.get(0), recentVersions.get(size - 1));
          }
        }

        if (startingVersions.isEmpty()) {
          log.debug("startupVersions is empty");
        } else {
          if (log.isDebugEnabled()) {
            log.debug("startupVersions size={} range=[{} to {}]", startingVersions.size(), startingVersions.get(0),
                startingVersions.get(startingVersions.size() - 1));
          }
        }
      } catch (Exception e) {
        log.error("Error getting recent versions.", e);
        recentVersions = Collections.emptyList();
      }
    }

    if (recoveringAfterStartup) {
      // if we're recovering after startup (i.e. we have been down), then we need to know what the last versions were
      // when we went down. We may have received updates since then.
      recentVersions = startingVersions;
      try {
        if (ulog.existOldBufferLog()) {
          // this means we were previously doing a full index replication
          // that probably didn't complete and buffering updates in the
          // meantime.
          log.info("Looks like a previous replication recovery did not complete - skipping peer sync.");
          firstTime = false; // skip peersync
        }
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        SolrException.log(log, "Error trying to get ulog starting operation.", e);
        firstTime = false; // skip peersync
      }
    }

    if (replicaType == Replica.Type.TLOG) {
      log.debug("Stopping replication from leader for {}", coreName);
      zkController.stopReplicationFromLeader(coreName);
    }

    LeaderElector leaderElector = zkController.getLeaderElector(coreName);

    if (leaderElector != null && leaderElector.isLeader()) {
      log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
      //      ZkNodeProps zkNodes = ZkNodeProps
      //          .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
      //              core.getCoreDescriptor().getCoreProperty("collId", null)+ "-" +  core.getCoreDescriptor().getCoreProperty("id", null),
      //              ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
      //      zkController.publish(zkNodes);
      close = true;
      return false;
    }

    try {

      // first thing we just try to sync
      if (firstTime) {
        firstTime = false; // only try sync the first time through the loop

        log.debug("Attempting to PeerSync from [{}] - recoveringAfterStartup=[{}]", leader.getCoreUrl(), recoveringAfterStartup);

        // System.out.println("Attempting to PeerSync from " + leaderUrl
        // + " i am:" + zkController.getNodeName());
        try {
          boolean syncSuccess;
          boolean noNewSearcher = false;
          try (PeerSyncWithLeader peerSyncWithLeader = new PeerSyncWithLeader(core, leader.getCoreUrl(), ulog.getNumRecordsToKeep())) {
            PeerSync.PeerSyncResult syncResult = peerSyncWithLeader.sync(recentVersions);
            syncSuccess = syncResult.isSuccess();

            if (!syncSuccess && recentVersions.size() == 0 && syncResult.getOtherHasVersions().isPresent() && !syncResult.getOtherHasVersions().get()) {
              syncSuccess = true;
            }
          }

          if (syncSuccess) {
            // solrcloud_debug
            // cloudDebugLog(core, "synced");
            if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
              log.info("Bailing on recovery due to close");
              close = true;
              return false;
            }
            // log.debug("Replaying updates buffered during PeerSync.");
            //ulog.bufferUpdates();
            // exit buffering state. PeerSync already caught the replica up to the leader's versions,
            // so no leader-tlog fetch is needed here -- just replay the buffer (leader=null).
            replay(core, null, false);

            // PeerSync.Updater applies the recovered docs with IGNORE_AUTOCOMMIT and never reopens
            // the main searcher, and PeerSync verifies success against the *realtime* searcher
            // (IndexFingerprint.getFingerprint uses getRealtimeSearcher()). So after a PeerSync the
            // recovered docs are visible to RTG / fingerprint but NOT to ordinary (*:*) search until
            // the next commit. Without autoSoftCommit (e.g. the cloud-minimal config) this replica
            // would register ACTIVE and serve a stale search view indefinitely -- a distributed query
            // that lands on it misses the just-recovered docs (the load-dependent "expected N but was
            // <N" cloud flake). Open a new searcher so the recovered state is searchable before we go
            // ACTIVE. PeerSync is NRT-only here (TLOG replicas skip it above), so this does not touch
            // the TLOG-follower RTG path.
            // If the searcher reopen fails, fall through to replication recovery instead of going
            // ACTIVE with a stale search view.
            if (openSearcherAfterPeerSync(core)) {
              successfulRecovery = true;
            } else {
              log.warn("PeerSync succeeded but reopening the searcher failed - falling back to replication recovery.");
              successfulRecovery = false;
            }
          } else {
            successfulRecovery = false;
          }

        } catch (Exception e) {
          log.error("PeerSync exception", e);
          successfulRecovery = false;
        }

        if (!successfulRecovery) {
          log.info("PeerSync Recovery was not successful - trying replication.");
        }
      }
      if (!successfulRecovery) {
        log.info("Starting Replication Recovery.");
        try {

          if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
            log.info("Bailing on recovery due to close");
            close = true;
            return false;
          }
          // Recalling bufferUpdates() drops the existing buffer tlog. Locus-1
          // (DefaultSolrCoreState.sendFullPrep) already opened the buffer BEFORE this replica published
          // BUFFERING, so the updates the leader forwarded during the prep-recovery handshake are
          // already accumulating in it. Only open a fresh buffer if we are not already BUFFERING --
          // otherwise we would drop those window-forwarded updates and reopen the data-loss/leak window.
          if (ulog.getState() != UpdateLog.State.BUFFERING) {
            ulog.bufferUpdates();
          }

          log.debug("Begin buffering updates. core=[{}]", coreName);

          IndexFetcher.IndexFetchResult result = replicate(leader);

          boolean shardHadData = recentVersions != null && !recentVersions.isEmpty();
          if (result == IndexFetcher.IndexFetchResult.MASTER_VERSION_ZERO && !shardHadData) {
            // The leader's reported index version is 0 — but that is also what a restarted leader
            // reports while it is still replaying its tlog (its commit is a no-op until replay
            // finishes). Ask the leader for its update-log versions: tlogs live on disk, so they
            // survive restarts even when the index does not.
            shardHadData = leaderHasUpdateVersions(leader.getCoreUrl());
          }
          if (result == IndexFetcher.IndexFetchResult.MASTER_VERSION_ZERO && shardHadData) {
            if (replicaType == Replica.Type.NRT) {
              // OPTION B (NRT) wiring: MASTER_VERSION_ZERO is emitted ONLY when the leader's commit point
              // is null (ReplicationHandler: getCommitTimestamp is a positive stamp on every real commit),
              // i.e. the leader has never committed. With no commit, nothing has been pruned or rolled off
              // the leader's tlog, so the leader's getLogList enumerates its ENTIRE accepted dataset -- all
              // of which is still uncommitted (down-window) and therefore absent from the (empty) commit
              // point the replication fetch above copied. So deleteAll'ing our index and replaying the
              // leader's full tlog converges us exactly. Rather than retrying forever waiting for a leader
              // commit that may never come, fetch the leader's tlog and replay it (ahead of our buffer) so
              // those docs become RTG-visible + tlog-durable here WITHOUT q=*:* exposure, then publish
              // ACTIVE — identically to the leader. (Correctness here rests on the empty-commit ⇒ full-tlog
              // identity above, NOT on tlog-vs-IndexWriter write ordering.) On ANY fetch/replay failure
              // replay() throws before touching the buffer (download/validation) or after an error-flagged
              // replay (buffer only dropped on a clean replay), so we fall back to retry with the buffer
              // left fully intact (never partial-apply-then-drop).
              log.info("OptionB: MASTER_VERSION_ZERO with leader update history -> NRT leader-tlog catch-up. core={} leaderUrl={}",
                  coreName, leader.getCoreUrl());
              try {
                replay(core, leader, true);
                log.info("Replication Recovery (leader-tlog catch-up) was successful. core={}", coreName);
                successfulRecovery = true;
              } catch (InterruptedException | AlreadyClosedException | RejectedExecutionException e) {
                throw e; // let the outer catch bail on close/interrupt
              } catch (Exception e) {
                log.warn("OptionB: leader-tlog catch-up failed -> fallback to retry (buffer left intact). core={}", coreName, e);
                successfulRecovery = false;
              }
            } else {
              // TLOG follower: Option B is NRT-only — leave the original version-0 retry semantics
              // untouched (the TLOG branch of replay() does copyOverBufferingUpdates, not leader-tlog fetch).
              log.warn("Leader reports an empty index but the shard has update history — leader likely still recovering; will retry. core={}", coreName);
              successfulRecovery = false;
            }
          } else if (result.getSuccessful()) {
            log.info("replication fetch reported as success");

            // Replication only copies the leader's latest *commit point*. Any updates the leader has
            // accepted but not yet committed (its open tlog / IndexWriter RAM) are absent from the
            // fetched index. For an NRT follower, replay the leader's tlog (those uncommitted updates)
            // followed by our buffer so they become RTG-visible and tlog-durable on the follower --
            // without exposing them to q=*:* -- identically to the leader. Pass the leader so replay()
            // can fetch its tlog. The fetched commit point already converges us, so an empty leader tlog
            // (no uncommitted docs) is legitimate here -- do NOT require a non-empty fetch.
            replay(core, leader, false);

            log.info("Replication Recovery was successful.");
            successfulRecovery = true;
          } else {
            log.error("replication fetch reported as failed: {} {}", result.getMessage(), result, result.getException());
            successfulRecovery = false;
          }

          if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
            log.info("Bailing on recovery due to close");
            close = true;
            return false;
          }

        } catch (InterruptedException | AlreadyClosedException | RejectedExecutionException e) {
          log.info("{} bailing on recovery", e.getClass().getSimpleName());
          close = true;
          return false;
        } catch (Exception e) {
          successfulRecovery = false;
          log.error("Error while trying to recover", e);
        }
      }
    } catch (Exception e) {
      log.error("Error while trying to recover. core={}", coreName, e);
      successfulRecovery = false;
    }
    if (successfulRecovery) {
      log.info("Registering as Active after recovery {}", coreName);
      try {
        // Always re-seed version buckets after a successful recovery, regardless of whether recovery ran
        // via full replication or PeerSync (the SOLR-7625 invariant). A freshly-created replica never
        // seeds at core startup (SolrCore.seedVersionBuckets is skipped for newCore), and the
        // PeerSync-with-empty-buffer path returns from applyBufferedUpdates without running a LogReplayer,
        // so the seed in LogReplayer's finally block never fires either. Gating this on didReplication
        // (only set on the replication path) left maxVersionFromIndex null after PeerSync recovery, so
        // getCurrentMaxVersion() returned null and HttpPartitionTest/ForceLeaderTest failed with
        // "max version bucket seed not set". seedVersionBuckets() is idempotent and cheap, so running it
        // on both paths (a harmless second time after replication) is safe.
        if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
          log.info("Bailing on recovery due to close");
          // Recovery succeeded; only the close raced ahead of publish(ACTIVE). The "<core>_recovering"
          // marker (set by startRecovering when we published BUFFERING/RECOVERING) is cleared ONLY by
          // publish(ACTIVE). If we return without clearing it, this fully-caught-up replica keeps the
          // recoveringTerm flag in ZK, so canBecomeLeader stays false forever and it can never win an
          // election after restart (E5-3). Clear the marker here on the success-then-close path.
          clearRecoveringMarkerOnClose(core);
          return false;
        }
        log.info("Updating version bucket highest from index after successful recovery.");
        try {
          core.seedVersionBuckets();
        } catch (Exception e) {
          log.error("Exception seeding version buckets");
        }

        if (replicaType == Replica.Type.TLOG) {
          zkController.startReplicationFromLeader(coreName, true);
        }

        if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
          log.info("Bailing on recovery due to close");
          // See E5-3 above: recovery completed but close raced publish(ACTIVE); clear the recovering
          // marker so this caught-up replica is electable on restart.
          clearRecoveringMarkerOnClose(core);
          return false;
        }
        // SOLR-9504 / TestCloudConsistency.testOutOfSyncReplicasCannotBecomeLeader*: we technically
        // caught up, but if a DATA-BEARING peer is currently DOWN (it was LEADER/ACTIVE in cluster
        // state, or carries a shard term > 0) then the source we just recovered from may NOT be
        // authoritative -- an out-of-sync peer can win/serve leadership while the real leader is down,
        // and converging to it would leave us missing the down leader's last updates. Publishing
        // ACTIVE now would clear our "<core>_recovering" marker and bump our term to max
        // (ZkController.publish -> isRecovering -> setTermEqualsToLeader), making this out-of-sync
        // replica leader-electable (data loss). Instead stay RECOVERING and let the outer doRecovery
        // loop retry with backoff; when the down data-bearing peer returns it has the highest term and
        // no marker, wins the election, and we then recover from IT (dataBearingPeerIsDown is false
        // once it is live) and converge correctly with all its docs. This makes deliberate what the
        // baseline achieved only accidentally (via the hidden-replay no-searcher livelock). NRT only:
        // TLOG/PULL followers track the leader via ReplicateFromLeader after going ACTIVE.
        boolean stayRecovering = false;
        if (replicaType == Replica.Type.NRT) {
          if (dataBearingPeerIsDown(core, leader)) {
            log.info("Recovery caught up but a data-bearing peer is currently DOWN; staying RECOVERING "
                + "and retrying so we converge from the authoritative leader when it returns. core={}", coreName);
            stayRecovering = true;
          } else if (replicaDivergesFromLeader(core, leader)) {
            // OptionB / replication recovery replays a POINT-IN-TIME snapshot of the leader (its tlog at
            // fetch time plus our buffer). Under concurrent indexing the leader can accept more updates
            // between that snapshot and this publish(ACTIVE) -- and the leader serves its live, still-
            // growing tlog (LocalFsFileStream streams to EOF while the follower only reads the length
            // advertised at list time), so a single pass can land us with a doc set that differs from the
            // leader's. A max-version check is NOT enough: a replica can hold the leader's NEWEST version
            // yet still be missing earlier docs, so we compare the full INDEX FINGERPRINT (the same
            // primitive PeerSync uses to confirm sync) -- it diverges if any doc differs by value, not
            // just if we are behind on the frontier. An NRT replica has NO poller to catch up after going
            // ACTIVE, so publishing ACTIVE while divergent would serve a view permanently inconsistent with
            // the leader (the ChaosMonkey "replica count short of the leader" divergence). Stay RECOVERING
            // and let the retry loop re-fetch+replay until our fingerprint matches the leader's; once
            // indexing quiesces the two converge and we go ACTIVE caught up. checkShardConsistency only
            // compares live+ACTIVE replicas, so a still-RECOVERING replica is correctly skipped rather than
            // asserted against a divergent view.
            log.info("Recovery completed but this replica's index fingerprint still differs from the "
                + "leader's; staying RECOVERING and retrying so we converge to the leader's doc set before "
                + "serving. core={}", coreName);
            stayRecovering = true;
          }
        }
        if (stayRecovering) {
          successfulRecovery = false;
        } else {
        // The index is already caught up; only the ZK state publish remains. A transient publish
        // failure here used to fall through to successfulRecovery=false and re-run the WHOLE
        // PeerSync+replication cycle, burning the bounded retry budget for work that was already
        // done (E5-4). Retry just the publish a few times first; only fall back to the full cycle
        // if it keeps failing.
        publishActiveAfterRecovery(core);
        recoveryListener.recovered();
        return true;
        }
      } catch (AlreadyClosedException | RejectedExecutionException e) {
        log.error("Already closed");
        close = true;
        return false;
      } catch (Exception e) {
        log.error("Could not publish as ACTIVE after successful recovery", e);
        successfulRecovery = false;
      }

    } else {
      log.info("Recovery was not successful, will not register as ACTIVE {}", coreName);
    }

    if (!isClosed()) {
      // lets pause for a moment and we need to try again...
      // TODO: we don't want to retry for some problems?
      // Or do a fall off retry...
      try {

        if (close || cc.isShutDown() || core.isClosing() || core.isClosed()) {
          close = true;
          throw new AlreadyClosedException();
        }
        log.error("Recovery failed - trying again... ({})", retries);

        if (retries.incrementAndGet() >= maxRetries) {
          SolrException.log(log, "Recovery failed - max retries exceeded (" + retries + ").");
          close = true;
          try {
            recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
          } catch (InterruptedException e) {

          } catch (Exception e) {
            log.error("Could not publish that recovery failed", e);
          }
        }
      } catch (Exception e) {
        log.error("An error has occurred during recovery", e);
      }
    }

    if (!isClosed() && !core.isClosing() && !core.isClosed()) {
      waitForRetry(core);
    } else {
      close = true;
      return false;
    }

    log.debug("Finished doSyncOrReplicateRecovery process, successful=[{}]", successfulRecovery);

    return false;
  }

  private final void waitForRetry(SolrCore core) {
    try {
      if (close) throw new AlreadyClosedException();
      long wait;

      if (retries.get() >= 0 && retries.get() < 3) {
        wait = this.startingRecoveryDelayMilliSeconds;
      } else if (retries.get() < 5) {
        wait = 1000;
      } else {
        wait = 15000;
      }

      log.info("Wait [{}] ms before trying to recover again (attempt={})", wait, retries);

      TimeOut timeout = new TimeOut(wait, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      while (true) {
        final boolean hasTimedOut = timeout.hasTimedOut();
        if (hasTimedOut) break;
        if (isClosed() && !core.isClosing() && !core.isClosed()) {
          log.info("RecoveryStrategy has been closed");
          return;
        }
        if (wait > 1000) {
          Thread.sleep(1000);
        } else {
          Thread.sleep(wait);
        }
      }

    } catch (InterruptedException e) {

    }

  }

  /**
   * A node that has (re)gained leadership must not leave its update log in BUFFERING. A TLOG replica
   * can begin buffering for recovery and then win (or already hold) leadership -- e.g. via the
   * leaderVoteWait/setTermToMax bypass -- and this recovery attempt then aborts here. If the log is
   * left BUFFERING, every write the node later accepts as leader is flagged BUFFERING by
   * DistributedUpdateProcessor and written to the buffer tlog instead of the live RTG map, so
   * realtime-get returns null for docs indexed after leadership was acquired.
   * copyOverBufferingUpdates() drains any buffered updates into the live tlog (preserving RTG
   * visibility) and returns the log to ACTIVE.
   */
  private void exitBufferingForLeader(SolrCore core) {
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    if (ulog != null && ulog.getState() == UpdateLog.State.BUFFERING) {
      try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
        ulog.copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
      }
    }
  }

  public static Runnable testing_beforeReplayBufferingUpdates;

  private void replay(SolrCore core, Replica leader, boolean requireLeaderTlog)
      throws InterruptedException, ExecutionException {
    if (testing_beforeReplayBufferingUpdates != null) {
      testing_beforeReplayBufferingUpdates.run();
    }

    if (replicaType == Replica.Type.TLOG) {
      // roll over all updates during buffering to new tlog, make RTG available
      try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
        core.getUpdateHandler().getUpdateLog().copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
      }
      // Do NOT fall through to applyBufferedUpdates()/openRealtimeSearcher() for a TLOG follower:
      // the copied-over updates live only in the ulog RTG maps (adds bypass the IndexWriter), and
      // openRealtimeSearcher() clears those maps — losing RTG visibility for every doc newer than
      // the last fetched commit until the next segment replication.
      return;
    }

    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

    Future<RecoveryInfo> future;
    File tmpTlogDir = null;
    if (leader != null) {
      // NRT replication-recovery catch-up: download the leader's tlog (its updates accepted-but-not-
      // committed, hence absent from the fetched commit point) and replay them ahead of our buffer.
      List<TransactionLog> leaderLogs = new ArrayList<>();
      tmpTlogDir = new File(new File(ulog.getLogDir()).getParentFile(), "leadertlog." + System.nanoTime());
      try {
        List<File> files = replicationHandler.fetchLeaderTlogFiles(leader.getCoreUrl(), tmpTlogDir);
        for (File f : files) {
          // openExisting=true reads + validates the header (a corrupt/short tlog throws here), so a
          // bad download is caught before any replay touches the buffer.
          leaderLogs.add(UpdateLog.newTransactionLog(f, null, true));
        }
        if (requireLeaderTlog && leaderLogs.isEmpty()) {
          // We entered this path because the leader reported index version 0 BUT advertised update
          // history (leaderHasUpdateVersions) -- with no commit point, its ENTIRE dataset lives in its
          // tlog. An empty fetch now means the leader committed and pruned/rotated its tlog between the
          // probe and this fetch. replicate() already deleteAll'd our index, so replaying nothing would
          // publish ACTIVE with the leader's data missing. Fail (caught below -> recovery retries, buffer
          // left intact); the retry sees the leader's new commit point and converges via normal replication.
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "Leader tlog fetch returned empty but leader reported update history -- retrying recovery");
        }
      } catch (Exception e) {
        // Download/validation failed: release anything opened, discard the temp dir, and fail the
        // recovery so it retries -- the buffer is left completely untouched (never partial-apply).
        for (TransactionLog l : leaderLogs) {
          try {
            l.decref();
          } catch (Exception ignore) {
            // best effort
          }
        }
        deleteDirTree(tmpTlogDir);
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Failed to fetch leader tlog for NRT recovery catch-up", e);
      }
      log.info("OptionB: fetched {} leader tlog file(s) for NRT catch-up; replaying leader-tlog then buffer. core={} leaderUrl={}",
          leaderLogs.size(), core.getName(), leader.getCoreUrl());
      future = ulog.applyLeaderTlogThenBuffer(leaderLogs);
    } else {
      // SolrCloud peersync recovery: hide the replayed buffer from ordinary q=*:* search
      // (openSearcher=false) so leader-uncommitted forwarded docs are not exposed before the leader
      // durably commits -- the replica converges on the leader's next real commit.
      future = ulog.applyBufferedUpdates(false);
    }

    try {
      if (future == null) {
        // no replay needed\
        log.debug("No replay needed.");
        return;
      } else {
        log.info("Replaying buffered documents.");
        // wait for replay
        RecoveryInfo report;
        try {
          report = future.get(10, TimeUnit.MINUTES); // MRM TODO: - how long? make configurable too
        } catch (InterruptedException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
        } catch (TimeoutException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
        // On the leader-tlog catch-up path a record-level error must also fail the recovery: the
        // buffer is only dropped on an error-free replay (see applyLeaderTlogThenBuffer), so treating
        // errors>0 as success would publish ACTIVE with the buffer's updates still un-applied.
        if (report.failed || (leader != null && report.errors > 0)) {
          SolrException.log(log, "Replay failed");
          throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
        }
        if (leader != null) {
          log.info("OptionB: leader-tlog catch-up replay applied {} core={}", report, core.getName());
        }
      }

      // the index may ahead of the tlog's caches after recovery, by calling this tlog's caches will be purged
      if (ulog != null) {
        ulog.openRealtimeSearcher();
      }
    } finally {
      // The replayer deletes the downloaded tlog files (deleteOnClose) as it decrefs them; remove the
      // now-empty temp dir. On the error path above, deleteDirTree already ran before the throw.
      if (tmpTlogDir != null) {
        deleteDirTree(tmpTlogDir);
      }
    }

    // solrcloud_debug
    // cloudDebugLog(core, "replayed");
  }

  /** Recursively delete a directory tree, best effort (used to clean the leader-tlog temp dir). */
  private static void deleteDirTree(File dir) {
    if (dir == null) {
      return;
    }
    File[] children = dir.listFiles();
    if (children != null) {
      for (File child : children) {
        if (child.isDirectory()) {
          deleteDirTree(child);
        } else {
          child.delete();
        }
      }
    }
    dir.delete();
  }

  /**
   * Open a new (top-level) searcher so docs recovered via PeerSync become visible to ordinary
   * search. {@link PeerSync.Updater#applyUpdates} applies updates with IGNORE_AUTOCOMMIT and ends
   * with only {@code proc.finish()} (no commit / openSearcher), and PeerSync verifies sync against
   * the realtime searcher, so without this the recovered docs would be visible only to RTG until the
   * next commit -- leaving an ACTIVE replica serving a stale {@code *:*} view. A soft commit is
   * enough (the recovered docs are already durable in the tlog); it just reopens the searcher.
   */
  private boolean openSearcherAfterPeerSync(SolrCore core) {
    try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
      CommitUpdateCommand cuc = new CommitUpdateCommand(req, false);
      cuc.softCommit = true;
      cuc.openSearcher = true;
      cuc.waitSearcher = true;
      core.getUpdateHandler().commit(cuc);
      return true;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.warn("Could not open a new searcher after PeerSync recovery for core={}", core.getName(), e);
      return false;
    }
  }

  /**
   * Mirror of {@link org.apache.solr.cloud.ShardLeaderElectionContext}'s {@code anotherReplicaHasData}:
   * returns true if some OTHER non-PULL replica in this shard is currently DOWN and is data-bearing --
   * it was LEADER/ACTIVE in cluster state, or carries a shard term &gt; 0. Used to refuse publishing
   * ACTIVE after a recovery that may have converged us to a non-authoritative source while the real
   * data-bearing leader is down (SOLR-9504). Short-circuits false for a pristine, never-indexed shard
   * (maxTerm &lt;= 0): the first update bumps every replica's term off 0, so maxTerm==0 means no data
   * exists anywhere and there is nothing to protect. Fail-open on any bookkeeping error so a stuck
   * lookup never strands an otherwise-complete recovery.
   */
  private boolean dataBearingPeerIsDown(SolrCore core, Replica recoveredFromLeader) {
    try {
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      String collection = coreDescriptor.getCollectionName();
      String shardId = coreDescriptor.getCloudDescriptor().getShardId();

      ZkShardTerms terms = zkController.getShardTerms(collection, shardId);
      if (terms == null) {
        return false;
      }
      try {
        terms.refreshTerms(-1);
      } catch (Exception e) {
        log.warn("Could not refresh shard terms for {}/{} during down-data-bearing-peer check", collection, shardId, e);
      }
      if (terms.getHighestTerm() <= 0) {
        return false;
      }

      DocCollection coll = zkController.getZkStateReader().getClusterState().getCollectionOrNull(collection);
      if (coll == null) return false;
      Slice slice = coll.getSlice(shardId);
      if (slice == null) return false;

      // Our own current shard term. After startRecovering (run at BUFFERING-publish time) this equals
      // the leader's term, so a replica that successfully caught up from the authoritative leader holds
      // the current max term. A down peer with a STRICTLY LOWER term cannot hold any update we lack, so
      // it must not block us from going ACTIVE. We only block for a down peer at an equal-or-higher term
      // (it may hold data we are missing) -- which is exactly the SOLR-9504 case (an empty/behind replica
      // whose term was raised by the onlyLiveReplica bypass keeps mySelfTerm <= the real data-bearing
      // peer's term, so '<' never lets it through). The tie case stays conservative on purpose.
      long mySelfTerm = terms.getTerm(coreName);

      // If we converged to a CURRENTLY-LIVE authoritative leader (the very replica we just recovered
      // from, and still the shard's elected leader), a down peer whose term does not EXCEED that live
      // leader's term is subordinate to it: when the down peer returns it recovers from this same live
      // leader, so it cannot hold any update the leader -- and therefore we -- lack. Only a down peer at
      // a term STRICTLY GREATER than the live leader's could be more authoritative. This lets force-leader
      // complete recovery (it equalizes every replica's term and promotes a live leader while the old
      // leader stays DOWN at the same term), WITHOUT weakening SOLR-9504: in that scenario the down
      // data-bearing peer IS the (unreachable) leader, so there is no live leader distinct from it and
      // liveLeaderTerm stays -1, leaving the guard fully in force.
      long liveLeaderTerm = -1;
      if (recoveredFromLeader != null
          && !recoveredFromLeader.getName().equals(coreName)
          && zkController.getZkStateReader().isNodeLive(recoveredFromLeader.getNodeName())) {
        Replica curLeader = slice.getLeader();
        if (curLeader != null
            && curLeader.getName().equals(recoveredFromLeader.getName())
            && zkController.getZkStateReader().isNodeLive(curLeader.getNodeName())) {
          liveLeaderTerm = terms.getTerm(curLeader.getName());
        }
      }

      for (Replica r : slice) {
        if (r.getName().equals(coreName)) continue;
        if (r.getType() == Replica.Type.PULL) continue;
        if (zkController.getZkStateReader().isNodeLive(r.getNodeName())) continue; // only DOWN peers
        long rTerm = terms.getTerm(r.getName());
        // A down peer strictly behind us holds nothing we lack -> it does not block our ACTIVE.
        if (rTerm < mySelfTerm) continue;
        // A down peer no higher than the live authoritative leader we converged to is subordinate to it
        // and will recover from it on return -> it does not block our ACTIVE.
        if (liveLeaderTerm >= 0 && rTerm <= liveLeaderTerm) continue;
        // getState() uses published=true, so shortState=1 (LEADER) resolves to ACTIVE here -- both count.
        Replica.State rState = r.getState();
        if (rState == Replica.State.LEADER || rState == Replica.State.ACTIVE) {
          return true;
        }
        if (rTerm > 0) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      log.warn("Error checking for a down data-bearing peer; not blocking recovery", e);
      return false;
    }
  }

  /**
   * Recovery completed but a concurrent close raced ahead of {@code publish(ACTIVE)}, which is the
   * only place the "&lt;core&gt;_recovering" term marker (set by startRecovering) is normally cleared.
   * Mirror that completion logic here so a fully-caught-up replica does not keep the recoveringTerm
   * flag in ZK — otherwise canBecomeLeader stays false and it can never win an election after
   * restart (E5-3). Best-effort: failures are logged, not propagated, since we are already closing.
   */
  private void clearRecoveringMarkerOnClose(SolrCore core) {
    if (replicaType == Replica.Type.PULL) {
      return; // PULL terms are not tracked
    }
    try {
      CoreDescriptor cd = core.getCoreDescriptor();
      ZkShardTerms shardTerms = zkController.getShardTerms(cd.getCollectionName(),
          cd.getCloudDescriptor().getShardId());
      if (shardTerms.isRecovering(coreName)) {
        // setTermEqualsToLeader (not doneRecovering) so the term tracks any leader advance during
        // recovery, matching the normal publish(ACTIVE) completion path in ZkController.
        shardTerms.setTermEqualsToLeader(coreName);
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.warn("Could not clear recovering term marker on close for core={}", coreName, e);
    }
  }

  /**
   * Publish ACTIVE after a successful recovery, retrying a transient ZK publish failure a bounded
   * number of times. The index is already caught up at this point, so a failed publish should not
   * discard the recovery and re-run the entire PeerSync+replication cycle (E5-4). AlreadyClosed /
   * RejectedExecution and the close flag are propagated/honored so shutdown is not retried over.
   */
  private void publishActiveAfterRecovery(SolrCore core) throws Exception {
    final int publishRetries = 3;
    Exception last = null;
    for (int i = 0; i < publishRetries; i++) {
      if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
        throw new AlreadyClosedException();
      }
      try {
        zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
        return;
      } catch (AlreadyClosedException | RejectedExecutionException e) {
        throw e; // shutdown in progress — let the caller stop, do not retry
      } catch (Exception e) {
        last = e;
        log.warn("Transient failure publishing ACTIVE after recovery (attempt {}/{}), retrying", i + 1, publishRetries, e);
        Thread.sleep(250);
      }
    }
    throw last; // exhausted publish retries — fall back to the full retry loop
  }

  static private void cloudDebugLog(SolrCore core, String op) {
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
      ParWork.propagateInterrupt(e);
      log.debug("Error in solrcloud_debug block", e);
    }
  }

  final public boolean isClosed() {
    return close || cc.isShutDown();
  }




}
