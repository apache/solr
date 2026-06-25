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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer cc;
  private final SyncStrategy syncStrategy;

  protected final String shardId;

  protected final String collection;

  private final ZkController zkController;

  public ShardLeaderElectionContext(LeaderElector leaderElector,
                                    final String shardId, final String collection,
                                    Replica replica, ZkController zkController, CoreContainer cc, CoreDescriptor cd) {
    super(leaderElector, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                    + "/leader_elect/" + shardId,  ZkStateReader.getShardLeadersPath(
            collection, shardId), replica, cd,
            zkController.getZkClient());
    this.cc = cc;
    this.syncStrategy = new SyncStrategy(cc);
    this.shardId = shardId;
    this.zkController = zkController;
    this.collection = collection;
  }

  @Override
  protected void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
  }

  @Override
  public ElectionContext copy() {
    return new ShardLeaderElectionContext(leaderElector, shardId, collection, replica, zkController, cc, cd);
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }


  /*
   * weAreReplacement: has someone else been the leader already?
   */
  @Override
  synchronized boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStart) throws KeeperException,
          InterruptedException, IOException {

    if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
      return true;
    }

    String coreName = replica.getName();

    log.debug("Run leader process for shard [{}] election, first step is to try and sync with the shard", shardId);
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        log.info("No SolrCore found, cannot become leader {}", coreName);
        cancelElection();
        throw new AlreadyClosedException("No SolrCore found, cannot become leader " + coreName);
      }

      // NOTE: recovery is intentionally NOT cancelled here at election entry. A candidate that ends
      // up stepping aside (a higher-term replica must lead) needs its in-flight recovery to keep
      // running so it converges from the new leader. Cancelling here used to strand such replicas
      // BUFFERING ({1=L,2=B,3=B}), and the rapid step-aside rotation re-cancelled recovery on every
      // pass. We cancel recovery later, only once we have actually decided to become leader (just
      // before the leader sync below).

      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

      if (log.isDebugEnabled()) {
        log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId, weAreReplacement, leaderVoteWait);
      }

      if (core.getUpdateHandler().getUpdateLog() == null) {
        log.error("No UpdateLog found - cannot sync");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replica with no update log configured cannot be leader");
      }

      Replica.Type replicaType;
      boolean setTermToMax = false;

      CoreDescriptor cd = core.getCoreDescriptor();
      CloudDescriptor cloudCd = cd.getCloudDescriptor();
      replicaType = cloudCd.getReplicaType();
      // should I be leader?

      if (log.isDebugEnabled()) {
        log.debug("Check zkShardTerms");
      }
      // Load (creating + reading from ZK if necessary) rather than getShardTermsOrNull. The terms
      // znode is the authoritative, persistent record of which replicas are in sync. On a node
      // restart the LeaderElector can run this leader process before ZkController.register() has
      // lazily created this node's in-memory ZkShardTerms, so getShardTermsOrNull would return null
      // and the SOLR-9504 term-eligibility guard below (gated on `zkShardTerms != null && registered`)
      // would be SILENTLY BYPASSED — letting an out-of-sync replica with local data seize leadership
      // (TestCloudConsistency.testOutOfSyncReplicasCannotBecomeLeader*: leader holds doc N, restarts;
      // an out-of-sync follower whose terms object wasn't created yet became leader). getShardTerms
      // loads the persisted terms from ZK (empty map if the znode is absent, e.g. a never-indexed
      // shard — registered() stays false there so a pristine shard still elects normally).
      ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
      try {
        if (zkShardTerms != null) {
          //  guarantees that a live-fetch will be enough for this core to see max term published
          if (log.isDebugEnabled()) log.debug("refresh shard terms for core {}", coreName);
          zkShardTerms.refreshTerms(-1);
        }
      } catch (Exception e) {
        log.error("Exception while looking at refreshing shard terms", e);
      }

      if (zkShardTerms != null && zkShardTerms.registered(coreName) && !zkShardTerms.canBecomeLeader(coreName)) {
        boolean onlyLiveReplica = true;
        DocCollection coll = zkController.zkStateReader.getClusterState().getCollectionOrNull(collection);

        if (coll != null) {
          Slice slice = coll.getSlice(shardId);
          if (slice != null) {
            for (Replica replica : slice) {
              if (replica.getName().equals(coreName)) {
                continue;
              }
              // PULL replicas can never become leader, so they must not count as a "live peer"
              // for the onlyLiveReplica determination. Otherwise, when the only surviving replica
              // is a PULL replica, a freshly-added NRT/TLOG candidate would conclude it is not
              // alone and wait the full leaderVoteWait for an eligible leader that can never appear,
              // hanging leader election (no leader is ever elected for the shard).
              if (replica.getType() == Replica.Type.PULL) {
                continue;
              }
              if (zkController.zkStateReader.isNodeLive(replica.getNodeName())) {
                onlyLiveReplica = false;
                break;
              }
            }
          }
        }

        if (!onlyLiveReplica) {
          if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, coreName, leaderVoteWait)) {
            return false;
          }
        } else {
          // SOLR-9504: sole live replica but NOT highest term. Don't let a behind replica seize
          // leadership and truncate real data when the up-to-date leader returns. If ANY other replica
          // (currently down) holds a strictly higher term it is more up-to-date than us -- it has
          // committed updates we lack -- so honor leaderVoteWait for it to return and lead, regardless
          // of whether we happen to hold *some* local data. (Having docs 1..N-1 but missing doc N is
          // exactly the out-of-sync case TestCloudConsistency.testOutOfSyncReplicasCannotBecomeLeader*
          // guards: the down leader carries the higher term, so we must not bypass on "haveData".)
          // If no higher-term replica exists, or none returns within leaderVoteWait, we become leader
          // anyway -- standard semantics for a permanently lost leader (LeaderVoteWaitTimeoutTest).
          if (anotherReplicaHasHigherTerm(zkShardTerms, coreName)) {
            if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, coreName, leaderVoteWait)) {
              return false;
            }
          }
        }

        // only log an error if this replica win the election
        setTermToMax = true;
      }

      if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
        cancelElection();
        return true;
      }

      log.debug("I may be the new leader - try and sync");

      // We have passed the eligibility gate and are now committing to becoming leader: cancel any
      // in-flight recovery so it does not race the leader sync below. (Replicas that stepped aside
      // above returned before reaching this point, so their recovery is left untouched and converges
      // from the new leader.)
      core.getSolrCoreState().cancelRecovery(false, false);

      PeerSync.PeerSyncResult result;
      boolean success = false;
      result = syncStrategy.sync(zkController, core, replica, weAreReplacement);
      success = result.isSuccess();

      // SOLR-9504: PeerSync returns success=true when no peers responded (no live replicas), but
      // that doesn't mean we're safe to become leader. If we have no local update-log data (brand-new
      // empty replica added via addReplica) and another replica in cluster state appears to have data
      // (either LEADER/ACTIVE in cluster state or term > 0 in ZK), we must NOT seize leadership —
      // doing so would cause the data-bearing replica (currently down or restarting) to recover FROM
      // us and lose all committed data (SOLR-9504 data-loss). The guard fires even when zkShardTerms
      // is null (unregistered brand-new replica) because the check falls back to ZK term reads.
      //
      // GATE on !setTermToMax. setTermToMax becomes true only after we passed through the
      // !canBecomeLeader block AND waitForEligibleBecomeLeaderAfterTimeout returned true — i.e. we
      // already waited out the full leaderVoteWait for a better (higher-term, live) candidate and none
      // appeared. At that point standard leaderVoteWait semantics say we become leader anyway (data
      // loss is accepted because the up-to-date replica did not return in time) — yielding here instead
      // would hang the shard with no leader forever (LeaderVoteWaitTimeoutTest.basicTest: stop the sole
      // data replica, add a fresh empty replica, expect it to lead after leaderVoteWait → {1=D,2=D} if
      // we yield). Conversely when setTermToMax==false we reached this point WITHOUT waiting: either we
      // had the highest term on a flat-terms shard (canBecomeLeader==true even though a down replica
      // holds the data — LeaderVoteWaitTimeoutTest.testMostInSyncReplicasCanWinElection: termsMap
      // {n1=1,n2=1,n3=1}, empty candidate, down ACTIVE replica n1 has the docs) — there we MUST yield so
      // the data-bearing replica leads. So !setTermToMax is exactly the "we have not yet earned the
      // right to lead despite being behind" condition. hasLocalData==true (an in-sync follower taking
      // over) is never blocked either way, preserving DeleteReplicaTest.raceConditionOnDeleteAndRegisterReplica
      // and CloudHttp2SolrClientTest.testRetryUpdatesWhenClusterStateIsStale.
      if (!setTermToMax && success && !result.getOtherHasVersions().orElse(false)) {
        boolean hasLocalData = false;
        UpdateLog ulog2 = core.getUpdateHandler().getUpdateLog();
        if (ulog2 != null) {
          try (UpdateLog.RecentUpdates recentUpdates = ulog2.getRecentUpdates()) {
            hasLocalData = recentUpdates != null && !recentUpdates.getVersions(1).isEmpty();
          }
        }
        if (!hasLocalData) {
          // The update log can be empty even though this replica genuinely holds committed data:
          // a replica that recovered via full index replication (IndexFetcher) has the documents in
          // its INDEX but not necessarily in its tlog. Treat a non-empty committed index as local
          // data too, otherwise such a recovered replica is wrongly judged "empty" here, needlessly
          // waits out the full leaderVoteWait, and leadership bounces back to the just-expired leader
          // when it reconnects (HttpPartitionTest.testLeaderZkSessionLoss). A truly empty replica
          // (no tlog versions AND no indexed docs) still falls through to the SOLR-9504 guard below.
          try {
            hasLocalData = core.withSearcher(s -> s.getIndexReader().numDocs() > 0);
          } catch (Exception e) {
            // treat as no local data on any searcher error
          }
        }
        if (!hasLocalData && anotherReplicaHasData(zkShardTerms, coreName)) {
          // We are an empty/behind replica, no LIVE peer responded to PeerSync, yet a (currently DOWN)
          // replica in this shard holds data. Do NOT seize leadership immediately — that would make the
          // data-bearing replica recover FROM us and lose committed data (SOLR-9504). Instead honor
          // leaderVoteWait: wait for the data-bearing replica to return and lead. If a live higher-term
          // peer appears during the wait, step aside so it wins (the data is preserved). If none appears
          // within leaderVoteWait, become leader anyway — standard Solr semantics for a permanently lost
          // leader (LeaderVoteWaitTimeoutTest.basicTest stops the sole data replica for good and adds a
          // fresh empty replica that MUST take over; yielding forever left the shard leaderless {1=D,2=D}).
          ZkShardTerms waitTerms = zkShardTerms != null ? zkShardTerms : zkController.getShardTerms(collection, shardId);
          if (!waitForEligibleBecomeLeaderAfterTimeout(waitTerms, coreName, leaderVoteWait)) {
            log.info("SOLR-9504: no local data and a data-bearing replica returned during leaderVoteWait; " +
                "yielding election so it can lead. coreName={}", coreName);
            cancelElection();
            return false;
          }
          log.info("SOLR-9504: no local data and a down replica had data, but it did not return within " +
              "leaderVoteWait; becoming leader anyway. coreName={}", coreName);
          setTermToMax = true;
        }
      }

      if (!success) {
        boolean hasRecentUpdates = false;

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        if (ulog != null) {
          // TODO: we could optimize this if necessary
          try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
            hasRecentUpdates = recentUpdates != null && !recentUpdates.getVersions(1).isEmpty();
          }
        }

        log.warn("Checking for recent versions in the update log hasRecentUpdates={}", hasRecentUpdates);
        if (!hasRecentUpdates) {
          // we failed sync, but we have no versions - we can't sync in that case
          // - we were active before, so become leader anyway if no one else has any versions either
          if (result.getOtherHasVersions().orElse(false)) {
            log.info("We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader {}", coreName);
            return false;
          } else if (anotherReplicaHasData(zkShardTerms, coreName)) {
            // SOLR-9504: PeerSync found no live peers, but another replica in the shard has data
            // (visible via cluster-state or shard terms). An empty replica must not seize leadership
            // while a data-bearing replica exists, even if that replica is currently down.
            log.info("We failed sync, have no versions, and no live peers responded, but another replica in this shard has data. Not becoming leader to avoid data loss {}", coreName);
            return false;
          } else {
            log.info("We failed sync, but we have no versions - we can't sync in that case - we did not find versions on other replicas, so become leader anyway {}", coreName);
            success = true;
          }
        }
      }
      if (!success) {
        log.info("Sync with potential leader failed, rejoining election {} ...", coreName);
        return false;
      }

      if (replicaType == Replica.Type.TLOG) {
        // stop replicate from old leader
        zkController.stopReplicationFromLeader(coreName);
        if (weAreReplacement) {

          Future<UpdateLog.RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().recoverFromCurrentLog();
          if (future != null) {
            log.info("Replaying tlog before become new leader");
            future.get();
          } else {
            log.info("New leader does not have old tlog to replay");
          }

        }
        // A leader's update log must never be left in BUFFERING. A TLOG replica can win
        // leadership via the leaderVoteWait/setTermToMax bypass while a recovery already had
        // its log buffering, and recoverFromCurrentLog() above only replays the current tlog --
        // it neither drains the buffer tlog nor guarantees a BUFFERING->ACTIVE transition (it is
        // a no-op when there is no current tlog). A log left BUFFERING means every add we later
        // accept as leader gets the BUFFERING flag in DistributedUpdateProcessor and is written
        // to the buffer tlog instead of the live RTG map, so realtime-get returns null for docs
        // indexed after we became leader. copyOverBufferingUpdates() drains any buffered updates
        // into the live tlog (preserving RTG visibility) and flips the log to ACTIVE.
        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        if (ulog != null && ulog.getState() == UpdateLog.State.BUFFERING) {
          try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
            ulog.copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
          }
        }
      }
      // in case of leaderVoteWait timeout, a replica with lower term can win the election
      if (setTermToMax) {
        log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " + "without being up-to-date with the previous leader", coreName);
        try {
          zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreName);
        } catch (Exception e) {
          log.error("Exception trying to set shard terms equal to leader", e);
          // MRM TODO
        }
      }

      if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
        cancelElection();
        throw new AlreadyClosedException();
      }

      boolean leaderSuccess = super.runLeaderProcess(context, weAreReplacement, 0);
      if (!leaderSuccess) {
        return false;
      }

      ZkNodeProps zkNodes = ZkNodeProps
          .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.CORE_NAME_PROP, replica.getName(), "id",
              cd.getCoreProperty("collId", null)+ "-" + cd.getCoreProperty("id", null), ZkStateReader.STATE_PROP, Replica.State.LEADER);

      if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
        cancelElection();
        throw new AlreadyClosedException();
      }
      log.debug("I am the new leader, publishing as active: {} {}", replica.getCoreUrl(), shardId);

      zkController.publish(zkNodes);

    } catch (AlreadyClosedException e) {
      log.info("Already closed, bailing..");
      cancelElection();
    } catch (InterruptedException e) {
      log.warn("Interrupted, bailing..");;
      ParWork.propagateInterrupt(e);
      cancelElection();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (Exception e) {
      SolrException.log(log, "There was a problem trying to register as the leader", e);
      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null) {
          log.info("No SolrCore found, cannot become leader {}", coreName);
          cancelElection();
          throw new AlreadyClosedException("No SolrCore found, cannot become leader " + coreName);
        }
        return false;
      }
    }

    return true;
  }

  /**
   * Wait for other replicas with higher terms participate in the electioon
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, String coreNodeName, int timeout) throws InterruptedException {
    if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
      throw new AlreadyClosedException();
    }
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (true) {
      if (System.nanoTime() > timeoutAt) {
        log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (core_term:{}, highest_term:{})",
            timeout, coreNodeName, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
        return true;
      }
      if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
        log.info("Can't become leader, other replicas with higher term participated in leader election {}", zkShardTerms);
        return false;
      }

      // TODO: if we know eveyrone has already particpated, we should bail early...
      if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
        throw new AlreadyClosedException();
      }
      Thread.sleep(50L);
    }
  }

  /**
   * Do other replicas with higher term participated in the election
   *
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreName) {
    if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
      throw new AlreadyClosedException();
    }

    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) return false;

    long replicaTerm = zkShardTerms.getTerm(coreName);
    boolean isRecovering = zkShardTerms.isRecovering(coreName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreName)) continue;
      // PULL replicas never participate in leader election; skip them.
      if (replica.getType() == Replica.Type.PULL) continue;
      if (zkController.getZkStateReader().getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());
        if (isRecovering && !isOtherReplicaRecovering) return true;
        if (otherTerm > replicaTerm) return true;
      }
    }
    return false;
  }

  /** True if some other registered replica in this shard has a strictly higher term than coreName,
   *  regardless of whether that replica is currently live. */
  private boolean anotherReplicaHasHigherTerm(ZkShardTerms zkShardTerms, String coreName) {
    long myTerm = zkShardTerms.getTerm(coreName);
    DocCollection coll = zkController.zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (coll == null) return false;
    Slice slice = coll.getSlice(shardId);
    if (slice == null) return false;
    for (Replica r : slice) {
      if (r.getName().equals(coreName)) continue;
      // PULL replicas can never become leader and only track the leader's term for replication
      // bookkeeping; their term must not force a leader-eligible candidate to wait out leaderVoteWait
      // for an eligible peer that can never appear.
      if (r.getType() == Replica.Type.PULL) continue;
      if (zkShardTerms.getTerm(r.getName()) > myTerm) return true;
    }
    return false;
  }

  /**
   * SOLR-9504: Returns true if any replica other than {@code coreName} in this shard appears to
   * have data — either because it is currently in LEADER or ACTIVE state in the cluster state, or
   * because its shard term is greater than zero (fetching from ZK if needed).
   *
   * <p>This prevents an empty/brand-new replica from seizing leadership and truncating data when a
   * data-bearing replica is temporarily down. The check uses cluster state (not liveness) so it
   * fires even when the old leader cleaned up its term entry on a graceful shutdown.
   */
  private boolean anotherReplicaHasData(ZkShardTerms zkShardTerms, String coreName) {
    DocCollection coll = zkController.zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (coll == null) return false;
    Slice slice = coll.getSlice(shardId);
    if (slice == null) return false;

    // Load the shard terms up front. In this fork the FIRST update to a shard bumps every replica's
    // term from 0 (registered) to >=1 (ZkShardTerms.ensureHighestTermsAreNotZero, called on the
    // distributed-update path as well as on restore/split). So a shard whose MAXIMUM term is still 0
    // has never received any data, and NO replica in it — live or down — can be data-bearing.
    // In that case the SOLR-9504 protection must NOT fire: blocking leadership on a pristine empty
    // shard whose leader was just killed strands the shard with no leader forever. That is exactly
    // LeaderElectionIntegrationTest's collection1 phase — kill the (empty) s1 leader and every one of
    // the 5 empty survivors would otherwise yield to the just-killed, still-ACTIVE-in-snapshot empty
    // leader via the cluster-state check below, so no leader is ever elected (30s getLeaderRetry
    // timeout). This maxTerm==0 short-circuit is robust to per-replica term cleanup on a graceful
    // leader shutdown: if the shard ever held data the surviving in-sync replicas still carry term
    // >=1, so maxTerm stays > 0 and the cluster-state check still protects the down data-bearing
    // replica (LeaderVoteWaitTimeoutTest / TestCloudConsistency, where data was indexed => maxTerm>=1).
    ZkShardTerms terms = zkShardTerms;
    if (terms == null) {
      try {
        terms = zkController.getShardTerms(collection, shardId);
        terms.refreshTerms(-1);
      } catch (Exception e) {
        log.warn("Could not load shard terms for {}/{} during SOLR-9504 data check", collection, shardId, e);
        return false;
      }
    }
    if (terms.getHighestTerm() <= 0) {
      // pristine, never-indexed shard: no data to protect anywhere, so any registered replica may lead.
      return false;
    }

    // We only reach this guard when no *live* peer reported update-log versions via PeerSync (if a
    // live peer had data, getOtherHasVersions() would be true and we would not consult this method).
    // The guard therefore exists solely to protect a data-bearing replica that is currently DOWN and
    // expected to restart. A replica whose node is still LIVE needs no such protection: it competes in
    // the election itself, and because it is live the candidate's onlyLiveReplica bypass cannot fire,
    // so the normal highest-term rule decides leadership. Counting a LIVE replica here would instead
    // wrongly block leadership forever when that replica is being removed (e.g. deleteReplica of the
    // current leader) and so will never come back to win the election.
    // Primary check: any OTHER replica that was/is LEADER or ACTIVE in cluster state had real data.
    // Note: getState() with published=true maps shortState=1 (LEADER) -> ACTIVE, so both states
    // resolve to ACTIVE here — this is intentional, we want to catch either case.
    // PULL replicas are excluded: they can never become leader and only ever hold a (possibly stale)
    // copy replicated from the leader, so an ACTIVE PULL replica is NOT evidence of a data-bearing
    // leader-eligible peer. Counting it here would wrongly block a freshly-added empty NRT/TLOG from
    // becoming leader when the only survivor is a PULL replica, hanging the shard with no leader.
    for (Replica r : slice) {
      if (r.getName().equals(coreName)) continue;
      if (r.getType() == Replica.Type.PULL) continue;
      if (zkController.zkStateReader.isNodeLive(r.getNodeName())) continue;
      Replica.State rState = r.getState();
      if (rState == Replica.State.LEADER || rState == Replica.State.ACTIVE) {
        return true;
      }
    }

    // Secondary check: any DOWN replica with term > 0 held data.
    for (Replica r : slice) {
      if (!r.getName().equals(coreName) && r.getType() != Replica.Type.PULL
          && !zkController.zkStateReader.isNodeLive(r.getNodeName())
          && terms.getTerm(r.getName()) > 0) return true;
    }
    return false;
  }

  public String getShardId() {
    return shardId;
  }

  public String getCollection() {
    return collection;
  }

}
