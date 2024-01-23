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

package org.apache.solr.update.processor;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.cloud.api.collections.SplitShardCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.zero.process.ZeroCoreIndexingBatchProcessor;

/**
 * This class is extending and replacing {@link DistributedZkUpdateProcessor} to be used in {@link
 * DistributedUpdateProcessorFactory} when dealing with cores for {@link
 * org.apache.solr.common.cloud.Replica.Type#ZERO} replicas.
 *
 * <p>It the glue for adding Zero related code to normal SolrCloud processing: Rejection if shard
 * splits are in progress is done in this class, and actual work delegated to {@link
 * ZeroCoreIndexingBatchProcessor} (this only happens if the request is to be processed locally as
 * opposed to being forwarded to other nodes/replicas).
 *
 * <p>Delegated work is calling {@link
 * ZeroCoreIndexingBatchProcessor#addOrDeleteGoingToBeIndexedLocally} when an indexing batch starts
 * and {@link ZeroCoreIndexingBatchProcessor#hardCommitCompletedLocally} after local hard commit was
 * done.
 *
 * <p>Plugging into the {@code updateRequestProcessorChain} of {@code solrconfig.xml} is not ideal,
 * it assumes {@link DistributedUpdateProcessorFactory} is configured there in the right place.
 * Would be nice to find another way of plugging the Zero processing into SolrCloud (pulling before
 * data is needed, pushing after indexing has completed).
 *
 * <p>Given the locking in use, it is assumed that indexing of a given batch (from the first doc to
 * the hard commit, sent by the client or added automatically in {@link
 * org.apache.solr.servlet.HttpSolrCall#addCommitIfAbsent}) is done by a single thread.
 */
public class ZeroStoreUpdateProcessor extends DistributedZkUpdateProcessor {
  private ZeroCoreIndexingBatchProcessor zeroCoreIndexingBatchProcessor;

  public ZeroStoreUpdateProcessor(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {

    if (isReadOnly()) {
      throw new SolrException(
          SolrException.ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }

    if (!cmd.softCommit) {
      doLocalCommit(cmd);
      getZeroCoreIndexingBatchProcessor().hardCommitCompletedLocally();
    }
  }

  /**
   * Override the behavior of setupRequest so that it rejects an update if the target shard is
   * splitting
   */
  @Override
  protected List<SolrCmdDistributor.Node> setupRequest(
      String id, SolrInputDocument doc, String route, UpdateCommand cmd) {
    DocCollection coll = clusterState.getCollection(cloudDesc.getCollectionName());
    Slice slice = getTargetSlice(coll, id, doc, route);

    rejectUpdateIfSplitting(coll, slice);

    return super.setupRequest(id, doc, route, cmd);
  }

  @Override
  protected void postSetupHook() {
    // this should be called after setupRequest and before the doc is indexed locally
    // because it needs to know whether current replica is leader or subShardLeader for the doc or
    // not
    if (willBeIndexedLocally()) {
      getZeroCoreIndexingBatchProcessor().addOrDeleteGoingToBeIndexedLocally();
    }
  }

  /**
   * Override the behavior of doDeleteByQuery so that it rejects an update if any shard in the
   * collection is splitting
   */
  @Override
  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    DocCollection coll = clusterState.getCollection(cloudDesc.getCollectionName());

    // This will reject the update if any shard in the collection is currently being split
    rejectIfConstructing(coll.getSlices());
    super.doDeleteByQuery(cmd);
  }

  /** Reject the update if the target shard is splitting. */
  private void rejectUpdateIfSplitting(DocCollection coll, Slice shard) {
    // Find the sub shards, if any
    Set<Slice> subShards =
        coll.getSlices().stream()
            .filter(s -> (s.getParent() != null && s.getParent().equals(shard.getName())))
            .collect(Collectors.toSet());
    if (!subShards.isEmpty()) {
      // Reject if they are in construction or recovery state
      rejectIfConstructing(subShards);
    }
  }

  /**
   * Reject the update if any of the shards in the provided collection are in construction or
   * recovery state, indicating that the parent is splitting. This is because the transaction log is
   * not trusted to work (nodes are stateless) so we avoid using it.
   *
   * <p>Do note though that there is a related issue, see comment at the end of constructor of
   * {@link ZeroCoreIndexingBatchProcessor#ZeroCoreIndexingBatchProcessor}
   *
   * @param shards The collection of shards to check for construction or recovery state
   */
  private void rejectIfConstructing(Collection<Slice> shards) {

    Set<String> splittingShards =
        shards.stream()
            .filter(
                s ->
                    (s.getState() == Slice.State.CONSTRUCTION
                        || s.getState() == Slice.State.RECOVERY))
            .map(s -> s.getParent())
            .collect(Collectors.toSet());

    for (String splittingShard : splittingShards) {
      // To avoid potential deadlock in the case of catastrophic Overseer failure during a split, it
      // is necessary
      // to verify that the split lock exists, and that the split is actually "in progress"
      try {
        if (SplitShardCmd.shardSplitLockHeld(
            zkController, cloudDesc.getCollectionName(), splittingShard)) {
          throw new SolrException(
              SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Rejecting updates to shard " + splittingShard + " while a split is in progress.");
        }
      } catch (SolrException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVICE_UNAVAILABLE,
            "Rejecting updates to shard "
                + splittingShard
                + ", can't check if a split is in progress.",
            e);
      }
    }
  }

  @Override
  protected void doClose() {
    if (zeroCoreIndexingBatchProcessor != null) {
      zeroCoreIndexingBatchProcessor.close();
    }
    super.doClose();
  }

  private boolean willBeIndexedLocally() {
    // forwardToLeader: if true, then the update is going to be forwarded to its rightful leader.
    // The doc being added or deleted might not even belong to the current core's (req.getCore())
    // shard.
    // isLeader: if true, then the current core (req.getCore()) is the leader of the shard to which
    // the doc being added or deleted belongs to.
    // For ZERO replicas only leader replicas do local indexing. Follower ZERO replicas do not do
    // any local indexing and forward the add/delete updates to the leader replica.
    // isSubShardLeader: if true, then the current core (req.getCore()) is the leader of a sub shard
    // being built (why is this rejected then? Should be rejected if the subshard is not active yet
    // but logically it would end up being indexed locally).
    //
    // Therefore, only the leader replicas of the ZERO active shards will process docs locally and
    // thus need to pull/push from the Zero store during indexing.
    return !forwardToLeader && isLeader && !isSubShardLeader;
  }

  private ZeroCoreIndexingBatchProcessor getZeroCoreIndexingBatchProcessor() {
    assert Type.ZERO.equals(replicaType);

    if (zeroCoreIndexingBatchProcessor == null) {
      updateCoreIndexingBatchProcessor(
          new ZeroCoreIndexingBatchProcessor(req.getCore(), clusterState, rsp));
    }
    return zeroCoreIndexingBatchProcessor;
  }

  @VisibleForTesting
  public void updateCoreIndexingBatchProcessor(
      ZeroCoreIndexingBatchProcessor zeroCoreIndexingBatchProcessor) {
    this.zeroCoreIndexingBatchProcessor = zeroCoreIndexingBatchProcessor;
  }
}
