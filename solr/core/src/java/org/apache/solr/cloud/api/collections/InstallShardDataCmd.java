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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overseer processing for the "install shard data" API.
 *
 * <p>Largely this overseer processing consists of ensuring that read-only mode is enabled for the
 * specified collection, identifying the core hosting the shard leader, and sending it a core- admin
 * 'install' request.
 */
public class InstallShardDataCmd implements CollApiCmds.CollectionApiCommand {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;

  public InstallShardDataCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void call(
      ClusterState state, ZkNodeProps message, String lockId, NamedList<Object> results)
      throws Exception {
    final RemoteMessage typedMessage =
        new ObjectMapper().convertValue(message.getProperties(), RemoteMessage.class);
    final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker =
        CollectionHandlingUtils.asyncRequestTracker(typedMessage.asyncId, ccc);
    final ClusterState clusterState = ccc.getZkStateReader().getClusterState();
    typedMessage.validate();

    // Fetch the specified Slice
    final DocCollection installCollection = clusterState.getCollection(typedMessage.collection);
    final Slice installSlice = installCollection.getSlice(typedMessage.shard);
    if (installSlice == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The specified shard [" + typedMessage.shard + "] does not exist.");
    }

    // Build the core-admin request
    final ModifiableSolrParams coreApiParams = new ModifiableSolrParams();
    coreApiParams.set(
        CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RESTORECORE.toString());
    coreApiParams.set(CoreAdminParams.BACKUP_LOCATION, typedMessage.location);
    coreApiParams.set(CoreAdminParams.BACKUP_REPOSITORY, typedMessage.repository);
    coreApiParams.set(CoreAdminParams.NAME, typedMessage.name);
    coreApiParams.set(CoreAdminParams.SHARD_BACKUP_ID, typedMessage.shardBackupId);

    // Send the core-admin request to each replica in the slice
    final ShardHandler shardHandler = ccc.newShardHandler();
    List<Replica> notLiveReplicas =
        shardRequestTracker.sliceCmd(clusterState, coreApiParams, null, installSlice, shardHandler);
    final String errorMessage =
        String.format(
            Locale.ROOT,
            "Could not install data to collection [%s] and shard [%s] on any leader-eligible replicas",
            typedMessage.collection,
            typedMessage.shard);
    shardRequestTracker.processResponses(results, shardHandler, false, errorMessage);
    Collection<Replica> allReplicas =
        clusterState
            .getCollection(typedMessage.collection)
            .getSlice(typedMessage.shard)
            .getReplicas();

    // Ensure that terms are correct for this shard after the execution is done
    // We only care about leader eligible replicas, all others will eventually get updated.
    List<Replica> leaderEligibleReplicas =
        allReplicas.stream().filter(r -> r.getType().leaderEligible).collect(Collectors.toList());

    NamedList<Object> failures = (NamedList<Object>) results.get("failure");
    Set<Replica> successfulReplicas =
        leaderEligibleReplicas.stream()
            .filter(replica -> !notLiveReplicas.contains(replica))
            .filter(
                replica ->
                    failures == null
                        || failures.get(CollectionHandlingUtils.requestKey(replica)) == null)
            .collect(Collectors.toSet());

    if (successfulReplicas.isEmpty()) {
      // No leader-eligible replicas succeeded, return failure
      if (failures == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            errorMessage + ". No leader-eligible replicas are live.");
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, errorMessage, (Throwable) failures.getVal(0));
      }
    } else if (successfulReplicas.size() < leaderEligibleReplicas.size()) {
      // Some, but not all, leader-eligible replicas succeeded.
      // Ensure the shard terms are correct so that the non-successful replicas go into recovery
      ZkShardTerms shardTerms =
          ccc.getCoreContainer()
              .getZkController()
              .getShardTerms(typedMessage.collection, typedMessage.shard);
      shardTerms.ensureHighestTerms(
          successfulReplicas.stream().map(Replica::getName).collect(Collectors.toSet()));
      Set<String> replicasToRecover =
          leaderEligibleReplicas.stream()
              .filter(r -> !successfulReplicas.contains(r))
              .map(Replica::getName)
              .collect(Collectors.toSet());
      ccc.getZkStateReader()
          .waitForState(
              typedMessage.collection,
              10,
              TimeUnit.SECONDS,
              (liveNodes, collectionState) ->
                  collectionState.getSlice(typedMessage.shard).getReplicas().stream()
                      .filter(r -> replicasToRecover.contains(r.getName()))
                      .allMatch(r -> Replica.State.RECOVERING.equals(r.getState())));

      // In order for the async request to succeed, we need to ensure that there is no failure
      // message
      NamedList<Object> successes = (NamedList<Object>) results.get("success");
      failures.forEach(
          (replicaKey, value) -> {
            successes.add(
                replicaKey,
                new NamedList<>(
                    Map.of(
                        "explanation",
                        "Core install failed, but is now recovering from the leader",
                        "failure",
                        value)));
          });
      results.remove("failure");
    } else {
      // other replicas to-be-created will know that they are out of date by
      // looking at their term : 0 compare to term of this core : 1
      ccc.getCoreContainer()
          .getZkController()
          .getShardTerms(typedMessage.collection, typedMessage.shard)
          .ensureHighestTermsAreNotZero();
    }
  }

  /*
  public static void handleCoreRestoreResponses(
      CoreContainer coreContainer,
      String collection,
      List<String> shards,
      NamedList<Object> results,
      List<Replica> notLiveReplicas,
      String errorMessage) {
    DocCollection collectionState = coreContainer.getZkController().getZkStateReader().getCollectionLive(collection);
    Collection<Replica> allReplicas =
        shards.stream()
            .flatMap(shard -> collectionState.getSlice(shard).getReplicas().stream())
            .toList();

    // Ensure that terms are correct for this shard after the execution is done
    // We only care about leader eligible replicas, all others will eventually get updated.
    List<Replica> leaderEligibleReplicas =
        allReplicas.stream()
            .filter(r -> r.getType().leaderEligible)
            .toList();

    NamedList<Object> failures = (NamedList<Object>) results.get("failure");
    Set<Replica> successfulReplicas =
        leaderEligibleReplicas.stream()
            .filter(replica -> !notLiveReplicas.contains(replica))
            .filter(
                replica ->
                    failures == null
                        || failures.get(CollectionHandlingUtils.requestKey(replica)) == null)
            .collect(Collectors.toSet());

    if (successfulReplicas.isEmpty()) {
      // No leader-eligible replicas succeeded, return failure
      if (failures == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            errorMessage + ". No leader-eligible replicas are live.");
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, errorMessage, (Throwable) failures.getVal(0));
      }
    } else if (successfulReplicas.size() < leaderEligibleReplicas.size()) {
      // Some, but not all, leader-eligible replicas succeeded.
      // Ensure the shard terms are correct so that the non-successful replicas go into recovery
      ZkShardTerms shardTerms =
          coreContainer
              .getZkController()
              .getShardTerms(typedMessage.collection, typedMessage.shard);
      shardTerms.ensureHighestTerms(
          successfulReplicas.stream().map(Replica::getName).collect(Collectors.toSet()));
      Set<String> replicasToRecover =
          leaderEligibleReplicas.stream()
              .filter(r -> !successfulReplicas.contains(r))
              .map(Replica::getName)
              .collect(Collectors.toSet());
      coreContainer
          .getZkController()
          .getZkStateReader()
          .waitForState(
              collection,
              10,
              TimeUnit.SECONDS,
              (liveNodes, colState) ->
                  colState.getSlice(typedMessage.shard).getReplicas().stream()
                      .filter(r -> replicasToRecover.contains(r.getName()))
                      .allMatch(r -> Replica.State.RECOVERING.equals(r.getState())));

      // In order for the async request to succeed, we need to ensure that there is no failure
      // message
      NamedList<Object> successes = (NamedList<Object>) results.get("success");
      failures.forEach(
          (replicaKey, value) -> {
            successes.add(
                replicaKey,
                new NamedList<>(
                    Map.of(
                        "explanation",
                        "Core install failed, but is now recovering from the leader",
                        "failure",
                        value)));
          });
      results.remove("failure");
    } else {
      // other replicas to-be-created will know that they are out of date by
      // looking at their term : 0 compare to term of this core : 1
      coreContainer
          .getZkController()
          .getShardTerms(collection, typedMessage.shard)
          .ensureHighestTermsAreNotZero();
    }
  }
  */

  /** A value-type representing the message received by {@link InstallShardDataCmd} */
  public static class RemoteMessage implements JacksonReflectMapWriter {

    @JsonProperty public String callingLockId;

    @JsonProperty(QUEUE_OPERATION)
    public String operation = CollectionParams.CollectionAction.INSTALLSHARDDATA.toLower();

    @JsonProperty public String collection;

    @JsonProperty public String shard;

    @JsonProperty public String repository;

    @JsonProperty public String location;

    @JsonProperty public String name = "";

    @JsonProperty public String shardBackupId;

    @JsonProperty(ASYNC)
    public String asyncId;

    public void validate() {
      if (StrUtils.isBlank(collection)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The 'Install Shard Data' API requires a valid collection name to be provided");
      }
      if (StrUtils.isBlank(shard)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The 'Install Shard Data' API requires a valid shard name to be provided");
      }
    }
  }
}
