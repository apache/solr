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

import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CommitOptions;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollection.CollectionStateProps;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains helper methods used by commands of the Collection API. Previously these
 * methods were in {@link OverseerCollectionMessageHandler} and were refactored out to (eventually)
 * allow Collection API commands to be executed outside the context of the Overseer.
 */
public class CollectionHandlingUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NUM_SLICES = "numShards";

  public static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE =
      CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  public static final String SHARDS_PROP = "shards";

  public static final String REQUESTID = "requestid";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  public static final String SHARD_UNIQUE = "shardUnique";

  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  // Immutable Maps are null-hostile, so build our own
  public static final Map<String, Object> COLLECTION_PROPS_AND_DEFAULTS =
      Collections.unmodifiableMap(makeCollectionPropsAndDefaults());

  private static Map<String, Object> makeCollectionPropsAndDefaults() {
    Map<String, Object> propsAndDefaults =
        Utils.makeMap(
            CollectionStateProps.DOC_ROUTER,
            (Object) DocRouter.DEFAULT_NAME,
            CollectionStateProps.REPLICATION_FACTOR,
            "1",
            CollectionStateProps.PER_REPLICA_STATE,
            null);
    for (Replica.Type replicaType : Replica.Type.values()) {
      propsAndDefaults.put(
          replicaType.numReplicasPropertyName,
          replicaType == Replica.Type.defaultType() ? "1" : "0");
    }
    return propsAndDefaults;
  }

  /** Returns names of properties that are used to specify a number of replicas of a given type. */
  public static Set<String> numReplicasProperties() {
    return Arrays.stream(Replica.Type.values())
        .map(t -> t.numReplicasPropertyName)
        .collect(Collectors.toSet());
  }

  /** Returns replica types that are eligible to be leader. */
  public static EnumSet<Replica.Type> leaderEligibleReplicaTypes() {
    return Arrays.stream(Replica.Type.values())
        .filter(t -> t.leaderEligible)
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(Replica.Type.class)));
  }

  static boolean waitForCoreNodeGone(
      String collectionName,
      String shard,
      String replicaName,
      int timeoutms,
      ZkStateReader zkStateReader)
      throws InterruptedException {
    try {
      zkStateReader.waitForState(
          collectionName,
          timeoutms,
          TimeUnit.MILLISECONDS,
          (c) -> {
            if (c == null) return true;
            Slice slice = c.getSlice(shard);
            if (slice == null || slice.getReplica(replicaName) == null) {
              return true;
            }
            return false;
          });
    } catch (TimeoutException e) {
      return false;
    }

    return true;
  }

  static void deleteCoreNode(
      String collectionName,
      String replicaName,
      Replica replica,
      String core,
      CollectionCommandContext ccc)
      throws Exception {
    ZkNodeProps m =
        new ZkNodeProps(
            Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
            ZkStateReader.CORE_NAME_PROP, core,
            ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
            ZkStateReader.COLLECTION_PROP, collectionName,
            ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      ccc.getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.SliceRemoveReplica,
              m,
              ccc.getSolrCloudManager(),
              ccc.getZkStateReader());
    } else {
      ccc.offerStateUpdate(m);
    }
  }

  static void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if (message.get(prop) == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            StrUtils.join(Arrays.asList(props), ',') + " are required params");
      }
    }
  }

  static void checkResults(String label, NamedList<Object> results, boolean failureIsFatal)
      throws SolrException {
    Object failure = results.get("failure");
    if (failure == null) {
      failure = results.get("error");
    }
    if (failure != null) {
      String msg = "Error: " + label + ": " + Utils.toJSONString(results);
      if (failureIsFatal) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      } else {
        log.error(msg);
      }
    }
  }

  static void commit(
      HttpJettySolrClient solrClient,
      NamedList<Object> results,
      String slice,
      Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse =
          softCommit(solrClient, parentShardLeader.getBaseUrl(), parentShardLeader.getCoreName());
      CollectionHandlingUtils.processResponse(
          results,
          null,
          parentShardLeader.getNodeName(),
          parentShardLeader.getName(),
          updateResponse,
          slice,
          Collections.emptySet(),
          null);
    } catch (Exception e) {
      CollectionHandlingUtils.processResponse(
          results,
          e,
          parentShardLeader.getNodeName(),
          parentShardLeader.getCoreName(),
          updateResponse,
          slice,
          Collections.emptySet(),
          null);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unable to call distrib softCommit on: " + parentShardLeader.getCoreUrl(),
          e);
    }
  }

  private static UpdateResponse softCommit(
      HttpJettySolrClient solrClient, String baseUrl, String coreName)
      throws SolrServerException, IOException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, CommitOptions.forSoftCommit());
    return ureq.processWithBaseUrl(solrClient, baseUrl, coreName);
  }

  public static String waitForCoreNodeName(
      String collectionName, String msgNodeName, String msgCore, ZkStateReader zkStateReader) {
    try {
      DocCollection collection =
          zkStateReader.waitForState(
              collectionName,
              320,
              TimeUnit.SECONDS,
              c -> ClusterStateMutator.getAssignedCoreNodeName(c, msgNodeName, msgCore) != null);
      return ClusterStateMutator.getAssignedCoreNodeName(collection, msgNodeName, msgCore);
    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed waiting for coreNodeName", e);
    }
  }

  static ClusterState waitForNewShard(
      String collectionName, String sliceName, ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    try {
      zkStateReader.waitForState(
          collectionName,
          320,
          TimeUnit.SECONDS,
          c -> {
            return c != null && c.getSlice(sliceName) != null;
          });
    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Failed waiting for new slice", e);
    }
    return zkStateReader.getClusterState();
  }

  static void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  static void addPropertyParams(ZkNodeProps message, Map<String, Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(CollectionAdminParams.PROPERTY_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }

  static void cleanupCollection(
      AdminCmdContext adminCmdContext,
      String collectionName,
      NamedList<Object> results,
      CollectionCommandContext ccc)
      throws Exception {
    log.error("Cleaning up collection [{}].", collectionName);
    new DeleteCollectionCmd(ccc)
        .call(
            adminCmdContext.subRequestContext(DELETE),
            new ZkNodeProps(NAME, collectionName),
            results);
  }

  static Map<String, Replica> waitToSeeReplicasInState(
      ZkStateReader zkStateReader,
      TimeSource timeSource,
      String collectionName,
      Collection<String> coreNames)
      throws InterruptedException {
    assert coreNames.size() > 0;
    Map<String, Replica> results = new ConcurrentHashMap<>();

    long maxWait =
        Long.getLong("solr.waitToSeeReplicasInStateTimeoutSeconds", 120); // could be a big cluster
    try {
      zkStateReader.waitForState(
          collectionName,
          maxWait,
          TimeUnit.SECONDS,
          c -> {
            if (c == null) return false;

            // We write into a ConcurrentHashMap, which will be ok if called multiple times by
            // multiple threads
            c.getSlices().stream()
                .flatMap(slice -> slice.getReplicas().stream())
                .filter(
                    r ->
                        coreNames.contains(
                            r.getCoreName())) // Only the elements that were asked for...
                .forEach(r -> results.putIfAbsent(r.getCoreName(), r)); // ...get added to the map

            log.debug("Expecting {} cores, found {}", coreNames, results);
            return results.size() == coreNames.size();
          });
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
    }

    return results;
  }

  static void cleanBackup(
      BackupRepository repository, URI backupUri, BackupId backupId, CollectionCommandContext ccc)
      throws Exception {
    new DeleteBackupCmd(ccc)
        .deleteBackupIds(backupUri, repository, Collections.singleton(backupId), new NamedList<>());
  }

  static void deleteBackup(
      BackupRepository repository,
      URI backupPath,
      int maxNumBackup,
      NamedList<Object> results,
      CollectionCommandContext ccc)
      throws Exception {
    new DeleteBackupCmd(ccc).keepNumberOfBackup(repository, backupPath, maxNumBackup, results);
  }

  static void validateConfigOrThrowSolrException(
      ConfigSetService configSetService, String configName) throws IOException {
    boolean isValid = configSetService.checkConfigExists(configName);
    if (!isValid) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Can not find the specified config set: " + configName);
    }
  }

  /**
   * Send request to all replicas of a collection
   *
   * @return List of replicas which is not live for receiving the request
   */
  static List<Replica> collectionCmd(
      AdminCmdContext adminCmdContext,
      ZkNodeProps message,
      ModifiableSolrParams params,
      NamedList<Object> results,
      Replica.State stateMatcher,
      Set<String> okayExceptions,
      CollectionCommandContext ccc) {
    if (log.isInfoEnabled()) {
      log.info("Executing Collection Cmd={}, asyncId={}", params, adminCmdContext.getAsyncId());
    }
    String collectionName = message.getStr(NAME);
    ShardHandler shardHandler = ccc.newShardHandler();
    DocCollection coll = ccc.getZkStateReader().getClusterState().getCollection(collectionName);
    List<Replica> notLivesReplicas = new ArrayList<>();
    final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker =
        asyncRequestTracker(adminCmdContext, ccc);
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(
          shardRequestTracker.sliceCmd(
              ccc.getZkStateReader().getClusterState(), params, stateMatcher, slice, shardHandler));
    }

    shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);
    return notLivesReplicas;
  }

  static void processResponse(
      NamedList<Object> results, ShardResponse srsp, Set<String> okayExceptions, String asyncId) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    // Use core or coreNodeName if given as a param, otherwise use nodeName
    String coreNodeName = srsp.getShardRequest().coreNodeName;
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(
        results, e, nodeName, coreNodeName, solrResponse, shard, okayExceptions, asyncId);
  }

  static void processResponse(
      NamedList<Object> results,
      Throwable e,
      String nodeName,
      String coreNodeName,
      SolrResponse solrResponse,
      String shard,
      Set<String> okayExceptions,
      String asyncId) {
    String rootThrowable = null;
    if (e instanceof RemoteSolrException remoteSolrException) {
      rootThrowable = remoteSolrException.getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: {}", shard, e);
      addFailure(results, nodeName, coreNodeName, e);
    } else if (asyncId == null) {
      // Do not add a success for async requests, that will be done when the async result is found
      addSuccess(results, nodeName, coreNodeName, solrResponse.getResponse());
    }
  }

  static void logFailedOperation(final Object operation, final Exception e, final String collName) {
    if (collName == null) {
      log.error("Operation {} failed", operation, e);
    } else {
      log.error("Collection {}, operation {} failed", collName, operation, e);
    }
  }

  /***
   * Creates a SimpleOrderedMap with the exception details and adds it to the results
   */
  public static void addExceptionToNamedList(
      final Object operation, final Exception e, final NamedList<Object> results) {
    results.add("Operation " + operation + " caused exception:", e);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
    nl.add("msg", e.getMessage());
    nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
    results.add("exception", nl);
  }

  public static String requestKey(Replica replica) {
    return requestKey(replica.getNodeName(), replica.getName());
  }

  public static String requestKey(String nodeName, String coreNodeName) {
    if (coreNodeName == null) {
      return nodeName;
    } else {
      return nodeName + "/" + coreNodeName;
    }
  }

  private static void addFailure(
      NamedList<Object> results, String nodeName, String coreNodeName, Object value) {
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> failure = (SimpleOrderedMap<Object>) results.get("failure");
    if (failure == null) {
      failure = new SimpleOrderedMap<>();
      results.add("failure", failure);
    }
    failure.add(requestKey(nodeName, coreNodeName), value);
  }

  private static void addSuccess(
      NamedList<Object> results, String nodeName, String coreNodeName, Object value) {
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> success = (SimpleOrderedMap<Object>) results.get("success");
    if (success == null) {
      success = new SimpleOrderedMap<>();
      results.add("success", success);
    }
    success.add(requestKey(nodeName, coreNodeName), value);
  }

  private static NamedList<Object> waitForCoreAdminAsyncCallToComplete(
      ShardHandlerFactory shardHandlerFactory,
      String adminPath,
      ZkStateReader zkStateReader,
      String nodeName,
      String coreNodeName,
      String requestId) {
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;
      sreq.nodeName = nodeName;
      sreq.coreNodeName = coreNodeName;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          if (srsp.getSolrResponse().getResponse() == null) {
            NamedList<Object> response = new NamedList<>();
            response.add("STATUS", "failed");
            return response;
          }

          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r == null) {
            // For Collections API Calls
            r = (String) srsp.getSolrResponse().getResponse()._get("status/state");
          }
          if (r == null) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Could not find status of async command in response: "
                    + srsp.getSolrResponse().getResponse().toString());
          }
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("submitted")) {
            log.debug("The task is still SUBMITTED, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              break;
            }
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Invalid status request for requestId: '"
                    + requestId
                    + "' - '"
                    + srsp.getSolrResponse().getResponse().get("STATUS")
                    + "'. Retried "
                    + counter
                    + " times");
          } else {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while (true);
  }

  public static ShardRequestTracker syncRequestTracker(
      AdminCmdContext adminCmdContext, CollectionCommandContext ccc) {
    return syncRequestTracker(adminCmdContext, ccc.getAdminPath(), ccc);
  }

  public static ShardRequestTracker syncRequestTracker(
      AdminCmdContext adminCmdContext, String adminPath, CollectionCommandContext ccc) {
    return requestTracker(null, adminPath, ccc);
  }

  public static ShardRequestTracker asyncRequestTracker(
      AdminCmdContext adminCmdContext, CollectionCommandContext ccc) {
    return asyncRequestTracker(adminCmdContext, ccc.getAdminPath(), ccc);
  }

  public static ShardRequestTracker asyncRequestTracker(
      AdminCmdContext adminCmdContext, String adminPath, CollectionCommandContext ccc) {
    return requestTracker(adminCmdContext.getAsyncId(), adminPath, ccc);
  }

  protected static ShardRequestTracker requestTracker(
      String asyncId, String adminPath, CollectionCommandContext ccc) {
    return new ShardRequestTracker(
        asyncId, adminPath, ccc.getZkStateReader(), ccc.newShardHandler().getShardHandlerFactory());
  }

  public static class ShardRequestTracker {
    private final String asyncId;
    private final String adminPath;
    private final ZkStateReader zkStateReader;
    private final ShardHandlerFactory shardHandlerFactory;
    private final List<AsyncCmdInfo> shardAsyncCmds = new ArrayList<>();

    public ShardRequestTracker(
        String asyncId,
        String adminPath,
        ZkStateReader zkStateReader,
        ShardHandlerFactory shardHandlerFactory) {
      this.asyncId = asyncId;
      this.adminPath = adminPath;
      this.zkStateReader = zkStateReader;
      this.shardHandlerFactory = shardHandlerFactory;
    }

    /**
     * Send request to all replicas of a slice
     *
     * @return List of replicas which is not live for receiving the request
     */
    public List<Replica> sliceCmd(
        ClusterState clusterState,
        ModifiableSolrParams params,
        Replica.State stateMatcher,
        Slice slice,
        ShardHandler shardHandler) {
      List<Replica> notLiveReplicas = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        if ((stateMatcher == null
            || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
          if (clusterState.liveNodesContain(replica.getNodeName())) {
            // For thread safety, only simple clone the ModifiableSolrParams
            ModifiableSolrParams cloneParams = new ModifiableSolrParams();
            cloneParams.add(params);
            cloneParams.set(CoreAdminParams.CORE, replica.getCoreName());

            sendShardRequest(replica.getNodeName(), replica.getName(), cloneParams, shardHandler);
          } else {
            notLiveReplicas.add(replica);
          }
        }
      }
      return notLiveReplicas;
    }

    public void sendShardRequest(
        Replica replica, ModifiableSolrParams params, ShardHandler shardHandler) {
      sendShardRequest(
          replica.getNodeName(), replica.getName(), params, shardHandler, adminPath, zkStateReader);
    }

    public void sendShardRequest(
        String nodeName,
        String coreNodeName,
        ModifiableSolrParams params,
        ShardHandler shardHandler) {
      sendShardRequest(nodeName, coreNodeName, params, shardHandler, adminPath, zkStateReader);
    }

    public void sendShardRequest(
        String nodeName,
        String coreNodeName,
        ModifiableSolrParams params,
        ShardHandler shardHandler,
        String adminPath,
        ZkStateReader zkStateReader) {
      if (asyncId != null) {
        String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
        params.set(ASYNC, coreAdminAsyncId);
        // Track async requests
        shardAsyncCmds.add(AsyncCmdInfo.from(nodeName, coreNodeName, coreAdminAsyncId));
      }

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.nodeName = nodeName;
      sreq.coreNodeName = coreNodeName;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);
    }

    void processResponses(
        NamedList<Object> results,
        ShardHandler shardHandler,
        boolean abortOnError,
        String msgOnError) {
      processResponses(results, shardHandler, abortOnError, msgOnError, Collections.emptySet());
    }

    void processResponses(
        NamedList<Object> results,
        ShardHandler shardHandler,
        boolean abortOnError,
        String msgOnError,
        Set<String> okayExceptions) {
      // Processes all shard responses
      ShardResponse srsp;
      do {
        srsp =
            abortOnError
                ? shardHandler.takeCompletedOrError()
                : shardHandler.takeCompletedIncludingErrors();
        if (srsp != null) {
          processResponse(results, srsp, okayExceptions, asyncId);
          Throwable exception = srsp.getException();
          if (abortOnError && exception != null) {
            // drain pending requests
            while (srsp != null) {
              srsp = shardHandler.takeCompletedOrError();
            }
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msgOnError, exception);
          }
        }
      } while (srsp != null);

      // If request is async wait for the core admin to complete before returning
      if (asyncId != null) {
        // TODO: Shouldn't we abort with msgOnError exception when failure?
        waitForAsyncCallsToComplete(results);
        shardAsyncCmds.clear();
      }
    }

    private void waitForAsyncCallsToComplete(NamedList<Object> results) {
      for (AsyncCmdInfo asyncCmdInfo : shardAsyncCmds) {
        Object failure =
            results._get("failure/" + requestKey(asyncCmdInfo.nodeName, asyncCmdInfo.coreNodeName));
        // Do not wait for Async calls that have already failed
        if (failure != null) {
          return;
        }
        final String node = asyncCmdInfo.nodeName;
        final String coreNodeName = asyncCmdInfo.coreNodeName;
        final String shardAsyncId = asyncCmdInfo.asyncId;
        log.info("I am Waiting for: {}/{}/{}", node, coreNodeName, shardAsyncId);
        NamedList<Object> reqResult =
            waitForCoreAdminAsyncCallToComplete(
                shardHandlerFactory, adminPath, zkStateReader, node, coreNodeName, shardAsyncId);
        String status = (String) reqResult.get("STATUS");
        if (status == null) {
          // For Collections API Calls
          status = (String) reqResult._get("status/state");
        }
        if ("failed".equalsIgnoreCase(status)) {
          log.error("Error from shard {}/{}: {}", node, coreNodeName, reqResult);
          addFailure(results, node, coreNodeName, reqResult);
        } else {
          addSuccess(results, node, coreNodeName, reqResult);
        }
      }
    }
  }

  private static class AsyncCmdInfo {
    protected final String nodeName;
    protected final String coreNodeName;
    protected final String asyncId;

    public AsyncCmdInfo(String nodeName, String coreNodeName, String asyncId) {
      this.nodeName = nodeName;
      this.coreNodeName = coreNodeName;
      this.asyncId = asyncId;
    }

    public static AsyncCmdInfo from(String nodeName, String coreNodeName, String asyncId) {
      return new AsyncCmdInfo(nodeName, coreNodeName, asyncId);
    }
  }
}
