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


import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.component.ShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_TYPE;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

public class RestoreCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public RestoreCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public AddReplicaCmd.Response call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    // TODO maybe we can inherit createCollection's options/code

    String restoreCollectionName = message.getStr(COLLECTION_PROP);
    String backupName = message.getStr(NAME); // of backup
    ShardHandler shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);
    String asyncId = message.getStr(ASYNC);
    String repo = message.getStr(CoreAdminParams.BACKUP_REPOSITORY);

    CoreContainer cc = ocmh.overseer.getCoreContainer();
    BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo));

    URI location = repository.createURI(message.getStr(CoreAdminParams.BACKUP_LOCATION));
    URI backupPath = repository.resolve(location, backupName);
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    BackupManager backupMgr = new BackupManager(repository, zkStateReader);

    Properties properties = backupMgr.readBackupProperties(location, backupName);
    String backupCollection = properties.getProperty(BackupManager.COLLECTION_NAME_PROP);

    // Test if the collection is of stateFormat 1 (i.e. not 2) supported pre Solr 9, in which case can't restore it.
    Object format = properties.get("stateFormat");
    if (format != null && !"2".equals(format)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection " + backupCollection + " is in stateFormat=" + format +
          " no longer supported in Solr 9 and above. It can't be restored. If it originates in Solr 8 you can restore" +
          " it there, migrate it to stateFormat=2 and backup again, it will then be restorable on Solr 9");
    }
    String backupCollectionAlias = properties.getProperty(BackupManager.COLLECTION_ALIAS_PROP);
    DocCollection backupCollectionState = backupMgr.readCollectionState(location, backupName, backupCollection);

    // Get the Solr nodes to restore a collection.
    final List<String> nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(
        zkStateReader.getLiveNodes(), message, OverseerCollectionMessageHandler.RANDOM);

    int numShards = backupCollectionState.getActiveSlices().size();

    int numNrtReplicas;
    if (message.get(REPLICATION_FACTOR) != null) {
      numNrtReplicas = message.getInt(REPLICATION_FACTOR, 0);
    } else if (message.get(NRT_REPLICAS) != null) {
      numNrtReplicas = message.getInt(NRT_REPLICAS, 0);
    } else {
      //replicationFactor and nrtReplicas is always in sync after SOLR-11676
      //pick from cluster state of the backed up collection
      // NOTE: getReplicas(EnumSet) returns the count across ALL shards; the restore needs the
      // PER-SHARD count, so divide by numShards. (The String overload getReplicas(String) matches by
      // node name and always returned empty, which is why this previously needed fixing.)
      numNrtReplicas = backupCollectionState.getReplicas(EnumSet.of(Replica.Type.NRT)).size() / numShards;
    }
    // tlog/pull: honor explicit request values, otherwise derive the per-shard count from the backup
    // (mirrors the NRT derivation above). Without this, an un-parameterized restore of a collection
    // that had TLOG/PULL replicas would silently recreate it with zero of them.
    int numTlogReplicas = message.get(TLOG_REPLICAS) != null
        ? message.getInt(TLOG_REPLICAS, 0)
        : backupCollectionState.getReplicas(EnumSet.of(Replica.Type.TLOG)).size() / numShards;
    int numPullReplicas = message.get(PULL_REPLICAS) != null
        ? message.getInt(PULL_REPLICAS, 0)
        : backupCollectionState.getReplicas(EnumSet.of(Replica.Type.PULL)).size() / numShards;
    int totalReplicasPerShard = numNrtReplicas + numTlogReplicas + numPullReplicas;
    assert totalReplicasPerShard > 0;

    int availableNodeCount = nodeList.size();

    //Upload the configs
    String configName = (String) properties.get(CollectionAdminParams.COLL_CONF);
    String restoreConfigName = message.getStr(CollectionAdminParams.COLL_CONF, configName);
    if (zkStateReader.getConfigManager().configExists(restoreConfigName)) {
      log.info("Using existing config {}", restoreConfigName);
      //TODO add overwrite option?
    } else {
      log.info("Uploading config {}", restoreConfigName);
      backupMgr.uploadConfigDir(location, backupName, configName, restoreConfigName);
    }

    log.info("Starting restore into collection={} with backup_name={} at location={}", restoreCollectionName, backupName,
        location);

    //Create core-less collection
    {
      Object2ObjectMap<String, Object> propMap = new Object2ObjectLinkedOpenHashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATE.toString());
      propMap.put("fromApi", "true"); // mostly true.  Prevents autoCreated=true in the collection state.
      propMap.put(REPLICATION_FACTOR, numNrtReplicas);
      propMap.put(NRT_REPLICAS, numNrtReplicas);
      propMap.put(TLOG_REPLICAS, numTlogReplicas);
      propMap.put(PULL_REPLICAS, numPullReplicas);

      // inherit settings from input API, defaulting to the backup's setting.  Ex: replicationFactor
      for (String collProp : OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.keySet()) {
        Object val = message.getProperties().getOrDefault(collProp, backupCollectionState.get(collProp));
        if (val != null && propMap.get(collProp) == null) {
          propMap.put(collProp, val);
        }
      }

      propMap.put(NAME, restoreCollectionName);
      propMap.put(ZkStateReader.CREATE_NODE_SET, ZkStateReader.CREATE_NODE_SET_EMPTY); //no cores
      propMap.put(CollectionAdminParams.COLL_CONF, restoreConfigName);

      // router.*
      Map<String, Object> routerProps = (Map<String, Object>) backupCollectionState.get(DocCollection.DOC_ROUTER);
      for (Map.Entry<String, Object> pair : routerProps.entrySet()) {
        propMap.put(DocCollection.DOC_ROUTER + "." + pair.getKey(), pair.getValue());
      }

      Set<String> sliceNames = backupCollectionState.getActiveSlicesMap().keySet();
      if (backupCollectionState.getRouter() instanceof ImplicitDocRouter) {
        propMap.put(OverseerCollectionMessageHandler.SHARDS_PROP, StrUtils.join(sliceNames, ','));
      } else {
        propMap.put(ZkStateReader.NUM_SHARDS_PROP, sliceNames.size());
        // ClusterStateMutator.createCollection detects that "slices" is in fact a slice structure instead of a
        //   list of names, and if so uses this instead of building it.  We clear the replica list.
        Collection<Slice> backupSlices = backupCollectionState.getActiveSlices();
        Map<String, Slice> newSlices = new LinkedHashMap<>(backupSlices.size());
        for (Slice backupSlice : backupSlices) {
          newSlices.put(backupSlice.getName(),
              new Slice(backupSlice.getName(), Collections.emptyMap(), backupSlice.getProperties(), restoreCollectionName, -1)); // MRM TODO
        }
        propMap.put(OverseerCollectionMessageHandler.SHARDS_PROP, newSlices);
      }

      ocmh.commandMap.get(CREATE).call(zkStateReader.getClusterState(), new ZkNodeProps(propMap), new NamedList());
      // note: when createCollection() returns, the collection exists (no race)
    }

    // Restore collection properties
    backupMgr.uploadCollectionProperties(location, backupName, restoreCollectionName);

    DocCollection restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    //Mark all shards in CONSTRUCTION STATE while we restore the data
    {
      //TODO might instead createCollection accept an initial state?  Is there a race?
      Object2ObjectMap<String, Object> propMap = new Object2ObjectLinkedOpenHashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.CONSTRUCTION.toString());
      }
      propMap.put("id", restoreCollection.getId());
      ocmh.overseer.offerStateUpdate(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    // TODO how do we leverage the RULE / SNITCH logic in createCollection?

    ClusterState clusterState = zkStateReader.getClusterState();

    List<String> sliceNames = new ArrayList<>();
    restoreCollection.getSlices().forEach(x -> sliceNames.add(x.getName()));
    Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
        .forCollection(restoreCollectionName)
        .forShard(sliceNames)
        .assignNrtReplicas(numNrtReplicas)
        .assignTlogReplicas(numTlogReplicas)
        .assignPullReplicas(numPullReplicas)
        .onNodes(nodeList)
        .build();

    Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(ocmh.cloudManager);
    Assign.AssignStrategy assignStrategy = Assign.AssignStrategyFactory.create();
    List<ReplicaPosition> replicaPositions = assignStrategy.assign(ocmh.cloudManager, assignRequest);

    //Create one replica per shard and copy backed up data to it
    for (Slice slice : restoreCollection.getSlices()) {
      if (log.isInfoEnabled()) {
        log.info("Adding replica for shard={} collection={} ", slice.getName(), restoreCollection);
      }
      Object2ObjectMap<String, Object> propMap = new Object2ObjectLinkedOpenHashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD);
      propMap.put(COLLECTION_PROP, restoreCollectionName);
      propMap.put(SHARD_ID_PROP, slice.getName());

      if (numNrtReplicas >= 1) {
        propMap.put(REPLICA_TYPE, Replica.Type.NRT.name());
      } else if (numTlogReplicas >= 1) {
        propMap.put(REPLICA_TYPE, Replica.Type.TLOG.name());
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unexpected number of replicas, replicationFactor, " +
            Replica.Type.NRT + " or " + Replica.Type.TLOG + " must be greater than 0");
      }

      // Get the first node matching the shard to restore in
      String node;
      for (ReplicaPosition replicaPosition : replicaPositions) {
        if (Objects.equals(replicaPosition.shard, slice.getName())) {
          node = replicaPosition.node;
          propMap.put(CoreAdminParams.NODE, node);
          replicaPositions.remove(replicaPosition);
          break;
        }
      }

      // add async param
      if (asyncId != null) {
        propMap.put(ASYNC, asyncId);
      }
      OverseerCollectionMessageHandler.addPropertyParams(message, propMap);
      // Finalize so core CREATE actually runs and any failure (e.g. a bad config) is recorded in
      // results / thrown, letting us clean up the partially-created collection below.
      try {
        AddReplicaCmd.Response firstResp = ocmh.addReplicaWithResp(clusterState, new ZkNodeProps(propMap), results);
        if (firstResp != null && firstResp.clusterState != null) {
          clusterState = firstResp.clusterState;
        }
        if (firstResp != null && firstResp.asyncFinalRunner != null) {
          firstResp.asyncFinalRunner.call();
        }
      } catch (Exception e) {
        log.error("Restore failed to create initial replica for shard={} collection={}", slice.getName(), restoreCollectionName, e);
        ocmh.cleanupCollection(restoreCollectionName, new NamedList<Object>());
        throw e;
      }
    }

    Object failures = results.get("failure");
    if (failures != null && ((SimpleOrderedMap) failures).size() > 0) {
      log.error("Restore failed to create initial replicas.");
      ocmh.cleanupCollection(restoreCollectionName, new NamedList<Object>());
      return null;
    }

    //refresh the location copy of collection state
    // Wait briefly for the per-shard leader cores created above to register in cluster state before
    // sending RESTORECORE. Only ONE replica per shard exists at this point (the remaining replicas are
    // added after the data is restored), so we wait for numShards, not the full replica count. We do
    // NOT wait for ACTIVE — the shards are in CONSTRUCTION state and stay so until after the restore.
    int totalExpected = numShards;
    long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < waitDeadline) {
      DocCollection coll = zkStateReader.getClusterState().getCollectionOrNull(restoreCollectionName);
      if (coll != null) {
        int totalReplicas = coll.getReplicas().size();
        if (totalReplicas >= totalExpected) {
          break;
        }
      }
      Thread.sleep(200);
    }
    restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    {
      ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId, message.getStr(Overseer.QUEUE_OPERATION));
      // Copy data from backed up index to each replica.
      // Set a generous idleTimeout on the per-shard HTTP request: restoring an index can take
      // considerably longer than the default 120s client idle timeout.
      for (Slice slice : restoreCollection.getSlices()) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RESTORECORE.toString());
        params.set(NAME, "snapshot." + slice.getName());
        params.set(CoreAdminParams.BACKUP_LOCATION, backupPath.toASCIIString());
        params.set(CoreAdminParams.BACKUP_REPOSITORY, repo);
        params.set("idleTimeout", 600000); // 10 minutes
        shardRequestTracker.sliceCmd(params, null, slice, shardHandler);
      }
      shardRequestTracker.processResponses(new NamedList(), shardHandler, true, "Could not restore core");
    }

    {
      ShardRequestTracker shardRequestTracker = ocmh.asyncRequestTracker(asyncId, message.getStr(Overseer.QUEUE_OPERATION));

      for (Slice s : restoreCollection.getSlices()) {
        for (Replica r : s.getReplicas()) {
          String nodeName = r.getNodeName();
          String coreName = r.getName();
          Replica.State stateRep = r.getState();

          if (log.isDebugEnabled()) {
            log.debug("Calling REQUESTAPPLYUPDATES on: nodeName={}, coreName={}, state={}", nodeName, coreName,
                stateRep.name());
          }

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES.toString());
          params.set(CoreAdminParams.NAME, coreName);

          shardRequestTracker.sendShardRequest(nodeName, params, shardHandler);
        }

        shardRequestTracker.processResponses(new NamedList(), shardHandler, true,
            "REQUESTAPPLYUPDATES calls did not succeed");
      }
    }

    //Mark all shards in ACTIVE STATE so the additional replicas added below recover from an active
    //leader. (The threaded clusterState's slice state is fixed up to ACTIVE right after this so the
    //second-loop structure writes don't revert the shards back to CONSTRUCTION.)
    {
      Object2ObjectMap<String, Object> propMap = new Object2ObjectLinkedOpenHashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      propMap.put("id", restoreCollection.getId());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.ACTIVE.toString());
      }
      ocmh.overseer.offerStateUpdate((Utils.toJSON(new ZkNodeProps(propMap))));
    }

    if (totalReplicasPerShard > 1) {
      if (log.isInfoEnabled()) {
        log.info("Adding replicas to restored collection={}", restoreCollection.getName());
      }
      // Thread an accumulating ClusterState through the addReplica calls. zkStateReader.getClusterState()
      // lags behind the per-replica structure writes (the watch fires asynchronously after the ZK
      // write), so re-reading it for each call would build every addition on a stale base containing
      // only the leader. Because enqueueStructureChange replaces a slice's replica set wholesale with
      // the incoming one, those stale-based writes overwrote each other and only the last replica per
      // shard survived. Passing the state returned by the previous addReplica makes each call build on
      // the full, up-to-date replica set.
      ClusterState restoreClusterState = zkStateReader.getClusterState();
      // Reflect the ACTIVE shard state we just offered into our threaded snapshot. enqueueStructureChange
      // merges each addReplica from this snapshot's slice (state included); a stale CONSTRUCTION state
      // here would revert the shards we just activated, leaving the collection permanently non-active.
      {
        DocCollection dcNow = restoreClusterState.getCollectionOrNull(restoreCollectionName);
        if (dcNow != null) {
          Map<String,Slice> activeSlices = new LinkedHashMap<>();
          for (Slice s : dcNow.getSlices()) {
            Map<String,Object> sp = new LinkedHashMap<>(s.getProperties());
            sp.put(ZkStateReader.STATE_PROP, Slice.State.ACTIVE.toString());
            activeSlices.put(s.getName(), new Slice(s.getName(), s.getReplicasMap(), sp, restoreCollectionName, dcNow.getId()));
          }
          DocCollection activeDc = dcNow.copyWithSlices(activeSlices, dcNow.getProperties());
          restoreClusterState = restoreClusterState.copyWith(restoreCollectionName, activeDc);
        }
      }
      for (Slice slice : restoreCollection.getSlices()) {

        //Add the remaining replicas for each shard, considering it's type
        int createdNrtReplicas = 0, createdTlogReplicas = 0, createdPullReplicas = 0;

        // We already created either a NRT or an TLOG replica as leader
        if (numNrtReplicas > 0) {
          createdNrtReplicas++;
        } else if (createdTlogReplicas > 0) {
          createdTlogReplicas++;
        }

        for (int i = 1; i < totalReplicasPerShard; i++) {
          Replica.Type typeToCreate;
          if (createdNrtReplicas < numNrtReplicas) {
            createdNrtReplicas++;
            typeToCreate = Replica.Type.NRT;
          } else if (createdTlogReplicas < numTlogReplicas) {
            createdTlogReplicas++;
            typeToCreate = Replica.Type.TLOG;
          } else {
            createdPullReplicas++;
            typeToCreate = Replica.Type.PULL;
            assert createdPullReplicas <= numPullReplicas: "Unexpected number of replicas";
          }

          if (log.isDebugEnabled()) {
            log.debug("Adding replica for shard={} collection={} of type {} ", slice.getName(), restoreCollection, typeToCreate);
          }
          Object2ObjectMap<String, Object> propMap = new Object2ObjectLinkedOpenHashMap<>();
          propMap.put(COLLECTION_PROP, restoreCollectionName);
          propMap.put(SHARD_ID_PROP, slice.getName());
          propMap.put(REPLICA_TYPE, typeToCreate.name());

          // Get the first node matching the shard to restore in
          String node;
          for (ReplicaPosition replicaPosition : replicaPositions) {
            if (Objects.equals(replicaPosition.shard, slice.getName())) {
              node = replicaPosition.node;
              propMap.put(CoreAdminParams.NODE, node);
              replicaPositions.remove(replicaPosition);
              break;
            }
          }

          // add async param
          if (asyncId != null) {
            propMap.put(ASYNC, asyncId);
          }
          OverseerCollectionMessageHandler.addPropertyParams(message, propMap);

          AddReplicaCmd.Response addResp = ocmh.addReplicaWithResp(restoreClusterState, new ZkNodeProps(propMap), results);
          if (addResp != null && addResp.clusterState != null) {
            restoreClusterState = addResp.clusterState;
          }
          // Finalize the add: this runs processResponses, which flushes/awaits the core CREATE shard
          // request. Without it the replica is registered in cluster state (DOWN) but its core is never
          // actually created on the node, leaving it stuck DOWN.
          if (addResp != null && addResp.asyncFinalRunner != null) {
            addResp.asyncFinalRunner.call();
          }
        }
      }
    }


    if (backupCollectionAlias != null && !backupCollectionAlias.equals(backupCollection)) {
      log.debug("Restoring alias {} -> {}", backupCollectionAlias, backupCollection);
      ocmh.zkStateReader.aliasesManager
          .applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias(backupCollectionAlias, backupCollection));
    }

    log.info("Completed restoring collection={} backupName={}", restoreCollection, backupName);
    AddReplicaCmd.Response response = new AddReplicaCmd.Response();

    response.clusterState = null;

    return response;
  }

  private static int getInt(ZkNodeProps message, String propertyName, Integer count, int defaultValue) {
    Integer value = message.getInt(propertyName, count);
    return value!=null ? value:defaultValue;
  }
}
