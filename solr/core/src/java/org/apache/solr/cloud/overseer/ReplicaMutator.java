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
package org.apache.solr.cloud.overseer;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.overseer.CollectionMutator.checkCollectionKeyExistence;
import static org.apache.solr.cloud.overseer.CollectionMutator.checkKeyExistence;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ReplicaMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;

  public ReplicaMutator(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
  }

  protected static Replica setProperty(Replica replica, String key, String value) {
    assert key != null;
    assert value != null;

    if (StringUtils.equalsIgnoreCase(replica.getStr(key), value))
      return replica; // already the value we're going to set

    ZkNodeProps replicaProps = replica.plus(key, value);
    return new Replica(replica.getName(), replicaProps.getProperties(), replica.getCollection(), replica.getCollectionId(), replica.getSlice(), (replica.getSliceOwner()));
  }

//  protected Replica setProperty(Replica replica, String... keyVals) {
//
//    ZkNodeProps replicaProps =  replica.plus(keyVals);
//    return new Replica(replica.getName(), replicaProps.getProperties(), replica.getCollection(), replica.getCollectionId(), replica.getSlice(), replica.getSliceOwner());
//  }

//  protected Replica setLeader(Replica replica) {
//    return setProperty(replica, ZkStateReader.LEADER_PROP, "true", ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
//  }

//  protected Replica unsetLeader(Replica replica) {
//    return new Replica(replica.getName(), replica.minus(ZkStateReader.LEADER_PROP), replica.getCollection(), replica.getCollectionId(), replica.getSlice(), replica.getSliceOwner());
//  }

  protected static Replica setState(Replica replica, String state) {
    assert state != null;

    return setProperty(replica, ZkStateReader.STATE_PROP, state);
  }

  public static ClusterState deleteReplicaProperty(ClusterState clusterState, ZkNodeProps message) {
    if (checkKeyExistence(message, ZkStateReader.COLLECTION_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.SHARD_ID_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.REPLICA_PROP) == false ||
        checkKeyExistence(message, ZkStateReader.PROPERTY_PROP) == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer DELETEREPLICAPROP requires " +
              ZkStateReader.COLLECTION_PROP + " and " + ZkStateReader.SHARD_ID_PROP + " and " +
              ZkStateReader.REPLICA_PROP + " and " + ZkStateReader.PROPERTY_PROP + " no action taken.");
    }
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String replicaName = message.getStr(ZkStateReader.REPLICA_PROP);
    String property = message.getStr(ZkStateReader.PROPERTY_PROP).toLowerCase(Locale.ROOT);
    if (StringUtils.startsWith(property, OverseerCollectionMessageHandler.COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionMessageHandler.COLL_PROP_PREFIX + property;
    }

    DocCollection collection = clusterState.getCollection(collectionName);
    Replica replica = collection.getReplica(replicaName);

    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find collection/slice/replica " +
          collectionName + "/" + sliceName + "/" + replicaName + " no action taken.");
    }

    log.info("Deleting property {} for collection: {} slice: {} replica: {}", property, collectionName, sliceName, replicaName);
    log.debug("Full message: {}", message);
    String curProp = replica.getStr(property);
    if (curProp == null) return null; // not there anyway, nothing to do.

    Slice slice = collection.getSlice(sliceName);
    DocCollection newCollection = SliceMutator.updateReplica(collection,
        slice.getName(), (Replica) replica.minus(property));
    return clusterState.copyWith(collectionName, newCollection);
  }

  /**
   * Handles state updates
   */
  public static ClusterState setState(ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

    if (collectionName == null || sliceName == null) {
      log.error("Invalid collection and slice {}", message);
      return null;
    }
    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    Slice slice = collection != null ? collection.getSlice(sliceName) : null;
    if (slice == null) {
      log.error("No such slice exists {}", message);
      return null;
    }

    return updateState(clusterState, message);
  }

  protected static ClusterState updateState(ClusterState clusterState, ZkNodeProps message) {
    final String cName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return null;
    Integer numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, null);
    log.debug("Update state numShards={} message={}", numShards, message);


    return updateState(clusterState,
        message, cName, numShards);
  }

  private static ClusterState updateState(ClusterState clusterState, ZkNodeProps message, String collectionName, Integer numShards) {
    String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
    String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);

    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection.getReplica(coreName) == null) {
      log.info("Failed to update state because the replica does not exist, {}", message);
      return clusterState;
    }

    // MRM TODO:
    //    if (coreNodeName == null) {
    //      coreNodeName = ClusterStateMutator.getAssignedCoreNodeName(collection,
    //          message.getStr(ZkStateReader.NODE_NAME_PROP), message.getStr(ZkStateReader.CORE_NAME_PROP));
    //      if (coreNodeName != null) {
    //        log.debug("node={} is already registered", coreNodeName);
    //      } else {
    //        if (!forceSetState) {
    //          log.info("Failed to update state because the replica does not exist, {}", message);
    //          return null;
    //        }
    //        // if coreNodeName is null, auto assign one
    //        int replicas = 0;
    //        DocCollection docCollection = prevState.getCollectionOrNull(collectionName);
    //        if (docCollection != null) {
    //          replicas = docCollection.getReplicas().size();
    //        }
    //
    //        coreNodeName = Assign.assignCoreNodeName(stateManager, collectionName,replicas );
    //      }
    //      message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
    //          coreNodeName);
    //    }

    // use the provided non null shardId
    if (sliceName == null) {
      throw new RuntimeException();
    }

    Slice slice = collection != null ? collection.getSlice(sliceName) : null;
    Integer id = -1;

    Object2ObjectMap<String,Object> replicaProps = new Object2ObjectLinkedOpenHashMap<>();
    if (slice != null) {
      Replica oldReplica = slice.getReplica(coreName);

      if (oldReplica != null) {
        id = oldReplica.getCollectionId();
        if (oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
          replicaProps.put(ZkStateReader.LEADER_PROP, oldReplica.get(ZkStateReader.LEADER_PROP));
        }

        replicaProps.put(ZkStateReader.REPLICA_TYPE, oldReplica.getType().toString());
        // Move custom props over.
        for (Object obj : oldReplica.getProperties().entrySet()) {
          Map.Entry ent = (Map.Entry) obj;
          if (((String) ent.getKey()).startsWith(OverseerCollectionMessageHandler.COLL_PROP_PREFIX)) {
            replicaProps.put((String) ent.getKey(), ent.getValue());
          }
        }
      }
    }

    // we don't put these in the clusterstate
    replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
    replicaProps.remove(ZkStateReader.CORE_NAME_PROP);
    replicaProps.remove(ZkStateReader.SHARD_ID_PROP);
    replicaProps.remove(ZkStateReader.COLLECTION_PROP);
    replicaProps.remove(Overseer.QUEUE_OPERATION);

    // remove any props with null values
    Set<Map.Entry<String,Object>> entrySet = replicaProps.entrySet();
    List<String> removeKeys = new ArrayList<>();
    for (Map.Entry<String,Object> entry : entrySet) {
      if (entry.getValue() == null) {
        removeKeys.add(entry.getKey());
      }
    }
    for (String removeKey : removeKeys) {
      replicaProps.remove(removeKey);
    }

    // remove shard specific properties
    String shardRange = (String) replicaProps.remove(ZkStateReader.SHARD_RANGE_PROP);
    String shardState = (String) replicaProps.remove(ZkStateReader.SHARD_STATE_PROP);
    String shardParent = (String) replicaProps.remove(ZkStateReader.SHARD_PARENT_PROP);

    Replica replica = new Replica(coreName, replicaProps, collectionName, id, sliceName, slice);

    log.debug("Will update state for replica: {}", replica);

    Map<String,Replica> replicas;

    //prevState =checkAndCompleteShardSplit(prevState, collection, coreName, sliceName, replica);
    // get the current slice again because it may have been updated due to checkAndCompleteShardSplit method
    slice = collection.getSlice(sliceName);
    replicas = slice.getReplicasCopy();

    replicas.put(replica.getName(), replica);
    slice = new Slice(sliceName, replicas, slice.getProperties(), collectionName, collection.getId());

    DocCollection newCollection = CollectionMutator.updateSlice(collectionName, collection, slice);
    log.debug("Collection is now: {}", newCollection);
    return clusterState.copyWith(collectionName, newCollection);
  }
}

