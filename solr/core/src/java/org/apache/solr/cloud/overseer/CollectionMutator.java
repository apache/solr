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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CONFIGNAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;

public class CollectionMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;
  protected final SolrZkClient zkClient;
  ClusterState modifiedClusterState;

  public CollectionMutator(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.zkClient = SliceMutator.getZkClient(cloudManager);
  }

  public ZkWriteCommand createShard(final ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    String shardId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(shardId);
    if (slice == null) {
      Map<String, Replica> replicas = Collections.emptyMap();
      Map<String, Object> sliceProps = new HashMap<>();
      String shardRange = message.getStr(ZkStateReader.SHARD_RANGE_PROP);
      String shardState = message.getStr(ZkStateReader.SHARD_STATE_PROP);
      String shardParent = message.getStr(ZkStateReader.SHARD_PARENT_PROP);
      String shardParentZkSession = message.getStr("shard_parent_zk_session");
      String shardParentNode = message.getStr("shard_parent_node");
      sliceProps.put(Slice.RANGE, shardRange);
      sliceProps.put(ZkStateReader.STATE_PROP, shardState);
      if (shardParent != null) {
        sliceProps.put(Slice.PARENT, shardParent);
      }
      if (shardParentZkSession != null) {
        sliceProps.put("shard_parent_zk_session", shardParentZkSession);
      }
      if (shardParentNode != null)  {
        sliceProps.put("shard_parent_node", shardParentNode);
      }
      collection = updateSlice(collectionName, collection, new Slice(shardId, replicas, sliceProps, collectionName));
      return new ZkWriteCommand(collectionName, collection);
    } else {
      log.error("Unable to create Shard: {} because it already exists in collection: {}", shardId, collectionName);
      return ZkStateWriter.NO_OP;
    }
  }

  public ZkWriteCommand deleteShard(final ClusterState clusterState, ZkNodeProps message) {
    final String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;

    log.info("Removing collection: {} shard: {}  from clusterstate", collection, sliceId);

    DocCollection coll = clusterState.getCollection(collection);

    Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlicesMap());
    newSlices.remove(sliceId);

    DocCollection newCollection = coll.copyWithSlices(newSlices);
    return new ZkWriteCommand(collection, newCollection);
  }

  public ZkWriteCommand modifyCollection(final ClusterState clusterState, ZkNodeProps message) {
    if (!checkCollectionKeyExistence(message)) return ZkStateWriter.NO_OP;
    DocCollection coll = clusterState.getCollection(message.getStr(COLLECTION_PROP));
    boolean hasAnyOps = false;
    Map<String, Object> props = coll.shallowCopy();
    for (String prop : CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES) {
      if (message.containsKey(prop)) {
        hasAnyOps = true;
        if (message.get(prop) == null)  {
          props.remove(prop);
        } else  {
          // rename key from collection.configName to configName
          if (prop.equals(COLL_CONF)) {
            props.put(CONFIGNAME_PROP, message.get(prop));
          } else {
            props.put(prop, message.get(prop));
          }
        }
        if (prop == REPLICATION_FACTOR) { //SOLR-11676 : keep NRT_REPLICAS and REPLICATION_FACTOR in sync
          props.put(NRT_REPLICAS, message.get(REPLICATION_FACTOR));
        }
      }
    }
    // other aux properties are also modifiable
    for (String prop : message.keySet()) {
      if (prop.startsWith(CollectionAdminRequest.PROPERTY_PREFIX)) {
        hasAnyOps = true;
        if (message.get(prop) == null) {
          props.remove(prop);
        } else {
          props.put(prop, message.get(prop));
        }
      }
    }

    if (!hasAnyOps) {
      return ZkStateWriter.NO_OP;
    }

    assert !props.containsKey(COLL_CONF);

    DocCollection collection = new DocCollection(coll.getName(), coll.getSlicesMap(), props, coll.getRouter(), coll.getZNodeVersion());
    return new ZkWriteCommand(coll.getName(), collection);

  }

  /**
   * Check whether a PRS modify is requested
   */
  public boolean isModifyPrs(ZkNodeProps message) {
    boolean hasPrs = false;
    boolean hasOther = false;
    for (String prop : CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES) {
      String val = message.getStr(prop);
      if (val != null) {
        if (DocCollection.PER_REPLICA_STATE.equals(prop)) {
          hasPrs = true;
        } else {
          hasOther = true;
        }
      }
    }
    if (hasOther && hasPrs) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Cannot modify collection with perReplicaState and other properties in same command");
    }
    return hasPrs;

  }

  public ClusterState doModifyPrs(ZkNodeProps message, ClusterState clusterState) {
    String prsVal = message.getStr(DocCollection.PER_REPLICA_STATE);
    boolean enable  = Boolean.parseBoolean(prsVal);
    final DocCollection coll = clusterState.getCollection(message.getStr(COLLECTION_PROP));
    if(coll.isPerReplicaState() == enable) {
      //idempotent call
      return clusterState;
    }
    Map<String, Object> props = coll.shallowCopy();
    props.put(DocCollection.PER_REPLICA_STATE, enable);
    String name = coll.getName();
    final AtomicReference<DocCollection> result = new AtomicReference<>(new DocCollection(name, coll.getSlicesMap(), props, coll.getRouter(), coll.getZNodeVersion()));
    PerReplicaStates prsStates = PerReplicaStates.fetch(coll.getZNode(), zkClient, null);
    PerReplicaStatesOps prsOps = enable?
            enablePrsOps(result, prsStates) :
            disablePrsOps(result);

    try {
      prsOps.init(prsStates);
      prsOps.persist(coll.getZNode(), zkClient);
      Stat stat = zkClient.exists(coll.getZNode(), null, true);
      result.set(new DocCollection(name, result.get().getSlicesMap(), result.get().getProperties(), result.get().getRouter(), stat.getVersion()));
      clusterState.copyWith(name, result.get());
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to MODIFY perResplicaStates on Collection : "+ name);
    }

    return null;
  }

  private static PerReplicaStatesOps disablePrsOps( AtomicReference<DocCollection> coll) {
    return new PerReplicaStatesOps(PerReplicaStatesOps::disablePrs) {
      @Override
      protected List<Op> getZkOps(List<PerReplicaStates.Operation> operations, String znode, SolrZkClient zkClient) {
        coll.set(updateReplicasFromPrs(coll.get(),PerReplicaStates.fetch(coll.get().getZNode(), zkClient, null)));
        List<Op> zkOps = super.getZkOps(operations, znode, zkClient);
        zkOps.add(Op.setData(znode, Utils.toJSON(singletonMap(coll.get().getName(), coll.get())), coll.get().getZNodeVersion()));
        return zkOps;
      }
    };
  }

  private static PerReplicaStatesOps enablePrsOps(AtomicReference<DocCollection> coll, PerReplicaStates prsStates) {
    return new PerReplicaStatesOps(prs -> PerReplicaStatesOps.enablePrs(coll.get(), prsStates)) {
      @Override
      protected List<Op> getZkOps(List<PerReplicaStates.Operation> operations, String znode, SolrZkClient zkClient) {
        List<Op> ops = super.getZkOps(operations, znode, zkClient);
        ops.add(Op.setData(znode, Utils.toJSON(singletonMap(coll.get().getName(), coll.get())), coll.get().getZNodeVersion()));
        return ops;
      }
    };
  }

  /**
   *  Update the state/leader of replicas in state.json from PRS data
   */
  public static DocCollection updateReplicasFromPrs(DocCollection coll, PerReplicaStates prs) {
    //we are disabling PRS. Update the replica states
    Map<String, Slice> modifiedSlices = new LinkedHashMap<>();
    coll.forEachReplica((s, replica) -> {
      PerReplicaStates.State prsState = prs.states.get(replica.getName());
      if (prsState != null) {
        if (prsState.state != replica.getState()) {
          Slice slice = modifiedSlices.getOrDefault(replica.shard, coll.getSlice(replica.shard));
          replica = ReplicaMutator.setState(replica, prsState.state.toString());
          modifiedSlices.put(replica.shard, slice.copyWith(replica));
        }
        if (prsState.isLeader != replica.isLeader()) {
          Slice slice = modifiedSlices.getOrDefault(replica.shard, coll.getSlice(replica.shard));
          replica = prsState.isLeader ?
                  ReplicaMutator.setLeader(replica) :
                  ReplicaMutator.unsetLeader(replica);
          modifiedSlices.put(replica.shard, slice.copyWith(replica));
        }
      }
    });

    if(!modifiedSlices.isEmpty()) {
      Map<String,Slice> slices = new LinkedHashMap<>(coll.getSlicesMap());
      slices.putAll(modifiedSlices);
      return coll.copyWithSlices(slices);
    }
    return coll;
  }


  public static DocCollection updateSlice(String collectionName, DocCollection collection, Slice slice) {
    Map<String, Slice> slices = new LinkedHashMap<>(collection.getSlicesMap()); // make a shallow copy
    slices.put(slice.getName(), slice);
    return collection.copyWithSlices(slices);
  }

  static boolean checkCollectionKeyExistence(ZkNodeProps message) {
    return checkKeyExistence(message, ZkStateReader.COLLECTION_PROP);
  }

  static boolean checkKeyExistence(ZkNodeProps message, String key) {
    String value = message.getStr(key);
    if (value == null || value.trim().length() == 0) {
      log.error("Skipping invalid Overseer message because it has no {} specified '{}'", key, message);
      return false;
    }
    return true;
  }

  static class NoOpCommand extends RuntimeException {

  }
}
