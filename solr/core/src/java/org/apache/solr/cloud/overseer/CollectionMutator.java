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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CollectionMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCloudManager cloudManager;
  protected final DistribStateManager stateManager;
  private final ZkStateReader reader;

  public CollectionMutator(SolrCloudManager cloudManager, ZkStateReader reader) {
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.reader = reader;
  }

  public ClusterState createShard(final ClusterState clusterState, ZkNodeProps message) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String shardId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(shardId);
    if (slice == null) {
      Map<String, Replica> replicas = Collections.EMPTY_MAP;
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
      collection = updateSlice(collectionName, collection, new Slice(shardId, replicas, sliceProps, collectionName, collection.getId()));

      Map<String,byte[]> paths = new HashMap<>();

        try {
       //   paths.put(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect", null);
          paths.put(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect/" +  shardId + "/election", null);
       //   paths.put(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leaders/", null);
          paths.put(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leaders/" +  shardId, null);
          paths.put(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leaders/" +  shardId + "/leader", null);
      //    paths.put(ZkStateReader.COLLECTIONS_ZKNODE  + "/" + collectionName + "/terms", null);
          paths.put(ZkStateReader.COLLECTIONS_ZKNODE  + "/" + collectionName + "/terms/" + shardId, ZkStateReader.emptyJson);
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      try {
        reader.getZkClient().mkdirs(paths, 3);
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      return clusterState.copyWith(collectionName, collection);
    } else {
      log.error("Unable to create Shard: {} because it already exists in collection: {}", shardId, collectionName);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to create Shard: " + shardId + " because it already exists in collection: " + collectionName);
    }
  }

  public static ClusterState deleteShard(final ClusterState clusterState, ZkNodeProps message) {
    final String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

    log.info("Removing collection: {} shard: {}  from clusterstate", collection, sliceId);

    DocCollection coll = clusterState.getCollection(collection);

    Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlicesMap());
    newSlices.get(sliceId).getProperties().put("remove", "true");

    DocCollection newCollection = coll.copyWithSlicesShallow(newSlices);
    return clusterState.copyWith(collection, newCollection);
  }

  public static ClusterState modifyCollection(final ClusterState clusterState, ZkNodeProps message) {
    DocCollection coll = clusterState.getCollection(message.getStr(COLLECTION_PROP));
    Map<String, Object> m = new LinkedHashMap<>(coll.getProps());
    boolean hasAnyOps = false;
    for (String prop : CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES) {
      if (message.containsKey(prop)) {
        hasAnyOps = true;
        if (message.get(prop) == null)  {
          m.remove(prop);
        } else  {
          m.put(prop, message.get(prop));
        }
        if (prop == REPLICATION_FACTOR) { //SOLR-11676 : keep NRT_REPLICAS and REPLICATION_FACTOR in sync
          m.put(NRT_REPLICAS, message.get(REPLICATION_FACTOR));
        }
      }
    }
    // other aux properties are also modifiable
    for (Map.Entry<String,Object> theEntry : message.getProperties().entrySet()) {
      Map.Entry<String,Object> entry = theEntry;
      String prop = entry.getKey();
      if (prop.startsWith(CollectionAdminRequest.PROPERTY_PREFIX)) {
        hasAnyOps = true;
        if (entry.getValue() == null) {
          m.remove(prop);
        } else {
          m.put(prop, entry.getValue());
        }
      }
    }

    if (!hasAnyOps) {
      return clusterState;
    }

    return clusterState.copyWith(coll.getName(),
        new DocCollection(coll.getName(), coll.getSlicesMap(), m, coll.getRouter(), coll.getZNodeVersion()));
  }

  public static DocCollection updateSlice(String collectionName, DocCollection collection, Slice slice) {
    DocCollection newCollection;
    Map<String, Slice> slices;

    if (collection == null) {
      throw new IllegalStateException("Collection does not exist: " + collectionName);
    } else {
      slices = new LinkedHashMap<>(collection.getSlicesMap()); // make a shallow copy
      slices.put(slice.getName(), slice);
      newCollection = collection.copyWithSlicesShallow(slices);
    }
    log.debug("docCollection={} with slice={} to newDocCollection={} via toString, but physical replicas={}", collection, slice, newCollection, newCollection.getReplicas());

   // assert newCollection.getSlice(slice.getName()).getReplicasMap().keySet().containsAll(slice.getReplicasMap().keySet());

    return newCollection;
  }

  static boolean checkCollectionKeyExistence(ZkNodeProps message) {
    return checkKeyExistence(message, ZkStateReader.COLLECTION_PROP);
  }

  static boolean checkKeyExistence(ZkNodeProps message, String key) {
    String value = message.getStr(key);
    if (value == null || StringUtils.isBlank(value)) {
      log.error("Skipping invalid Overseer message because it has no {} specified '{}'", key, message);
      return false;
    }
    return true;
  }
}

