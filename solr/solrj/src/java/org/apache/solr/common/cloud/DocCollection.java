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
package org.apache.solr.common.cloud;

import static org.apache.solr.common.util.Utils.toJSONString;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.apache.solr.common.cloud.Replica.ReplicaStateProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Models a Collection in zookeeper (but that Java name is obviously taken, hence "DocCollection")
 */
public class DocCollection extends ZkNodeProps implements Iterable<Slice> {

  public static final String COLLECTIONS_ZKNODE = "/collections";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final int znodeVersion;

  private final String name;
  private final String configName;
  private final Map<String, Slice> slices;
  private final Map<String, Slice> activeSlices;
  private final Slice[] activeSlicesArr;
  private final Map<String, List<Replica>> nodeNameReplicas;
  private final Map<String, List<Replica>> nodeNameLeaderReplicas;
  private final DocRouter router;
  private final String znode;

  private final Integer replicationFactor;
  private final Integer numNrtReplicas;
  private final Integer numTlogReplicas;
  private final Integer numPullReplicas;
  private final Boolean readOnly;
  private final Boolean perReplicaState;
  private final Map<String, Replica> replicaMap = new HashMap<>();
  private AtomicReference<PerReplicaStates> perReplicaStatesRef;

  /**
   * @see DocCollection#create(String, Map, Map, DocRouter, int, PrsSupplier)
   */
  @Deprecated
  public DocCollection(
      String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router) {
    this(name, slices, props, router, Integer.MAX_VALUE, null);
  }

  /**
   * @see DocCollection#create(String, Map, Map, DocRouter, int, PrsSupplier)
   */
  @Deprecated
  public DocCollection(
      String name,
      Map<String, Slice> slices,
      Map<String, Object> props,
      DocRouter router,
      int zkVersion) {
    this(name, slices, props, router, zkVersion, null);
  }

  /**
   * @param name The name of the collection
   * @param slices The logical shards of the collection. This is used directly and a copy is not
   *     made.
   * @param props The properties of the slice. This is used directly and a copy is not made.
   * @param zkVersion The version of the Collection node in Zookeeper (used for conditional
   *     updates).
   * @see DocCollection#create(String, Map, Map, DocRouter, int, PrsSupplier)
   */
  private DocCollection(
      String name,
      Map<String, Slice> slices,
      Map<String, Object> props,
      DocRouter router,
      int zkVersion,
      AtomicReference<PerReplicaStates> perReplicaStatesRef) {
    super(props);
    // -1 means any version in ZK CAS, so we choose Integer.MAX_VALUE instead to avoid accidental
    // overwrites
    this.znodeVersion = zkVersion == -1 ? Integer.MAX_VALUE : zkVersion;
    this.name = name;
    this.configName = (String) props.get(CollectionStateProps.CONFIGNAME);
    this.slices = slices;
    this.activeSlices = new HashMap<>();
    this.nodeNameLeaderReplicas = new HashMap<>();
    this.nodeNameReplicas = new HashMap<>();
    this.replicationFactor = (Integer) verifyProp(props, CollectionStateProps.REPLICATION_FACTOR);
    this.numNrtReplicas = (Integer) verifyProp(props, CollectionStateProps.NRT_REPLICAS, 0);
    this.numTlogReplicas = (Integer) verifyProp(props, CollectionStateProps.TLOG_REPLICAS, 0);
    this.numPullReplicas = (Integer) verifyProp(props, CollectionStateProps.PULL_REPLICAS, 0);
    this.perReplicaState =
        (Boolean) verifyProp(props, CollectionStateProps.PER_REPLICA_STATE, Boolean.FALSE);
    if (this.perReplicaState) {
      if (perReplicaStatesRef == null || perReplicaStatesRef.get() == null) {
        throw new RuntimeException(
            CollectionStateProps.PER_REPLICA_STATE
                + " = true , but perReplicaStates param is not provided");
      }
      this.perReplicaStatesRef = perReplicaStatesRef;
      for (Slice s : this.slices.values()) { // set the same reference to all slices too
        s.setPerReplicaStatesRef(this.perReplicaStatesRef);
      }
    }
    Boolean readOnly = (Boolean) verifyProp(props, CollectionStateProps.READ_ONLY);
    this.readOnly = readOnly == null ? Boolean.FALSE : readOnly;

    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      if (slice.getValue().getState() == Slice.State.ACTIVE) {
        this.activeSlices.put(slice.getKey(), slice.getValue());
      }
      for (Replica replica : slice.getValue()) {
        addNodeNameReplica(replica);
        if (perReplicaState) {
          replicaMap.put(replica.getName(), replica);
        }
      }
    }
    this.activeSlicesArr = activeSlices.values().toArray(new Slice[0]);
    this.router = router;
    this.znode = getCollectionPath(name);
    assert name != null && slices != null;
  }

  /**
   * Builds a DocCollection with an optional PrsSupplier
   *
   * @param name The name of the collection
   * @param slices The logical shards of the collection. This is used directly and a copy is not
   *     made.
   * @param props The properties of the slice. This is used directly and a copy is not made.
   * @param router router to partition int range into n ranges
   * @param zkVersion The version of the Collection node in Zookeeper (used for conditional
   *     updates).
   * @param prsSupplier optional supplier for PerReplicaStates (PRS) for PRS enabled collections
   * @return a newly constructed DocCollection
   */
  public static DocCollection create(
      String name,
      Map<String, Slice> slices,
      Map<String, Object> props,
      DocRouter router,
      int zkVersion,
      DocCollection.PrsSupplier prsSupplier) {
    boolean perReplicaState =
        (Boolean) verifyProp(props, CollectionStateProps.PER_REPLICA_STATE, Boolean.FALSE);
    PerReplicaStates perReplicaStates;
    if (perReplicaState) {
      if (prsSupplier == null) {
        throw new IllegalArgumentException(
            CollectionStateProps.PER_REPLICA_STATE + " = true , but prsSuppler is not provided");
      }

      if (!hasAnyReplica(
          slices)) { // a special case, if there is no replica, it should not fetch (first PRS
        // collection creation with no replicas). Otherwise, it would trigger exception
        // on fetching a state.json that does not exist yet
        perReplicaStates = PerReplicaStates.empty(name);
      } else {
        perReplicaStates = prsSupplier.get();
      }
    } else {
      perReplicaStates = null;
    }
    return new DocCollection(
        name,
        slices,
        props,
        router,
        zkVersion,
        perReplicaStates != null ? new AtomicReference<>(perReplicaStates) : null);
  }

  private static boolean hasAnyReplica(Map<String, Slice> slices) {
    for (Slice slice : slices.values()) {
      if (!slice.getReplicasMap().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static String getCollectionPath(String coll) {
    return getCollectionPathRoot(coll) + "/state.json";
  }

  public static String getCollectionPathRoot(String coll) {
    return COLLECTIONS_ZKNODE + "/" + coll;
  }

  /**
   * Update our state with a state of a {@link PerReplicaStates} which could override states of
   * {@link Replica}.
   *
   * <p>Take note that it updates the underlying AtomicReference such that all Slice and Replica
   * that holds the same AtomicReference will see the same update
   *
   * <p>This does not create a new DocCollection.
   */
  public final DocCollection setPerReplicaStates(PerReplicaStates newPerReplicaStates) {
    if (this.perReplicaStatesRef != null) {
      log.debug("In-place update of PRS: {}", newPerReplicaStates);
      this.perReplicaStatesRef.set(newPerReplicaStates);
    }

    return this;
  }

  private void addNodeNameReplica(Replica replica) {
    List<Replica> replicas = nodeNameReplicas.get(replica.getNodeName());
    if (replicas == null) {
      replicas = new ArrayList<>();
      nodeNameReplicas.put(replica.getNodeName(), replicas);
    }
    replicas.add(replica);

    if (replica.getStr(ReplicaStateProps.LEADER) != null) {
      List<Replica> leaderReplicas = nodeNameLeaderReplicas.get(replica.getNodeName());
      if (leaderReplicas == null) {
        leaderReplicas = new ArrayList<>();
        nodeNameLeaderReplicas.put(replica.getNodeName(), leaderReplicas);
      }
      leaderReplicas.add(replica);
    }
  }

  public static Object verifyProp(Map<String, Object> props, String propName) {
    return verifyProp(props, propName, null);
  }

  public static Object verifyProp(Map<String, Object> props, String propName, Object def) {
    Object o = props.get(propName);
    if (o == null) return def;
    switch (propName) {
      case CollectionStateProps.REPLICATION_FACTOR:
      case CollectionStateProps.NRT_REPLICAS:
      case CollectionStateProps.PULL_REPLICAS:
      case CollectionStateProps.TLOG_REPLICAS:
        return Integer.parseInt(o.toString());
      case CollectionStateProps.PER_REPLICA_STATE:
      case CollectionStateProps.READ_ONLY:
        return Boolean.parseBoolean(o.toString());
      case "snitch":
      default:
        return o;
    }
  }

  /**
   * Use this to make an exact copy of DocCollection with a new set of Slices and every other
   * property as is
   *
   * @param slices the new set of Slices
   * @return the resulting DocCollection
   */
  public DocCollection copyWithSlices(Map<String, Slice> slices) {
    DocCollection result =
        new DocCollection(getName(), slices, propMap, router, znodeVersion, perReplicaStatesRef);
    return result;
  }

  /** Return collection name. */
  public String getName() {
    return name;
  }

  /** Return non-null config name */
  public String getConfigName() {
    assert configName != null;
    return configName;
  }

  public Slice getSlice(String sliceName) {
    return slices.get(sliceName);
  }

  /**
   * @param consumer consume shardName vs. replica
   */
  public void forEachReplica(BiConsumer<String, Replica> consumer) {
    slices.forEach(
        (shard, slice) ->
            slice.getReplicasMap().forEach((s, replica) -> consumer.accept(shard, replica)));
  }

  /** Gets the list of all slices for this collection. */
  public Collection<Slice> getSlices() {
    return slices.values();
  }

  /** Return the list of active slices for this collection. */
  public Collection<Slice> getActiveSlices() {
    return activeSlices.values();
  }

  /** Return array of active slices for this collection (performance optimization). */
  public Slice[] getActiveSlicesArr() {
    return activeSlicesArr;
  }

  /** Get the map of all slices (sliceName-&gt;Slice) for this collection. */
  public Map<String, Slice> getSlicesMap() {
    return slices;
  }

  /** Get the map of active slices (sliceName-&gt;Slice) for this collection. */
  public Map<String, Slice> getActiveSlicesMap() {
    return activeSlices;
  }

  /** Get the list of replicas hosted on the given node or <code>null</code> if none. */
  public List<Replica> getReplicas(String nodeName) {
    return nodeNameReplicas.get(nodeName);
  }

  /** Get the list of all leaders hosted on the given node or <code>null</code> if none. */
  public List<Replica> getLeaderReplicas(String nodeName) {
    return nodeNameLeaderReplicas.get(nodeName);
  }

  public int getZNodeVersion() {
    return znodeVersion;
  }

  public int getChildNodesVersion() {
    return perReplicaStatesRef == null ? 0 : perReplicaStatesRef.get().cversion;
  }

  public boolean isModified(int dataVersion, int childVersion) {
    if (dataVersion > znodeVersion) return true;
    if (childVersion > getChildNodesVersion()) return true;
    return false;
  }

  /**
   * @return replication factor for this collection or null if no replication factor exists.
   */
  public Integer getReplicationFactor() {
    return replicationFactor;
  }

  public String getZNode() {
    return znode;
  }

  public DocRouter getRouter() {
    return router;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    return "DocCollection("
        + name
        + "/"
        + getZNode()
        + "/"
        + znodeVersion
        + " "
        + (perReplicaStatesRef == null ? "" : perReplicaStatesRef.get())
        + ")="
        + toJSONString(this);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    propMap.forEach(ew.getBiConsumer());
    ew.put(CollectionStateProps.SHARDS, slices);
  }

  public Replica getReplica(String coreNodeName) {
    if (perReplicaState) {
      return replicaMap.get(coreNodeName);
    }
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplica(coreNodeName);
      if (replica != null) return replica;
    }
    return null;
  }

  public Replica getLeader(String sliceName) {
    Slice slice = getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader();
  }

  /**
   * Check that all replicas in a collection are live
   *
   * @see CollectionStatePredicate
   */
  public static boolean isFullyActive(
      Set<String> liveNodes,
      DocCollection collectionState,
      int expectedShards,
      int expectedReplicas) {
    Objects.requireNonNull(liveNodes);
    if (collectionState == null) return false;
    int activeShards = 0;
    for (Slice slice : collectionState) {
      int activeReplicas = 0;
      for (Replica replica : slice) {
        if (replica.isActive(liveNodes) == false) return false;
        activeReplicas++;
      }
      if (activeReplicas != expectedReplicas) return false;
      activeShards++;
    }
    return activeShards == expectedShards;
  }

  @Override
  public Iterator<Slice> iterator() {
    return slices.values().iterator();
  }

  public List<Replica> getReplicas() {
    List<Replica> replicas = new ArrayList<>();
    for (Slice slice : this) {
      replicas.addAll(slice.getReplicas());
    }
    return replicas;
  }

  /**
   * @param predicate test against shardName vs. replica
   * @return the first replica that matches the predicate
   */
  public Replica getReplica(BiPredicate<String, Replica> predicate) {
    final Replica[] result = new Replica[1];
    forEachReplica(
        (s, replica) -> {
          if (result[0] != null) return;
          if (predicate.test(s, replica)) {
            result[0] = replica;
          }
        });
    return result[0];
  }

  public List<Replica> getReplicas(EnumSet<Replica.Type> s) {
    List<Replica> replicas = new ArrayList<>();
    for (Slice slice : this) {
      replicas.addAll(slice.getReplicas(s));
    }
    return replicas;
  }

  /** Get the shardId of a core on a specific node */
  public String getShardId(String nodeName, String coreName) {
    for (Slice slice : this) {
      for (Replica replica : slice) {
        if (Objects.equals(replica.getNodeName(), nodeName)
            && Objects.equals(replica.getCoreName(), coreName)) return slice.getName();
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof DocCollection)) return false;
    DocCollection other = (DocCollection) that;
    return super.equals(that)
        && Objects.equals(this.name, other.name)
        && this.znodeVersion == other.znodeVersion
        && this.getChildNodesVersion() == other.getChildNodesVersion();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, znodeVersion, getChildNodesVersion());
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#NRT}
   *     this collection was created with
   */
  public Integer getNumNrtReplicas() {
    return numNrtReplicas;
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#TLOG}
   *     this collection was created with
   */
  public Integer getNumTlogReplicas() {
    return numTlogReplicas;
  }

  /**
   * @return the number of replicas of type {@link org.apache.solr.common.cloud.Replica.Type#PULL}
   *     this collection was created with
   */
  public Integer getNumPullReplicas() {
    return numPullReplicas;
  }

  public boolean isPerReplicaState() {
    return Boolean.TRUE.equals(perReplicaState);
  }

  public PerReplicaStates getPerReplicaStates() {
    return perReplicaStatesRef != null ? perReplicaStatesRef.get() : null;
  }

  public int getExpectedReplicaCount(Replica.Type type, int def) {
    Integer result = null;
    if (type == Replica.Type.NRT) result = numNrtReplicas;
    if (type == Replica.Type.PULL) result = numPullReplicas;
    if (type == Replica.Type.TLOG) result = numTlogReplicas;
    return result == null ? def : result;
  }

  /** JSON properties related to a collection's state. */
  public interface CollectionStateProps {
    String NRT_REPLICAS = "nrtReplicas";
    String PULL_REPLICAS = "pullReplicas";
    String TLOG_REPLICAS = "tlogReplicas";
    String REPLICATION_FACTOR = "replicationFactor";
    String READ_ONLY = "readOnly";
    String CONFIGNAME = "configName";
    String DOC_ROUTER = "router";
    String SHARDS = "shards";
    String PER_REPLICA_STATE = "perReplicaState";
  }

  public interface PrsSupplier extends Supplier<PerReplicaStates> {}
}
