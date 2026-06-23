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

import org.noggit.JSONWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.READ_ONLY;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * Models a Collection in zookeeper (but that Java name is obviously taken, hence "DocCollection")
 */
public class DocCollection extends ZkNodeProps implements Iterable<Slice> {
  //private static transient final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DOC_ROUTER = "router";
  public static final String SHARDS = "shards";

  private volatile int znodeVersion;

  private final Map<Integer,Replica> idToReplica;

  private final String name;
  private final Map<String, Slice> slices;
  private final DocRouter router;
  private final Boolean readOnly;
  private volatile StateUpdates<Integer, AtomicInteger> stateUpdates;

  // Delta-plane reader cursors: per-shard (epoch, seq) last-applied position. Null until the first
  // delta is applied to this collection's state.
  private volatile StatePlaneCursors statePlaneCursors;

  private final Integer id;

  private final int hashcode;

  public DocCollection(String name, Map<String,Slice> slices, Map<String,Object> props, DocRouter router) {
    this(name, slices, props, router, 0);
  }

  /**
   * @param name  The name of the collection
   * @param slices The logical shards of the collection.  This is used directly and a copy is not made.
   * @param props  The properties of the slice.  This is used directly and a copy is not made.
   * @param zkVersion The version of the Collection node in Zookeeper (used for conditional updates).
   */
  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router, int zkVersion) {
    this.znodeVersion = zkVersion;
    this.name = name;
    this.stateUpdates = new StateUpdates<>();

//    if (stateUpdates == null) {
//      throw new NullPointerException("Null stateUpdates");
//    } else {
//      this.stateUpdatesVersion = stateUpdatesVersion;
//      this.stateUpdates = new LinkedHashMap(stateUpdates);
//    }

    this.idToReplica = new HashMap<>(32);

    Objects.requireNonNull(slices, "'slices' must not be null");

    this.slices = slices;

    setStates();

    Boolean readOnly = (Boolean) verifyProp(props, READ_ONLY);
    this.readOnly = readOnly == null ? Boolean.FALSE : readOnly;

    Object idObject = props.get("id");
    if (idObject instanceof Long) {
      this.id = ((Long) props.get("id")).intValue();
    } else {
      this.id = (Integer) props.get("id");
    }


    Objects.requireNonNull(this.id, () -> "'id' must not be null " + props);

    this.router = router;
    assert name != null && slices != null;
    hashcode = Objects.hash(name, id);

    propMap.putAll(props);
  }

  private void setStates() {
    for (Slice slice : slices.values()) {
      if (slice.replicas != null) {
        slice.replicas.forEach((s, replica) -> {

          Integer id = replica.getInternalId();
          if (id != null) {
            // Default to the replica's published state (from state.json / CLUSTERSTATUS) when there is no live
            // StateUpdates entry, rather than hardcoding DOWN(5). A cluster state fetched over HTTP has no
            // state-updates channel, so hardcoding DOWN made every such replica look down and clients failed
            // with "Could not find a healthy node". The live channel still overrides this when an entry exists.
            AtomicInteger replicaState = (AtomicInteger) this.stateUpdates.computeIfAbsent(id,
                state -> new AtomicInteger(Replica.State.getShortState(replica.getPublishedState())));
            this.idToReplica.put(replica.getInternalId(), replica);
            //
            //        AtomicInteger replicaState = (AtomicInteger) this.stateUpdates.computeIfAbsent(id, state -> {
            //
            //
            //         return slice.getReplicaState(replica);
            //
            //        });
            //    //    AtomicInteger replicaState = (AtomicInteger) this.stateUpdates.compute(id, (k, v) -> new AtomicInteger(Replica.State.getShortState(replica.getState())));
            replica.linkState(replicaState, this);
          }
        });
      }
    }
  }

  public static Object verifyProp(Map<String, Object> props, String propName) {
    return verifyProp(props, propName, null);
  }

  public static Object verifyProp(Map<String, Object> props, String propName, Object def) {
    Object o = props.get(propName);
    if (o == null) return def;
    switch (propName) {
      case MAX_SHARDS_PER_NODE:
      case REPLICATION_FACTOR:
      case NRT_REPLICAS:
      case PULL_REPLICAS:
      case TLOG_REPLICAS:
        return Integer.parseInt(o.toString());
      case READ_ONLY:
        return Boolean.parseBoolean(o.toString());
      case "snitch":
      default:
        return o;
    }

  }

//  /**Use this to make an exact copy of DocCollection with a new set of Slices and every other property as is
//   * @param slices the new set of Slices
//   * @return the resulting DocCollection
//   */
//  public DocCollection copyWithSlices(){
//
//    return new DocCollection(getName(), slices, propMap, router, znodeVersion, stateUpdates);
//  }

  public DocCollection copyWithSlicesShallow(Map<String, Slice> slices){

    return new DocCollection(name, slices, propMap, router, znodeVersion);
  }

  public DocCollection copyWithSlices(Map<String, Slice> slices, Map docCollProperties){

    return new DocCollection(name, slices, docCollProperties, router, znodeVersion);
  }

  public DocCollection copy(){
    return new DocCollection(name, getSlicesCopy(), propMap, router, znodeVersion);
  }

  /**
   * Return collection name.
   */
  public String getName() {
    return name;
  }

  public Slice getSlice(String sliceName) {
    return slices.get(sliceName);
  }

  /**
   * @param consumer consume shardName vs. replica
   */
  public void forEachReplica(BiConsumer<String, Replica> consumer) {
    slices.forEach((shard, slice) -> slice.getReplicasMap().forEach((s, replica) -> consumer.accept(shard, replica)));
  }

  /**
   * Gets the list of all slices for this collection.
   */
  public Collection<Slice> getSlices() {
    return slices.values();
  }


  /**
   * Return the list of active slices for this collection.
   */
  public Collection<Slice> getActiveSlices() {
    List<Slice> activeSlices = new ArrayList<>(slices.size());
    slices.values().forEach(slice -> {
      if (slice.getState() == Slice.State.ACTIVE) {
        activeSlices.add(slice);
      }
    });
    Collections.shuffle(activeSlices);
    return activeSlices;
  }

  /**
   * Get the map of all slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getSlicesMap() {
    return slices;
  }

  /**
   * Get the map of active slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getActiveSlicesMap() {
    Map<String, Slice> activeSlices = new HashMap<>(slices.size());
    slices.forEach((key, value) -> {
      if (value.getState() == Slice.State.ACTIVE) {
        activeSlices.put(key, value);
      }
    });
    return activeSlices;
  }

  /**
   * Get the list of replicas hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getReplicas(String nodeName) {
    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();
    List<Replica> replicas = new ArrayList<>(slices.size());
    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      for (Replica replica : slice.getValue()) {
        if (replica.getNodeName().equals(nodeName)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  public List<Replica> getLeaderReplicas(String nodeName) {
    return getLeaderReplicas(nodeName, Collections.emptySet());
  }
  /**
   * Get the list of all leaders hosted on the given node or <code>null</code> if none.
   */
  public List<Replica> getLeaderReplicas(String nodeName, Set<String> liveNodes) {
    List<String> shuffleSlices = new ArrayList<>(slices.keySet());
    Collections.shuffle(shuffleSlices);
    List<Replica> leaders = new ArrayList<>(slices.size());
    for (String s : shuffleSlices) {
      Slice slice = slices.get(s);
      if (slice != null) {
        Replica leader = slice.getLeader(liveNodes);
        if (leader != null && leader.getNodeName().equals(nodeName)) {
          leaders.add(leader);
        }
      }

    }
    return leaders;
  }

  public int getZNodeVersion(){
    return znodeVersion;
  }

  public static int getStateFormat() {
    return 2;
  }

  public DocRouter getRouter() {
    return router;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    try {
      StringBuilder sb = new StringBuilder(128);
      sb.append("DocCollection(").append(id).append(':').append(name).append(':').append("v=").append(znodeVersion).append(" u=").
          append(stateUpdates).append(" shards=").append(slices.size());
      for (Slice slice : slices.values()) {
        Replica leader = slice.getLeader(Collections.emptySet());
        sb.append(' ').append(slice.getName())
            .append("[leader=").append(leader == null ? "" : leader.getName()).append(']').append(" replicas=").append(slice.getReplicas().size());
      }
      return sb.toString();
    } catch (Exception e)  {
      return "EXCEPTION in DocCollection toString";
    }
  }


  public String toFullString() {
    return "\nDocCollection(" + id + ':' + name + ':' + "v=" + znodeVersion + " u=" + stateUpdates + ")=\n" + toJSONString(this);
  }


  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String, Object> all = new LinkedHashMap<>(slices.size() + 1);
    all.putAll(this.propMap);
    all.put(SHARDS, slices);
    jsonWriter.write(all);
  }

  public Replica getReplica(String coreName) {
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplica(coreName);
      if (replica != null) return replica;
    }
    return null;
  }

  public Replica getReplicaById(Integer id) {
    return idToReplica.get(id);
  }

  public Map<Integer,Replica> getReplicaByIds() {
    return idToReplica;
  }

  public Slice getSlice(Replica replica) {
    for (Slice slice : slices.values()) {
      Replica r = slice.getReplica(replica.getName());
      if (r != null) return slice;
    }
    return null;
  }

  public Replica getLeader(String sliceName) {
    return getLeader(sliceName, Collections.emptySet());
  }

  public Replica getLeader(String sliceName, Set<String> liveNodes) {
    Slice slice = getSlice(sliceName);
    if (slice == null) return null;
    return slice.getLeader(liveNodes);
  }

  public Integer getId() {
    return id;
  }

  /**
   * Check that all replicas in a collection are live
   *
   * @see CollectionStatePredicate
   */
  public static boolean isFullyActive(Set<String> liveNodes, DocCollection collectionState,
                                      int expectedShards, int expectedReplicas) {
    Objects.requireNonNull(liveNodes);
    if (collectionState == null)
      return false;
    int activeShards = 0;
    for (Slice slice : collectionState) {
      int activeReplicas = 0;
      for (Replica replica : slice) {
        if (replica.isActive(liveNodes) == false)
          return false;
        activeReplicas++;
      }
      if (activeReplicas != expectedReplicas)
        return false;
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
    forEachReplica((s, replica) -> {
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

  /**
   * Get the shardId of a core on a specific node
   */
  public String getShardId(String nodeName, String coreName) {
    for (Slice slice : this) {
      for (Replica replica : slice) {
        if (Objects.equals(replica.getNodeName(), nodeName) && Objects.equals(replica.getName(), coreName))
          return slice.getName();
      }
    }
    return null;
  }

  public String getShardId(String coreNodeName) {
    assert coreNodeName != null;

    for (Slice slice : this) {
      for (Replica replica : slice) {
        if (replica.getName().equals(coreNodeName)) {
          return slice.getName();
        }
      }
    }
    return null;
  }

  public boolean hasStateUpdates() {
    return stateUpdates != null && !stateUpdates.isEmpty();
  }

  public StateUpdates<Integer, AtomicInteger> getStateUpdates() {
    return stateUpdates;
  }

  /**
   * Monotone applied-generation of the delta plane's per-shard cursors, or {@code -1} before any
   * delta has been applied to this collection. This is a self-contained version domain and must only
   * be compared against another collection's generation.
   */
  public int getStatePlaneGeneration() {
    StatePlaneCursors c = statePlaneCursors;
    return (c == null || c.isEmpty()) ? -1 : c.getGeneration();
  }

  /** Per-shard delta-plane reader cursors, or null before any delta has been applied. */
  public StatePlaneCursors getStatePlaneCursorsOrNull() {
    return statePlaneCursors;
  }

  /** Lazily creates and returns the per-shard delta-plane reader cursors. */
  public StatePlaneCursors getOrCreateStatePlaneCursors() {
    StatePlaneCursors c = statePlaneCursors;
    if (c == null) {
      synchronized (this) {
        c = statePlaneCursors;
        if (c == null) {
          c = new StatePlaneCursors();
          statePlaneCursors = c;
        }
      }
    }
    return c;
  }

  /**
   * Carry another collection's per-shard cursors into this one when they are ahead (structure-refresh
   * clobber guard). No-op when {@code other} has no cursors.
   */
  public void adoptStatePlaneCursors(DocCollection other) {
    if (other == null) {
      return;
    }
    StatePlaneCursors src = other.statePlaneCursors;
    if (src == null || src.isEmpty()) {
      return;
    }
    getOrCreateStatePlaneCursors().adoptIfAhead(src);
  }

  public void setZnodeVersion(int version) {
    this.znodeVersion = version;
  }

  public Map<String, Slice> getSlicesCopy() {
    return new LinkedHashMap<>(slices);
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DocCollection that = (DocCollection) o;
    return name.equals(that.name) && id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  public boolean replicaCountMatchesStateUpdates() {
    return getReplicas().size() == (this.stateUpdates.size());
  }

  public boolean replicaIsInStateUpdates(Integer id) {
    return stateUpdates.containsKey(id);

  }

  public Object get(String key) {
    return propMap.get(key);
  }

  public String getStr(String key) {
    return (String) propMap.get(key);
  }

  public Map<String,Object> getProps() {
    return propMap;
  }

  public boolean getBool(String key, boolean def) {
    Object o = get(key);
    if (o == null) return def;
    if (o instanceof Boolean) return (boolean) o;
    return Boolean.parseBoolean(o.toString());
  }

  /**
   * Get a string property value.
   */
  public Integer getInt(String key, Integer state) {
    Object o = propMap.get(key);
    return o == null ? state : Integer.valueOf(o.toString());
  }

  public void updateState(Integer id, Integer state) {
    AtomicInteger sateForReplica = (AtomicInteger) stateUpdates.get(id);
    if (sateForReplica != null) {
      if (state == null) {
        //log.info("State is null, set to DOWN");
        stateUpdates.remove(id);
        return;
      }

      sateForReplica.set(state);
    }
  }

  public void setStateUpdates(StateUpdates<Integer,AtomicInteger> stateUpdates) {
    this.stateUpdates = stateUpdates;
    setStates();
  }
}
