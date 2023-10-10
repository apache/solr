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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replica extends ZkNodeProps implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The replica's state. In general, if the node the replica is hosted on is not under {@code
   * /live_nodes} in ZK, the replica's state should be discarded.
   */
  public enum State {

    /**
     * The replica is ready to receive updates and queries.
     *
     * <p><b>NOTE</b>: when the node the replica is hosted on crashes, the replica's state may
     * remain ACTIVE in ZK. To determine if the replica is truly active, you must also verify that
     * its {@link Replica#getNodeName() node} is under {@code /live_nodes} in ZK (or use {@link
     * ClusterState#liveNodesContain(String)}).
     */
    ACTIVE("A"),

    /**
     * The first state before {@link State#RECOVERING}. A node in this state should be actively
     * trying to move to {@link State#RECOVERING}.
     *
     * <p><b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node it's hosted on
     * gracefully shuts down. This is a best effort though, and should not be relied on.
     */
    DOWN("D"),

    /**
     * The node is recovering from the leader. This might involve peer-sync, full replication or
     * finding out things are already in sync.
     */
    RECOVERING("R"),

    /**
     * Recovery attempts have not worked, something is not right.
     *
     * <p><b>NOTE</b>: This state doesn't matter if the node is not part of {@code /live_nodes} in
     * ZK; in that case the node is not part of the cluster and it's state should be discarded.
     */
    RECOVERY_FAILED("F");

    /**
     * short name for a state. Used to encode this in the state node see {@link
     * PerReplicaStates.State}
     */
    public final String shortName;

    public final String longName;

    State(String c) {
      this.shortName = c;
      this.longName = super.toString().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
      return longName;
    }

    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }

  public enum Type {
    /**
     * Writes updates to transaction log and indexes locally. Replicas of type {@link Type#NRT}
     * support NRT (soft commits) and RTG. Any {@link Type#NRT} replica can become a leader. A shard
     * leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    NRT(true),
    /**
     * Writes to transaction log, but not to index, uses replication. Any {@link Type#TLOG} replica
     * can become leader (by first applying all local transaction log elements). If a replica is of
     * type {@link Type#TLOG} but is also the leader, it will behave as a {@link Type#NRT}. A shard
     * leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    TLOG(true),
    /**
     * Doesn’t index or writes to transaction log. Just replicates from {@link Type#NRT} or {@link
     * Type#TLOG} replicas. {@link Type#PULL} replicas can’t become shard leaders (i.e., if there
     * are only pull replicas in the collection at some point, updates will fail same as if there is
     * no leaders, queries continue to work), so they don’t even participate in elections.
     */
    PULL(false);

    public final boolean leaderEligible;

    Type(boolean b) {
      this.leaderEligible = b;
    }

    public static Type get(String name) {
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }

    /**
     * Only certain replica types can become leaders
     *
     * @param type the type of a replica
     * @return true if that type is able to be leader, false otherwise
     */
    public static boolean isLeaderType(Type type) {
      return type == null || type == NRT || type == TLOG;
    }
  }

  // immutable
  public final String name; // coreNode name
  public final String node;
  public final String core;
  public final Type type;
  public final String shard, collection;
  private AtomicReference<PerReplicaStates> perReplicaStatesRef;

  // mutable
  private State state;

  void setPerReplicaStatesRef(AtomicReference<PerReplicaStates> perReplicaStatesRef) {
    this.perReplicaStatesRef = perReplicaStatesRef;
  }

  public Replica(String name, Map<String, Object> map, String collection, String shard) {
    super(new HashMap<>());
    propMap.putAll(map);
    this.collection = collection;
    this.shard = shard;
    this.name = name;
    this.node = (String) propMap.get(ReplicaStateProps.NODE_NAME);
    this.core = (String) propMap.get(ReplicaStateProps.CORE_NAME);
    this.type = Type.get((String) propMap.get(ReplicaStateProps.TYPE));
    // default to ACTIVE
    this.state =
        State.getState(
            String.valueOf(propMap.getOrDefault(ReplicaStateProps.STATE, State.ACTIVE.toString())));
    validate();
  }

  // clone constructor
  public Replica(
      String name,
      String node,
      String collection,
      String shard,
      String core,
      State state,
      Type type,
      Map<String, Object> props) {
    super(new HashMap<>());
    this.name = name;
    this.node = node;
    this.state = state;
    this.type = type;
    this.collection = collection;
    this.shard = shard;
    this.core = core;
    if (props != null) {
      this.propMap.putAll(props);
    }
    validate();
  }

  /**
   * This constructor uses a map with one key (coreNode name) and a value that is a map containing
   * all replica properties.
   *
   * @param nestedMap nested map containing replica properties
   */
  @SuppressWarnings("unchecked")
  public Replica(Map<String, Object> nestedMap) {
    this.name = nestedMap.keySet().iterator().next();
    Map<String, Object> details = (Map<String, Object>) nestedMap.get(name);
    Objects.requireNonNull(details);
    details = Utils.getDeepCopy(details, 4);
    this.collection = String.valueOf(details.get("collection"));
    this.shard = String.valueOf(details.get("shard"));
    this.core = String.valueOf(details.get("core"));
    this.node = String.valueOf(details.get("node_name"));

    this.propMap.putAll(details);
    type =
        Replica.Type.valueOf(String.valueOf(propMap.getOrDefault(ReplicaStateProps.TYPE, "NRT")));
    if (state == null)
      state =
          State.getState(String.valueOf(propMap.getOrDefault(ReplicaStateProps.STATE, "active")));
    validate();
  }

  private final void validate() {
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.core, "'core' must not be null");
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.shard, "'shard' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.state, "'state' must not be null");
    Objects.requireNonNull(this.node, "'node' must not be null");

    String baseUrl = (String) propMap.get(ReplicaStateProps.BASE_URL);
    Objects.requireNonNull(baseUrl, "'base_url' must not be null");

    // make sure all declared props are in the propMap
    propMap.put(ReplicaStateProps.NODE_NAME, node);
    propMap.put(ReplicaStateProps.CORE_NAME, core);
    propMap.put(ReplicaStateProps.TYPE, type.toString());
    propMap.put(ReplicaStateProps.STATE, state.toString());
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  @Override
  public Map<String, Object> getProperties() {
    return super.getProperties();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Replica)) return false;
    if (!super.equals(o)) return false;

    Replica other = (Replica) o;

    return name.equals(other.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return ZkCoreNodeProps.getCoreUrl(getBaseUrl(), core);
  }

  public String getBaseUrl() {
    return getStr(ReplicaStateProps.BASE_URL);
  }

  /** SolrCore name. */
  public String getCoreName() {
    return core;
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return node;
  }

  /** Returns the {@link State} of this replica. */
  public State getState() {
    if (perReplicaStatesRef != null) {
      PerReplicaStates.State s = perReplicaStatesRef.get().get(name);
      if (s != null) {
        return s.state;
      } else {
        return State.DOWN;
      }
    }
    return state;
  }

  public void setState(State state) {
    this.state = state;
    propMap.put(ReplicaStateProps.STATE, this.state.toString());
  }

  public boolean isActive(Set<String> liveNodes) {
    return this.node != null && liveNodes.contains(this.node) && getState() == State.ACTIVE;
  }

  public Type getType() {
    return this.type;
  }

  public boolean isLeader() {
    if (perReplicaStatesRef != null) {
      PerReplicaStates.State st = perReplicaStatesRef.get().get(name);
      return st == null ? false : st.isLeader;
    }
    return getBool(ReplicaStateProps.LEADER, false);
  }

  public Object get(String key, Object defValue) {
    Object o = get(key);
    if (o != null) {
      return o;
    } else {
      return defValue;
    }
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ReplicaStateProps.PROPERTY_PREFIX)) {
      propertyKey = ReplicaStateProps.PROPERTY_PREFIX + propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  public Replica copyWith(PerReplicaStates.State state) {
    log.debug("A replica is updated with new state : {}", state);
    Map<String, Object> props = new LinkedHashMap<>(propMap);
    if (state == null) {
      props.put(ReplicaStateProps.STATE, State.DOWN.toString());
      props.remove(ReplicaStateProps.LEADER);
    } else {
      props.put(ReplicaStateProps.STATE, state.state.toString());
      if (state.isLeader) props.put(ReplicaStateProps.LEADER, "true");
    }
    Replica r = new Replica(name, props, collection, shard);
    return r;
  }

  public PerReplicaStates.State getReplicaState() {
    if (perReplicaStatesRef != null) {
      return perReplicaStatesRef.get().get(name);
    }
    return null;
  }

  @Override
  public Object clone() {
    return new Replica(name, node, collection, shard, core, getState(), type, propMap);
  }

  private static final Map<String, State> STATES = new HashMap<>();

  static {
    STATES.put(Replica.State.ACTIVE.shortName, Replica.State.ACTIVE);
    STATES.put(Replica.State.DOWN.shortName, Replica.State.DOWN);
    STATES.put(Replica.State.RECOVERING.shortName, Replica.State.RECOVERING);
    STATES.put(Replica.State.RECOVERY_FAILED.shortName, Replica.State.RECOVERY_FAILED);
  }

  public static State getState(String shortName) {
    return STATES.get(shortName);
  }

  @Override
  public void writeMap(MapWriter.EntryWriter ew) throws IOException {
    ew.putIfNotNull(ReplicaStateProps.CORE_NAME, core)
        .putIfNotNull(ReplicaStateProps.NODE_NAME, node)
        .putIfNotNull(ReplicaStateProps.TYPE, type.toString())
        .putIfNotNull(ReplicaStateProps.STATE, getState().toString())
        .putIfNotNull(ReplicaStateProps.LEADER, () -> isLeader() ? "true" : null)
        .putIfNotNull(
            ReplicaStateProps.FORCE_SET_STATE, propMap.get(ReplicaStateProps.FORCE_SET_STATE))
        .putIfNotNull(ReplicaStateProps.BASE_URL, propMap.get(ReplicaStateProps.BASE_URL));
    for (Map.Entry<String, Object> e : propMap.entrySet()) {
      if (!ReplicaStateProps.WELL_KNOWN_PROPS.contains(e.getKey())) {
        ew.putIfNotNull(e.getKey(), e.getValue());
      }
    }
  }

  @Override
  public String toString() {
    return name
        + ':'
        + Utils.toJSONString(propMap); // small enough, keep it on one line (i.e. no indent)
  }

  /** JSON properties related to a replica's state. */
  public interface ReplicaStateProps {
    String COLLECTION = "collection";
    String SHARD_ID = "shard";
    String REPLICA_ID = "replica";
    String LEADER = "leader";
    String STATE = "state";
    String CORE_NAME = "core";
    String CORE_NODE_NAME = "core_node_name";
    String TYPE = "type";
    String NODE_NAME = "node_name";
    String BASE_URL = "base_url";
    String PROPERTY_PREFIX = "property.";
    String FORCE_SET_STATE = "force_set_state";
    Set<String> WELL_KNOWN_PROPS =
        Set.of(
            LEADER, STATE, CORE_NAME, CORE_NODE_NAME, TYPE, NODE_NAME, BASE_URL, FORCE_SET_STATE);
  }

  public ZkNodeProps toFullProps() {
    return new ZkNodeProps()
        .plus(propMap)
        .plus(ReplicaStateProps.COLLECTION, getCollection())
        .plus(ReplicaStateProps.SHARD_ID, getShard())
        .plus(ReplicaStateProps.REPLICA_ID, getName());
  }
}
