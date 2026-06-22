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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.solr.common.util.Utils;

public class Replica extends ZkNodeProps {


  /**
   * The replica's state. In general, if the node the replica is hosted on is
   * not under {@code /live_nodes} in ZK, the replica's state should be
   * discarded.
   */
  public enum State {
    
    /**
     * The replica is ready to receive updates and queries.
     * <p>
     * <b>NOTE</b>: when the node the replica is hosted on crashes, the
     * replica's state may remain ACTIVE in ZK. To determine if the replica is
     * truly active, you must also verify that its {@link Replica#getNodeName()
     * node} is under {@code /live_nodes} in ZK (or use
     * {@link ZkStateReader#isNodeLive(String)} (String)}).
     * </p>
     */
    ACTIVE,

    LEADER,
    
    /**
     * The first state before {@link State#RECOVERING}. A node in this state
     * should be actively trying to move to {@link State#RECOVERING}.
     * <p>
     * <b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node
     * it's hosted on gracefully shuts down. This is a best effort though, and
     * should not be relied on.
     * </p>
     */
    DOWN,
    
    /**
     * The node is recovering from the leader. This might involve peer-sync,
     * full replication or finding out things are already in sync.
     */
    RECOVERING,

    BUFFERING,
    
    /**
     * Recovery attempts have not worked, something is not right.
     * <p>
     * <b>NOTE</b>: This state doesn't matter if the node is not part of
     * {@code /live_nodes} in ZK; in that case the node is not part of the
     * cluster and it's state should be discarded.
     * </p>
     */
    RECOVERY_FAILED;
    
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }

    public static Integer getShortState(State state) {
      switch (state) {
        case LEADER:
          return 1;
        case ACTIVE:
          return 2;
        case RECOVERING:
          return 4;
        case BUFFERING:
          return 3;
        case RECOVERY_FAILED:
          return 6;
        case DOWN:
          return 5;
        default:
          throw new IllegalStateException();
      }
    }

    public static State shortStateToState(Integer shortState) {
      return shortStateToState(shortState, false);
    }

    public static State shortStateToState(Integer shortState, boolean published) {
      if (shortState.equals(1)) {
        if (published) {
          return State.ACTIVE;
        } else {
          return State.LEADER;
        }
      } if (shortState.equals(0)) {
        // 0 is the "unknown/uninitialized" sentinel (shortStateToLetterState(0)=='U'). Treat it as DOWN to
        // stay consistent with the letter mapping and the fork's DOWN-is-default convention, rather than
        // throwing. Does not touch shortState 1, so the LEADER/ACTIVE invariant is preserved.
        return State.DOWN;
      } if (shortState.equals(2)) {
        return State.ACTIVE;
      } if (shortState.equals(4)) {
        return State.RECOVERING;
      } else if (shortState.equals(3)) {
        return State.BUFFERING;
      } else if (shortState.equals(5)) {
        return State.DOWN;
      } else if (shortState.equals(6)) {
        return State.RECOVERY_FAILED;
      }
      throw new IllegalStateException("Unknown state: " + shortState);
    }

    public static Character shortStateToLetterState(Integer shortState) {
      if (shortState.equals(2)) {
        return 'A';
      } if (shortState.equals(1)) {
        return 'L';
      } else if (shortState.equals(4)) {
        return 'R';
      } else if (shortState.equals(3)) {
        return 'B';
      } else if (shortState.equals(5)) {
        return 'D';
      } else if (shortState.equals(6)) {
        return 'R';
      } else if (shortState.equals(0)) {
        return 'U';
      }
      throw new IllegalStateException("Unknown state: " + shortState);
    }


    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }

  public enum Type {
    /**
     * Writes updates to transaction log and indexes locally. Replicas of type {@link Type#NRT} support NRT (soft commits) and RTG. 
     * Any {@link Type#NRT} replica can become a leader. A shard leader will forward updates to all active {@link Type#NRT} and
     * {@link Type#TLOG} replicas. 
     */
    NRT,
    /**
     * Writes to transaction log, but not to index, uses replication. Any {@link Type#TLOG} replica can become leader (by first
     * applying all local transaction log elements). If a replica is of type {@link Type#TLOG} but is also the leader, it will behave 
     * as a {@link Type#NRT}. A shard leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    TLOG,
    /**
     * Doesn’t index or writes to transaction log. Just replicates from {@link Type#NRT} or {@link Type#TLOG} replicas. {@link Type#PULL}
     * replicas can’t become shard leaders (i.e., if there are only pull replicas in the collection at some point, updates will fail
     * same as if there is no leaders, queries continue to work), so they don’t even participate in elections.
     */
    PULL;

    public static Type get(String name){
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  public interface NodeNameToBaseUrl {
    String getBaseUrlForNodeName(final String nodeName);
  }

  private final String name;
  private final String nodeName;
  private volatile AtomicInteger state;
  private volatile Object stateLinkOwner;
  // The state published in state.json / a CLUSTERSTATUS response (captured before STATE_PROP is stripped
  // below). Used as the base/default when there is no live PerReplicaState/StateUpdates entry — e.g. a cluster
  // state fetched over HTTP has no state-updates channel, so without this every replica would default to DOWN.
  private final State publishedState;

  private final Type type;
  public volatile Slice slice;
  public final String collection;
  private final String sliceName;

  public Replica(String name, Object2ObjectMap<String,Object> newProps, String collection, Integer collectionId, String sliceName) {
    this(name, newProps , collection, collectionId, sliceName,null);
  }


  public Replica(String name, Object2ObjectMap<String,Object> propMap, String collection, Integer collectionId, String sliceName, Slice slice) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.sliceName = sliceName;
    this.name = name;

    // Capture the published state from the prop map (state.json / CLUSTERSTATUS JSON) before removing it, and
    // seed the live state with it. This keeps getState() non-null and correct on read-only/HTTP-fetched cluster
    // states (which carry no StateUpdates channel); the live channel overrides it via setState() when present.
    State published = Replica.State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
    this.publishedState = published == null ? State.DOWN : published;
    this.state = new AtomicInteger(Replica.State.getShortState(this.publishedState));

    propMap.remove(ZkStateReader.STATE_PROP);

    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);

    Object rawId = propMap.get("id");
    if (rawId instanceof Integer) {
      this.id = (Integer) rawId;
    } else if (rawId instanceof Long) {
      this.id = ((Long) rawId).intValue();
    } else if (rawId instanceof String) {
      // ids are serialized as strings in some JSON sources
      this.id = Integer.valueOf((String) rawId);
    } else {
      // A bare/synthetic replica (e.g. an election-only participant) may carry no numeric id.
      this.id = null;
    }


    this.collectionId = collectionId;
    if (this.collectionId == null) {
      Object collId = propMap.get("collId");
      if (collId != null) {
        this.collectionId = Integer.parseInt((String) collId);
      }
    } else {
      propMap.put("collId", collectionId);
    }

    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.nodeName, "'node_name' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.collectionId, "'collectionId' must not be null");


    hashcode = Objects.hash(name, id, collectionId);
  }

  Integer id;
  Integer collectionId;

  private final int hashcode;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Replica replica = (Replica) o;
    return name.equals(replica.name)
        && Objects.equals(id, replica.id)
        && collectionId.equals(replica.collectionId);
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  public String getId() {
    // For synthetic replicas (no numeric id) fall back to the unique name instead of the literal "null",
    // otherwise every id-less replica in a collection collides to "<collId>-null". name is non-null and unique
    // (it is the core/replica name). equals() keys off the numeric id field, not getId(), so this is safe.
    return collectionId + "-" + (id == null ? name : id.toString());
  }

  public Integer getInternalId() {
    return id;
  }

  public Integer getCollectionId() {
    return collectionId;
  }


  public String getCollection() {
    return collection;
  }

  public String getSlice() {
    return sliceName;
  }

  public Slice getSliceOwner(){
    return slice;
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return getCoreUrl(getBaseUrl(), name);
  }

  public String getBaseUrl() {
    return Utils.getBaseUrlForNodeName(nodeName, ZkStateReader.getUrlScheme());
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return nodeName;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
//    if (state == null) {
//      return State.DOWN;
//    }
    return Replica.State.shortStateToState(state.get(), true);
  }

  /** The state published in state.json / CLUSTERSTATUS (the base state when no live StateUpdates entry exists). */
  public State getPublishedState() {
    return publishedState;
  }

  @Override
  public void write(org.noggit.JSONWriter jsonWriter) {
    // The ctor removes STATE_PROP from propMap (the live state lives in the state channel, not props), so
    // re-add the current state before serializing. Otherwise a JSON-serialized cluster state (e.g. a
    // CLUSTERSTATUS response) omits each replica's state and clients treat every replica as DOWN, failing with
    // "Could not find a healthy node". getState() reflects the live channel value.
    //
    // Serialize a private copy rather than mutating propMap in place: propMap is shared across DocCollection
    // graphs and is an unsynchronized fastutil open-hash map, so a concurrent CLUSTERSTATUS serialize racing
    // an overseer state write that touches the same map could corrupt it (lost entry / resize race) or emit a
    // torn state. The copy makes write() read-only on the shared map.
    Object2ObjectMap<String,Object> snapshot = shallowCopy();
    snapshot.put(ZkStateReader.STATE_PROP, getState().toString());
    new ZkNodeProps(snapshot).write(jsonWriter);
  }

  public State getRawState() {
    Integer rawState = state == null ? null : state.get();
    if (rawState == null) {
      return State.DOWN;
    }
    return Replica.State.shortStateToState(rawState, false);
  }

  /**
   * Whether this replica is the shard leader. In this fork leadership is carried in the StateUpdates
   * channel as {@link State#LEADER} rather than as a serialized "leader" property, so leadership is
   * derived from the live raw state. {@link #getBool}, {@link #getStr}, {@link #get} and
   * {@link #containsKey} are overridden below to honor the legacy public {@code "leader"} property
   * contract (used by tests, tools and e.g. {@link ZkCoreNodeProps#isLeader()} / ReplaceNodeCmd) by
   * deriving it from this method -- without re-adding the key to the serialized propMap.
   */
  public boolean isLeader() {
    return getRawState() == State.LEADER;
  }

  @Override
  public boolean getBool(String key, boolean def) {
    if (ZkStateReader.LEADER_PROP.equals(key)) {
      return isLeader();
    }
    return super.getBool(key, def);
  }

  @Override
  public String getStr(String key) {
    if (ZkStateReader.LEADER_PROP.equals(key)) {
      return isLeader() ? "true" : null;
    }
    return super.getStr(key);
  }

  @Override
  public String getStr(String key, String def) {
    if (ZkStateReader.LEADER_PROP.equals(key)) {
      return isLeader() ? "true" : def;
    }
    return super.getStr(key, def);
  }

  @Override
  public Object get(String key) {
    if (ZkStateReader.LEADER_PROP.equals(key)) {
      return isLeader() ? "true" : null;
    }
    return super.get(key);
  }

  @Override
  public boolean containsKey(String key) {
    if (ZkStateReader.LEADER_PROP.equals(key)) {
      return isLeader();
    }
    return super.containsKey(key);
  }

  public void removeStateUpdates() {
    AtomicInteger currentState = state;
    if (currentState != null) {
      currentState.set(5);
    }
  }

  public void setState(AtomicInteger replicaState) {
    if (replicaState != null) {
      this.state = replicaState;
    }
  }

  /**
   * Link this replica's live-state AtomicInteger to a DocCollection's StateUpdates map entry.
   * Replica objects are SHARED between DocCollection object graphs (slice copies are shallow —
   * e.g. {@code DocCollection.copy()} via {@code getSlicesCopy()}, used by the overseer's
   * ZkStateWriter on every node-down/recovery-node processing). Only the DocCollection that
   * first linked the replica may re-seat it (it does so legitimately when adopting the previous
   * watched collection's map in ZkStateReader's merge). A FOREIGN DocCollection construction
   * sharing the object must not steal the link, or the original reader's state updates land in
   * atomics nobody reads and the replica goes permanently stale on that node — observed as the
   * overseer-host node forever seeing a recovered replica as DOWN.
   */
  void linkState(AtomicInteger replicaState, Object owner) {
    if (replicaState == null) {
      return;
    }
    if (stateLinkOwner == null || stateLinkOwner == owner) {
      this.state = replicaState;
      this.stateLinkOwner = owner;
    }
  }

  void setSlice(Slice slice) {
    this.slice = slice;
    if (state != null && state.get() == State.getShortState(State.LEADER)) {
      slice.setLeader(this);
    }
  }

  public boolean isActive(Set<String> liveNodes) {
    State currentState = State.shortStateToState(state.get(), true);
    return this.nodeName != null && liveNodes.contains(this.nodeName) && (currentState == State.ACTIVE || currentState == State.LEADER);
  }

  public boolean isActive() {

    State currentState = State.shortStateToState(state.get(), true);
    return currentState == State.ACTIVE || currentState == State.LEADER;
  }
  
  public Type getType() {
    return this.type;
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ZkStateReader.PROPERTY_PROP_PREFIX)) {
      propertyKey = ZkStateReader.PROPERTY_PROP_PREFIX + propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  public static String getCoreUrl(String baseUrl, String coreName) {
    StringBuilder sb = new StringBuilder(baseUrl.length() + coreName.length() + 1);
    sb.append(baseUrl);
    if (!(!baseUrl.isEmpty() && baseUrl.charAt(baseUrl.length() - 1) == '/')) sb.append("/");
    sb.append(coreName);
    return sb.toString();
  }


  public Replica copyWithProps(Map props) {
    Object2ObjectLinkedOpenHashMap newProps = new Object2ObjectLinkedOpenHashMap(propMap.size() + props.size(), 0.5f);
    newProps.putAll(propMap);
    newProps.putAll(props);
    Replica r = new Replica(name, newProps, collection, collectionId, sliceName, slice);
    return r;
  }

  public Replica copyWithProps(Slice slice, Map props) {
    Object2ObjectLinkedOpenHashMap newProps = new Object2ObjectLinkedOpenHashMap(propMap, 0.5f);
    newProps.putAll(props);
    Replica r = new Replica(name, newProps, collection, collectionId, sliceName, slice);
    return r;
  }

  @Override
  public String toString() {
    return name + "(" + getId() + ")" + ':' + Utils.toJSONString(this); // small enough, keep it on one line (i.e. no indent)
  }
}
