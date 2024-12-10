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

import static org.apache.solr.common.util.Utils.STANDARDOBJBUILDER;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection.CollectionStateProps;
import org.apache.solr.common.cloud.Replica.ReplicaStateProps;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable state of the cloud. Normally you can get the state by using {@code
 * ZkStateReader#getClusterState()}.
 *
 * <p>However, the {@link #setLiveNodes list of live nodes} is updated when nodes go up and down.
 *
 * @lucene.experimental
 */
public class ClusterState implements MapWriter {

  /** Cluster Prop that is http or https. */
  public static final String URL_SCHEME = "urlScheme";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, CollectionRef> collectionStates, immutableCollectionStates;
  private volatile Set<String> liveNodes;

  /** Use this constr when ClusterState is meant for consumption. */
  public ClusterState(Set<String> liveNodes, Map<String, DocCollection> collectionStates) {
    this(getRefMap(collectionStates), liveNodes);
  }

  private static Map<String, CollectionRef> getRefMap(Map<String, DocCollection> collectionStates) {
    Map<String, CollectionRef> collRefs = CollectionUtil.newLinkedHashMap(collectionStates.size());
    for (Entry<String, DocCollection> entry : collectionStates.entrySet()) {
      final DocCollection c = entry.getValue();
      collRefs.put(entry.getKey(), new CollectionRef(c));
    }
    return collRefs;
  }

  /**
   * Use this if all the collection states are not readily available and some needs to be lazily
   * loaded (parameter order different from constructor above to have different erasures)
   */
  public ClusterState(Map<String, CollectionRef> collectionStates, Set<String> liveNodes) {
    setLiveNodes(liveNodes);
    this.collectionStates = new LinkedHashMap<>(collectionStates);
    this.immutableCollectionStates = Collections.unmodifiableMap(this.collectionStates);
  }

  /**
   * Returns a new cluster state object modified with the given collection.
   *
   * @param collectionName the name of the modified (or deleted) collection
   * @param collection the collection object. A null value deletes the collection from the state
   * @return the updated cluster state which preserves the current live nodes
   * @lucene.internal
   */
  public ClusterState copyWith(String collectionName, DocCollection collection) {
    LinkedHashMap<String, CollectionRef> collections = new LinkedHashMap<>(collectionStates);
    if (collection == null) {
      collections.remove(collectionName);
    } else {
      collections.put(collectionName, new CollectionRef(collection));
    }
    return new ClusterState(collections, liveNodes);
  }

  /**
   * Returns true if the specified collection name exists, false otherwise.
   *
   * <p>Implementation note: This method resolves the collection reference by calling {@link
   * CollectionRef#get()} which can make a call to ZooKeeper. This is necessary because the
   * semantics of how collection list is loaded have changed in SOLR-6629.
   */
  public boolean hasCollection(String collectionName) {
    return getCollectionOrNull(collectionName) != null;
  }

  /** Get the named DocCollection object, or throw an exception if it doesn't exist. */
  public DocCollection getCollection(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null)
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection : " + collection);
    return coll;
  }

  public CollectionRef getCollectionRef(String coll) {
    return collectionStates.get(coll);
  }

  /**
   * Returns the corresponding {@link DocCollection} object for the given collection name if such a
   * collection exists. Returns null otherwise. Equivalent to getCollectionOrNull(collectionName,
   * false)
   */
  public DocCollection getCollectionOrNull(String collectionName) {
    return getCollectionOrNull(collectionName, false);
  }

  /**
   * Returns the corresponding {@link DocCollection} object for the given collection name if such a
   * collection exists. Returns null otherwise.
   *
   * @param collectionName Name of the collection
   * @param allowCached allow LazyCollectionRefs to use a time-based cached value
   *     <p>Implementation note: This method resolves the collection reference by calling {@link
   *     CollectionRef#get()} which may make a call to ZooKeeper. This is necessary because the
   *     semantics of how collection list is loaded have changed in SOLR-6629.
   */
  public DocCollection getCollectionOrNull(String collectionName, boolean allowCached) {
    CollectionRef ref = collectionStates.get(collectionName);
    return ref == null ? null : ref.get(allowCached);
  }

  /**
   * The collection names. Like a Set but might not implement it. Immutable; non-null. Some names
   * returned might not resolve via {@link #getCollectionOrNull(String)}, so consider this a close
   * approximation.
   */
  public Collection<String> getCollectionNames() {
    // should we document we are sorted too?  Albeit that ties our hands.
    return immutableCollectionStates.keySet();
  }

  /**
   * Get a map of collection name vs DocCollection objects
   *
   * <p>Implementation note: This method resolves the collection reference by calling {@link
   * CollectionRef#get()} which can make a call to ZooKeeper. This is necessary because the
   * semantics of how collection list is loaded have changed in SOLR-6629.
   *
   * @return a map of collection name vs DocCollection object
   * @deprecated see {@link #collectionStream()}
   */
  @Deprecated
  public Map<String, DocCollection> getCollectionsMap() {
    Map<String, DocCollection> result = CollectionUtil.newHashMap(collectionStates.size());
    for (Entry<String, CollectionRef> entry : collectionStates.entrySet()) {
      DocCollection collection = entry.getValue().get();
      if (collection != null) {
        result.put(entry.getKey(), collection);
      }
    }
    return result;
  }

  /** Get names of the currently live nodes. */
  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  @Deprecated
  public String getShardId(String nodeName, String coreName) {
    return getShardId(null, nodeName, coreName);
  }

  @Deprecated
  public String getShardId(String collectionName, String nodeName, String coreName) {
    if (coreName == null || nodeName == null) {
      return null;
    }
    Collection<CollectionRef> states = Collections.emptyList();
    if (collectionName != null) {
      CollectionRef c = collectionStates.get(collectionName);
      if (c != null) states = Collections.singletonList(c);
    } else {
      states = collectionStates.values();
    }

    for (CollectionRef ref : states) {
      DocCollection coll = ref.get();
      if (coll == null) continue; // this collection got removed in between, skip
      // TODO: for really large clusters, we could 'index' on this
      return Optional.ofNullable(coll.getReplicas(nodeName)).stream()
          .flatMap(List::stream)
          .filter(r -> coreName.equals(r.getStr(ReplicaStateProps.CORE_NAME)))
          .map(Replica::getShard)
          .findAny()
          .orElse(null);
    }
    return null;
  }

  @Deprecated
  public Map<String, List<Replica>> getReplicaNamesPerCollectionOnNode(final String nodeName) {
    Map<String, List<Replica>> replicaNamesPerCollectionOnNode = new HashMap<>();
    collectionStates.values().stream()
        .map(CollectionRef::get)
        .filter(Objects::nonNull)
        .forEach(
            col -> {
              List<Replica> replicas = col.getReplicas(nodeName);
              if (replicas != null && !replicas.isEmpty()) {
                replicaNamesPerCollectionOnNode.put(col.getName(), replicas);
              }
            });
    return replicaNamesPerCollectionOnNode;
  }

  /** Check if node is alive. */
  public boolean liveNodesContain(String name) {
    return liveNodes.contains(name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("live nodes:").append(liveNodes);
    sb.append("\n");
    sb.append("collections:").append(collectionStates);
    return sb.toString();
  }

  /**
   * Create a ClusterState from Json. This method doesn't support legacy configName location and
   * thus don't call it where that's important
   *
   * @param bytes a byte array of a Json representation of a mapping from collection name to the
   *     Json representation of a {@link DocCollection} as written by {@link #write(JSONWriter)}. It
   *     can represent one or more collections.
   * @param liveNodes list of live nodes
   * @param creationTime assigns this date to all {@link DocCollection} referenced by the returned
   *     {@link ClusterState}
   * @return the ClusterState
   */
  @Deprecated
  public static ClusterState createFromJson(
      int version,
      byte[] bytes,
      Set<String> liveNodes,
      Instant creationTime,
      DocCollection.PrsSupplier prsSupplier) {
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(liveNodes, Collections.<String, DocCollection>emptyMap());
    }
    @SuppressWarnings({"unchecked"})
    Map<String, Object> stateMap =
        (Map<String, Object>) Utils.fromJSON(bytes, 0, bytes.length, STR_INTERNER_OBJ_BUILDER);
    return createFromCollectionMap(version, stateMap, liveNodes, creationTime, prsSupplier);
  }

  @Deprecated
  public static ClusterState createFromJson(int version, byte[] bytes, Set<String> liveNodes) {
    return createFromJson(version, bytes, liveNodes, Instant.EPOCH, null);
  }

  @Deprecated
  public static ClusterState createFromCollectionMap(
      int version,
      Map<String, Object> stateMap,
      Set<String> liveNodes,
      Instant creationTime,
      DocCollection.PrsSupplier prsSupplier) {
    Map<String, CollectionRef> collections = CollectionUtil.newLinkedHashMap(stateMap.size());
    for (Entry<String, Object> entry : stateMap.entrySet()) {
      String collectionName = entry.getKey();
      @SuppressWarnings({"unchecked"})
      DocCollection coll =
          collectionFromObjects(
              collectionName,
              (Map<String, Object>) entry.getValue(),
              version,
              creationTime,
              prsSupplier);
      collections.put(collectionName, new CollectionRef(coll));
    }

    return new ClusterState(collections, liveNodes);
  }

  @Deprecated
  public static ClusterState createFromCollectionMap(
      int version, Map<String, Object> stateMap, Set<String> liveNodes) {
    return createFromCollectionMap(version, stateMap, liveNodes, Instant.EPOCH, null);
  }

  /**
   * @lucene.internal
   */
  // TODO move to static DocCollection.loadFromMap
  public static DocCollection collectionFromObjects(
      String name,
      Map<String, Object> objs,
      int version,
      Instant creationTime,
      DocCollection.PrsSupplier prsSupplier) {
    Map<String, Object> props;
    Map<String, Slice> slices;

    if (Boolean.parseBoolean(String.valueOf(objs.get(CollectionStateProps.PER_REPLICA_STATE)))) {
      if (log.isDebugEnabled()) {
        log.debug("a collection {} has per-replica state", name);
      }
    }
    @SuppressWarnings({"unchecked"})
    Map<String, Object> sliceObjs = (Map<String, Object>) objs.get(CollectionStateProps.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection
      // shards.
      slices = Slice.loadAllFromMap(name, objs);
      props = Collections.emptyMap();
    } else {
      slices = Slice.loadAllFromMap(name, sliceObjs);
      objs.remove(CollectionStateProps.SHARDS);
      props = new HashMap<>(objs);
    }

    Object routerObj = props.get(CollectionStateProps.DOC_ROUTER);
    DocRouter router;
    if (routerObj == null) {
      router = DocRouter.DEFAULT;
    } else if (routerObj instanceof String) {
      // back compat with Solr4.4
      router = DocRouter.getDocRouter((String) routerObj);
    } else {
      @SuppressWarnings({"rawtypes"})
      Map routerProps = (Map) routerObj;
      router = DocRouter.getDocRouter((String) routerProps.get("name"));
    }

    return DocCollection.create(name, slices, props, router, version, creationTime, prsSupplier);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    for (Entry<String, CollectionRef> e : collectionStates.entrySet()) {
      if (e.getValue().getClass() == CollectionRef.class) {
        DocCollection coll = e.getValue().get();
        ew.put(coll.getName(), coll);
      }
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((liveNodes == null) ? 0 : liveNodes.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof ClusterState other)) return false;
    if (liveNodes == null) {
      return other.liveNodes == null;
    } else return liveNodes.equals(other.liveNodes);
  }

  /**
   * @lucene.internal used only by ZkStateReader
   */
  void setLiveNodes(Set<String> liveNodes) {
    this.liveNodes = Set.copyOf(liveNodes);
  }

  /**
   * Be aware that this may return collections which may not exist now. You can confirm that this
   * collection exists after verifying CollectionRef.get() != null
   *
   * @deprecated see {@link #collectionStream()}
   */
  @Deprecated
  public Map<String, CollectionRef> getCollectionStates() {
    return immutableCollectionStates;
  }

  /**
   * Streams the resolved {@link DocCollection}s, which will often fetch from ZooKeeper for each one
   * for a many-collection scenario. Use this sparingly; some users have thousands of collections!
   */
  public Stream<DocCollection> collectionStream() {
    return collectionStates.values().stream().map(CollectionRef::get).filter(Objects::nonNull);
  }

  /**
   * Calls {@code consumer} with a resolved {@link DocCollection}s for all collections. Use this
   * sparingly in case there are many collections.
   *
   * @deprecated see {@link #collectionStream()}
   */
  @Deprecated
  public void forEachCollection(Consumer<DocCollection> consumer) {
    collectionStream().forEach(consumer);
  }

  public static class CollectionRef {
    protected final AtomicInteger gets = new AtomicInteger();
    private final DocCollection coll;

    public int getCount() {
      return gets.get();
    }

    public CollectionRef(DocCollection coll) {
      this.coll = coll;
    }

    /**
     * Return the DocCollection, always re-fetching if lazy. Equivalent to get(false)
     *
     * @return The collection state modeled in zookeeper
     */
    public DocCollection get() {
      return get(false);
    }

    /**
     * Return the DocCollection
     *
     * @param allowCached Determines if cached value can be used. Applies only to LazyCollectionRef.
     * @return The collection state modeled in zookeeper
     */
    public DocCollection get(boolean allowCached) {
      gets.incrementAndGet();
      return coll;
    }

    public boolean isLazilyLoaded() {
      return false;
    }

    @Override
    public String toString() {
      if (coll != null) {
        return coll.toString();
      } else {
        return "null DocCollection ref";
      }
    }
  }

  /** The approximate number of collections. */
  public int size() {
    return collectionStates.size();
  }

  private static volatile Function<JSONParser, ObjectBuilder> STR_INTERNER_OBJ_BUILDER =
      STANDARDOBJBUILDER;

  /**
   * @lucene.internal
   */
  public static void setStrInternerParser(Function<JSONParser, ObjectBuilder> fun) {
    if (fun == null) return;
    STR_INTERNER_OBJ_BUILDER = fun;
  }
}
