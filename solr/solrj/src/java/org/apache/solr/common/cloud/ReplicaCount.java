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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/** Tracks the number of replicas per replica type. This class is mutable. */
public final class ReplicaCount {
  private final EnumMap<Replica.Type, Integer> countByType;

  private ReplicaCount() {
    this.countByType = new EnumMap<>(Replica.Type.class);
  }

  public static ReplicaCount empty() {
    return new ReplicaCount();
  }

  public static ReplicaCount of(Replica.Type type, Integer count) {
    ReplicaCount replicaCount = new ReplicaCount();
    replicaCount.put(type, count);
    return replicaCount;
  }

  public static ReplicaCount of(Integer nrtReplicas, Integer tlogReplicas, Integer pullReplicas) {
    ReplicaCount replicaCount = new ReplicaCount();
    replicaCount.put(Replica.Type.NRT, nrtReplicas);
    replicaCount.put(Replica.Type.TLOG, tlogReplicas);
    replicaCount.put(Replica.Type.PULL, pullReplicas);
    return replicaCount;
  }

  public static ReplicaCount fromMessage(ZkNodeProps message) {
    return fromMessage(message, null);
  }

  public static ReplicaCount fromMessage(ZkNodeProps message, DocCollection collection) {
    return fromMessage(message, collection, null);
  }

  /**
   * Creates a {@link ReplicaCount} from a message.
   *
   * <p>This method has a rich logic for defaulting parameters. A collection can optionally be
   * provided as a fallback for all properties. Among all properties, replica factor is applied as
   * the number of replicas for the default replica type. The default replica type can be provided
   * as a property, or else comes from {@link Replica.Type#defaultType()}.
   *
   * <p>Not all capabilities apply on every call of this method. For example, the message may not
   * contain any replication factor.
   *
   * @param message a message.
   * @param collection an optional collection providing defaults.
   * @param defaultReplicationFactor an additional optional default replica factor, if none is found
   *     in the collection.
   */
  public static ReplicaCount fromMessage(
      ZkNodeProps message, DocCollection collection, Integer defaultReplicationFactor) {
    // Default replica type can be overridden by a property.
    Replica.Type defaultReplicaType =
        Replica.Type.valueOf(
            message
                .getStr(CollectionAdminParams.REPLICA_TYPE, Replica.Type.defaultType().name())
                .toUpperCase(Locale.ROOT));
    ReplicaCount replicaCount = new ReplicaCount();
    for (Replica.Type replicaType : Replica.Type.values()) {
      final Integer count;
      if (replicaType == defaultReplicaType) {
        // Number of replicas for the default replica type defaults to the replication factor.
        // Replication factor can be overridden by a property, or a custom default.
        Integer defaultCount =
            (null != collection)
                ? collection.getInt(
                    replicaType.numReplicasPropertyName,
                    collection.getInt(
                        CollectionAdminParams.REPLICATION_FACTOR, defaultReplicationFactor))
                : defaultReplicationFactor;
        count =
            message.getInt(
                replicaType.numReplicasPropertyName,
                message.getInt(CollectionAdminParams.REPLICATION_FACTOR, defaultCount));
      } else {
        Integer defaultCount =
            (null != collection)
                ? collection.getInt(replicaType.numReplicasPropertyName, null)
                : null;
        count = message.getInt(replicaType.numReplicasPropertyName, defaultCount);
      }
      replicaCount.put(replicaType, count);
    }
    return replicaCount;
  }

  /**
   * Creates a {@link ReplicaCount} from a properties map.
   *
   * <p>This method is much simpler than {@link #fromMessage}, as it only considers properties
   * containing a number of replicas (and ignores other properties such as the replication factor).
   *
   * @param propMap a properties map
   */
  public static ReplicaCount fromProps(Map<String, Object> propMap) {
    ReplicaCount replicaCount = new ReplicaCount();
    for (Replica.Type replicaType : Replica.Type.values()) {
      Object value = propMap.get(replicaType.numReplicasPropertyName);
      if (null != value) {
        replicaCount.put(replicaType, Integer.parseInt(value.toString()));
      }
    }
    return replicaCount;
  }

  /**
   * Returns the number of replicas for a given type.
   *
   * @param type a replica type
   */
  public int get(Replica.Type type) {
    return countByType.getOrDefault(type, 0);
  }

  /**
   * Returns whether the number of replicas for a given type was explicitly defined.
   *
   * @param type a replica type
   */
  public boolean contains(Replica.Type type) {
    return countByType.containsKey(type);
  }

  /** Returns the replica types for which a number of replicas was explicitely defined. */
  public Set<Replica.Type> keySet() {
    return countByType.keySet();
  }

  /**
   * Add properties for replica counts as integers to a properties map.
   *
   * @param propMap a properties map.
   */
  public void writeProps(Map<String, Object> propMap) {
    for (Map.Entry<Replica.Type, Integer> entry : countByType.entrySet()) {
      propMap.put(entry.getKey().numReplicasPropertyName, entry.getValue());
    }
  }

  /**
   * Add properties for replica counts as integers to Solr parameters.
   *
   * @param params a set of modifiable Solr parameters.
   */
  public void writeProps(ModifiableSolrParams params) {
    for (Map.Entry<Replica.Type, Integer> entry : countByType.entrySet()) {
      params.set(entry.getKey().numReplicasPropertyName, entry.getValue());
    }
  }

  /**
   * Defines the number of replicas for a given type.
   *
   * @param type a replica type
   * @param count a number of replicas (if null, mark the value as missing)
   */
  public void put(Replica.Type type, Integer count) {
    if (null != count && count >= 0) {
      // If it considered as an error to put a negative value. In this case,
      // we choose to fail gracefully  by not changing anything.
      countByType.put(type, count);
    } else if (null == count) {
      countByType.remove(type);
    }
  }

  /**
   * Increment the number of replicas for a given type.
   *
   * @param type a replica type
   */
  public void increment(Replica.Type type) {
    put(type, get(type) + 1);
  }

  /**
   * Decrement the number of replicas for a given type.
   *
   * @param type a replica type
   */
  public void decrement(Replica.Type type) {
    put(type, get(type) - 1);
  }

  /** Returns the total number of replicas. */
  public int total() {
    return countByType.values().stream().reduce(Integer::sum).orElse(0);
  }

  /** Returns whether there is at least one replica which can be a leader. */
  public boolean hasLeaderReplica() {
    return countByType.entrySet().stream()
        .anyMatch(e -> e.getKey().leaderEligible && e.getValue() > 0);
  }

  /**
   * Returns the first non-zero replica type that can be a leader.
   *
   * @throws SolrException if no such replica exists
   */
  public Replica.Type getLeaderType() {
    for (Replica.Type type : Replica.Type.values()) {
      if (type.leaderEligible && get(type) >= 1) {
        return type;
      }
    }
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Unexpected number of replicas ("
            + this
            + "), there must be at least one leader-eligible replica");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicaCount that = (ReplicaCount) o;
    return Objects.equals(countByType, that.countByType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(countByType);
  }

  /** Returns a representation of this class which can be used for debugging. */
  @Override
  public String toString() {
    return Arrays.stream(Replica.Type.values())
        .map(t -> t.name().toLowerCase(Locale.ROOT) + "=" + get(t))
        .collect(Collectors.joining(", "));
  }
}
