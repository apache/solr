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
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CollectionAdminParams;

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

  public static ReplicaCount fromMessage(
      ZkNodeProps message, DocCollection collection, Integer defaultReplicationFactor) {
    Replica.Type defaultReplicaType =
        Replica.Type.valueOf(
            message
                .getStr(CollectionAdminParams.REPLICA_TYPE, Replica.Type.defaultType().name())
                .toUpperCase(Locale.ROOT));
    ReplicaCount replicaCount = new ReplicaCount();
    for (Replica.Type replicaType : Replica.Type.values()) {
      final Integer count;
      if (replicaType == defaultReplicaType) {
        Integer defaultCount =
            (null != collection)
                ? collection.getInt(
                    replicaType.numReplicasProperty,
                    collection.getInt(
                        CollectionAdminParams.REPLICATION_FACTOR, defaultReplicationFactor))
                : defaultReplicationFactor;
        count =
            message.getInt(
                replicaType.numReplicasProperty,
                message.getInt(CollectionAdminParams.REPLICATION_FACTOR, defaultCount));
      } else {
        Integer defaultCount =
            (null != collection) ? collection.getInt(replicaType.numReplicasProperty, null) : null;
        count = message.getInt(replicaType.numReplicasProperty, defaultCount);
      }
      replicaCount.put(replicaType, count);
    }
    return replicaCount;
  }

  public static ReplicaCount fromProps(Map<String, Object> props) {
    ReplicaCount replicaCount = new ReplicaCount();
    for (Replica.Type replicaType : Replica.Type.values()) {
      Object value = props.get(replicaType.numReplicasProperty);
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

  public int count(EnumSet<Replica.Type> replicaTypes) {
    return replicaTypes.stream().mapToInt(this::get).sum();
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
