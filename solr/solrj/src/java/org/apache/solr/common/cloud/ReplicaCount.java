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
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;

/** Tracks the number of replicas per replica type. This class is mutable. */
public final class ReplicaCount {
  private final Map<Replica.Type, Integer> countByType;

  public ReplicaCount(Integer nrtReplicas, Integer tlogReplicas, Integer pullReplicas) {
    this.countByType = new EnumMap<>(Replica.Type.class);
    put(Replica.Type.NRT, nrtReplicas);
    put(Replica.Type.TLOG, tlogReplicas);
    put(Replica.Type.PULL, pullReplicas);
  }

  public static ReplicaCount empty() {
    return new ReplicaCount(null, null, null);
  }

  public static ReplicaCount of(Replica.Type type, Integer count) {
    ReplicaCount replicaCount = empty();
    replicaCount.put(type, count);
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
            + toDebugString()
            + "), there must be at least one leader-eligible replica");
  }

  /** Validate that there is at least one replica that can be a leader. */
  public void validate() {
    getLeaderType();
  }

  /** Returns a representation of this class which can be used for debugging. */
  public String toDebugString() {
    return Arrays.stream(Replica.Type.values())
        .map(t -> t.name().toLowerCase(Locale.ROOT) + "=" + get(t))
        .collect(Collectors.joining(", "));
  }
}
