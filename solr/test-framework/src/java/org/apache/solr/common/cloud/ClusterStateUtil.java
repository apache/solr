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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Utils;

public class ClusterStateUtil {

  private static final int TIMEOUT_POLL_MS = 1000;

  /**
   * Wait to see *all* cores live and active.
   *
   * @param zkStateReader to use for ClusterState
   * @param timeoutInMs how long to wait before giving up
   * @return false if timed out
   */
  public static boolean waitForAllActiveAndLiveReplicas(
      ZkStateReader zkStateReader, int timeoutInMs) {
    return waitForAllActiveAndLiveReplicas(zkStateReader, null, timeoutInMs);
  }

  /**
   * Wait to see *all* cores live and active.
   *
   * @param zkStateReader to use for ClusterState
   * @param collection to look at
   * @param timeoutInMs how long to wait before giving up
   * @return false if timed out
   */
  public static boolean waitForAllActiveAndLiveReplicas(
      ZkStateReader zkStateReader, String collection, int timeoutInMs) {
    return waitFor(
        zkStateReader,
        collection,
        timeoutInMs,
        TimeUnit.MILLISECONDS,
        (liveNodes, state) ->
            replicasOfActiveSlicesStream(state)
                .allMatch(replica -> liveAndActivePredicate(replica, liveNodes)));
  }

  private static boolean liveAndActivePredicate(Replica replica, Set<String> liveNodes) {
    // on a live node?
    final boolean live = liveNodes.contains(replica.getNodeName());
    final boolean isActive = replica.getState() == Replica.State.ACTIVE;
    return live && isActive;
  }

  public static boolean waitForAllReplicasNotLive(ZkStateReader zkStateReader, int timeoutInMs) {
    return waitForAllReplicasNotLive(zkStateReader, null, timeoutInMs);
  }

  public static boolean waitForAllReplicasNotLive(
      ZkStateReader zkStateReader, String collection, int timeoutInMs) {
    return waitFor(
        zkStateReader,
        collection,
        timeoutInMs,
        TimeUnit.MILLISECONDS,
        (liveNodes, state) ->
            replicasOfActiveSlicesStream(state)
                .noneMatch(replica -> liveNodes.contains(replica.getNodeName())));
  }

  public static int getLiveAndActiveReplicaCount(ZkStateReader zkStateReader, String collection) {
    ClusterState clusterState = zkStateReader.getClusterState();
    var liveNodes = clusterState.getLiveNodes();
    var state = clusterState.getCollection(collection);
    return (int)
        replicasOfActiveSlicesStream(state)
            .filter(replica -> liveAndActivePredicate(replica, liveNodes))
            .count();
  }

  public static Stream<Replica> replicasOfActiveSlicesStream(DocCollection collectionState) {
    return collectionState.getActiveSlices().stream()
        .map(Slice::getReplicas)
        .flatMap(Collection::stream);
  }

  public static boolean waitForLiveAndActiveReplicaCount(
      ZkStateReader zkStateReader, String collection, int replicaCount, int timeoutInMs) {
    return waitFor(
        zkStateReader,
        collection,
        timeoutInMs,
        TimeUnit.MILLISECONDS,
        (liveNodes, state) ->
            replicasOfActiveSlicesStream(state)
                    .filter(replica -> liveAndActivePredicate(replica, liveNodes))
                    .count()
                == replicaCount);
  }

  /**
   * Calls {@link ZkStateReader#waitForState(String, long, TimeUnit, CollectionStatePredicate)} but
   * has an alternative implementation if {@code collection} is null, in which the predicate must
   * match *all* collections. Returns whether the predicate matches or not in the allotted time;
   * does *NOT* throw {@link TimeoutException}.
   */
  public static boolean waitFor(
      ZkStateReader zkStateReader,
      String collection,
      long timeout,
      TimeUnit timeUnit,
      CollectionStatePredicate predicate) {
    // ideally a collection is specified...
    if (collection != null) {
      try {
        zkStateReader.waitForState(collection, timeout, timeUnit, predicate);
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
      } catch (TimeoutException e) {
        return false;
      }
    }

    // otherwise we check all collections...

    final long timeoutAtNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
    while (true) {
      ClusterState clusterState = zkStateReader.getClusterState(); // fresh state
      if (clusterState != null) { // it's sad to deal with this; API contract should forbid
        var liveNodes = clusterState.getLiveNodes();
        if (clusterState
            .collectionStream()
            .allMatch(state -> predicate.matches(liveNodes, state))) {
          return true;
        }
      }

      if (System.nanoTime() > timeoutAtNs) {
        return false;
      }

      try {
        Thread.sleep(TIMEOUT_POLL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
      }
    }
  }

  /** Produces a String of all the collection states for debugging. ZK may be consulted. */
  public static String toDebugAllStatesString(ClusterState clusterState) {
    // note: ClusterState.toString prints the in-memory state info it has without consulting ZK

    // Collect to a Map by name, loading each DocCollection expressed as a Map
    var stateMap =
        clusterState
            .collectionStream()
            .collect(
                LinkedHashMap::new,
                (map, state) -> map.put(state.getName(), state.toMap(new LinkedHashMap<>())),
                Map::putAll);
    // toJSON requires standard types like Map; doesn't know about DocCollection etc.
    return Utils.toJSONString(stateMap);
  }
}
