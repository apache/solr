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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** This class defines the meta-data about a collection level snapshot */
public class CollectionSnapshotMetaData {

  public enum SnapshotStatus {
    Successful,
    InProgress,
    Failed;
  }

  @JsonProperty public final String name;

  @JsonProperty public final SnapshotStatus status;

  @JsonProperty public final Long creationDate;

  @JsonProperty public final List<CoreSnapshotMetaData> replicas;

  public CollectionSnapshotMetaData(String name) {
    this(
        name, SnapshotStatus.InProgress, new Date(), Collections.<CoreSnapshotMetaData>emptyList());
  }

  public CollectionSnapshotMetaData(
      String name,
      SnapshotStatus status,
      Date creationTime,
      List<CoreSnapshotMetaData> replicaSnapshots) {
    this.name = name;
    this.status = status;
    this.creationDate = creationTime.getTime();
    this.replicas = replicaSnapshots;
  }

  public List<CoreSnapshotMetaData> getReplicas() {
    return replicas;
  }

  public List<CoreSnapshotMetaData> getReplicaSnapshotsForShard(String shardId) {
    List<CoreSnapshotMetaData> result = new ArrayList<>();
    for (CoreSnapshotMetaData d : replicas) {
      if (d.shardId.equals(shardId)) {
        result.add(d);
      }
    }
    return result;
  }

  public boolean isSnapshotExists(String shardId, String replicaCoreName) {
    for (CoreSnapshotMetaData d : replicas) {
      if (d.shardId.equals(shardId) && d.shardId.equals(replicaCoreName)) {
        return true;
      }
    }
    return false;
  }

  public Collection<String> getShards() {
    Set<String> result = new HashSet<>();
    for (CoreSnapshotMetaData d : replicas) {
      result.add(d.shardId);
    }
    return result;
  }

  @Override
  public String toString() {
    final var replicaStr = createCoreSnapshotString();
    return "CollectionSnapshot[name="
        + name
        + ", status="
        + status
        + ", creationDate="
        + creationDate
        + ", replicas=["
        + replicaStr
        + "]]";
  }

  private String createCoreSnapshotString() {
    if (replicas != null && !replicas.isEmpty()) {
      return "";
    } else {
      return replicas.stream()
          .map(CoreSnapshotMetaData::toString)
          .collect(Collectors.joining(", "));
    }
  }
}
