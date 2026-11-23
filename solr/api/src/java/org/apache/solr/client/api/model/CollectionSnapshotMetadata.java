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
import java.util.List;
import org.apache.solr.client.api.util.ReflectWritable;

/** Metadata for a collection snapshot and its replica snapshots. */
public class CollectionSnapshotMetadata implements ReflectWritable {
  @JsonProperty public String name;
  @JsonProperty public String status;
  @JsonProperty public Long creationDate;
  @JsonProperty public List<ReplicaSnapshotMetadata> replicaSnapshots;
  @JsonProperty public List<String> shards;

  /** The index commit captured on a replica. */
  public static class ReplicaSnapshotMetadata implements ReflectWritable {
    @JsonProperty public String coreName;
    @JsonProperty public String indexDirPath;
    @JsonProperty public long generationNumber;
    @JsonProperty public boolean leader;
    @JsonProperty public String shardId;
    @JsonProperty public List<String> files;
  }
}
