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
import java.util.Collection;

public class CoreSnapshotMetaData {

  @JsonProperty public final String core;

  @JsonProperty public final String indexDirPath;

  @JsonProperty public final long generation;

  @JsonProperty public final Boolean leader;

  @JsonProperty("shard_id")
  public final String shardId;

  @JsonProperty public final Collection<String> files;

  // TODO Do I need a noarg constructor for Jackson?

  public CoreSnapshotMetaData(
      String coreName,
      String indexDirPath,
      long generationNumber,
      String shardId,
      boolean leader,
      Collection<String> files) {
    this.core = coreName;
    this.indexDirPath = indexDirPath;
    this.generation = generationNumber;
    this.shardId = shardId;
    this.leader = leader;
    this.files = files;
  }

  @Override
  public String toString() {
    return "CoreSnapshot[name="
        + core
        + ", leader="
        + leader
        + ", generation="
        + generation
        + ", indexDirPath="
        + indexDirPath
        + "]";
  }
}
