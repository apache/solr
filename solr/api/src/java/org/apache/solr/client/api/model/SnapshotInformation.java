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

import static org.apache.solr.client.api.model.Constants.SNAPSHOT_GENERATION_NUM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Contained in {@link ListCoreSnapshotsResponse}, this holds information for a given core's
 * Snapshot
 */
public class SnapshotInformation {
  @Schema(name = "generationNumber", description = "The generation value for the snapshot.")
  @JsonProperty(SNAPSHOT_GENERATION_NUM)
  public final long generationNumber;

  @Schema(description = "The path to the directory containing the index files.")
  @JsonProperty
  public final String indexDirPath;

  public SnapshotInformation(long generationNumber, String indexDirPath) {
    this.generationNumber = generationNumber;
    this.indexDirPath = indexDirPath;
  }
}
