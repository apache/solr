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
import io.swagger.v3.oas.annotations.media.Schema;

public class ReplicationBackupRequestBody {

  public ReplicationBackupRequestBody() {}

  public ReplicationBackupRequestBody(
      String location, String name, int numberToKeep, String repository, String commitName) {
    this.location = location;
    this.name = name;
    this.numberToKeep = numberToKeep;
    this.repository = repository;
    this.commitName = commitName;
  }

  @Schema(description = "The path where the backup will be created")
  @JsonProperty
  public String location;

  @Schema(description = "The backup will be created in a directory called snapshot.<name>")
  @JsonProperty
  public String name;

  @Schema(description = "The number of backups to keep.")
  @JsonProperty
  public int numberToKeep;

  @Schema(description = "The name of the repository to be used for e backup.")
  @JsonProperty
  public String repository;

  @Schema(
      description =
          "The name of the commit which was used while taking a snapshot using the CREATESNAPSHOT command.")
  @JsonProperty
  public String commitName;
}
