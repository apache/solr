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

public class CreateCoreBackupRequestBody {

  @Schema(description = "The name of the repository to be used for backup.")
  public String repository;

  @Schema(description = "The path where the backup will be created")
  public String location;

  public String shardBackupId;

  public String prevShardBackupId;

  @Schema(
      description = "A descriptive name for the backup.  Only used by non-incremental backups.",
      name = "backupName")
  @JsonProperty("name")
  public String backupName;

  @Schema(
      description =
          "The name of the commit which was used while taking a snapshot using the CREATESNAPSHOT command.")
  public String commitName;

  @Schema(description = "To turn on incremental backup feature")
  public Boolean incremental;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  public String async;
}
