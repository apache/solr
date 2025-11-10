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

import static org.apache.solr.client.api.model.Constants.COLL_CONF;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class CollectionBackupDetails {
  @JsonProperty public Integer backupId;
  @JsonProperty public String indexVersion;
  @JsonProperty public String startTime;
  @JsonProperty public String endTime;
  @JsonProperty public Integer indexFileCount;
  @JsonProperty public Double indexSizeMB;

  @JsonProperty public Map<String, String> shardBackupIds;

  @JsonProperty(COLL_CONF)
  public String configsetName;

  @JsonProperty public String collectionAlias;
  @JsonProperty public Map<String, String> extraProperties;
}
