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
import java.util.Date;
import java.util.Map;

public class CoreStatusResponse extends SolrJerseyResponse {

  // Map values are often exceptions on the server side, but often serialized as strings.
  @JsonProperty public Map<String, Object> initFailures;

  @JsonProperty public Map<String, SingleCoreData> status;

  public static class SingleCoreData {
    @JsonProperty public String name;

    @JsonProperty public Boolean isLoaded;
    @JsonProperty public Boolean isLoading;

    @JsonProperty public String instanceDir;
    @JsonProperty public String dataDir;
    @JsonProperty public String config;
    @JsonProperty public String schema;
    @JsonProperty public Date startTime;
    @JsonProperty public Long uptime;
    @JsonProperty public String lastPublished;
    @JsonProperty public Integer configVersion;
    @JsonProperty public CloudDetails cloud;
    @JsonProperty public IndexDetails index;
  }

  public static class CloudDetails {
    @JsonProperty public String collection;
    @JsonProperty public String shard;
    @JsonProperty public String replica;
    @JsonProperty public String replicaType; // TODO enum?
  }

  public static class IndexDetails {
    @JsonProperty public Integer numDocs;
    @JsonProperty public Integer maxDoc;
    @JsonProperty public Integer deletedDocs;
    @JsonProperty public Long version;
    @JsonProperty public Integer segmentCount;
    @JsonProperty public Boolean current;
    @JsonProperty public Boolean hasDeletions;
    @JsonProperty public String directory;
    @JsonProperty public String segmentsFile;
    @JsonProperty public Long segmentsFileSizeInBytes;
    @JsonProperty public Map<String, String> userData;
    @JsonProperty public Date lastModified;
    @JsonProperty public Long sizeInBytes;
    @JsonProperty public String size; // Human readable representation of 'sizeInBytes'
  }
}
