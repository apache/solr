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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response of the CollectionStatusApi.getCollectionStatus() API
 *
 * <p>Note that the corresponding v1 API has a slightly different response format. Users should not
 * attempt to convert a v1 response into this type.
 */
public class CollectionStatusResponse extends SolrJerseyResponse {

  @JsonProperty public String name;
  @JsonProperty public Integer znodeVersion;

  // TODO - consider 'Instant' once SOLR-17608 is finished
  @JsonProperty
  @JsonFormat(shape = JsonFormat.Shape.NUMBER)
  public Date creationTimeMillis;

  @JsonProperty public CollectionMetadata properties;
  @JsonProperty public Integer activeShards;
  @JsonProperty public Integer inactiveShards;
  @JsonProperty public List<String> schemaNonCompliant;

  @JsonProperty public Map<String, ShardMetadata> shards;

  // Always present in response
  public static class CollectionMetadata {
    @JsonProperty public String configName;
    @JsonProperty public Integer nrtReplicas;
    @JsonProperty public Integer pullReplicas;
    @JsonProperty public Integer tlogReplicas;
    @JsonProperty public Map<String, String> router;
    @JsonProperty public Integer replicationFactor;

    private Map<String, Object> unknownFields = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return unknownFields;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      unknownFields.put(field, value);
    }
  }

  // Always present in response
  public static class ShardMetadata {
    @JsonProperty public String state; // TODO Make this an enum?
    @JsonProperty public String range;
    @JsonProperty public ReplicaSummary replicas;
    @JsonProperty public LeaderSummary leader;
  }

  // Always present in response
  public static class ReplicaSummary {
    @JsonProperty public Integer total;
    @JsonProperty public Integer active;
    @JsonProperty public Integer down;
    @JsonProperty public Integer recovering;

    @JsonProperty("recovery_failed")
    public Integer recoveryFailed;
  }

  // Always present in response unless otherwise specified
  public static class LeaderSummary {
    @JsonProperty public String coreNode;
    @JsonProperty public String core;
    @JsonProperty public Boolean leader;

    @JsonProperty("node_name")
    public String nodeName;

    @JsonProperty("base_url")
    public String baseUrl;

    @JsonProperty public String state; // TODO Make this an enum?
    @JsonProperty public String type; // TODO Make this an enum?

    @JsonProperty("force_set_state")
    public Boolean forceSetState;

    // Present with coreInfo=true || sizeInfo=true unless otherwise specified
    @JsonProperty public SegmentInfo segInfos;

    private Map<String, Object> unknownFields = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return unknownFields;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      unknownFields.put(field, value);
    }
  }

  // Present with segments=true || coreInfo=true || sizeInfo=true || fieldInfo=true unless otherwise
  // specified

  /**
   * Same properties as {@link GetSegmentDataResponse}, but uses a different class to avoid
   * inheriting "responseHeader", etc.
   */
  public static class SegmentInfo {
    @JsonProperty public GetSegmentDataResponse.SegmentSummary info;

    @JsonProperty public Map<String, Object> runningMerges;

    // Present with segments=true || sizeInfo=true || fieldInfo=true
    @JsonProperty public Map<String, GetSegmentDataResponse.SingleSegmentData> segments;

    // Present with rawSize=true
    @JsonProperty public GetSegmentDataResponse.RawSize rawSize;

    // Present only with fieldInfo=true
    @JsonProperty public List<String> fieldInfoLegend;
  }
}
