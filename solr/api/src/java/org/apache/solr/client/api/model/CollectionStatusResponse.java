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
import java.util.Map;

/** Response of the CollectionStatusApi.getCollectionStatus() API */
public class CollectionStatusResponse extends SolrJerseyResponse {

  @JsonProperty public String name;
  @JsonProperty public Integer znodeVersion;
  @JsonProperty public Long creationTimeMillis;
  @JsonProperty public CollectionMetadata properties;
  @JsonProperty public Integer activeShards;
  @JsonProperty public Integer inactiveShards;
  @JsonProperty public List<String> schemaNonCompliant;

  @JsonProperty public Map<String, ShardMetadata> shards;

  public static class CollectionMetadata {
    @JsonProperty public String configName;
    @JsonProperty public Integer nrtReplicas;
    @JsonProperty public Integer pullReplicas;
    @JsonProperty public Integer tlogReplicas;
    @JsonProperty public Map<String, String> router;
    @JsonProperty public Integer replicationFactor;
  }

  public static class ShardMetadata {
    @JsonProperty public String state; // TODO Make this an enum?
    @JsonProperty public String range;
    @JsonProperty public ReplicaSummary replicas;
    @JsonProperty public LeaderSummary leader;
  }

  public static class ReplicaSummary {
    @JsonProperty public Integer total;
    @JsonProperty public Integer active;
    @JsonProperty public Integer down;
    @JsonProperty public Integer recovering;

    @JsonProperty("recovery_failed")
    public Integer recoveryFailed;
  }

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
  }

  // Present with coreInfo=true || sizeInfo=true unless otherwise specified
  public static class SegmentInfo {
    // Present with coreInfo=true || sizeInfo=true unless otherwise specified
    @JsonProperty public SegmentSummary info;

    // Present with rawSize=true
    @JsonProperty public RawSize rawSize;

    // Present with fieldInfo=true....this seems pretty useless in isolation?  Is it maybe a bad
    // param name?
    @JsonProperty public List<String> fieldInfoLegend;
  }

  // Present with rawSize=true unless otherwise specified
  public static class RawSize {
    @JsonProperty public Map<String, String> fieldsBySize;
    @JsonProperty public Map<String, String> typesBySize;

    // Present with rawSizeDetails=true
    @JsonProperty public Object details;

    // Present with rawSizeSummary=true
    @JsonProperty public Map<String, Object> summary;
  }

  // Present with rawSizeSummary=true
  public static class SingleFieldSizeSummary {
    @JsonProperty Integer totalSize;
    @JsonProperty Map<String, Integer> perType;
  }

  // Present with coreInfo=true || sizeInfo=true unless otherwise specified
  public static class SegmentSummary {
    @JsonProperty public String minSegmentLuceneVersion;
    @JsonProperty public String commitLuceneVersion;
    @JsonProperty public Integer numSegments;
    @JsonProperty public String segmentsFileName;
    @JsonProperty public Integer totalMaxDoc;

    @JsonProperty
    public Map<String, String>
        userData; // Typically keys are 'commitCommandVer' and 'commitTimeMSec'

    // Present for coreInfo=true only
    @JsonProperty public CoreSummary core;
  }

  // Present with coreInfo=true unless otherwise specified
  public static class CoreSummary {
    @JsonProperty public String startTime;
    @JsonProperty public String dataDir;
    @JsonProperty public String indexDir;
    @JsonProperty public Double sizeInGB;
    @JsonProperty public IndexWriterConfigSummary indexWriterConfig;
  }

  // Present with coreInfo=true unless otherwise specified
  public static class IndexWriterConfigSummary {
    @JsonProperty public String analyzer;
    @JsonProperty public Double ramBufferSizeMB;
    @JsonProperty public Integer maxBufferedDocs;
    @JsonProperty public String mergedSegmentWarmer;
    @JsonProperty public String delPolicy;
    @JsonProperty public String commit;
    @JsonProperty public String openMode;
    @JsonProperty public String similarity;
    @JsonProperty public String mergeScheduler;
    @JsonProperty public String codec;
    @JsonProperty public String InfoStream;
    @JsonProperty public String mergePolicy;
    @JsonProperty public Boolean readerPooling;
    @JsonProperty public Integer perThreadHardLimitMB;
    @JsonProperty public Boolean useCompoundFile;
    @JsonProperty public Boolean commitOnClose;
    @JsonProperty public String indexSort;
    @JsonProperty public Boolean checkPendingFlushOnUpdate;
    @JsonProperty public String softDeletesField;
    @JsonProperty public Long maxFullFlushMergeWaitMillis;
    @JsonProperty public String leafSorter;
    @JsonProperty public String eventListener;
    @JsonProperty public String parentField;
    @JsonProperty public String writer;
  }
}
