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
 * Response for {@link org.apache.solr.client.api.endpoint.SegmentsApi#getSegmentData(Boolean,
 * Boolean, Boolean, Boolean, Boolean, Float, Boolean)} API
 */
public class GetSegmentDataResponse extends SolrJerseyResponse {
  @JsonProperty public SegmentSummary info;

  @JsonProperty public Map<String, Object> runningMerges;

  @JsonProperty public Map<String, SingleSegmentData> segments;

  // Present only with fieldInfo=true
  @JsonProperty public List<String> fieldInfoLegend;

  // Present with rawSize=true
  @JsonProperty public RawSize rawSize;

  // Always present in response
  public static class SegmentSummary {
    @JsonProperty public String minSegmentLuceneVersion;
    @JsonProperty public String commitLuceneVersion;
    @JsonProperty public Integer numSegments;
    @JsonProperty public String segmentsFileName;
    @JsonProperty public Integer totalMaxDoc;
    // Typically keys are 'commitCommandVer' and 'commitTimeMSec'
    @JsonProperty public Map<String, String> userData;

    // Present for coreInfo=true only
    @JsonProperty public CoreSummary core;
  }

  // Always present in response, provided that the specified core has segments
  public static class SingleSegmentData {
    @JsonProperty public String name;
    @JsonProperty public Integer delCount;
    @JsonProperty public Integer softDelCount;
    @JsonProperty public Boolean hasFieldUpdates;
    @JsonProperty public Long sizeInBytes;
    @JsonProperty public Integer size;

    // TODO - consider 'Instant' once SOLR-17608 is finished
    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "YYYY-MM-DD'T'hh:mm:ss.S'Z'")
    public Date age;

    @JsonProperty public String source;
    @JsonProperty public String version;
    @JsonProperty public Integer createdVersionMajor;
    @JsonProperty public String minVersion;
    @JsonProperty public SegmentDiagnosticInfo diagnostics;
    @JsonProperty public Map<String, String> attributes;
    // Only present when index-sorting is in use
    @JsonProperty public String sort;
    @JsonProperty public Boolean mergeCandidate;

    // Present only when fieldInfo=true
    @JsonProperty public Map<String, SegmentSingleFieldInfo> fields;

    // Present only when sizeInfo=true
    @JsonProperty("largestFiles")
    public Map<String, String> largestFilesByName;
  }

  // Always present in response, provided that the specified core has segments
  public static class SegmentSingleFieldInfo {
    @JsonProperty public String flags;
    @JsonProperty public Integer docCount;
    @JsonProperty public Long termCount;
    @JsonProperty public Long sumDocFreq;
    @JsonProperty public Long sumTotalTermFreq;
    @JsonProperty public String schemaType;
    @JsonProperty public Map<String, String> nonCompliant;
  }

  // Always present in response
  public static class SegmentDiagnosticInfo {
    @JsonProperty("os.version")
    public String osVersion;

    @JsonProperty("lucene.version")
    public String luceneVersion;

    @JsonProperty public String source;

    // TODO - consider 'Instant' once SOLR-17608 is finished
    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    public Date timestamp;

    @JsonProperty("java.runtime.version")
    public String javaRuntimeVersion;

    @JsonProperty public String os;

    @JsonProperty("java.vendor")
    public String javaVendor;

    @JsonProperty("os.arch")
    public String osArchitecture;

    private Map<String, Object> additionalDiagnostics = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getAdditionalDiagnostics() {
      return additionalDiagnostics;
    }

    @JsonAnySetter
    public void getAdditionalDiagnostics(String field, Object value) {
      additionalDiagnostics.put(field, value);
    }
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

  /** A serializable representation of Lucene's "LiveIndexWriterConfig" */
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
    @JsonProperty public String infoStream;
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

  // Present with rawSize=true unless otherwise specified
  public static class RawSize {
    @JsonProperty public Map<String, String> fieldsBySize;
    @JsonProperty public Map<String, String> typesBySize;

    // Present with rawSizeDetails=true
    @JsonProperty public Object details;

    // Present with rawSizeSummary=true
    @JsonProperty public Map<String, Object> summary;
  }
}
