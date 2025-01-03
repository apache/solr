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
package org.apache.solr.handler.admin.api;

import static org.apache.lucene.index.IndexOptions.DOCS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.SegmentsApi;
import org.apache.solr.client.api.model.GetSegmentDataResponse;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.IndexSizeEstimator;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API implementation for {@link SegmentsApi}
 *
 * <p>Equivalent to the v1 /solr/coreName/admin/segments endpoint.
 */
public class GetSegmentData extends JerseyResource implements SegmentsApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final double GB = 1024.0 * 1024.0 * 1024.0;

  private static final List<String> FI_LEGEND =
      Arrays.asList(
          FieldFlag.INDEXED.toString(),
          FieldFlag.DOC_VALUES.toString(),
          "xxx - DocValues type",
          FieldFlag.TERM_VECTOR_STORED.toString(),
          FieldFlag.OMIT_NORMS.toString(),
          FieldFlag.OMIT_TF.toString(),
          FieldFlag.OMIT_POSITIONS.toString(),
          FieldFlag.STORE_OFFSETS_WITH_POSITIONS.toString(),
          "p - field has payloads",
          "s - field uses soft deletes",
          ":x:x:x - point data dim : index dim : num bytes");

  protected final SolrCore solrCore;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  @Inject
  public GetSegmentData(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.solrCore = solrCore;
    this.solrQueryRequest = req;
    this.solrQueryResponse = rsp;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.METRICS_READ_PERM)
  public GetSegmentDataResponse getSegmentData(
      Boolean coreInfo,
      Boolean fieldInfo,
      Boolean rawSize,
      Boolean rawSizeSummary,
      Boolean rawSizeDetails,
      Float rawSizeSamplingPercent,
      Boolean sizeInfo)
      throws Exception {
    boolean withFieldInfo = Boolean.TRUE.equals(fieldInfo);
    boolean withCoreInfo = Boolean.TRUE.equals(coreInfo);
    boolean withSizeInfo = Boolean.TRUE.equals(sizeInfo);
    boolean withRawSizeInfo = Boolean.TRUE.equals(rawSize);
    boolean withRawSizeSummary = Boolean.TRUE.equals(rawSizeSummary);
    boolean withRawSizeDetails = Boolean.TRUE.equals(rawSizeDetails);
    if (withRawSizeSummary || withRawSizeDetails) {
      withRawSizeInfo = true;
    }
    SolrIndexSearcher searcher = solrQueryRequest.getSearcher();
    SolrCore core = solrQueryRequest.getCore();

    final var response = new GetSegmentDataResponse();

    SegmentInfos infos = SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());
    response.info = new GetSegmentDataResponse.SegmentSummary();
    Version minVersion = infos.getMinSegmentLuceneVersion();
    if (minVersion != null) {
      response.info.minSegmentLuceneVersion = minVersion.toString();
    }
    Version commitVersion = infos.getCommitLuceneVersion();
    if (commitVersion != null) {
      response.info.commitLuceneVersion = commitVersion.toString();
    }
    response.info.numSegments = infos.size();
    response.info.segmentsFileName = infos.getSegmentsFileName();
    response.info.totalMaxDoc = infos.totalMaxDoc();
    response.info.userData = infos.userData;

    if (withCoreInfo) {
      final var coreSummary = new GetSegmentDataResponse.CoreSummary();
      response.info.core = coreSummary;
      coreSummary.startTime =
          core.getStartTimeStamp().getTime() + "(" + core.getStartTimeStamp() + ")";
      coreSummary.dataDir = core.getDataDir();
      coreSummary.indexDir = core.getIndexDir();
      coreSummary.sizeInGB = (double) core.getIndexSize() / GB;

      RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(core);
      if (iwRef != null) {
        try {
          IndexWriter iw = iwRef.get();
          coreSummary.indexWriterConfig = convertIndexWriterConfigToResponse(iw.getConfig());
        } finally {
          iwRef.decref();
        }
      }
    }

    List<SegmentCommitInfo> sortable = new ArrayList<>(infos.asList());
    // Order by the number of live docs. The display is logarithmic so it is a little jumbled
    // visually
    sortable.sort(
        (s1, s2) -> (s2.info.maxDoc() - s2.getDelCount()) - (s1.info.maxDoc() - s1.getDelCount()));

    List<String> mergeCandidates = new ArrayList<>();
    final var runningMerges = getMergeInformation(solrQueryRequest, infos, mergeCandidates);
    List<LeafReaderContext> leafContexts = searcher.getIndexReader().leaves();
    IndexSchema schema = solrQueryRequest.getSchema();
    response.segments = new HashMap<>();
    for (SegmentCommitInfo segmentCommitInfo : sortable) {
      final var singleSegmentData =
          getSegmentInfo(segmentCommitInfo, withSizeInfo, withFieldInfo, leafContexts, schema);
      if (mergeCandidates.contains(segmentCommitInfo.info.name)) {
        singleSegmentData.mergeCandidate = true;
      }
      response.segments.put(singleSegmentData.name, singleSegmentData);
    }

    if (runningMerges.size() > 0) {
      response.runningMerges = runningMerges;
    }
    if (withFieldInfo) {
      response.fieldInfoLegend = FI_LEGEND;
    }

    if (withRawSizeInfo) {
      IndexSizeEstimator estimator =
          new IndexSizeEstimator(
              searcher.getRawReader(), 20, 100, withRawSizeSummary, withRawSizeDetails);
      if (rawSizeSamplingPercent != null) {
        estimator.setSamplingPercent(rawSizeSamplingPercent);
      }
      IndexSizeEstimator.Estimate estimate = estimator.estimate();
      final var rawSizeResponse = new GetSegmentDataResponse.RawSize();
      // make the units more user-friendly
      rawSizeResponse.fieldsBySize = estimate.getHumanReadableFieldsBySize();
      rawSizeResponse.typesBySize = estimate.getHumanReadableTypesBySize();
      if (estimate.getSummary() != null) {
        rawSizeResponse.summary = estimate.getSummary();
      }
      if (estimate.getDetails() != null) {
        rawSizeResponse.details = estimate.getDetails();
      }
      response.rawSize = rawSizeResponse;
    }

    return response;
  }

  /**
   * Converts Lucene's IndexWriter configuration object into a response type fit for serialization
   *
   * <p>Based on {@link LiveIndexWriterConfig#toString()} for legacy reasons.
   *
   * @param iwConfig the Lucene configuration object to convert
   */
  private GetSegmentDataResponse.IndexWriterConfigSummary convertIndexWriterConfigToResponse(
      LiveIndexWriterConfig iwConfig) {
    final var iwConfigResponse = new GetSegmentDataResponse.IndexWriterConfigSummary();
    iwConfigResponse.analyzer =
        iwConfig.getAnalyzer() != null ? iwConfig.getAnalyzer().getClass().getName() : "null";
    iwConfigResponse.ramBufferSizeMB = iwConfig.getRAMBufferSizeMB();
    iwConfigResponse.maxBufferedDocs = iwConfig.getMaxBufferedDocs();
    iwConfigResponse.mergedSegmentWarmer = String.valueOf(iwConfig.getMergedSegmentWarmer());
    iwConfigResponse.delPolicy = iwConfig.getIndexDeletionPolicy().getClass().getName();
    iwConfigResponse.commit = String.valueOf(iwConfig.getIndexCommit());
    iwConfigResponse.openMode = String.valueOf(iwConfig.getOpenMode());
    iwConfigResponse.similarity = iwConfig.getSimilarity().getClass().getName();
    iwConfigResponse.mergeScheduler = String.valueOf(iwConfig.getMergeScheduler());
    iwConfigResponse.codec = String.valueOf(iwConfig.getCodec());
    iwConfigResponse.infoStream = iwConfig.getInfoStream().getClass().getName();
    iwConfigResponse.mergePolicy = String.valueOf(iwConfig.getMergePolicy());
    iwConfigResponse.readerPooling = iwConfig.getReaderPooling();
    iwConfigResponse.perThreadHardLimitMB = iwConfig.getRAMPerThreadHardLimitMB();
    iwConfigResponse.useCompoundFile = iwConfig.getUseCompoundFile();
    iwConfigResponse.commitOnClose = iwConfig.getCommitOnClose();
    iwConfigResponse.indexSort = String.valueOf(iwConfig.getIndexSort());
    iwConfigResponse.checkPendingFlushOnUpdate = iwConfig.isCheckPendingFlushOnUpdate();
    iwConfigResponse.softDeletesField = iwConfig.getSoftDeletesField();
    iwConfigResponse.maxFullFlushMergeWaitMillis = iwConfig.getMaxFullFlushMergeWaitMillis();
    iwConfigResponse.leafSorter = String.valueOf(iwConfig.getLeafSorter());
    iwConfigResponse.eventListener = String.valueOf(iwConfig.getIndexWriterEventListener());
    iwConfigResponse.parentField = iwConfig.getParentField();
    return iwConfigResponse;
  }

  // returns a map of currently running merges, and populates a list of candidate segments for merge
  private Map<String, Object> getMergeInformation(
      SolrQueryRequest req, SegmentInfos infos, List<String> mergeCandidates) throws IOException {
    final var result = new HashMap<String, Object>();
    RefCounted<IndexWriter> refCounted =
        req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    try {
      IndexWriter indexWriter = refCounted.get();
      if (indexWriter instanceof SolrIndexWriter) {
        result.putAll(((SolrIndexWriter) indexWriter).getRunningMerges());
      }
      // get chosen merge policy
      MergePolicy mp = indexWriter.getConfig().getMergePolicy();
      // Find merges
      MergePolicy.MergeSpecification findMerges =
          mp.findMerges(MergeTrigger.EXPLICIT, infos, indexWriter);
      if (findMerges != null && findMerges.merges != null && findMerges.merges.size() > 0) {
        for (MergePolicy.OneMerge merge : findMerges.merges) {
          // TODO: add merge grouping
          for (SegmentCommitInfo mergeSegmentInfo : merge.segments) {
            mergeCandidates.add(mergeSegmentInfo.info.name);
          }
        }
      }

      return result;
    } finally {
      refCounted.decref();
    }
  }

  private GetSegmentDataResponse.SingleSegmentData getSegmentInfo(
      SegmentCommitInfo segmentCommitInfo,
      boolean withSizeInfo,
      boolean withFieldInfos,
      List<LeafReaderContext> leafContexts,
      IndexSchema schema)
      throws IOException {
    final var segmentInfo = new GetSegmentDataResponse.SingleSegmentData();
    segmentInfo.name = segmentCommitInfo.info.name;
    segmentInfo.delCount = segmentCommitInfo.getDelCount();
    segmentInfo.softDelCount = segmentCommitInfo.getSoftDelCount();
    segmentInfo.hasFieldUpdates = segmentCommitInfo.hasFieldUpdates();
    segmentInfo.sizeInBytes = segmentCommitInfo.sizeInBytes();
    segmentInfo.size = segmentCommitInfo.info.maxDoc();
    Long timestamp = Long.parseLong(segmentCommitInfo.info.getDiagnostics().get("timestamp"));
    segmentInfo.age = new Date(timestamp);
    segmentInfo.source = segmentCommitInfo.info.getDiagnostics().get("source");
    segmentInfo.version = segmentCommitInfo.info.getVersion().toString();

    // don't open a new SegmentReader - try to find the right one from the leaf contexts
    SegmentReader seg = null;
    for (LeafReaderContext lrc : leafContexts) {
      LeafReader leafReader = lrc.reader();
      leafReader = FilterLeafReader.unwrap(leafReader);
      if (leafReader instanceof SegmentReader sr) {
        if (sr.getSegmentInfo().info.equals(segmentCommitInfo.info)) {
          seg = sr;
          break;
        }
      }
    }
    if (seg != null) {
      LeafMetaData metaData = seg.getMetaData();
      if (metaData != null) {
        segmentInfo.createdVersionMajor = metaData.getCreatedVersionMajor();
        segmentInfo.minVersion = metaData.getMinVersion().toString();
        if (metaData.getSort() != null) {
          segmentInfo.sort = metaData.getSort().toString();
        }
      }
    }

    if (!segmentCommitInfo.info.getDiagnostics().isEmpty()) {
      segmentInfo.diagnostics =
          SolrJacksonMapper.getObjectMapper()
              .convertValue(
                  segmentCommitInfo.info.getDiagnostics(),
                  GetSegmentDataResponse.SegmentDiagnosticInfo.class);
    }
    if (!segmentCommitInfo.info.getAttributes().isEmpty()) {
      segmentInfo.attributes = segmentCommitInfo.info.getAttributes();
    }
    if (withSizeInfo) {
      Directory dir = segmentCommitInfo.info.dir;
      List<Pair<String, Long>> files =
          segmentCommitInfo.files().stream()
              .map(
                  f -> {
                    long size = -1;
                    try {
                      size = dir.fileLength(f);
                    } catch (IOException e) {
                    }
                    return new Pair<String, Long>(f, size);
                  })
              .sorted(
                  (p1, p2) -> {
                    if (p1.second() > p2.second()) {
                      return -1;
                    } else if (p1.second() < p2.second()) {
                      return 1;
                    } else {
                      return 0;
                    }
                  })
              .collect(Collectors.toList());
      if (!files.isEmpty()) {
        final var topFiles = new HashMap<String, String>();
        for (int i = 0; i < Math.min(files.size(), 5); i++) {
          Pair<String, Long> p = files.get(i);
          topFiles.put(p.first(), RamUsageEstimator.humanReadableUnits(p.second()));
        }
        segmentInfo.largestFilesByName = topFiles;
      }
    }

    if (withFieldInfos) {
      if (seg == null) {
        log.debug(
            "Skipping segment info - not available as a SegmentReader: {}", segmentCommitInfo);
      } else {
        FieldInfos fis = seg.getFieldInfos();
        final var fields = new HashMap<String, GetSegmentDataResponse.SegmentSingleFieldInfo>();
        for (FieldInfo fi : fis) {
          fields.put(fi.name, getFieldInfo(seg, fi, schema));
        }
        segmentInfo.fields = fields;
      }
    }

    return segmentInfo;
  }

  private GetSegmentDataResponse.SegmentSingleFieldInfo getFieldInfo(
      SegmentReader reader, FieldInfo fi, IndexSchema schema) {
    final var responseFieldInfo = new GetSegmentDataResponse.SegmentSingleFieldInfo();
    StringBuilder flags = new StringBuilder();
    IndexOptions opts = fi.getIndexOptions();
    flags.append((opts != IndexOptions.NONE) ? FieldFlag.INDEXED.getAbbreviation() : '-');
    DocValuesType dvt = fi.getDocValuesType();
    if (dvt != DocValuesType.NONE) {
      flags.append(FieldFlag.DOC_VALUES.getAbbreviation());
      switch (dvt) {
        case NUMERIC:
          flags.append("num");
          break;
        case BINARY:
          flags.append("bin");
          break;
        case SORTED:
          flags.append("srt");
          break;
        case SORTED_NUMERIC:
          flags.append("srn");
          break;
        case SORTED_SET:
          flags.append("srs");
          break;
        default:
          flags.append("???"); // should not happen
      }
    } else {
      flags.append("----");
    }
    flags.append((fi.hasVectors()) ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-');
    flags.append((fi.omitsNorms()) ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-');

    flags.append((DOCS == opts) ? FieldFlag.OMIT_TF.getAbbreviation() : '-');

    flags.append((DOCS_AND_FREQS == opts) ? FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');

    flags.append(
        (DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS == opts)
            ? FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation()
            : '-');

    flags.append((fi.hasPayloads() ? "p" : "-"));
    flags.append((fi.isSoftDeletesField() ? "s" : "-"));
    if (fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0) {
      flags.append(":");
      flags.append(fi.getPointDimensionCount()).append(':');
      flags.append(fi.getPointIndexDimensionCount()).append(':');
      flags.append(fi.getPointNumBytes());
    }

    responseFieldInfo.flags = flags.toString();

    try {
      Terms terms = reader.terms(fi.name);
      if (terms != null) {
        responseFieldInfo.docCount = terms.getDocCount();
        responseFieldInfo.termCount = terms.size();
        responseFieldInfo.sumDocFreq = terms.getSumDocFreq();
        responseFieldInfo.sumTotalTermFreq = terms.getSumTotalTermFreq();
      }
    } catch (Exception e) {
      log.debug("Exception retrieving term stats for field {}", fi.name, e);
    }

    // check compliance of the index with the current schema
    SchemaField sf = schema.getFieldOrNull(fi.name);
    boolean hasPoints = fi.getPointDimensionCount() > 0 || fi.getPointIndexDimensionCount() > 0;

    if (sf != null) {
      responseFieldInfo.schemaType = sf.getType().getTypeName();
      final var nonCompliant = new HashMap<String, String>();
      if (sf.hasDocValues()
          && fi.getDocValuesType() == DocValuesType.NONE
          && fi.getIndexOptions() != IndexOptions.NONE) {
        nonCompliant.put(
            "docValues", "schema=" + sf.getType().getUninversionType(sf) + ", segment=false");
      }
      if (!sf.hasDocValues() && fi.getDocValuesType() != DocValuesType.NONE) {
        nonCompliant.put("docValues", "schema=false, segment=" + fi.getDocValuesType().toString());
      }
      if (!sf.isPolyField()) { // difficult to find all sub-fields in a general way
        if (sf.indexed() != ((fi.getIndexOptions() != IndexOptions.NONE) || hasPoints)) {
          nonCompliant.put(
              "indexed", "schema=" + sf.indexed() + ", segment=" + fi.getIndexOptions());
        }
      }
      if (!hasPoints && (sf.omitNorms() != fi.omitsNorms())) {
        nonCompliant.put("omitNorms", "schema=" + sf.omitNorms() + ", segment=" + fi.omitsNorms());
      }
      if (sf.storeTermVector() != fi.hasVectors()) {
        nonCompliant.put(
            "termVectors", "schema=" + sf.storeTermVector() + ", segment=" + fi.hasVectors());
      }
      if (sf.storeOffsetsWithPositions()
          != (fi.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)) {
        nonCompliant.put(
            "storeOffsetsWithPositions",
            "schema=" + sf.storeOffsetsWithPositions() + ", segment=" + fi.getIndexOptions());
      }

      if (nonCompliant.size() > 0) {
        nonCompliant.put("schemaField", sf.toString());
        responseFieldInfo.nonCompliant = nonCompliant;
      }
    } else {
      responseFieldInfo.schemaType = "(UNKNOWN)";
    }

    return responseFieldInfo;
  }
}
