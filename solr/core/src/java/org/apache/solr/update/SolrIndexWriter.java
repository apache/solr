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
package org.apache.solr.update;

import static org.apache.solr.metrics.SolrMetricProducer.CATEGORY_ATTR;
import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.metrics.otel.instruments.AttributedLongCounter;
import org.apache.solr.metrics.otel.instruments.AttributedLongTimer;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IndexWriter that is configured via Solr config mechanisms.
 *
 * @since solr 0.9
 */
public class SolrIndexWriter extends IndexWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();

  /**
   * Stored into each Lucene commit to record the System.currentTimeMillis() when commit was called.
   */
  public static final String COMMIT_TIME_MSEC_KEY = "commitTimeMSec";

  public static final String COMMIT_COMMAND_VERSION = "commitCommandVer";

  public static final AttributeKey<String> MERGE_TYPE_ATTR = AttributeKey.stringKey("merge_type");

  private final Object CLOSE_LOCK = new Object();

  String name;
  private DirectoryFactory directoryFactory;
  private InfoStream infoStream;
  private Directory directory;

  // metrics
  private long majorMergeDocs = 512 * 1024;
  private AttributedLongTimer majorMerge;
  private AttributedLongTimer minorMerge;
  private AttributedLongCounter majorMergedDocs;
  private AttributedLongCounter majorDeletedDocs;
  private AttributedLongCounter mergeErrors;
  private AttributedLongCounter flushes; // original counter is package-private in IndexWriter
  private boolean mergeTotals = false;
  private boolean mergeDetails = false;
  private final AtomicInteger runningMajorMerges = new AtomicInteger();
  private final AtomicInteger runningMinorMerges = new AtomicInteger();
  private final AtomicInteger runningMajorMergesSegments = new AtomicInteger();
  private final AtomicInteger runningMinorMergesSegments = new AtomicInteger();
  private final AtomicLong runningMajorMergesDocs = new AtomicLong();
  private final AtomicLong runningMinorMergesDocs = new AtomicLong();
  private ObservableLongGauge mergeStats;

  private final SolrMetricsContext solrMetricsContext;
  // merge diagnostics.
  private final Map<String, Long> runningMerges = new ConcurrentHashMap<>();

  public static SolrIndexWriter create(
      SolrCore core,
      String name,
      String path,
      DirectoryFactory directoryFactory,
      boolean create,
      IndexSchema schema,
      SolrIndexConfig config,
      IndexDeletionPolicy delPolicy,
      Codec codec)
      throws IOException {

    SolrIndexWriter w = null;
    final Directory d = directoryFactory.get(path, DirContext.DEFAULT, config.lockType);
    try {
      w = new SolrIndexWriter(core, name, path, d, create, schema, config, delPolicy, codec);
      w.setDirectoryFactory(directoryFactory);
      return w;
    } finally {
      if (null == w && null != d) {
        directoryFactory.doneWithDirectory(d);
        directoryFactory.release(d);
      }
    }
  }

  public SolrIndexWriter(String name, Directory d, IndexWriterConfig conf) throws IOException {
    super(d, conf);
    this.name = name;
    this.infoStream = conf.getInfoStream();
    this.directory = d;
    numOpens.incrementAndGet();
    log.debug("Opened Writer {}", name);
    // no metrics
    mergeTotals = false;
    mergeDetails = false;
    solrMetricsContext = null;
  }

  private SolrIndexWriter(
      SolrCore core,
      String name,
      String path,
      Directory directory,
      boolean create,
      IndexSchema schema,
      SolrIndexConfig config,
      IndexDeletionPolicy delPolicy,
      Codec codec)
      throws IOException {
    super(
        directory,
        config
            .toIndexWriterConfig(core)
            .setOpenMode(
                create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND)
            .setIndexDeletionPolicy(delPolicy)
            .setCodec(codec));
    log.debug("Opened Writer {}", name);
    this.name = name;
    infoStream = getConfig().getInfoStream();
    this.directory = directory;
    numOpens.incrementAndGet();
    solrMetricsContext = core.getSolrMetricsContext().getChildContext(this);
    if (config.metricsInfo != null && config.metricsInfo.initArgs != null) {
      Object v = config.metricsInfo.initArgs.get("majorMergeDocs");
      if (v != null) {
        try {
          majorMergeDocs = Long.parseLong(String.valueOf(v));
        } catch (Exception e) {
          log.warn("Invalid 'majorMergeDocs' argument, using default 512k", e);
        }
      }
      Boolean Totals = config.metricsInfo.initArgs.getBooleanArg("merge");
      Boolean Details = config.metricsInfo.initArgs.getBooleanArg("mergeDetails");
      if (Details != null) {
        mergeDetails = Details;
      } else {
        mergeDetails = false;
      }
      if (Totals != null) {
        mergeTotals = Totals;
      } else {
        mergeTotals = false;
      }
      var baseAttributes =
          core.getCoreAttributes().toBuilder()
              .put(CATEGORY_ATTR, SolrInfoBean.Category.INDEX.toString())
              .build();
      if (mergeDetails) {
        mergeTotals = true; // override
        majorMergedDocs =
            new AttributedLongCounter(
                solrMetricsContext.longCounter(
                    "solr_indexwriter_major_merged_docs",
                    "Number of documents merged while merging segments above the majorMergeDocs threshold ("
                        + majorMergeDocs
                        + ")"),
                baseAttributes);
        majorDeletedDocs =
            new AttributedLongCounter(
                solrMetricsContext.longCounter(
                    "solr_indexwriter_major_deleted_docs",
                    "Number of deleted documents that were expunged while merging segments above the majorMergeDocs threshold ("
                        + majorMergeDocs
                        + ")"),
                baseAttributes);
      }
      if (mergeTotals) {
        minorMerge =
            new AttributedLongTimer(
                solrMetricsContext.longHistogram(
                    "solr_indexwriter_merge",
                    "Time spent merging segments below or equal to the majorMergeDocs threshold ("
                        + majorMergeDocs
                        + ")",
                    OtelUnit.MILLISECONDS),
                baseAttributes.toBuilder().put(MERGE_TYPE_ATTR, "minor").build());
        majorMerge =
            new AttributedLongTimer(
                solrMetricsContext.longHistogram(
                    "solr_indexwriter_merge",
                    "Time spent merging segments above the majorMergeDocs threshold ("
                        + majorMergeDocs
                        + ")",
                    OtelUnit.MILLISECONDS),
                baseAttributes.toBuilder().put(MERGE_TYPE_ATTR, "major").build());
        mergeErrors =
            new AttributedLongCounter(
                solrMetricsContext.longCounter(
                    "solr_indexwriter_merge_errors", "Number of merge errors"),
                baseAttributes);
        String tag = core.getMetricTag();
        mergeStats =
            solrMetricsContext.observableLongGauge(
                "solr_indexwriter_merge_stats",
                "Metrics around currently running segment merges; major := above the majorMergeDocs threshold ("
                    + majorMergeDocs
                    + "), minor := below or equal to the threshold",
                (observableLongMeasurement -> {
                  observableLongMeasurement.record(
                      runningMajorMerges.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running")
                          .put(MERGE_TYPE_ATTR, "major")
                          .build());
                  observableLongMeasurement.record(
                      runningMajorMergesDocs.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running_docs")
                          .put(MERGE_TYPE_ATTR, "major")
                          .build());
                  observableLongMeasurement.record(
                      runningMajorMergesSegments.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running_segments")
                          .put(MERGE_TYPE_ATTR, "major")
                          .build());
                  observableLongMeasurement.record(
                      runningMinorMerges.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running")
                          .put(MERGE_TYPE_ATTR, "minor")
                          .build());
                  observableLongMeasurement.record(
                      runningMinorMergesDocs.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running_docs")
                          .put(MERGE_TYPE_ATTR, "minor")
                          .build());
                  observableLongMeasurement.record(
                      runningMinorMergesSegments.get(),
                      baseAttributes.toBuilder()
                          .put(TYPE_ATTR, "running_segments")
                          .put(MERGE_TYPE_ATTR, "minor")
                          .build());
                }));
        flushes =
            new AttributedLongCounter(
                solrMetricsContext.longCounter(
                    "solr_indexwriter_flush",
                    "Number of times added/deleted documents have been flushed to the Directory"),
                baseAttributes);
      }
    }
  }

  @SuppressForbidden(
      reason =
          "Need currentTimeMillis, commit time should be used only for debugging purposes, "
              + " but currently suspiciously used for replication as well")
  public static void setCommitData(
      IndexWriter iw, long commitCommandVersion, Map<String, String> commitData) {
    log.debug(
        "Calling setCommitData with IW:{} commitCommandVersion:{} commitData:{}",
        iw,
        commitCommandVersion,
        commitData);
    Map<String, String> finalCommitData =
        commitData == null ? new HashMap<>(4) : new HashMap<>(commitData);
    finalCommitData.put(COMMIT_TIME_MSEC_KEY, String.valueOf(System.currentTimeMillis()));
    finalCommitData.put(COMMIT_COMMAND_VERSION, String.valueOf(commitCommandVersion));
    iw.setLiveCommitData(finalCommitData.entrySet());
  }

  private void setDirectoryFactory(DirectoryFactory factory) {
    this.directoryFactory = factory;
  }

  // we override this method to collect metrics for merges.
  @Override
  protected void merge(MergePolicy.OneMerge merge) throws IOException {
    String segString = merge.segString();
    long totalNumDocs = merge.totalNumDocs();
    runningMerges.put(segString, totalNumDocs);
    if (!mergeTotals) {
      try {
        super.merge(merge);
      } finally {
        runningMerges.remove(segString);
      }
      return;
    }
    long deletedDocs = 0;
    for (SegmentCommitInfo info : merge.segments) {
      totalNumDocs -= info.getDelCount();
      deletedDocs += info.getDelCount();
    }
    boolean major = totalNumDocs > majorMergeDocs;
    int segmentsCount = merge.segments.size();
    AttributedLongTimer.MetricTimer context;
    if (major) {
      runningMajorMerges.incrementAndGet();
      runningMajorMergesDocs.addAndGet(totalNumDocs);
      runningMajorMergesSegments.addAndGet(segmentsCount);
      if (mergeDetails) {
        majorMergedDocs.add(totalNumDocs);
        majorDeletedDocs.add(deletedDocs);
      }
      context = majorMerge.start();
    } else {
      runningMinorMerges.incrementAndGet();
      runningMinorMergesDocs.addAndGet(totalNumDocs);
      runningMinorMergesSegments.addAndGet(segmentsCount);
      context = minorMerge.start();
    }
    try {
      super.merge(merge);
    } catch (Throwable t) {
      mergeErrors.inc();
      throw t;
    } finally {
      runningMerges.remove(segString);
      context.stop();
      if (major) {
        runningMajorMerges.decrementAndGet();
        runningMajorMergesDocs.addAndGet(-totalNumDocs);
        runningMajorMergesSegments.addAndGet(-segmentsCount);
      } else {
        runningMinorMerges.decrementAndGet();
        runningMinorMergesDocs.addAndGet(-totalNumDocs);
        runningMinorMergesSegments.addAndGet(-segmentsCount);
      }
    }
  }

  public Map<String, Object> getRunningMerges() {
    return Collections.unmodifiableMap(runningMerges);
  }

  @Override
  protected void doAfterFlush() throws IOException {
    if (flushes != null) { // this is null when writer is used only for snapshot cleanup
      flushes.inc(); // or if mergeTotals == false
    }
    super.doAfterFlush();
  }

  // use DocumentBuilder now...
  //  private final void addField(Document doc, String name, String val) {
  //    SchemaField ftype = schema.getField(name);
  //
  //    // we don't check for a null val ourselves because a solr.FieldType
  //    // might actually want to map it to something.  If createField()
  //    // returns null, then we don't store the field.
  //
  //    Field field = ftype.createField(val, boost);
  //    if (field != null) {
  //      doc.add(field);
  //    }
  //  }
  //
  //  public void addRecord(String[] fieldNames, String[] fieldValues) throws IOException {
  //    Document doc = new Document();
  //    for (int i = 0; i < fieldNames.length; i++) {
  //      String name = fieldNames[i];
  //      String val = fieldNames[i];
  //
  //      // first null is end of list.  client can reuse arrays if they want
  //      // and just write a single null if there is unused space.
  //      if (name == null) {
  //        break;
  //      }
  //
  //      addField(doc, name, val);
  //    }
  //    addDocument(doc);
  //  }

  private volatile boolean isClosed = false;

  @Override
  public void close() throws IOException {
    log.debug("Closing Writer {}", name);
    try {
      super.close();
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      log.error("Error closing IndexWriter", t);
    } finally {
      cleanup();
    }
  }

  @Override
  public void rollback() throws IOException {
    log.debug("Rollback Writer {}", name);
    try {
      super.rollback();
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      log.error("Exception rolling back IndexWriter", t);
    } finally {
      cleanup();
    }
  }

  private void cleanup() throws IOException {
    // It's kind of an implementation detail whether
    // or not IndexWriter#close calls rollback, so
    // we assume it may or may not
    boolean doClose = false;
    synchronized (CLOSE_LOCK) {
      if (!isClosed) {
        doClose = true;
        isClosed = true;
      }
    }
    if (doClose) {

      if (infoStream != null) {
        IOUtils.closeQuietly(infoStream);
      }
      numCloses.incrementAndGet();

      if (directoryFactory != null) {
        directoryFactory.release(directory);
      }
      IOUtils.closeQuietly(mergeStats);
    }
  }
}
