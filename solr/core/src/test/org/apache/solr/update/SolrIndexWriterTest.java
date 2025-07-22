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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrIndexWriter is pretty much identical to Lucene's IndexWriter so these tests are only for
 * testing metrics collection
 */
public class SolrIndexWriterTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  IndexWriter iw;
  SolrMetricsContext solrMetricsContext;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.NRTCachingDirectoryFactory");
    System.setProperty("solr.tests.lockType", DirectoryFactory.LOCK_TYPE_SIMPLE);

    initCore("solrconfig.xml", "schema15.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    SolrCore core = h.getCore();
    iw = core.getSolrCoreState().getIndexWriter(core).get();
    ((SolrIndexWriter) iw).setMajorMergeDocs(SolrIndexWriter.DEFAULT_MAJOR_MERGE_DOCS);
    solrMetricsContext = core.getSolrMetricsContext();
  }

  @Test
  public void testMinorMergeMetrics() throws Exception {
    Map<String, Counter> metrics = solrMetricsContext.getMetricRegistry().getCounters();

    // we are intentionally only testing "started" metrics because it is difficult to block until
    // merge fully completes
    Counter minorMergeStartedMetrics = metrics.get("INDEX.merge.minor.started.merges");
    Counter minorMergeStartedDocs = metrics.get("INDEX.merge.minor.started.docs");
    Counter minorMergeStartedSegments = metrics.get("INDEX.merge.minor.started.segments");
    long initialMerges = minorMergeStartedMetrics.getCount();
    long initialDocs = minorMergeStartedDocs.getCount();
    long initialSegments = minorMergeStartedSegments.getCount();

    for (int i = 1; i <= 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      doc.add(new IntField("number", i, Field.Store.YES));
      iw.addDocument(doc);
      if (i % 10 == 0) iw.commit(); // make a new segment every 10th document
    }
    iw.forceMerge(1, true);

    assertEquals(
        "should be single merge operation", initialMerges + 1, minorMergeStartedMetrics.getCount());
    assertEquals("should merge all documents", initialDocs + 100, minorMergeStartedDocs.getCount());
    assertEquals(
        "should merge all segments", initialSegments + 10, minorMergeStartedSegments.getCount());
  }

  @Test
  public void testMinorMergeMetrics_deletedDocs() throws Exception {
    Map<String, Counter> metrics = solrMetricsContext.getMetricRegistry().getCounters();

    // we are intentionally only testing "started" metrics because it is difficult to block until
    // merge fully completes
    Counter minorMergeStartedMetrics = metrics.get("INDEX.merge.minor.started.merges");
    Counter minorMergeStartedDocs = metrics.get("INDEX.merge.minor.started.docs");
    Counter minorMergeStartedSegments = metrics.get("INDEX.merge.minor.started.segments");
    Counter minorMergeStartedDeletedDocs = metrics.get("INDEX.merge.minor.started.deletedDocs");
    long initialMerges = minorMergeStartedMetrics.getCount();
    long initialDocs = minorMergeStartedDocs.getCount();
    long initialSegments = minorMergeStartedSegments.getCount();
    long initialDeletedDocs = minorMergeStartedDeletedDocs.getCount();

    for (int i = 1; i <= 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      doc.add(new IntField("number", i, Field.Store.YES));
      iw.addDocument(doc);
      if (i % 10 == 0) iw.commit(); // make a new segment every 10th document
    }
    for (int i = 1; i <= 3; i++) {
      iw.deleteDocuments(new Term("id", String.valueOf(i)));
    }
    iw.commit();
    iw.forceMerge(1, true);

    assertEquals(
        "should be mulitple merge operations",
        initialMerges + 1,
        minorMergeStartedMetrics.getCount());
    assertEquals(
        "should count deleted docs",
        initialDeletedDocs + 3,
        minorMergeStartedDeletedDocs.getCount());
    assertEquals("should merge all documents", initialDocs + 97, minorMergeStartedDocs.getCount());
    assertEquals(
        "should merge all segments", initialSegments + 10, minorMergeStartedSegments.getCount());
  }

  @Test
  public void testMajorMergeMetrics() throws Exception {
    ((SolrIndexWriter) iw).setMajorMergeDocs(10);
    Map<String, Counter> metrics = solrMetricsContext.getMetricRegistry().getCounters();

    // we are intentionally only testing "started" metrics because it is difficult to block until
    // merge fully completes
    Counter majorMergeStartedMetrics = metrics.get("INDEX.merge.major.started.merges");
    Counter majorMergeStartedDocs = metrics.get("INDEX.merge.major.started.docs");
    Counter majorMergeStartedSegments = metrics.get("INDEX.merge.major.started.segments");

    for (int i = 1; i <= 100; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      doc.add(new IntField("number", i, Field.Store.YES));
      iw.addDocument(doc);
      if (i % 10 == 0) iw.commit(); // make a new segment every 10th document
    }
    iw.forceMerge(1, true);

    assertEquals("should be single merge operation", 1, majorMergeStartedMetrics.getCount());
    assertEquals("should merge all documents", 100, majorMergeStartedDocs.getCount());
    assertEquals("should merge all segments", 10, majorMergeStartedSegments.getCount());
  }

  @Test
  public void testFlushMetrics() throws Exception {
    Map<String, Meter> metrics = solrMetricsContext.getMetricRegistry().getMeters();
    Meter flushMeter = metrics.get("INDEX.merge.flushes");
    long initialFlushCount = flushMeter.getCount();
    iw.flush();
    assertEquals(initialFlushCount + 1, flushMeter.getCount());
  }
}
