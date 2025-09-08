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
import static org.apache.solr.update.SolrIndexWriter.MERGE_TYPE_ATTR;

import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Test;

/** Test proper registration and collection of index and directory metrics. */
public class SolrIndexMetricsTest extends SolrTestCaseJ4 {

  @After
  public void afterMethod() {
    deleteCore();
  }

  private void addDocs() throws Exception {
    SolrQueryRequest req = lrf.makeRequest();
    UpdateHandler uh = req.getCore().getUpdateHandler();
    AddUpdateCommand add = new AddUpdateCommand(req);
    for (int i = 0; i < 1000; i++) {
      add.clear();
      add.solrDoc = new SolrInputDocument();
      add.solrDoc.addField("id", "" + i);
      add.solrDoc.addField("foo_s", "foo-" + i);
      uh.addDoc(add);
    }
    uh.commit(new CommitUpdateCommand(req, false));
    // make sure all merges are finished
    h.reload();
  }

  @Test
  public void testIndexMetricsNoDetails() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "true");
    System.setProperty("solr.tests.metrics.mergeDetails", "false");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();

    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      // check basic index meters
      var minorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .build());
      assertTrue("minorMerge: " + minorMergeTimer.getCount(), minorMergeTimer.getCount() >= 3);
      var majorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .build());
      // major merge timer should have a value of 0, and because 0 values are not reported, no
      // datapoint is available
      assertNull("majorMergeTimer", majorMergeTimer);

      // check detailed meters
      var majorMergeDocs =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_major_merged_docs",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      // major merge docs should be null because mergeDetails is false
      assertNull("majorMergeDocs", majorMergeDocs);

      var flushCounter =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_flush",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      assertTrue("flush: " + flushCounter.getValue(), flushCounter.getValue() > 10);
    }
  }

  @Test
  public void testIndexNoMetrics() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "false");
    System.setProperty("solr.tests.metrics.mergeDetails", "false");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");
    addDocs();
    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      var indexSize =
          SolrMetricTestUtils.getGaugeDatapoint(
              core,
              "solr_core_index_size_bytes",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label("category", "CORE")
                  .build());
      var segmentSize =
          SolrMetricTestUtils.getGaugeDatapoint(
              core,
              "solr_core_segments",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label("category", "CORE")
                  .build());
      assertNotNull(indexSize);
      assertNotNull(segmentSize);
    }
  }

  @Test
  public void testIndexMetricsWithDetails() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "false"); // test mergeDetails override too
    System.setProperty("solr.tests.metrics.mergeDetails", "true");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();

    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      var prometheusMetricReader = SolrMetricTestUtils.getPrometheusMetricReader(core);
      assertNotNull(prometheusMetricReader);
      MetricSnapshots otelMetrics = prometheusMetricReader.collect();
      assertTrue("Metrics count: " + otelMetrics.size(), otelMetrics.size() >= 19);

      // check basic index meters
      var minorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .build());
      assertTrue("minorMergeTimer: " + minorMergeTimer.getCount(), minorMergeTimer.getCount() >= 3);
      var majorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .build());
      // major merge timer should have a value of 0, and because 0 values are not reported, no
      // datapoint is available
      assertNull("majorMergeTimer", majorMergeTimer);

      // check detailed meters
      var majorMergeDocs =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_major_merged_docs",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      // major merge docs should have a value of 0, and because 0 values are not reported, no
      // datapoint is available
      assertNull("majorMergeDocs", majorMergeDocs);

      var flushCounter =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_flush",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      assertTrue("flush: " + flushCounter.getValue(), flushCounter.getValue() > 10);
    }
  }

  public void testIndexMetricsMajorAndMinorMergesWithDetails() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "false"); // test mergeDetails override too
    System.setProperty("solr.tests.metrics.mergeDetails", "true");
    System.setProperty("solr.tests.metrics.majorMergeDocs", "450");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();

    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      var prometheusMetricReader = SolrMetricTestUtils.getPrometheusMetricReader(core);
      assertNotNull(prometheusMetricReader);
      MetricSnapshots otelMetrics = prometheusMetricReader.collect();
      assertTrue("Metrics count: " + otelMetrics.size(), otelMetrics.size() >= 18);

      // addDocs() adds 1000 documents and then sends a commit.  maxBufferedDocs==100,
      // segmentsPerTier==3,
      //     maxMergeAtOnce==3 and majorMergeDocs==450.  Thus, new documents form segments with 100
      // docs, merges are
      //     called for when there are 3 segments at the lowest tier, and the merges are as follows:
      //     1. 100 + 100 + 100 ==> new 300 doc segment, below the 450 threshold ==> minor merge
      //     2. 100 + 100 + 100 ==> new 300 doc segment, below the 450 threshold ==> minor merge
      //     3. 300 + 100 + 100 ==> new 500 doc segment, above the 450 threshold ==> major merge
      //     4. 300 + 100 + 100 ==> new 500 doc segment, above the 450 threshold ==> major merge

      // check basic index meters
      var minorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .build());
      assertTrue("minorMergeTimer: " + minorMergeTimer.getCount(), minorMergeTimer.getCount() == 2);
      var majorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_indexwriter_merge_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .build());
      assertTrue("majorMergeTimer: " + majorMergeTimer.getCount(), majorMergeTimer.getCount() == 2);

      // check detailed meters
      var majorMergeDocs =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_major_merged_docs",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      // majorMergeDocs is the total number of docs merged during major merge operations
      assertTrue("majorMergeDocs: " + majorMergeDocs.getValue(), majorMergeDocs.getValue() == 1000);

      var flushCounter =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_indexwriter_flush",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      assertTrue("flush: " + flushCounter.getValue(), flushCounter.getValue() >= 10);
    }
  }
}
