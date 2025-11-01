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
import static org.apache.solr.update.SolrIndexWriter.MERGE_OP_ATTR;
import static org.apache.solr.update.SolrIndexWriter.MERGE_STATE_ATTR;
import static org.apache.solr.update.SolrIndexWriter.MERGE_TYPE_ATTR;
import static org.apache.solr.update.SolrIndexWriter.RESULT_ATTR;

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
    for (int i = 0; i < 800; i++) {
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
  public void testIndexNoMetrics() throws Exception {
    initCore("solrconfig-indexmetrics.xml", "schema.xml");
    addDocs();
    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      var indexSize =
          SolrMetricTestUtils.getGaugeDatapoint(
              core,
              "solr_core_index_size_megabytes",
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
  public void testIndexMetricsMajorAndMinorMerges() throws Exception {
    System.setProperty("solr.tests.metrics.majorMergeDocs", "450");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();

    try (SolrCore core = h.getCoreContainer().getCore("collection1")) {
      var prometheusMetricReader = SolrMetricTestUtils.getPrometheusMetricReader(core);
      assertNotNull(prometheusMetricReader);
      MetricSnapshots otelMetrics = prometheusMetricReader.collect();
      assertTrue("Metrics count: " + otelMetrics.size(), otelMetrics.size() >= 18);

      // addDocs() adds 800 documents and then sends a commit.  maxBufferedDocs==100,
      // segmentsPerTier==3,
      //     maxMergeAtOnce==3 and majorMergeDocs==450.  Thus, new documents form segments with 100
      // docs, merges are
      //     called for when there are 3 segments at the lowest tier, and the merges are as follows:
      //     1. 100 + 100 + 100 ==> new 300 doc segment, below the 450 threshold ==> minor merge
      //     2. 100 + 100 + 100 ==> new 300 doc segment, below the 450 threshold ==> minor merge
      //     3. 300 + 100 + 100 ==> new 500 doc segment, above the 450 threshold ==> major merge

      // check basic index meters
      var minorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_core_indexwriter_merge_time_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .build());
      assertEquals(
          "minorMergeTimer instances should be at least 2, got: " + minorMergeTimer.getCount(),
          2,
          minorMergeTimer.getCount());
      var majorMergeTimer =
          SolrMetricTestUtils.getHistogramDatapoint(
              core,
              "solr_core_indexwriter_merge_time_milliseconds",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .build());
      assertEquals(
          "majorMergeTimer instances should be at least 1, got: " + majorMergeTimer.getCount(),
          1,
          majorMergeTimer.getCount());

      var minorMergeDocs =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_core_indexwriter_merge_docs",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .label(MERGE_OP_ATTR.toString(), "merge")
                  .label(MERGE_STATE_ATTR.toString(), "completed")
                  .label(RESULT_ATTR.toString(), "success")
                  .build());
      assertEquals(
          "minorMergeDocs should be 600, got: " + minorMergeDocs.getValue(),
          600,
          (long) minorMergeDocs.getValue());
      var majorMergeDocs =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_core_indexwriter_merge_docs",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .label(MERGE_OP_ATTR.toString(), "merge")
                  .label(MERGE_STATE_ATTR.toString(), "completed")
                  .label(RESULT_ATTR.toString(), "success")
                  .build());
      assertEquals(
          "majorMergeDocs should be 500, got: " + majorMergeDocs.getValue(),
          500,
          (long) majorMergeDocs.getValue());

      // segments metrics
      var minorSegmentsMergeMetric =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_core_indexwriter_merge_segments",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "minor")
                  .label(MERGE_STATE_ATTR.toString(), "completed")
                  .label(RESULT_ATTR.toString(), "success")
                  .build());
      assertNotNull("minor segment merges metric should exist", minorSegmentsMergeMetric);
      assertEquals(
          "number of minor segments merged should be 6, got: "
              + minorSegmentsMergeMetric.getValue(),
          6,
          (long) minorSegmentsMergeMetric.getValue());
      var majorSegmentsMergeMetric =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_core_indexwriter_merge_segments",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .label(MERGE_TYPE_ATTR.toString(), "major")
                  .label(MERGE_STATE_ATTR.toString(), "completed")
                  .label(RESULT_ATTR.toString(), "success")
                  .build());
      assertNotNull("major segment merges metric should exist", majorSegmentsMergeMetric);
      assertEquals(
          "number of major segments merged should be 3, got: "
              + majorSegmentsMergeMetric.getValue(),
          3,
          (long) majorSegmentsMergeMetric.getValue());

      var flushCounter =
          SolrMetricTestUtils.getCounterDatapoint(
              core,
              "solr_core_indexwriter_flushes",
              SolrMetricTestUtils.newStandaloneLabelsBuilder(core)
                  .label(CATEGORY_ATTR.toString(), SolrInfoBean.Category.INDEX.toString())
                  .build());
      assertTrue(
          "should be at greater than 10 flushes: " + flushCounter.getValue(),
          flushCounter.getValue() >= 10);
    }
  }
}
