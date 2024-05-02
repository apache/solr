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
package org.apache.solr.response;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the {@link PrometheusResponseWriter} behavior */
public class TestPrometheusResponseWriter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testPrometheusOutput() throws IOException {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    PrometheusResponseWriter w = new PrometheusResponseWriter();
    NamedList<Object> registries = new SimpleOrderedMap<>();
    ByteArrayOutputStream actual = new ByteArrayOutputStream();
    Labels expectedLabels = Labels.of("test", "test-value");

    SolrPrometheusCoreExporter registry = new SolrPrometheusCoreExporter("collection1", false);
    CounterSnapshot.CounterDataPointSnapshot counterDatapoint =
        registry.createCounterDatapoint(1.234, expectedLabels);
    GaugeSnapshot.GaugeDataPointSnapshot gaugeDataPoint =
        registry.createGaugeDatapoint(9.876, expectedLabels);
    registry.collectCounterDatapoint("test_counter_metric_name", counterDatapoint);
    registry.collectGaugeDatapoint("test_gauge_metric_name", gaugeDataPoint);
    registries.add("solr.core.collection1", registry);
    rsp.add("metrics", registries);

    w.write(actual, req, rsp);
    String test = actual.toString();
    assertEquals(
        "# TYPE test_counter_metric_name_total counter\ntest_counter_metric_name_total{test=\"test-value\"} 1.234\n# TYPE test_gauge_metric_name gauge\ntest_gauge_metric_name{test=\"test-value\"} 9.876\n",
        actual.toString());
    req.close();
  }
}
