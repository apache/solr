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
package org.apache.solr.metrics;

import com.codahale.metrics.Metric;
import io.prometheus.metrics.model.snapshots.Labels;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;
import org.apache.solr.metrics.prometheus.core.SolrCoreMetric;
import org.junit.Test;

public class SolrCoreMetricTest extends SolrTestCaseJ4 {

  @Test
  public void testStandaloneDefaultLabels() throws InterruptedException {
    String expectedCoreName = "test_core";
    String expectedMetricName = "test_core_metric";
    Labels expectedLabels = Labels.of("core", expectedCoreName);
    TestSolrCoreMetric testSolrCoreMetric =
        new TestSolrCoreMetric(null, expectedCoreName, expectedMetricName, false);

    assertEquals(expectedCoreName, testSolrCoreMetric.coreName);
    assertEquals(expectedMetricName, testSolrCoreMetric.metricName);
    assertEquals(expectedLabels, testSolrCoreMetric.getLabels());
  }

  @Test
  public void testCloudDefaultLabels() throws InterruptedException {
    String expectedCoreName = "core_test_core_shard1_replica_n1";
    String expectedMetricName = "test_core_metric";
    String expectedCollectionName = "test_core";
    String expectedShardName = "shard1";
    String expectedReplicaName = "replica_n1";

    Labels expectedLabels =
        Labels.of(
            "core",
            expectedCoreName,
            "collection",
            expectedCollectionName,
            "shard",
            expectedShardName,
            "replica",
            expectedReplicaName);
    TestSolrCoreMetric testSolrCoreMetric =
        new TestSolrCoreMetric(null, expectedCoreName, expectedMetricName, true);

    assertEquals(expectedCoreName, testSolrCoreMetric.coreName);
    assertEquals(expectedMetricName, testSolrCoreMetric.metricName);
    assertEquals(expectedLabels, testSolrCoreMetric.getLabels());
  }

  static class TestSolrCoreMetric extends SolrCoreMetric {
    public TestSolrCoreMetric(
        Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
      super(dropwizardMetric, coreName, metricName, cloudMode);
    }

    @Override
    public SolrCoreMetric parseLabels() {
      return null;
    }

    @Override
    public void toPrometheus(SolrPrometheusCoreExporter solrPrometheusCoreExporter) {}
  }
}
