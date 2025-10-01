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

import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.client.HttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.SolrMetricTestUtils;
import org.apache.solr.util.TestHarness;
import org.hamcrest.number.OrderingComparison;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricsIntegrationTest extends SolrTestCaseJ4 {
  private CoreContainer cc;
  private SolrMetricManager metricManager;

  @Before
  public void beforeTest() throws Exception {
    Path home = TEST_PATH();
    // define these properties, they are used in solrconfig.xml
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    String solrXml =
        Files.readString(home.resolve("solr-metricreporter.xml"), StandardCharsets.UTF_8);
    NodeConfig cfg = SolrXmlConfig.fromString(home, solrXml);
    cc =
        createCoreContainer(
            cfg,
            new TestHarness.TestCoresLocator(
                DEFAULT_TEST_CORENAME,
                initAndGetDataDir().toString(),
                "solrconfig.xml",
                "schema.xml"));

    h.coreName = DEFAULT_TEST_CORENAME;
    metricManager = h.getCore().getCoreContainer().getMetricManager();
  }

  @After
  public void afterTest() {
    if (null == metricManager) {
      return; // test failed to init, nothing to clean up
    }
    deleteCore(); // closes TestHarness which closes CoreContainer which closes SolrCore
  }

  @Test
  public void testCoreContainerMetrics() {
    var reader = metricManager.getPrometheusMetricReader("solr.node");

    assertNotNull(getGaugeOpt(reader, "solr_cores_loaded", "permanent"));
    assertNotNull(getGaugeOpt(reader, "solr_cores_loaded", "transient"));
    assertNotNull(getGaugeOpt(reader, "solr_cores_loaded", "unloaded"));

    assertNotNull(getGaugeOpt(reader, "solr_disk_space_bytes", "total_space"));
    assertNotNull(getGaugeOpt(reader, "solr_disk_space_bytes", "usable_space"));
  }

  private static GaugeDataPointSnapshot getGaugeOpt(
      PrometheusMetricReader reader, String metricName, String type) {
    return SolrMetricTestUtils.getGaugeDatapoint(
        reader,
        metricName,
        Labels.of("category", "CONTAINER", "otel_scope_name", "org.apache.solr", "type", type));
  }

  // NOCOMMIT: Comeback and fix this test after merging the SolrZKClient metrics migration
  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-17458")
  public void testZkMetrics() throws Exception {
    System.setProperty("metricsEnabled", "true");
    MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster.Builder(3, createTempDir())
            .addConfig("conf", configset("conf2"))
            .configure();
    System.clearProperty("metricsEnabled");
    try {
      JettySolrRunner j = cluster.getRandomJetty(random());
      String url = j.getBaseUrl() + "/admin/metrics?key=solr.node:CONTAINER.zkClient&wt=json";
      try (SolrClient solrClient = j.newClient()) {
        HttpClient httpClient = ((HttpSolrClient) solrClient).getHttpClient();
        @SuppressWarnings("unchecked")
        Map<String, Object> zkMmetrics =
            (Map<String, Object>)
                Utils.getObjectByPath(
                    HttpClientUtil.executeGET(httpClient, url, Utils.JSONCONSUMER),
                    false,
                    List.of("metrics", "solr.node:CONTAINER.zkClient"));

        Set<String> allKeys =
            Set.of(
                "watchesFired",
                "reads",
                "writes",
                "bytesRead",
                "bytesWritten",
                "multiOps",
                "cumulativeMultiOps",
                "childFetches",
                "cumulativeChildrenFetched",
                "existsChecks",
                "deletes");

        for (String k : allKeys) {
          assertNotNull(zkMmetrics.get(k));
        }
        HttpClientUtil.executeGET(
            httpClient,
            j.getBaseURLV2() + "/cluster/zookeeper/children/live_nodes",
            Utils.JSONCONSUMER);
        @SuppressWarnings("unchecked")
        Map<String, Object> zkMmetricsNew =
            (Map<String, Object>)
                Utils.getObjectByPath(
                    HttpClientUtil.executeGET(httpClient, url, Utils.JSONCONSUMER),
                    false,
                    List.of("metrics", "solr.node:CONTAINER.zkClient"));

        assertThat(
            findDelta(zkMmetrics, zkMmetricsNew, "childFetches"),
            OrderingComparison.greaterThanOrEqualTo(1L));
        assertThat(
            findDelta(zkMmetrics, zkMmetricsNew, "cumulativeChildrenFetched"),
            OrderingComparison.greaterThanOrEqualTo(3L));
        assertThat(
            findDelta(zkMmetrics, zkMmetricsNew, "existsChecks"),
            OrderingComparison.greaterThanOrEqualTo(4L));
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testCoreRename() {
    String newCoreName = "renamed_core";
    String originalRegistryName;

    try (SolrCore core = cc.getCore(DEFAULT_TEST_CORENAME)) {
      originalRegistryName = core.getCoreMetricManager().getRegistryName();
      assertTrue("Original registry should exist", metricManager.hasRegistry(originalRegistryName));
      assertQ(req("q", "*:*"), "//result[@numFound='0']");
      assertEquals(
          1.0, SolrMetricTestUtils.newStandaloneSelectRequestsDatapoint(core).getValue(), 0.0);
    }

    cc.rename(DEFAULT_TEST_CORENAME, newCoreName);
    h.coreName = newCoreName;

    try (SolrCore core = cc.getCore(newCoreName)) {
      assertFalse(
          "Original registry should not exist", metricManager.hasRegistry(originalRegistryName));
      assertTrue(
          "Renamed registry should exist",
          metricManager.hasRegistry(core.getCoreMetricManager().getRegistryName()));
      assertQ(req("q", "*:*"), "//result[@numFound='0']");
      assertEquals(
          1.0,
          SolrMetricTestUtils.newStandaloneSelectRequestsDatapoint(h.getCore()).getValue(),
          0.0);
    }
  }

  private long findDelta(Map<String, Object> m1, Map<String, Object> m2, String k) {
    return ((Number) m2.get(k)).longValue() - ((Number) m1.get(k)).longValue();
  }
}
