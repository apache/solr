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

import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
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
    MetricSnapshots metrics =
        new MetricSnapshots(
             metricManager.getPrometheusMetricReaders().entrySet().stream()
                .flatMap(
                    entry ->
                        entry.getValue().collect().stream()
                            .filter(m -> !m.getMetadata().getPrometheusName().startsWith("target")))
                .toList());

    GaugeSnapshot coresLoaded =
        SolrMetricTestUtils.getMetricSnapshot(GaugeSnapshot.class, metrics, "solr_cores_loaded");
    assertTrue(getGaugeOpt(coresLoaded, "permanent").isPresent());
    assertTrue(getGaugeOpt(coresLoaded, "transient").isPresent());
    assertTrue(getGaugeOpt(coresLoaded, "unloaded").isPresent());

    GaugeSnapshot fsDiskSpace =
        SolrMetricTestUtils.getMetricSnapshot(
            GaugeSnapshot.class, metrics, "solr_cores_filesystem_disk_space_bytes");
    assertTrue(getGaugeOpt(fsDiskSpace, "total_space").isPresent());
    assertTrue(getGaugeOpt(fsDiskSpace, "usable_space").isPresent());

    GaugeSnapshot rootDiskSpace =
        SolrMetricTestUtils.getMetricSnapshot(
            GaugeSnapshot.class, metrics, "solr_cores_root_disk_space_bytes");
    assertTrue(getGaugeOpt(rootDiskSpace, "total_space").isPresent());
    assertTrue(getGaugeOpt(rootDiskSpace, "usable_space").isPresent());
  }

  private static Optional<GaugeDataPointSnapshot> getGaugeOpt(GaugeSnapshot gauges, String type) {
    return gauges.getDataPoints().stream()
        .filter(g -> g.getLabels().get(TYPE_ATTR.toString()).equals(type))
        .findFirst();
  }

  @Test
  public void testZkMetrics() throws Exception {
    System.setProperty("metricsEnabled", "true");
    MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster.Builder(3, createTempDir())
            .addConfig("conf", configset("conf2"))
            .configure();
    System.clearProperty("metricsEnabled");
    JettySolrRunner j = cluster.getRandomJetty(random());
    var builder =
        Labels.builder().label("category", "CONTAINER").label("otel_scope_name", "org.apache.solr");
    var baseLabels = builder.build();

    var reader = j.getCoreContainer().getMetricManager().getPrometheusMetricReader("solr.node");

    assertNotNull(
        SolrMetricTestUtils.getCounterDatapoint(reader, "solr_zk_watches_fired", baseLabels));
    assertNotNull(
        SolrMetricTestUtils.getCounterDatapoint(reader, "solr_zk_read_bytes", baseLabels));
    assertNotNull(
        SolrMetricTestUtils.getCounterDatapoint(reader, "solr_zk_written_bytes", baseLabels));
    assertNotNull(
        SolrMetricTestUtils.getCounterDatapoint(
            reader, "solr_zk_cumulative_multi_ops", baseLabels));

    Set<String> types = Set.of("delete", "exists", "multi", "read", "write");

    for (String type : types) {
      assertNotNull(
          SolrMetricTestUtils.getCounterDatapoint(
              reader, "solr_zk_ops", baseLabels.merge(Labels.of("ops", type))));
    }

    try (SolrClient solrClient = j.newClient()) {
      HttpClient httpClient = ((HttpSolrClient) solrClient).getHttpClient();
      var initialChildrenFetched =
          SolrMetricTestUtils.getCounterDatapoint(
                  reader, "solr_zk_cumulative_children_fetched", baseLabels)
              .getValue();
      var initialChildFetches =
          SolrMetricTestUtils.getCounterDatapoint(reader, "solr_zk_child_fetches", baseLabels)
              .getValue();
      var initialExistsOp =
          SolrMetricTestUtils.getCounterDatapoint(
                  reader, "solr_zk_ops", baseLabels.merge(Labels.of("ops", "exists")))
              .getValue();

      // Send GET request to trigger some metrics
      HttpClientUtil.executeGET(
          httpClient,
          j.getBaseURLV2() + "/cluster/zookeeper/children/live_nodes",
          Utils.JSONCONSUMER);

      var childrenFetched =
          SolrMetricTestUtils.getCounterDatapoint(
                  reader, "solr_zk_cumulative_children_fetched", baseLabels)
              .getValue();
      var childFetches =
          SolrMetricTestUtils.getCounterDatapoint(reader, "solr_zk_child_fetches", baseLabels)
              .getValue();
      var existsOp =
          SolrMetricTestUtils.getCounterDatapoint(
                  reader, "solr_zk_ops", builder.label("ops", "exists").build())
              .getValue();

      assertTrue(childrenFetched - initialChildrenFetched >= 3.0);
      assertTrue(childFetches - initialChildFetches >= 1.0);
      assertTrue(existsOp - initialExistsOp >= 4.0);
    }

    cluster.shutdown();
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
