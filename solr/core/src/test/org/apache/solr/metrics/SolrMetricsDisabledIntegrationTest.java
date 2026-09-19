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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.util.TestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrMetricsDisabledIntegrationTest extends SolrTestCaseJ4 {
  private CoreContainer cc;
  private SolrMetricManager metricManager;
  private String previousMetricsEnabled;

  @Before
  public void beforeTest() throws Exception {
    Path home = TEST_PATH();
    previousMetricsEnabled = System.getProperty("metricsEnabled");
    System.setProperty("metricsEnabled", "false");

    String solrXml = Files.readString(home.resolve("solr.xml"), StandardCharsets.UTF_8);
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
    metricManager = cc.getMetricManager();
  }

  @After
  public void afterTest() {
    if (metricManager != null) {
      deleteCore();
    }
    if (previousMetricsEnabled == null) {
      System.clearProperty("metricsEnabled");
    } else {
      System.setProperty("metricsEnabled", previousMetricsEnabled);
    }
  }

  @Test
  public void testMetricsDisabledPreventsNodeAndCoreRegistries() throws Exception {
    assertFalse(cc.getConfig().getMetricsConfig().isEnabled());
    assertTrue(metricManager.registryNames().isEmpty());
    assertNull(metricManager.getPrometheusMetricReader("solr.node"));

    try (SolrCore core = cc.getCore(DEFAULT_TEST_CORENAME)) {
      assertNotNull(core);
      assertNull(
          metricManager.getPrometheusMetricReader(core.getCoreMetricManager().getRegistryName()));
    }

    assertQ(req("q", "*:*"), "//result[@numFound='0']");
    assertU(adoc("id", "1"));
    assertU(commit());

    assertTrue(metricManager.registryNames().isEmpty());
    assertNull(metricManager.getPrometheusMetricReader("solr.node"));
  }
}
