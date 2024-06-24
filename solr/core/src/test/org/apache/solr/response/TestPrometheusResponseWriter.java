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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.SettableGauge;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the {@link PrometheusResponseWriter} behavior */
public class TestPrometheusResponseWriter extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
  public static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrClientTestRule.startSolr(LuceneTestCase.createTempDir());
    jetty = solrClientTestRule.getJetty();
    solrClientTestRule.newCollection().withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    jetty.getCoreContainer().waitForLoadingCoresToFinish(30000);
    // Manually register metrics not initializing from JettyTestRule
    registerGauge(
        jetty.getCoreContainer().getMetricManager(),
        "solr.jvm",
        "buffers.mapped - 'non-volatile memory'.Count");
    registerGauge(
        jetty.getCoreContainer().getMetricManager(),
        "solr.jvm",
        "buffers.mapped - 'non-volatile memory'.MemoryUsed");
    registerGauge(
        jetty.getCoreContainer().getMetricManager(),
        "solr.jvm",
        "buffers.mapped - 'non-volatile memory'.TotalCapacity");
    registerGauge(jetty.getCoreContainer().getMetricManager(), "solr.jvm", "os.cpuLoad");
    registerGauge(jetty.getCoreContainer().getMetricManager(), "solr.jvm", "os.freeMemorySize");
    registerGauge(jetty.getCoreContainer().getMetricManager(), "solr.jvm", "os.totalMemorySize");
  }

  @Test
  public void testPrometheusOutput() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/metrics");
    params.set("wt", "prometheus");
    QueryRequest req = new QueryRequest(params);
    req.setResponseParser(new NoOpResponseParser("prometheus"));
    try (SolrClient adminClient = getHttpSolrClient(jetty.getBaseUrl().toString()); ) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
      String actual = (String) res.get("response");
      String expectedOutput =
          Files.readString(
              new File(TEST_PATH().toString(), "solr-prometheus-output.txt").toPath(),
              StandardCharsets.UTF_8);
      assertEquals(expectedOutput, actual.replaceAll("(?<=}).*", ""));
    }
  }

  public static void registerGauge(
      SolrMetricManager metricManager, String registry, String metricName) {
    Gauge<Number> metric =
        new SettableGauge<>() {
          @Override
          public void setValue(Number value) {}

          @Override
          public Number getValue() {
            return 0;
          }
        };
    metricManager.registerGauge(
        null, registry, metric, "", SolrMetricManager.ResolutionStrategy.IGNORE, metricName, "");
  }
}
