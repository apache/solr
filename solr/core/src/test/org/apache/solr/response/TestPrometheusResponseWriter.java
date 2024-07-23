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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SettableGauge;
import java.util.List;
import java.util.stream.Collectors;
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

public class TestPrometheusResponseWriter extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
  public static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrClientTestRule.startSolr(LuceneTestCase.createTempDir());
    jetty = solrClientTestRule.getJetty();
    solrClientTestRule.newCollection().withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    jetty.getCoreContainer().waitForLoadingCoresToFinish(30000);

    SolrMetricManager manager = jetty.getCoreContainer().getMetricManager();
    Counter c = manager.counter(null, "solr.core.collection1", "QUERY./dummy/metrics.requests");
    c.inc(10);
    c = manager.counter(null, "solr.node", "ADMIN./dummy/metrics.requests");
    c.inc(20);
    Meter m = manager.meter(null, "solr.jetty", "dummyMetrics.2xx-responses");
    m.mark(30);
    registerGauge(jetty.getCoreContainer().getMetricManager(), "solr.jvm", "gc.dummyMetrics.count");
  }

  @Test
  public void testPrometheusStructureOutput() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/metrics");
    params.set("wt", "prometheus");
    QueryRequest req = new QueryRequest(params);
    req.setResponseParser(new NoOpResponseParser("prometheus"));

    try (SolrClient adminClient = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
      String output = (String) res.get("response");
      List<String> filteredResponse =
          output.lines().filter(line -> !line.startsWith("#")).collect(Collectors.toList());
      filteredResponse.forEach(
          (actualMetric) -> {
            String actualValue = actualMetric.substring(actualMetric.lastIndexOf("} ") + 1);
            assertTrue(
                "All metrics should start with 'solr_metrics_'",
                actualMetric.startsWith("solr_metrics_"));
            try {
              Float.parseFloat(actualValue);
            } catch (NumberFormatException e) {
              throw new AssertionError("Prometheus value not parsed as a float: " + actualValue);
            }
          });
    }
  }

  public void testPrometheusDummyOutput() throws Exception {
    String expectedCore =
        "solr_metrics_core_requests_total{category=\"QUERY\",core=\"collection1\",handler=\"/dummy/metrics\",type=\"requests\"} 10.0";
    String expectedNode =
        "solr_metrics_node_requests_total{category=\"ADMIN\",handler=\"/dummy/metrics\",type=\"requests\"} 20.0";
    String expectedJetty = "solr_metrics_jetty_response_total{status=\"2xx\"} 30.0";
    String expectedJvm = "solr_metrics_jvm_gc{item=\"dummyMetrics\"} 0.0";

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/metrics");
    params.set("wt", "prometheus");
    QueryRequest req = new QueryRequest(params);
    req.setResponseParser(new NoOpResponseParser("prometheus"));

    try (SolrClient adminClient = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
      String output = (String) res.get("response");
      assertEquals(
          expectedCore,
          output
              .lines()
              .filter(line -> line.contains(expectedCore))
              .collect(Collectors.toList())
              .get(0));
      assertEquals(
          expectedNode,
          output
              .lines()
              .filter(line -> line.contains(expectedNode))
              .collect(Collectors.toList())
              .get(0));
      assertEquals(
          expectedJetty,
          output
              .lines()
              .filter(line -> line.contains(expectedJetty))
              .collect(Collectors.toList())
              .get(0));
      assertEquals(
          expectedJvm,
          output
              .lines()
              .filter(line -> line.contains(expectedJvm))
              .collect(Collectors.toList())
              .get(0));
    }
  }

  private static void registerGauge(
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
