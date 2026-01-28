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

import static org.apache.solr.client.solrj.response.InputStreamResponseParser.STREAM_KEY;
import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.MetricsRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPrometheusResponseWriter extends SolrTestCaseJ4 {
  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final List<String> VALID_PROMETHEUS_VALUES = Arrays.asList("NaN", "+Inf", "-Inf");

  @BeforeClass
  public static void beforeClass() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(LuceneTestCase.createTempDir());
    solrTestRule.newCollection("core1").withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    solrTestRule.newCollection("core2").withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();

    // Populate request metrics on both cores
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "*:*");

    solrTestRule.getSolrClient("core1").query(queryParams);
    solrTestRule.getSolrClient("core2").query(queryParams);
  }

  @Test
  public void testPrometheusStructureOutput() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("wt", "prometheus");
    var req = new MetricsRequest(params);
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);
      String output = InputStreamResponseParser.consumeResponseToString(res);

      Set<String> seenTypeInfo = new HashSet<>();

      List<String> filteredResponse =
          output
              .lines()
              .filter(
                  line -> {
                    if (!line.startsWith("#")) {
                      return true;
                    }
                    assertTrue(
                        "Prometheus exposition format cannot have duplicate TYPE information",
                        seenTypeInfo.add(line));
                    return false;
                  })
              .toList();
      filteredResponse.forEach(
          (actualMetric) -> {
            String actualValue;
            if (actualMetric.contains("}")) {
              actualValue = actualMetric.substring(actualMetric.lastIndexOf("} ") + 1);
            } else {
              actualValue = actualMetric.split(" ")[1];
            }
            if (actualMetric.startsWith("target_info")) {
              // Skip standard OTEL metric
              return;
            }
            assertTrue("All metrics should start with 'solr_'", actualMetric.startsWith("solr_"));
            try {
              Float.parseFloat(actualValue);
            } catch (NumberFormatException e) {
              log.warn("Prometheus value not a parsable float");
              assertTrue(VALID_PROMETHEUS_VALUES.contains(actualValue));
            }
          });
    }
  }

  @Test
  public void testAcceptHeaderOpenMetricsFormat() throws Exception {
    var req = new MetricsRequest();

    req.setResponseParser(new InputStreamResponseParser(null));

    req.addHeader("Accept", "application/openmetrics-text;version=1.0.0");

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);

      try (InputStream in = (InputStream) res.get(STREAM_KEY)) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Should use OpenMetrics format when Accept header is set",
            output.trim().endsWith("# EOF"));
      }
    }
  }

  @Test
  public void testWtParameterOpenMetricsFormat() throws Exception {
    var req = new MetricsRequest();

    req.setResponseParser(new InputStreamResponseParser("openmetrics"));

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);

      try (InputStream in = (InputStream) res.get(STREAM_KEY)) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(
            "Should use OpenMetrics format when wt=openmetrics is set",
            output.trim().endsWith("# EOF"));
      }
    }
  }

  @Test
  public void testDefaultPrometheusFormat() throws Exception {
    var req = new MetricsRequest();

    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);

      try (InputStream in = (InputStream) res.get(STREAM_KEY)) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        assertFalse(
            "Should use Prometheus format when wt=prometheus is set",
            output.trim().endsWith("# EOF"));
      }
    }
  }

  @Test
  public void testDefaultPrometheusFormatNoWtParam() throws Exception {
    var req = new MetricsRequest();

    req.setResponseParser(new InputStreamResponseParser(null));

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);

      try (InputStream in = (InputStream) res.get(STREAM_KEY)) {
        String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        assertFalse(
            "Should default to Prometheus format when no wt parameter is set",
            output.trim().endsWith("# EOF"));
      }
    }
  }

  @Test
  public void testUnsupportedMetricsFormat() throws Exception {
    var req = new MetricsRequest();

    req.setResponseParser(new InputStreamResponseParser("unknownFormat"));

    try (SolrClient adminClient = getHttpSolrClient(solrTestRule.getBaseUrl())) {
      NamedList<Object> res = adminClient.request(req);
      assertEquals(400, res.get("responseStatus"));
    }
  }
}
