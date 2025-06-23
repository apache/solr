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

import com.codahale.metrics.SharedMetricRegistries;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPrometheusResponseWriter extends SolrCloudTestCase {
  @ClassRule public static SolrJettyTestRule solrClientTestRule = new SolrJettyTestRule();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final List<String> VALID_PROMETHEUS_VALUES = Arrays.asList("NaN", "+Inf", "-Inf");

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void clearMetricsRegistries() {
    SharedMetricRegistries.clear();
  }

  @Before
  public void ensureCollectionsExist() throws Exception {
    SolrClient client = cluster.getSolrClient();

    // Tear down any previous run
    try {
      CollectionAdminRequest.deleteCollection("collection1").process(client);
    } catch (Exception ignored) {
    }
    try {
      CollectionAdminRequest.deleteCollection("collection2").process(client);
    } catch (Exception ignored) {
    }

    // Recreate both
    CollectionAdminRequest.createCollection("collection1", "config", 1, 1).process(client);
    CollectionAdminRequest.createCollection("collection2", "config", 1, 1).process(client);

    // Wait for both to come up
    cluster.waitForActiveCollection("collection1", 1, 1);
    cluster.waitForActiveCollection("collection2", 1, 1);
  }

  @Test
  public void testPrometheusStructureOutput() throws Exception {
    var solrClient = cluster.getSolrClient();

    // Increment solr_metrics_core_requests metric for /select
    SolrQuery query = new SolrQuery("*:*");
    solrClient.query("collection1", query);
    solrClient.query("collection2", query);

    var req =
        new GenericSolrRequest(
            METHOD.GET,
            "/admin/metrics",
            SolrRequestType.ADMIN,
            new ModifiableSolrParams().set("wt", "prometheus"));
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    NamedList<Object> prometheusResponse = solrClient.request(req);
    assertNotNull("null response from server", prometheusResponse);
    NamedList<Object> res = solrClient.request(req);
    InputStream in = (InputStream) prometheusResponse.get("stream");

    assertNotNull("null response from server", res);
    String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);

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
            .collect(Collectors.toList());
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
          assertTrue(
              "All metrics should start with 'solr_metrics_'",
              actualMetric.startsWith("solr_metrics_"));
          try {
            Float.parseFloat(actualValue);
          } catch (NumberFormatException e) {
            log.warn("Prometheus value not a parsable float");
            assertTrue(VALID_PROMETHEUS_VALUES.contains(actualValue));
          }
        });
  }

  @Test
  public void testCollectionDeletePrometheusOutput() throws Exception {
    var solrClient = cluster.getSolrClient();

    // Increment solr_metrics_core_requests metric for /select and assert it exists
    SolrQuery query = new SolrQuery("*:*");
    solrClient.query("collection1", query);
    solrClient.query("collection2", query);

    var promReq =
        new GenericSolrRequest(
            METHOD.GET,
            "/admin/metrics",
            SolrRequestType.ADMIN,
            new ModifiableSolrParams().set("wt", "prometheus"));
    promReq.setResponseParser(new InputStreamResponseParser("prometheus"));

    NamedList<Object> prometheusResponse = solrClient.request(promReq);
    assertNotNull("null response from server", prometheusResponse);

    InputStream in = (InputStream) prometheusResponse.get("stream");
    String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);

    assertTrue(
        "Prometheus output should contains solr_metrics_core_requests for collection1",
        output
            .lines()
            .anyMatch(
                line ->
                    line.startsWith("solr_metrics_core_requests") && line.contains("collection1")));
    assertTrue(
        "Prometheus output should contains solr_metrics_core_requests for collection2",
        output
            .lines()
            .anyMatch(
                line ->
                    line.startsWith("solr_metrics_core_requests") && line.contains("collection2")));

    // Delete collection and assert metrics have been removed
    var deleteRequest = CollectionAdminRequest.deleteCollection("collection1");
    deleteRequest.process(solrClient);

    prometheusResponse = solrClient.request(promReq);
    assertNotNull("null response from server", prometheusResponse);

    in = (InputStream) prometheusResponse.get("stream");
    output = new String(in.readAllBytes(), StandardCharsets.UTF_8);

    assertFalse(
        "Prometheus output should not contain solr_metrics_core_requests after collection was deleted",
        output
            .lines()
            .anyMatch(
                line ->
                    line.startsWith("solr_metrics_core_requests") && line.contains("collection1")));
    assertTrue(
        "Prometheus output should contains solr_metrics_core_requests for collection2",
        output
            .lines()
            .anyMatch(
                line ->
                    line.startsWith("solr_metrics_core_requests") && line.contains("collection2")));
  }
}
