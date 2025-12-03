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
package org.apache.solr.opentelemetry;

import static org.apache.solr.opentelemetry.TestDistributedTracing.getAndClearSpans;
import static org.apache.solr.opentelemetry.TestDistributedTracing.getRootTraceId;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMetricExemplars extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("otel.traces.sampler", "always_on");
    // force early init
    CustomTestOtelTracerConfigurator.prepareForTest();

    configureCluster(1)
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .withTraceIdGenerationDisabled()
        .configure();

    assertNotEquals(
        "Expecting active otel, not noop impl",
        TracerProvider.noop(),
        GlobalOpenTelemetry.get().getTracerProvider());

    CollectionAdminRequest.createCollection("collection1", "config", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("collection1", 1, 1);
  }

  @AfterClass
  public static void afterClass() {
    CustomTestOtelTracerConfigurator.resetForTest();
  }

  @Before
  private void resetSpanData() {
    getAndClearSpans();
  }

  @Test
  public void testOpenMetricExemplars() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // Generate exemplars
    cloudClient.add("collection1", sdoc("id", "1"));
    var spans = getAndClearSpans();
    var expectedTrace = getRootTraceId(spans);

    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET,
            "/admin/metrics",
            SolrRequest.SolrRequestType.ADMIN,
            new ModifiableSolrParams().set("wt", "openmetrics"));
    req.setResponseParser(new InputStreamResponseParser("openmetrics"));
    NamedList<Object> resp = cloudClient.request(req);

    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      var line =
          output
              .lines()
              .filter(
                  l ->
                      l.startsWith("solr_core_requests_total")
                          && l.contains("handler=\"/update\"")
                          && l.contains("collection=\"collection1\""))
              .findFirst()
              .orElseThrow(
                  () -> new AssertionError("Could not find /update solr_core_requests_total"));

      String actualExemplarTrace = line.split("trace_id=\"")[1].split("\"")[0];
      assertEquals(actualExemplarTrace, expectedTrace);
    }
  }
}
