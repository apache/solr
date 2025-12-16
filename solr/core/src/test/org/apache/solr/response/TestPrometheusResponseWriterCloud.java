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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPrometheusResponseWriterCloud extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Before
  public void ensureCollectionsExist() throws Exception {
    SolrClient client = cluster.getSolrClient();
    try {
      CollectionAdminRequest.deleteCollection("collection1").process(client);
    } catch (Exception ignored) {
    }
    try {
      CollectionAdminRequest.deleteCollection("collection2").process(client);
    } catch (Exception ignored) {
    }

    CollectionAdminRequest.createCollection("collection1", "config", 1, 1).process(client);
    CollectionAdminRequest.createCollection("collection2", "config", 1, 1).process(client);
    cluster.waitForActiveCollection("collection1", 1, 1);
    cluster.waitForActiveCollection("collection2", 1, 1);
  }

  @Test
  public void testPrometheusCloudLabels() throws Exception {
    var solrClient = cluster.getSolrClient();

    // Increment solr_core_requests metric for /select
    SolrQuery query = new SolrQuery("*:*");
    solrClient.query("collection1", query);

    var req =
        new GenericSolrRequest(
            METHOD.GET, "/admin/metrics", SolrRequestType.ADMIN, SolrParams.of("wt", "prometheus"));
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    NamedList<Object> resp = solrClient.request(req);
    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(
          "Missing expected Solr cloud mode prometheus metric with cloud labels",
          output
              .lines()
              .anyMatch(
                  line ->
                      line.startsWith("solr_core_requests_total")
                          && line.contains("handler=\"/select\"")
                          && line.contains("collection=\"collection1\"")
                          && line.contains("core=\"collection1_shard1_replica_n1\"")
                          && line.contains("replica_type=\"NRT\"")
                          && line.contains("shard=\"shard1\"")));
    }
  }

  @Test
  public void testCollectionDeletePrometheusOutput() throws Exception {
    var solrClient = cluster.getSolrClient();

    // Increment solr_core_requests metric for /select and assert it exists
    SolrQuery query = new SolrQuery("*:*");
    solrClient.query("collection1", query);
    solrClient.query("collection2", query);

    var req =
        new GenericSolrRequest(
            METHOD.GET, "/admin/metrics", SolrRequestType.ADMIN, SolrParams.of("wt", "prometheus"));
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    NamedList<Object> resp = solrClient.request(req);

    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);

      assertTrue(
          "Prometheus output should contains solr_core_requests for collection1",
          output
              .lines()
              .anyMatch(
                  line -> line.startsWith("solr_core_requests") && line.contains("collection1")));
      assertTrue(
          "Prometheus output should contains solr_core_requests for collection2",
          output
              .lines()
              .anyMatch(
                  line -> line.startsWith("solr_core_requests") && line.contains("collection2")));
    }

    // Delete collection and assert metrics have been removed
    var deleteRequest = CollectionAdminRequest.deleteCollection("collection1");
    deleteRequest.process(solrClient);

    resp = solrClient.request(req);
    try (InputStream in = (InputStream) resp.get("stream")) {
      String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertFalse(
          "Prometheus output should not contain solr_core_requests after collection was deleted",
          output
              .lines()
              .anyMatch(
                  line -> line.startsWith("solr_core_requests") && line.contains("collection1")));
      assertTrue(
          "Prometheus output should contains solr_core_requests for collection2",
          output
              .lines()
              .anyMatch(
                  line -> line.startsWith("solr_core_requests") && line.contains("collection2")));
    }
  }
}
