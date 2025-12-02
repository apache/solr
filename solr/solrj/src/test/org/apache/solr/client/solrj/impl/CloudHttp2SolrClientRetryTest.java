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

package org.apache.solr.client.solrj.impl;

import static org.apache.solr.util.SolrJMetricTestUtils.getPrometheusMetricValue;

import java.util.Collections;
import java.util.Optional;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudHttp2SolrClientRetryTest extends SolrCloudTestCase {
  private static final int NODE_COUNT = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODE_COUNT)
        .addConfig(
            "conf",
            getFile("solrj")
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .configure();
  }

  @Test
  public void testRetry() throws Exception {

    // Randomly decide to use either the Jetty Http Client or the JDK Http Client
    var jettyClientBuilder = new HttpJettySolrClient.Builder();

    // forcing Http/1.1 to avoid an extra HEAD request with the first update.
    // (This causes the counts to be 1 greater than what we test for here.)
    var jdkClientBuilder =
        new HttpJdkSolrClient.Builder()
            .useHttp1_1(true)
            .withSSLContext(MockTrustManager.ALL_TRUSTING_SSL_CONTEXT);

    var cloudSolrclientBuilder =
        new CloudSolrClient.Builder(
            Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty());
    cloudSolrclientBuilder.withHttpClientBuilder(
        random().nextBoolean() ? jettyClientBuilder : jdkClientBuilder);

    try (CloudSolrClient solrClient = cloudSolrclientBuilder.build()) {
      String collectionName = "testRetry";
      String prometheusMetric =
          "solr_core_requests_total{category=\"UPDATE\",collection=\"testRetry\",core=\"testRetry_shard1_replica_n1\",handler=\"/update\",otel_scope_name=\"org.apache.solr\",replica_type=\"NRT\",shard=\"shard1\"}";

      CollectionAdminRequest.createCollection(collectionName, 1, 1).process(solrClient);

      solrClient.add(collectionName, new SolrInputDocument("id", "1"));

      assertEquals(1.0, getPrometheusMetricValue(solrClient, prometheusMetric), 0.0);

      TestInjection.failUpdateRequests = "true:100";
      try {
        expectThrows(
            CloudSolrClient.RouteException.class,
            "Expected an exception on the client when failure is injected during updates",
            () -> {
              solrClient.add(collectionName, new SolrInputDocument("id", "2"));
            });
      } finally {
        TestInjection.reset();
      }

      assertEquals(2.0, getPrometheusMetricValue(solrClient, prometheusMetric), 0.0);
    }
  }
}
