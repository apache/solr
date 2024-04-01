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

package org.apache.solr.servlet;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

/** A test for bucketed rate limiting */
public class TestBucketedRateLimit extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testPerf() throws Exception {
    String config =
        "{\n"
            + "  \"rate-limiters\": {\n"
            + "    \"readBuckets\": [\n"
            + "      {\n"
            + "        \"name\": \"test-bucket\",\n"
            + "        \"header\": {\"q-bucket\": \"test-bucket\"},\n"
            + "        \"allowedRequests\": 2,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    SolrZkClient zkClient = cluster.getZkClient();
    zkClient.atomicUpdate(
        ZkStateReader.CLUSTER_PROPS, bytes -> config.getBytes(StandardCharsets.UTF_8));
    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);
    jetty.stop();
    jetty.start(true);
    String COLLECTION_NAME = "rateLimitTest";
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 2)
        .process(cluster.getSolrClient());
    CloudSolrClient solrClient = cluster.getSolrClient();
    GenericSolrRequest gsr =
        new GenericSolrRequest(
                SolrRequest.METHOD.GET, "/select", new MapSolrParams(Map.of("q", "*:*")))
            .setRequiresCollection(true);
    gsr.addHeader("q-bucket", "test-bucket");
    gsr.addHeader("Solr-Request-Type", "QUERY");
    SimpleSolrResponse r = gsr.process(solrClient, COLLECTION_NAME);

    NavigableObject rsp =
        cluster
            .getSolrClient()
            .request(
                new GenericSolrRequest(
                    SolrRequest.METHOD.GET,
                    "/admin/metrics",
                    new MapSolrParams(
                        Map.of("key", "solr.node:CONTAINER.bucketedQueryRateLimiter"))));

    assertEquals(
        "0",
        rsp._getStr(
            List.of(
                "metrics",
                "solr.node:CONTAINER.bucketedQueryRateLimiter",
                "test-bucket",
                "queueLength"),
            null));
    assertEquals(
        "2",
        rsp._getStr(
            List.of(
                "metrics",
                "solr.node:CONTAINER.bucketedQueryRateLimiter",
                "test-bucket",
                "available"),
            null));
    assertEquals(
        "1",
        rsp._getStr(
            List.of(
                "metrics", "solr.node:CONTAINER.bucketedQueryRateLimiter", "test-bucket", "tries"),
            null));
    assertEquals(
        "1",
        rsp._getStr(
            List.of(
                "metrics",
                "solr.node:CONTAINER.bucketedQueryRateLimiter",
                "test-bucket",
                "success"),
            null));
    assertEquals(
        "0",
        rsp._getStr(
            List.of(
                "metrics", "solr.node:CONTAINER.bucketedQueryRateLimiter", "test-bucket", "fails"),
            null));
  }

  public void testConfig() throws Exception {
    String config =
        "{\n"
            + "  \"rate-limiters\": {\n"
            + "    \"readBuckets\": [\n"
            + "      {\n"
            + "        \"name\": \"expensive\",\n"
            + "        \"header\": {\"solr_req_priority\": \"5\"},\n"
            + "        \"allowedRequests\": 5,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"low\",\n"
            + "        \"header\": {\"solr_req_priority\": \"20\"},\n"
            + "        \"allowedRequests\": 20,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"global\",\n"
            + "        \"header\": null,\n"
            + "        \"allowedRequests\": 50,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    RateLimitManager mgr =
        new RateLimitManager.Builder(
                () ->
                    new SolrZkClient.NodeData(new Stat(), config.getBytes(StandardCharsets.UTF_8)))
            .build();
    RequestRateLimiter rl = mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);
    assertTrue(rl instanceof BucketedQueryRateLimiter);
    BucketedQueryRateLimiter brl = (BucketedQueryRateLimiter) rl;
    assertEquals(3, brl.buckets.size());

    RequestRateLimiter.SlotMetadata smd =
        rl.handleRequest(
            new RequestRateLimiter.RequestWrapper() {
              @Override
              public String getParameter(String name) {
                return null;
              }

              @Override
              public String getHeader(String name) {
                if (name.equals("solr_req_priority")) return "20";
                else return null;
              }
            });

    // star
    assertEquals(19, smd.usedPool.availablePermits());
    smd.decrementRequest();
    assertEquals(20, smd.usedPool.availablePermits());
  }
}
