package org.apache.solr.servlet;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBucketedRateLimit extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPerf() throws Exception {
    String config =
        "{\n"
            + "  \"rate-limiters\": {\n"
            + "    \"readBuckets\": [\n"
            + "      {\n"
            + "        \"name\": \"test-bucket\",\n"
            + "        \"conditions\": [{\n"
            + "          \"queryParamPattern\": {\n"
            + "            \"q-bucket\": \"test-bucket\"\n"
            + "          }\n"
            + "        }],\n"
            + "        \"allowedRequests\": 2,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      }\n"
            + "\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    System.out.println(config);

    SolrZkClient zkClient = cluster.getZkClient();
    zkClient.atomicUpdate(ZkStateReader.CLUSTER_PROPS, bytes -> config.getBytes());
    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);
    jetty.stop();
    jetty.start(true);
    String COLLECTION_NAME = "rateLimitTest";
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 2)
        .process(cluster.getSolrClient());
    cluster
        .getSolrClient()
        .query(COLLECTION_NAME, new SolrQuery("*:*").add("q-bucket", "test-bucket"));
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
            + "        \"conditions\": [{\n"
            + "          \"queryParamPattern\": {\n"
            + "            \"q\": \".*multijoin.*\"\n"
            + "          }\n"
            + "        }],\n"
            + "        \"allowedRequests\": 5,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"low\",\n"
            + "        \"conditions\": [{\n"
            + "          \"headerPattern\": {\n"
            + "            \"solr_req_priority\": \"20\"\n"
            + "          }\n"
            + "        }],\n"
            + "        \"allowedRequests\": 20,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"global\",\n"
            + "        \"conditions\": [],\n"
            + "        \"allowedRequests\": 50,\n"
            + "        \"slotAcquisitionTimeoutInMS\": 100\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}\n"
            + "\n";
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
