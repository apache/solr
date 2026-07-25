package org.apache.solr.handler.admin.api;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.client.solrj.request.SystemApi;
public class GetNodeSystemInfoSolrCloudTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(
            DEFAULT_TEST_COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testGetNodeInfoWithNodesParam() throws Exception {
    String nodeName =
        cluster.getJettySolrRunners()
            .get(0)
            .getCoreContainer()
            .getZkController()
            .getNodeName();

    final var req = new SystemApi.GetNodeSystemInfo();
    req.setNodes(nodeName);

    final var infoRsp = req.process(cluster.getSolrClient());

    assertEquals(0, infoRsp.responseHeader.status);
    assertNotNull(infoRsp.remoteNodeData);
    assertTrue(infoRsp.remoteNodeData.containsKey(nodeName));
  }
}
