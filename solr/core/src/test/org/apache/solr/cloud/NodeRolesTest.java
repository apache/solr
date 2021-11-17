package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.core.NodeRole;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeRolesTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  @SuppressWarnings("unchecked")
  public void testRoleIntegration() throws Exception {

    JettySolrRunner j0 = cluster.getJettySolrRunner(0);
    JettySolrRunner j1 = null, j2 = null;
    System.setProperty(NodeRole.NODE_ROLE, "overseer");
    try {
      j1 = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRole.NODE_ROLE);
    }

    V2Response rsp = new V2Request
            .Builder("/cluster/node-roles")
            .GET()
            .build()
            .process(cluster.getSolrClient());
    assertEquals(Boolean.FALSE, rsp._get(List.of("node-roles", j1.getNodeName(), NodeRole.HAS_DATA), null));
    assertEquals("overseer", rsp._get(List.of("node-roles", j1.getNodeName(), "role"), null));

    OverseerRolesTest.waitForNewOverseer(20, j1.getNodeName(), false);
    //start another node that is overseer but has data
    System.setProperty(NodeRole.NODE_ROLE, "overseer,data");
    try {
      j2 = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRole.NODE_ROLE);
    }
    rsp = new V2Request
            .Builder("/cluster/node-roles")
            .GET()
            .build()
            .process(cluster.getSolrClient());

    assertEquals(Boolean.TRUE, rsp._get(List.of("node-roles", j2.getNodeName(), NodeRole.HAS_DATA), null));
    assertEquals("overseer", rsp._get(List.of("node-roles", j2.getNodeName(), "role"), null));

    rsp = new V2Request
            .Builder("/cluster/node-roles/overseer")
            .GET()
            .build()
            .process(cluster.getSolrClient());
    List<String> overseerNodes = (List<String>) rsp._get("nodes", null);
    assertTrue(overseerNodes.contains(j1.getNodeName()));
    assertTrue(overseerNodes.contains(j2.getNodeName()));


    String COLLECTION_NAME = "TEST_ROLES";
    CollectionAdminRequest
            .createCollection(COLLECTION_NAME, "conf", 3, 1)
            .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 3, 3);

    String overseer = j1.getNodeName();
    cluster.getSolrClient().getClusterStateProvider().getCollection(COLLECTION_NAME)
            .forEachReplica((s, replica) -> assertNotEquals(replica.node, overseer));

    //now  shutdown the overseer
    j1.stop();
    rsp = new V2Request
            .Builder("/cluster/node-roles")
            .GET()
            .build()
            .process(cluster.getSolrClient());
    assertNull(rsp._get(List.of("node-roles", overseer), null));
  }

}
