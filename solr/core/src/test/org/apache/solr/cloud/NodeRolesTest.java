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

package org.apache.solr.cloud;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeRolesTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testRoleIntegration() throws Exception {
    JettySolrRunner j0 = cluster.getJettySolrRunner(0);
    testSupportedRolesAPI();

    // Start a dedicated overseer node
    JettySolrRunner j1 = startNodeWithRoles("overseer:preferred,data:off");
    validateNodeRoles(
        j1.getNodeName(), "node-roles/overseer/preferred", j1.getNodeName(), "node-roles/data/off");

    V2Response rsp;
    OverseerRolesTest.waitForNewOverseer(20, j1.getNodeName(), true);

    // Start another node that is allowed or preferred overseer but has data
    String overseerModeOnDataNode = random().nextBoolean() ? "preferred" : "allowed";
    JettySolrRunner j2 = startNodeWithRoles("overseer:" + overseerModeOnDataNode + ",data:on");
    validateNodeRoles(
        j2.getNodeName(),
        "node-roles/overseer/" + overseerModeOnDataNode,
        j2.getNodeName(),
        "node-roles/data/on");

    // validate the preferred overseers
    validateNodeRoles(
        j2.getNodeName(),
        "node-roles/overseer/" + overseerModeOnDataNode,
        j1.getNodeName(),
        "node-roles/overseer/preferred");

    String COLLECTION_NAME = "TEST_ROLES";
    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 3, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 3, 3);

    // Assert that no replica was placed on the dedicated overseer node
    String dedicatedOverseer = j1.getNodeName();
    cluster
        .getSolrClient()
        .getClusterStateProvider()
        .getCollection(COLLECTION_NAME)
        .forEachReplica((s, replica) -> assertNotEquals(replica.node, dedicatedOverseer));

    // Shutdown the dedicated overseer, make sure that node disappears from the roles output
    j1.stop();

    // Wait and make sure that another node picks up overseer responsibilities
    OverseerRolesTest.waitForNewOverseer(20, it -> !dedicatedOverseer.equals(it), false);

    // Make sure the stopped node no longer has the role assigned
    rsp =
        new V2Request.Builder("/cluster/node-roles/role/overseer/" + overseerModeOnDataNode)
            .GET()
            .build()
            .process(cluster.getSolrClient());
    assertFalse(
        ((Collection) rsp._get("node-roles/overseer/" + overseerModeOnDataNode, null))
            .contains(j1.getNodeName()));
  }

  @SuppressWarnings("rawtypes")
  private void validateNodeRoles(String... nodenamePaths)
      throws org.apache.solr.client.solrj.SolrServerException, java.io.IOException {
    V2Response rsp =
        new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());
    for (int i = 0; i < nodenamePaths.length; i += 2) {
      String nodename = nodenamePaths[i];
      String path = nodenamePaths[i + 1];
      assertTrue(
          "Didn't find " + nodename + " at " + path + ". Full response: " + rsp.jsonStr(),
          ((Collection) rsp._get(path, Collections.emptyList())).contains(nodename));
    }
  }

  @SuppressWarnings("unchecked")
  private void testSupportedRolesAPI() throws Exception {
    V2Response rsp =
        new V2Request.Builder("/cluster/node-roles/supported")
            .GET()
            .build()
            .process(cluster.getSolrClient());
    Map<String, Object> l =
        (Map<String, Object>) rsp._get("supported-roles", Collections.emptyMap());
    assertTrue(l.containsKey("data"));
    assertTrue(l.containsKey("overseer"));
  }

  private JettySolrRunner startNodeWithRoles(String roles) throws Exception {
    JettySolrRunner jetty;
    System.setProperty(NodeRoles.NODE_ROLES_PROP, roles);
    try {
      jetty = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }
    return jetty;
  }
}
