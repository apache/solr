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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.TreeSet;

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
    j1 = startNodeWithRoles("overseer");

    V2Response rsp = new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());
    assertEquals(List.of("overseer"), rsp._get(List.of("node-roles", j1.getNodeName()), null));

    OverseerRolesTest.waitForNewOverseer(20, j1.getNodeName(), false);

    //start another node that is overseer but has data
    j2 = startNodeWithRoles("overseer,data");
    rsp = new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());
    assertEquals(List.of("data", "overseer"), rsp._get(List.of("node-roles", j2.getNodeName()), null));

    rsp = new V2Request.Builder("/cluster/node-roles/overseer").GET().build().process(cluster.getSolrClient());
    assertEquals(new TreeSet<String>(List.of(j1.getNodeName(), j2.getNodeName())), new TreeSet<String>((List<String>) rsp._get("nodes", null)));

    String COLLECTION_NAME = "TEST_ROLES";
    CollectionAdminRequest
            .createCollection(COLLECTION_NAME, "conf", 3, 1)
            .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 3, 3);

    // Assert that no replica was placed on the dedicated overseer node
    String dedicatedOverseer = j1.getNodeName();
    cluster.getSolrClient().getClusterStateProvider().getCollection(COLLECTION_NAME)
            .forEachReplica((s, replica) -> assertNotEquals(replica.node, dedicatedOverseer));

    // Shutdown the dedicated overseer, make sure that node disappears from the roles output
    j1.stop();
    rsp = new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());
    assertNull(rsp._get(List.of("node-roles", dedicatedOverseer), null));
  }

  private JettySolrRunner startNodeWithRoles(String roles) throws Exception {
    JettySolrRunner jetty;
    System.setProperty(NodeRole.NODE_ROLES_PROP, roles);
    try {
      jetty = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRole.NODE_ROLES_PROP);
    }
    return jetty;
  }

}
