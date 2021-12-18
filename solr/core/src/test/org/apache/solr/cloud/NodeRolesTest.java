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
import java.util.Collection;
import java.util.Collections;

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.core.NodeRoles;
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
    V2Response rsp = new V2Request.Builder("/cluster/node-roles/supported").GET().build().process(cluster.getSolrClient());
    Map<String, Object> l = (Map<String, Object>) rsp._get("supported-roles", Collections.emptyMap());
    assertTrue(l.containsKey("data"));
    assertTrue(l.containsKey("overseer"));

    j1 = startNodeWithRoles("overseer:preferred,data:off");

    rsp = new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());
    assertEquals(j1.getNodeName(), rsp._getStr("node-roles/overseer/preferred[0]", null));
    assertEquals(j1.getNodeName(), rsp._getStr("node-roles/data/off[0]", null));
    OverseerRolesTest.waitForNewOverseer(20, j1.getNodeName(), false);

    // start another node that is overseer but has data
    j2 = startNodeWithRoles("overseer:preferred,data:on");
    rsp = new V2Request.Builder("/cluster/node-roles").GET().build().process(cluster.getSolrClient());

    assertTrue( ((Collection)rsp._get("node-roles/overseer/preferred", Collections.emptyList())).contains(j2.getNodeName()));
    assertTrue( ((Collection)rsp._get("node-roles/data/on", Collections.emptyList())).contains(j2.getNodeName()));

    rsp = new V2Request.Builder("/cluster/node-roles/overseer").GET().build().process(cluster.getSolrClient());

    assertTrue( ((Collection)rsp._get("node-roles/overseer/preferred", Collections.emptyList())).contains(j2.getNodeName()));
    assertTrue( ((Collection)rsp._get("node-roles/overseer/preferred", Collections.emptyList())).contains(j1.getNodeName()));

    rsp = new V2Request.Builder("/cluster/node-roles/overseer/preferred").GET().build().process(cluster.getSolrClient());
    assertTrue( ((Collection)rsp._get("node-roles/overseer/preferred", Collections.emptyList())).contains(j2.getNodeName()));
    assertTrue( ((Collection)rsp._get("node-roles/overseer/preferred", Collections.emptyList())).contains(j1.getNodeName()));

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
    rsp = new V2Request.Builder("/cluster/node-roles/overseer/preferred").GET().build().process(cluster.getSolrClient());
    assertFalse (((Collection) rsp._get("node-roles/overseer/preferred" , null)). contains(j1.getNodeName()));
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
