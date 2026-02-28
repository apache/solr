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
package org.apache.solr.client.solrj.response;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.SystemInfoV2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SystemInfoV2ResponseTest extends SolrCloudTestCase {

  // private static MiniSolrCloudCluster cluster;
  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("config", getFile("solrj/solr/collection1/conf")).configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = cluster.getSolrClient();
  }

  @Test
  public void testDefaultResponse() throws SolrServerException, IOException {
    SystemInfoV2Request req = new SystemInfoV2Request();
    SystemInfoV2Response rsp = req.process(solrClient);

    Assert.assertEquals(1, rsp.getAllNodeResponses().size());
    Assert.assertEquals(1, rsp.getAllCoreRoots().size());
    Assert.assertEquals(1, rsp.getAllModes().size());
  }

  @Test
  @Ignore("AdminHandlersProxy does not support V2.")
  public void testAllNodesResponse() throws SolrServerException, IOException {
    MapSolrParams params = new MapSolrParams(Map.of("nodes", "all"));

    SystemInfoV2Request req = new SystemInfoV2Request(params);
    SystemInfoV2Response rsp = req.process(solrClient);

    try {
      rsp.getNodeResponse();
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(e.getMessage().startsWith("Multiple nodes system info available"));
    }

    Assert.assertEquals(2, rsp.getAllNodeResponses().size());
    Assert.assertEquals(2, rsp.getAllCoreRoots().size());
    Assert.assertEquals(2, rsp.getAllModes().size());

    for (String node : rsp.getAllNodes()) {
      String coreRoot = rsp.getCoreRootForNode(node);
      Assert.assertEquals(node, rsp.getNodeForCoreRoot(coreRoot));
      String solrHome = rsp.getCoreRootForNode(node);
      Assert.assertEquals(node, rsp.getNodeForSolrHome(solrHome));
    }
  }

  @Test
  @Ignore("AdminHandlersProxy does not support V2.")
  public void testResponseForGivenNode() throws SolrServerException, IOException {
    String queryNode = cluster.getJettySolrRunner(0).getNodeName();
    MapSolrParams params = new MapSolrParams(Map.of("nodes", queryNode));

    SystemInfoV2Request req = new SystemInfoV2Request(params);
    SystemInfoV2Response rsp = req.process(solrClient);

    Assert.assertEquals(1, rsp.getAllNodeResponses().size());
    Assert.assertEquals(1, rsp.getAllCoreRoots().size());
    Assert.assertEquals(1, rsp.getAllModes().size());
    String coreRoot = rsp.getCoreRootForNode(queryNode);
    Assert.assertEquals(queryNode, rsp.getNodeForCoreRoot(coreRoot));
    String solrHome = rsp.getCoreRootForNode(queryNode);
    Assert.assertEquals(queryNode, rsp.getNodeForSolrHome(solrHome));
  }
}
