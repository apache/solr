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
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SystemInfoResponseTest extends SolrCloudTestCase {

  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("config", getFile("solrj/solr/collection1/conf"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = cluster.getSolrClient();
  }

  @Test
  public void testAllNodesResponse() throws SolrServerException, IOException {
    MapSolrParams params = new MapSolrParams(Map.of("nodes", "all"));

    SystemInfoRequest req = new SystemInfoRequest(params);
    SystemInfoResponse rsp = req.process(solrClient);

    try {
      rsp.getNodeResponse();
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(e.getMessage().startsWith("Multiple nodes system info available"));
    }

    Assert.assertEquals(2, rsp.getAllNodeResponses().size());
    Assert.assertEquals(2, rsp.getAllCoreRoots().size());
    Assert.assertEquals(2, rsp.getAllModes().size());
  }

  @Test
  public void testResponseForGivenNode() throws SolrServerException, IOException {
    MapSolrParams params = new MapSolrParams(Map.of("nodes", "all"));

    SystemInfoRequest req = new SystemInfoRequest(params);
    SystemInfoResponse rsp = req.process(solrClient);

    for (String node : rsp.getAllNodes()) {
      String coreRoot = rsp.getCoreRootForNode(node);
      Assert.assertEquals(node, rsp.getNodeForCoreRoot(coreRoot));
      String solrHome = rsp.getCoreRootForNode(node);
      Assert.assertEquals(node, rsp.getNodeForSolrHome(solrHome));
    }
  }
}
