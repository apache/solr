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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.OK;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the node-health API, on SolrCloud clusters. Failure scenarios (ZK connection loss,
 * missing live node, unhealthy cores) are covered by the mock-based {@link NodeHealthTest}.
 *
 * @see NodeHealthTest
 * @see NodeHealthStandaloneTest
 */
public class NodeHealthSolrCloudTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();

    CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testHealthyNodeReturnsOkStatus() throws Exception {
    final var request = new NodeApi.Healthcheck();
    final var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNull("Expected no error on a healthy node", response.error);
  }

  @Test
  public void testRequireHealthyCoresReturnOkWhenAllCoresHealthy() throws Exception {
    final var request = new NodeApi.Healthcheck();
    request.setRequireHealthyCores(true);
    final var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertEquals("All cores are healthy", response.message);
  }
}
