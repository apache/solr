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

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.AliasesApi;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link DeleteAlias} using the V2 API */
public class DeleteAliasAPITest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  @Test
  public void testDeleteAlias() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "deletealiastest_coll";
    String aliasName = "deletealiastest_alias";

    // Create a collection
    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1).process(cloudClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);

    // Create an alias pointing to the collection
    CollectionAdminRequest.createAlias(aliasName, collectionName).process(cloudClient);

    // Verify the alias exists
    var clusterStateProvider = cloudClient.getClusterStateProvider();
    assertEquals(
        "Alias should exist before deletion",
        collectionName,
        clusterStateProvider.resolveSimpleAlias(aliasName));

    // Delete the alias using the V2 API
    var request = new AliasesApi.DeleteAlias(aliasName);
    SubResponseAccumulatingJerseyResponse response = request.process(cloudClient);
    assertNotNull(response);
    assertNull("Expected request to not fail", response.failedSubResponsesByNodeName);

    // Verify the alias is gone
    ZkStateReader.AliasesManager aliasesManager =
        ((ZkClientClusterStateProvider) clusterStateProvider)
            .getZkStateReader()
            .getAliasesManager();
    aliasesManager.update();
    assertFalse(
        "Alias should not exist after deletion", aliasesManager.getAliases().hasAlias(aliasName));
  }

  @Test
  public void testDeleteAliasAsync() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "deletealiastest_coll_async";
    String aliasName = "deletealiastest_alias_async";

    // Create a collection
    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1).process(cloudClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);

    // Create an alias pointing to the collection
    CollectionAdminRequest.createAlias(aliasName, collectionName).process(cloudClient);

    // Verify the alias exists
    var clusterStateProvider = cloudClient.getClusterStateProvider();

    // Delete the alias using the V2 API with async
    String asyncId = "deleteAlias001";
    var request = new AliasesApi.DeleteAlias(aliasName);
    request.setAsync(asyncId);
    var response = request.process(cloudClient);
    assertNotNull(response);
    assertNull("Expected request start to not fail", response.failedSubResponsesByNodeName);

    // Wait for the async request to complete
    CollectionAdminRequest.RequestStatusResponse rsp =
        waitForAsyncClusterRequest(asyncId, Duration.ofSeconds(5));

    assertEquals(
        "Expected async request to complete successfully",
        RequestStatusState.COMPLETED,
        rsp.getRequestStatus());

    // Verify the alias is gone
    ZkStateReader.AliasesManager aliasesManager =
        ((ZkClientClusterStateProvider) clusterStateProvider)
            .getZkStateReader()
            .getAliasesManager();
    aliasesManager.update();
    assertFalse(
        "Alias should not exist after deletion", aliasesManager.getAliases().hasAlias(aliasName));
  }
}
