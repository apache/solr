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
package org.apache.solr.util;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleManagedSchemasTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setUpCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "false");
    configureCluster(1).configure();
  }

  @Test
  public void testSameCollectionNameWithMultipleSchemas() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();

    String name = "COLL1";
    String zkPath = ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + name;

    ZkMaintenanceUtils.uploadToZK(cluster.getZkClient(), configset("_default"), zkPath, null);
    // Passing null for the second argument makes this test succeed
    CollectionAdminRequest.createCollection(name, name, 1, 1).process(client);

    // Verify that the config set and collection were created
    ConfigSetAdminRequest.List list = new ConfigSetAdminRequest.List();
    assertTrue(
        "Should have COLL1 config set", list.process(client).getConfigSets().contains("COLL1"));
    assertTrue(
        "Should have created COLL1",
        CollectionAdminRequest.listCollections(client).contains("COLL1"));

    // Delete the config set and collection, and verify
    CollectionAdminRequest.deleteCollection(name).process(client);
    new ConfigSetAdminRequest.Delete().setConfigSetName(name).process(client);

    assertFalse(
        "Should not have COLL1 config set", list.process(client).getConfigSets().contains("COLL1"));
    assertTrue(
        "Should have deleted all collections",
        CollectionAdminRequest.listCollections(client).isEmpty());

    // Upload the replacement config set
    ZkMaintenanceUtils.uploadToZK(cluster.getZkClient(), configset("cloud-managed"), zkPath, null);
    assertTrue(
        "Should have COLL1 config set", list.process(client).getConfigSets().contains("COLL1"));

    // This is the call that fails
    // Passing null for the config name here also lets the test pass!
    CollectionAdminRequest.createCollection(name, name, 1, 1).process(client);
    assertTrue(
        "Should have created COLL1",
        CollectionAdminRequest.listCollections(client).contains("COLL1"));
  }
}
