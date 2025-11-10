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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreDescriptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateCollectionWithPropertiesTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @Test
  public void testCreateCollectionWithUserDefinedProperties() throws Exception {
    // When creating a collection with user-defined properties
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "testCreateCollectionWithUserDefinedProperties";
    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
        .withProperty("customProp1", "val1")
        .process(cloudClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);

    // Verify that user-defined properties are stored in cluster state
    DocCollection collection = cloudClient.getClusterState().getCollection(collectionName);
    assertEquals("val1", collection.getProperties().get("property.customProp1"));

    // Verify that the core was created with user-defined properties
    Replica replica =
        cloudClient.getClusterState().getCollection(collectionName).getReplicas().get(0);
    CoreDescriptor coreDescriptor =
        cluster
            .getReplicaJetty(replica)
            .getCoreContainer()
            .getCoreDescriptor(replica.getCoreName());
    assertEquals("val1", coreDescriptor.getCoreProperty("customProp1", ""));
  }
}
