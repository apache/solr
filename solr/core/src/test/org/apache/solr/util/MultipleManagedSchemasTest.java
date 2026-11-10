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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.IndexSchemaFactory.VersionedConfig;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultipleManagedSchemasTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setUpCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "false");
    configureCluster(1).configure();
  }

  @Test
  public void testCacheDistinguishesRecreatedZnodesWithSameCreationTime() throws Exception {
    Stat originalStat = new Stat();
    originalStat.setCtime(7);
    originalStat.setCzxid(7);
    Stat recreatedStat = new Stat();
    recreatedStat.setCtime(7);
    recreatedStat.setCzxid(8);
    Stat updatedStat = new Stat();
    updatedStat.setCtime(7);
    updatedStat.setCzxid(8);
    updatedStat.setVersion(1);

    ZkSolrResourceLoader loader = mock(ZkSolrResourceLoader.class);
    when(loader.getZkResourceInfo("solrconfig.xml"))
        .thenReturn(
            new Pair<>("/configs/test/solrconfig.xml", originalStat),
            new Pair<>("/configs/test/solrconfig.xml", originalStat),
            new Pair<>("/configs/test/solrconfig.xml", recreatedStat),
            new Pair<>("/configs/test/solrconfig.xml", recreatedStat),
            new Pair<>("/configs/test/solrconfig.xml", updatedStat));
    VersionedConfig original = new VersionedConfig(0, 7, null);
    VersionedConfig recreated = new VersionedConfig(0, 8, null);
    VersionedConfig updated = new VersionedConfig(1, 8, null);
    try (ObjectCache cache = new ObjectCache()) {
      assertSame(
          original,
          IndexSchemaFactory.getFromCache("solrconfig.xml", loader, () -> cache, () -> original));
      assertSame(
          original,
          IndexSchemaFactory.getFromCache("solrconfig.xml", loader, () -> cache, () -> recreated));
      assertSame(
          recreated,
          IndexSchemaFactory.getFromCache("solrconfig.xml", loader, () -> cache, () -> recreated));
      assertSame(
          recreated,
          IndexSchemaFactory.getFromCache("solrconfig.xml", loader, () -> cache, () -> original));
      assertSame(
          updated,
          IndexSchemaFactory.getFromCache("solrconfig.xml", loader, () -> cache, () -> updated));
    }
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

  @Test
  public void testRecreatedConfigSetIsNotServedFromCache() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String name = "COLL2";
    String zkPath = ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + name;

    // Create a collection with the _default config set, so its solrconfig.xml and schema get cached
    ZkMaintenanceUtils.uploadToZK(cluster.getZkClient(), configset("_default"), zkPath, null);
    CollectionAdminRequest.createCollection(name, name, 1, 1).process(client);
    cluster.waitForActiveCollection(name, 1, 1);

    // Delete the collection and its config set
    CollectionAdminRequest.deleteCollection(name).process(client);
    new ConfigSetAdminRequest.Delete().setConfigSetName(name).process(client);

    // Make a copy of _default with a new request handler and a new field
    Path changed = createTempDir();
    PathUtils.copyDirectory(configset("_default"), changed);
    Path solrConfig = changed.resolve("solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "</config>",
                "<requestHandler name=\"/recreated\" class=\"solr.SearchHandler\"/></config>"));
    Path schema = changed.resolve("managed-schema.xml");
    Files.writeString(
        schema,
        Files.readString(schema)
            .replace(
                "</schema>",
                "<field name=\"recreated_field\" type=\"string\" indexed=\"true\" stored=\"true\"/>"
                    + "</schema>"));

    // Upload the changed copy under the same name
    ZkMaintenanceUtils.uploadToZK(cluster.getZkClient(), changed, zkPath, null);

    // Re-create the collection and check that its core uses the new solrconfig.xml and schema
    CollectionAdminRequest.createCollection(name, name, 1, 1).process(client);
    try {
      cluster.waitForActiveCollection(name, 1, 1);
      CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();
      String coreName =
          cc.getLoadedCoreNames().stream()
              .filter(n -> n.startsWith(name + "_"))
              .findFirst()
              .orElseThrow();
      try (SolrCore core = cc.getCore(coreName)) {
        assertNotNull("new solrconfig.xml was not loaded", core.getRequestHandler("/recreated"));
        assertNotNull(
            "new schema was not loaded", core.getLatestSchema().getFieldOrNull("recreated_field"));
      }
    } finally {
      // Clean up
      CollectionAdminRequest.deleteCollection(name).process(client);
    }
  }
}
