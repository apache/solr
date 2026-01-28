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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test "solr.cloud.delete.unknown.cores.enabled" property that can be used if Solr has an
 * inconsistent state with its cores lifecycle where remnant files are left on disk after various
 * operations that delete a core.
 */
public class DeleteCoreRemnantsOnCreateTest extends SolrCloudTestCase {
  private static final String DELETE_UNKNOWN_CORES_PROP = "solr.cloud.delete.unknown.cores.enabled";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  /**
   * Shared setup for testing collection creation with remnants. Creates a collection, deletes it,
   * and then leaves behind a remnant directory.
   */
  private String setupCollectionRemnant(String collectionName) throws Exception {
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    String primaryNode = jettys.get(0).getNodeName();

    CollectionAdminRequest.Create createRequest =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1);
    createRequest.process(cluster.getSolrClient());

    waitForState(
        "Expected collection to be fully active",
        collectionName,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    Replica primaryReplica = getReplicaOnNode(collectionName, "shard1", primaryNode);
    JettySolrRunner primaryJetty = cluster.getReplicaJetty(primaryReplica);
    String originalCoreName = primaryReplica.getCoreName();
    Path remnantInstanceDir;
    try (SolrCore core = primaryJetty.getCoreContainer().getCore(originalCoreName)) {
      CoreDescriptor cd = core.getCoreDescriptor();
      remnantInstanceDir = cd.getInstanceDir();
    }

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    waitForState("Expected collection deletion", collectionName, (n, c) -> c == null);

    // Simulate a core remnant still exists by creating the directory and core.properties
    Files.createDirectories(remnantInstanceDir);
    String propertiesContent = "";
    Files.writeString(
        remnantInstanceDir.resolve("core.properties"), propertiesContent, StandardCharsets.UTF_8);

    return originalCoreName;
  }

  /**
   * Shared setup for testing replica addition with remnants. Creates a collection, then simulates a
   * remnant directory on the single node that will impact the next addReplica command.
   */
  private void setupReplicaRemnant(String collectionName) throws Exception {
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    String primaryNode = jettys.get(0).getNodeName();

    CollectionAdminRequest.Create createRequest =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1);
    createRequest.process(cluster.getSolrClient());

    waitForState(
        "Expected collection to be fully active",
        collectionName,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    int nextReplicaIndex = 3; // Yep, from 1 to 3 due to how we count in ZK and setup.
    String expectedNewReplicaName = collectionName + "_shard1_replica_n" + nextReplicaIndex;

    // Simulate a core remnant on the single node adjacent to the existing replica instance path
    Replica existing = getReplicaOnNode(collectionName, "shard1", primaryNode);
    try (SolrCore core =
        cluster.getReplicaJetty(existing).getCoreContainer().getCore(existing.getCoreName())) {
      Path siblingDir = core.getInstancePath().getParent().resolve(expectedNewReplicaName);
      Files.createDirectories(siblingDir);
      Files.writeString(
          siblingDir.resolve("core.properties"),
          "name="
              + expectedNewReplicaName
              + "_remnant\n"
              + "collection="
              + collectionName
              + "_remnant\n"
              + "shard=shard1\n"
              + "coreNodeName=core_node_remnant\n",
          StandardCharsets.UTF_8);
    }
  }

  /**
   * Shared setup for testing DeleteCore admin API with remnants. Creates a collection, deletes it,
   * and then leaves behind a remnant core directory.
   */
  private String setupCoreRemnantForUnloadCoreOperation(String collectionName) throws Exception {
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    String primaryNode = jettys.get(0).getNodeName();

    CollectionAdminRequest.Create createRequest =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1);
    createRequest.process(cluster.getSolrClient());

    waitForState(
        "Expected collection to be fully active",
        collectionName,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    Replica primaryReplica = getReplicaOnNode(collectionName, "shard1", primaryNode);
    JettySolrRunner primaryJetty = cluster.getReplicaJetty(primaryReplica);
    String originalCoreName = primaryReplica.getCoreName();
    Path remnantInstanceDir;
    try (SolrCore core = primaryJetty.getCoreContainer().getCore(originalCoreName)) {
      CoreDescriptor cd = core.getCoreDescriptor();
      remnantInstanceDir = cd.getInstanceDir();
    }

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    waitForState("Expected collection deletion", collectionName, (n, c) -> c == null);

    // Simulate a core remnant still exists by creating the directory and core.properties
    Files.createDirectories(remnantInstanceDir);
    Files.writeString(
        remnantInstanceDir.resolve("core.properties"),
        "name=" + originalCoreName + "\n",
        StandardCharsets.UTF_8);

    return originalCoreName;
  }

  @Test
  public void testCreateCollectionWithRemnantsFailsWithoutSetting() throws Exception {
    assertNull(
        "Property should not be set by default", System.getProperty(DELETE_UNKNOWN_CORES_PROP));

    String collectionName = "coreRemnantCreateNoSetting";
    setupCollectionRemnant(collectionName);

    // Try to create the collection again - this demonstrates the behavior without the setting
    // In typical environments, this might fail, but behavior depends on configuration
    CollectionAdminRequest.Create recreateRequest =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1);

    // The request to create a collection SHOULD fail based on the remnant file, if it does not it
    // means we've changed Solr's behavior when creating a core and
    // remnants exist, and therefore we should rethink the utility of this setting.
    Exception e =
        assertThrows(
            "This request to recreate the collection should have failed due to remnant files.",
            Exception.class,
            () -> recreateRequest.process(cluster.getSolrClient()));

    assertTrue(
        "Verify the exception was due to core creation failed.",
        e.getMessage().contains("Underlying core creation failed"));
  }

  @Test
  public void testCreateCollectionWithRemnantsWithSetting() throws Exception {
    System.setProperty(DELETE_UNKNOWN_CORES_PROP, "true");

    String collectionName = "coreRemnantCreateWithSetting";
    setupCollectionRemnant(collectionName);

    // With the setting enabled, collection creation should succeed despite remnants
    CollectionAdminRequest.Create recreateRequest =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1);
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    recreateRequest.process(cluster.getSolrClient());

    waitForState(
        "Expected recreated collection to be fully active",
        collectionName,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    // Verify collection was created successfully
    DocCollection collection = getCollectionState(collectionName);
    assertNotNull("Collection should exist", collection);
    assertEquals("Should have 1 replica", 1, collection.getReplicas().size());

    // Verify replica on the node where we had the remnant is active
    Replica recreatedReplica =
        getReplicaOnNode(collectionName, "shard1", jettys.get(0).getNodeName());
    assertNotNull("Should have a replica on the primary node", recreatedReplica);
    assertEquals("Replica should be active", Replica.State.ACTIVE, recreatedReplica.getState());
  }

  @Test
  public void testAddReplicaWithRemnantFailsWithoutSetting() throws Exception {
    assertNull(
        "Property should not be set by default", System.getProperty(DELETE_UNKNOWN_CORES_PROP));

    String collectionName = "coreRemnantAddReplicaNoSetting";
    setupReplicaRemnant(collectionName);

    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    String primaryNode = jettys.get(0).getNodeName();

    // Try to add a new replica - this demonstrates the behavior without the setting
    CollectionAdminRequest.AddReplica addReplicaRequest =
        CollectionAdminRequest.addReplicaToShard(collectionName, "shard1");
    addReplicaRequest.setNode(primaryNode);

    Exception e =
        assertThrows(
            "This request to add a replica to the collection should have failed due to remnant files.",
            Exception.class,
            () -> addReplicaRequest.process(cluster.getSolrClient()));

    assertTrue(
        "Verify the exception was due to core creation failed.",
        e.getMessage().contains("ADDREPLICA failed to create replica"));
  }

  @Test
  public void testAddReplicaWithRemnantWithSetting() throws Exception {
    System.setProperty(DELETE_UNKNOWN_CORES_PROP, "true");

    String collectionName = "coreRemnantAddReplicaWithSetting";
    setupReplicaRemnant(collectionName);

    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    String primaryNode = jettys.get(0).getNodeName();

    // With the setting enabled, replica addition should succeed despite remnants
    CollectionAdminRequest.AddReplica addReplicaRequest =
        CollectionAdminRequest.addReplicaToShard(collectionName, "shard1");
    addReplicaRequest.setNode(primaryNode);
    addReplicaRequest.process(cluster.getSolrClient());

    waitForState(
        "Expected replica addition to finish",
        collectionName,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 2));

    // Verify collection now has 2 replicas
    DocCollection collection = getCollectionState(collectionName);
    assertNotNull("Collection should exist", collection);
    assertEquals("Should have 2 replicas after adding", 2, collection.getReplicas().size());

    // Verify the replica was added on the single node and is active
    Replica addedReplica = getReplicaOnNode(collectionName, "shard1", primaryNode);
    assertNotNull("Should have added a replica on the primary node", addedReplica);
    assertEquals("Added replica should be active", Replica.State.ACTIVE, addedReplica.getState());
  }

  /**
   * This test demonstrates that you can't call the direct core unload admin operation to get rid of
   * a remnant core, not because of the existence of the remnant, but because the core no longer has
   * a CoreDescriptor record in ZooKeeper.
   */
  @Test
  public void testDeleteCoreFailsWhenUnknown() throws Exception {

    String collectionName = "coreRemnantDelete";
    String coreName = setupCollectionRemnant(collectionName);

    // Try to delete a core that only exists as a remnant - and has no record in ZooKeeper
    CoreAdminRequest.Unload unloadRequest = new CoreAdminRequest.Unload(true);
    unloadRequest.setDeleteIndex(true);
    unloadRequest.setDeleteDataDir(true);
    unloadRequest.setDeleteInstanceDir(true);
    unloadRequest.setCoreName(coreName);
    unloadRequest.setResponseParser(new JsonMapResponseParser());

    Exception e =
        assertThrows(
            "Expected request to fail.",
            Exception.class,
            () -> cluster.getSolrClient().request(unloadRequest));

    assertTrue(
        "Verify the exception was due to ZK not knowing about the core existence.",
        e.getMessage().contains("Cannot unload non-existent core [" + coreName + "]"));
  }

  private Replica getReplicaOnNode(String collectionName, String shard, String nodeName) {
    DocCollection collectionState = getCollectionState(collectionName);
    Slice slice = collectionState.getSlice(shard);
    Optional<Replica> replica =
        slice.getReplicas().stream().filter(r -> nodeName.equals(r.getNodeName())).findFirst();
    return replica.orElseThrow(
        () -> new AssertionError("No replica found on node " + nodeName + " for " + shard));
  }
}
