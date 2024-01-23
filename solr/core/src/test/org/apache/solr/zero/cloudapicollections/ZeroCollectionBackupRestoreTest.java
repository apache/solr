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

package org.apache.solr.zero.cloudapicollections;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.process.ZeroStoreSolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZeroCollectionBackupRestoreTest extends ZeroStoreSolrCloudTestCase {

  // cluster details
  private static String backupLocation;
  private static ZeroMetadataController shardMetadataController;

  @BeforeClass
  public static void setupCluster() throws Exception {

    int numNodes = random().nextInt(4) + 2;

    // So we can take a backup in a temporary folder (out of Solr home)
    // Must be done before creating the cluster
    backupLocation = createTempDir("backup").toAbsolutePath().toString();
    System.setProperty("solr.allowPaths", backupLocation);

    // Add local backup repository and Zero store backup repository to default solr.xml
    // configuration
    String local =
        "<repository  name=\"local\" "
            + "class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\">"
            + "    </repository>";

    String zeroRepoLocal =
        "<repositories>"
            + "  <repository\n"
            + "    name=\"local\""
            + "    class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\""
            + "    default=\"true\">"
            + "    <str name=\"location\">"
            + zeroStoreDir
            + "</str>"
            + "  </repository>"
            + "</repositories>";

    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
            "</solr>",
            "<backup>" + local + "</backup>" + "<zero>" + zeroRepoLocal + "</zero>" + "</solr>");

    setupCluster(numNodes, solrXml);

    SolrCloudManager cloudManager =
        cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    shardMetadataController = new ZeroMetadataController(cloudManager);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solr.allowPaths");

    // clean static variables
    backupLocation = null;
    shardMetadataController = null;
  }

  @Test
  public void testZeroCollectionSplitThenBackupRestore() throws Exception {
    String collection = "split";
    int numShards = 2;
    int numReplicas = 2;

    // setup a Zero collection
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collection, numShards, 0)
            .setZeroIndex(true)
            .setZeroReplicas(numReplicas);
    create.process(cluster.getSolrClient());
    waitForState(
        "Waiting for collection to be created",
        collection,
        clusterShape(numShards, numReplicas * numShards));

    // Split first shard. Then wait we have more replicas
    CollectionAdminRequest.SplitShard split =
        CollectionAdminRequest.splitShard(collection).setShardName("shard1");
    assertEquals(0, split.process(cluster.getSolrClient()).getStatus());
    waitForState(
        "Waiting for split",
        collection,
        activeClusterShape(numShards + 1, numShards * numReplicas + 2));

    doBackupAndTestRestores(collection, numReplicas, true);
  }

  /**
   * Tests a Zero collection can be backed up then restored as a ZERO collection or as a "normal"
   * non ZERO collection
   */
  @Test
  public void testBackupRestoreZeroCollection() throws Exception {
    testBackupRestoreCollection(true);
  }

  /**
   * Tests a non-Zero collection can be backed up then restored as a ZERO collection or as a
   * "normal" non ZERO collection
   */
  @Test
  public void testBackupRestoreNonZeroCollection() throws Exception {
    testBackupRestoreCollection(false);
  }

  private void testBackupRestoreCollection(boolean isZero) throws Exception {
    String collection = isZero ? "zeroCol" : "normalCol";
    int numCreatedShards = random().nextInt(2) + 2;
    int numCreatedReplicas = random().nextInt(2) + 2;

    // set up a collection of the requested type (ZERO or not)
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collection, numCreatedShards, 0);
    if (isZero) {
      create.setZeroReplicas(numCreatedReplicas).setZeroIndex(true);
    } else {
      // Not calling setZeroIndex(false) because existing code doesn't do that
      create.setNrtReplicas(numCreatedReplicas);
    }
    create.process(cluster.getSolrClient());
    waitForState(
        "Waiting for collection to be created",
        collection,
        clusterShape(numCreatedShards, numCreatedShards * numCreatedReplicas));

    doBackupAndTestRestores(collection, numCreatedReplicas, isZero);
  }

  /**
   * Restore a backup twice and verify both restores succeed. The first restore is done into a Zero
   * collection, the second into a non Zero collection.
   */
  private void doBackupAndTestRestores(
      String collection, int numReplicasInCollection, boolean backupIsZero) throws Exception {

    CloudSolrClient client = cluster.getSolrClient();

    // index some docs, so we can check they are restored
    int numDocs = random().nextInt(10) + 10;
    indexDocs(collection, numDocs);

    // Trigger backup command
    String backupName = collection + "_backup";
    CollectionAdminRequest.Backup backup =
        CollectionAdminRequest.backupCollection(collection, backupName)
            .setLocation(backupLocation)
            .setRepositoryName("local");
    assertEquals(0, backup.process(client).getStatus());

    // Triggering Restore command, restoring into a Zero collection
    testRestore(
        backupName,
        backupIsZero,
        numReplicasInCollection,
        numDocs,
        collection + "_restoredZero",
        true);
    // Now restore same backup into a non Zero collection
    testRestore(
        backupName,
        backupIsZero,
        numReplicasInCollection,
        numDocs,
        collection + "_restoredNonZero",
        false);
  }

  private void testRestore(
      String backupName,
      boolean backupIsZero,
      int backupReplicas,
      int backupDocs,
      String restoreCollectionName,
      boolean restoreAsZero)
      throws Exception {
    CloudSolrClient client = cluster.getSolrClient();

    CollectionAdminRequest.Restore restore =
        CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
            .setLocation(backupLocation)
            .setZeroIndex(restoreAsZero)
            .setRepositoryName("local");
    assertEquals(0, restore.process(client).getStatus());

    // Check that we restored into a ZERO or non ZERO collection as requested
    DocCollection restoredCollection =
        cluster.getZkStateReader().getClusterState().getCollection(restoreCollectionName);
    assertEquals(restoreAsZero, restoredCollection.isZeroIndex());

    // The number of restored replicas depends on the type of backed up collection and the type of
    // the restored collection. When the two types are identical, we should get the same number of
    // replicas. When they are different, the restored collection should have two replicas
    // (See RestoreCmd.RestoreOnANewCollection).
    // This test does not test that the number of desired replicas can be passed in the restore
    // request.
    int replicasInRestoredCollection =
        restoredCollection.getNumZeroReplicas() + restoredCollection.getNumNrtReplicas();
    assertEquals(backupIsZero == restoreAsZero ? backupReplicas : 2, replicasInRestoredCollection);

    // for each shard in the collection, ensure we do have the 'metadataSuffix' ZK node if restored
    // as a ZERO collection, and check we don't if restored as NON ZERO collection.
    for (Slice shard : restoredCollection.getSlices()) {
      String shardName = shard.getName();
      if (restoreAsZero) {
        ZeroMetadataVersion metadata =
            shardMetadataController.readMetadataValue(restoreCollectionName, shardName);
        String suffix = metadata.getMetadataSuffix();

        // should be a valid UUID (other, parsing will fail)
        assertFalse(StrUtils.isNullOrEmpty(suffix));
        assertNotNull(UUID.fromString(suffix));
      } else {
        try {
          shardMetadataController.readMetadataValue(restoreCollectionName, shardName);
          fail("collection " + restoreCollectionName + " should not have a metadata suffix in ZK");
        } catch (SolrException se) {
          assertEquals(NoSuchElementException.class, se.getCause().getClass());
        }
      }
    }

    // Check we have the same number of docs in the restored collection
    int restoredDocs = queryDocs(restoreCollectionName);
    assertEquals(backupDocs, restoredDocs);
  }

  /** Index some docs in the collection. All IDs are unique. */
  private void indexDocs(String collectionName, int numDocs) throws Exception {

    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", UUID.randomUUID().toString());
      docs.add(doc);
    }

    CloudSolrClient client = cluster.getSolrClient();
    client.add(collectionName, docs);
    client.commit(collectionName);
  }

  /** Query to count the number of records in the collection. */
  private int queryDocs(String collectionName) throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = client.query(collectionName, query);
    return (int) response.getResults().getNumFound();
  }
}
