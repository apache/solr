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
package org.apache.solr.zero.process;

import static org.apache.solr.zero.process.CorePullStatus.ZERO_METADATA_MISSING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.exception.CorruptedCoreException;
import org.apache.solr.zero.metadata.LocalCoreMetadata;
import org.apache.solr.zero.metadata.MetadataComparisonResult;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests for {@link CorePuller} */
public class CorePullerTest extends ZeroStoreSolrCloudTestCase {

  private String collectionName;
  private String shardName;
  private Replica newReplica;

  private static final DeleteProcessor deleteProcessor = Mockito.mock(DeleteProcessor.class);

  private static ZeroStoreClient zeroStoreClient;

  @BeforeClass
  public static void setupClass() throws Exception {
    assumeWorkingMockito();
    setupCluster(2);

    zeroStoreClient = Mockito.spy(setupLocalZeroStoreClient(new ZeroConfig()));

    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
      manager.replaceZeroStoreClient(zeroStoreClient);
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    shutdownCluster();
    zeroStoreClient.shutdown();
  }

  @Before
  public void resetZeroStoreClient() {
    // We keep the same spy for all test method. Reset the recorded
    // integration before next test execution
    Mockito.reset(zeroStoreClient);
  }

  /**
   * testSyncLocalCoreWithZeroStore_syncSkipOnDefault checks that syncLocalCoreWithZeroStore will
   * skip sync if metadataSuffix is set to default in the ZK.
   */
  @Test
  public void testSyncLocalCoreWithZeroStore_syncSkipOnDefault() throws Exception {

    collectionName = "zeroCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupZeroCollectionWithShardNames(collectionName, 1, shardName);

    ClusterState clusterState = cluster.getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    CoreContainer cc = getCoreContainer(newReplica.getNodeName());
    try (SolrCore core = cc.getCore(newReplica.getCoreName())) {
      ZeroStoreManager storeManager = cc.getZeroStoreManager();
      new CorePuller(
              core,
              storeManager.getZeroStoreClient(),
              storeManager.getMetadataCacheManager(),
              storeManager.getZeroMetadataController(),
              1,
              null)
          .pullCoreWithRetries(
              storeManager.isLeader(core), 10, storeManager.getConfig().getCorePullRetryDelay());
      verify(zeroStoreClient, never()).pullShardMetadata(any());
    } catch (Exception ex) {
      fail("syncLocalCoreWithZeroStore failed with exception: " + ex.getMessage());
    }
  }

  /**
   * Checks that syncLocalCoreWithZeroStore will throw exception if shard.metadata file is missing
   * from the zeroStore.
   */
  @Test
  public void testSyncLocalCoreWithZeroStore_missingMetadataFile() throws Exception {

    collectionName = "zeroCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupZeroCollectionWithShardNames(collectionName, 1, shardName);

    ClusterState clusterState = cluster.getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    CoreContainer cc = getCoreContainer(newReplica.getNodeName());

    try (SolrCore core = cc.getCore(newReplica.getCoreName())) {
      ZeroStoreManager storeManager = cc.getZeroStoreManager();
      CorePuller corePuller = Mockito.spy(setupCorePuller(core, 1));
      corePuller.pullCoreWithRetries(
          storeManager.isLeader(core), 10, storeManager.getConfig().getCorePullRetryDelay());
      fail(
          "syncLocalCoreWithZeroStore should throw exception if Zero store doesn't have the shard.metadata file.");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains(ZERO_METADATA_MISSING.toString()));
    }
  }

  /**
   * testSyncLocalCoreWithZeroStore_syncEquivalent checks that syncLocalCoreWithZeroStore doesn't
   * throw an exception if already in sync, Zero store and local files are.
   */
  @Test
  public void testSyncLocalCoreWithZeroStore_syncEquivalent() throws Exception {

    CloudSolrClient cloudClient = cluster.getSolrClient();

    collectionName = "zeroCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupZeroCollectionWithShardNames(collectionName, 1, shardName);

    ClusterState clusterState = cluster.getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    newReplica = collection.getReplicas().get(0);
    CoreContainer cc = getCoreContainer(newReplica.getNodeName());
    SolrCore core = cc.getCore(newReplica.getCoreName());
    ZeroStoreManager storeManager = cc.getZeroStoreManager();

    // Add a document.
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.commit(cloudClient, collectionName);

    try {
      // we push and already have the latest updates so we should not pull here
      new CorePuller(
              core,
              storeManager.getZeroStoreClient(),
              storeManager.getMetadataCacheManager(),
              storeManager.getZeroMetadataController(),
              1,
              null)
          .pullCoreWithRetries(
              storeManager.isLeader(core), 10, storeManager.getConfig().getCorePullRetryDelay());
      verify(zeroStoreClient, never()).pullShardMetadata(any());
    } catch (Exception ex) {
      fail("syncLocalCoreWithZeroStore failed with exception: " + ex.getMessage());
    } finally {
      core.close();
    }
  }

  /**
   * testSyncLocalCoreWithZeroStore_syncSuccess checks that syncLocalCoreWithZeroStore pulls index
   * files from Zero store if missing locally and present there
   */
  @Test
  public void testSyncLocalCoreWithZeroStore_syncSuccess() throws Exception {

    // set up two nodes with one shard and two replicas
    collectionName = "zeroCol" + UUID.randomUUID();
    shardName = "shard" + UUID.randomUUID();
    setupZeroCollectionWithShardNames(collectionName, 2, shardName);

    // Add a document.
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("cat", "cat123");
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.commit(cluster.getSolrClient(), collectionName);

    // get the follower replica
    ClusterState clusterState = cluster.getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    Replica leaderReplica = collection.getLeader(shardName);
    Replica follower = null;
    for (Replica replica : collection.getReplicas()) {
      if (!replica.getName().equals(leaderReplica.getName())) {
        follower = replica;
        break;
      }
    }

    // verify this last update didn't happen on the follower, it should only have its default
    // segment file
    CoreContainer cc = getCoreContainer(follower.getNodeName());
    try (SolrCore core = cc.getCore(follower.getCoreName())) {
      assertEquals(1, core.getDeletionPolicy().getLatestCommit().getFileNames().size());
      ZeroStoreManager storeManager = cc.getZeroStoreManager();
      // we pushed on the leader, try sync on the follower
      new CorePuller(
              core,
              storeManager.getZeroStoreClient(),
              storeManager.getMetadataCacheManager(),
              storeManager.getZeroMetadataController(),
              1,
              null)
          .pullCoreWithRetries(
              storeManager.isLeader(core), 10, storeManager.getConfig().getCorePullRetryDelay());
      // did we pull?
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);

      // query just the replica we pulled on
      try (SolrClient directClient =
          getHttpSolrClient(follower.getBaseUrl() + "/" + follower.getCoreName())) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*").set("distrib", "false");
        QueryResponse resp = directClient.query(params);
        assertEquals(1, resp.getResults().getNumFound());
        assertEquals("cat123", resp.getResults().get(0).getFieldValue("cat"));
      }
    } catch (Exception ex) {
      fail("syncLocalCoreWithZeroStore failed with exception: " + ex.getMessage());
    }
  }

  /*
   * Test that pull from Zero store is successful when the local core needs to be refreshed
   */
  @Test
  public void testPullSucceedsAfterUpdate() throws Exception {
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");

    String collectionName = "collectionTest";
    String shardName = "shardTest";

    SolrCore core = h.getCore();

    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), "//*[@numFound='1']");

    // do a push via CorePusher, the returned ZeroStoreShardMetadata is what we'd expect to find
    // on the Zero store
    ZeroStoreShardMetadata returnedZcm =
        doPush(core, collectionName, shardName, deleteProcessor, zeroStoreClient);
    // Delete the core to clear the index data and then re-create it
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");
    core = h.getCore();

    // the doc should not be present
    assertQ(req("*:*"), "//*[@numFound='0']");

    // now perform a pull
    doPull(core, returnedZcm);

    // the doc should be present, we should be able to index and query again
    assertQ(req("*:*"), "//*[@numFound='1']");
    assertU(adoc("id", docId + "1"));
    assertU(commit());
    assertQ(req("*:*"), "//*[@numFound='2']");

    deleteCore();
  }

  @Test
  public void testPullRecoversWhenExceptionBeforeSearcherIsOpened() throws Exception {
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");

    String collectionName = "collectionTest";
    String shardName = "shardTest";

    SolrCore core = h.getCore();

    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), "//*[@numFound='1']");

    // do a push via CorePusher, the returned ZeroStoreShardMetadata is what we'd expect to find
    // on the Zero store
    ZeroStoreShardMetadata returnedZcm =
        doPush(core, collectionName, shardName, deleteProcessor, zeroStoreClient);
    // Delete the core to clear the index data and then re-create it
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");
    core = h.getCore();

    // the doc should not be present
    assertQ(req("*:*"), "//*[@numFound='0']");

    // now perform a pull and this pull will fail after files are fetched from Zero store,
    // when the searcher is opened
    try {
      doPullAndOptionallyFailOpenSearcher(core, returnedZcm, true);
    } catch (CorruptedCoreException ex) {
      // expected
    }

    // now pull, but let the call succeed and confirm the records are searchable
    doPullAndOptionallyFailOpenSearcher(core, returnedZcm, false);

    // the doc should be present, we should be able to index and query again
    assertQ(req("*:*"), "//*[@numFound='1']");
    assertU(adoc("id", docId + "1"));
    assertU(commit());
    assertQ(req("*:*"), "//*[@numFound='2']");
    deleteCore();
  }

  /** Tests that pull in the presence of higher local generation number is successful */
  @Test
  public void testLocalHigherGenerationConflictingPullSucceeds() throws Exception {
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");

    String collectionName = "collectionTest";
    String shardName = "shardTest";

    SolrCore core = h.getCore();

    // add a doc that would be pushed to Zero store
    assertU(adoc("id", "1"));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), xpathMatches("1"));

    // do a push via CorePusher, the returned ZeroStoreShardMetadata is what we'd expect to find
    // on the Zero store
    ZeroStoreShardMetadata returnedZcm =
        doPush(core, collectionName, shardName, deleteProcessor, zeroStoreClient);

    // add another doc but that would not be pushed to Zero store
    assertU(adoc("id", "2"));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), xpathMatches("1", "2"));

    long localGeneration = core.getDeletionPolicy().getLatestCommit().getGeneration();
    assertTrue(
        "Local generation is incorrectly not greater than Zero store generation",
        localGeneration > returnedZcm.getGeneration());

    // now perform a pull, since Zero store is the source of truth this pull should undo the
    // addition of doc 2
    boolean isLocalConflictingWithZero = doPull(core, returnedZcm);

    assertTrue("Pull is incorrectly not identified as conflicting", isLocalConflictingWithZero);

    // doc 1 should be present but not doc 2
    assertQ(req("*:*"), xpathMatches("1"));
    // for sanity index another doc
    assertU(adoc("id", "3"));
    assertU(commit());
    assertQ(req("*:*"), xpathMatches("1", "3"));

    deleteCore();
  }

  /** Tests that pull in the presence of conflicting files is successful */
  @Test
  public void testConflictingFilesPullSucceeds() throws Exception {
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");

    String collectionName = "collectionTest";
    String shardName = "shardTest";

    SolrCore core = h.getCore();

    // add a doc that would be pushed to Zero store
    assertU(adoc("id", "1"));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), xpathMatches("1"));

    // do a push via CorePusher, the returned ZeroStoreShardMetadata is what we'd expect to find
    // on the Zero store
    ZeroStoreShardMetadata returnedZcm =
        doPush(core, collectionName, shardName, deleteProcessor, zeroStoreClient);

    // Delete the core to clear the index data and then re-create it
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");
    core = h.getCore();

    // add a different doc, we will not push this to the Zero store
    assertU(adoc("id", "2"));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), xpathMatches("2"));

    // now Zero store and local should be at same generation number but different contents
    // (conflicting
    // files)
    long localGeneration = core.getDeletionPolicy().getLatestCommit().getGeneration();
    assertEquals(
        "Local generation is not equal to Zero store generation",
        localGeneration,
        returnedZcm.getGeneration());

    // now perform a pull
    boolean isLocalConflictingWithZero = doPull(core, returnedZcm);

    assertTrue("Pull is not identified as conflicting", isLocalConflictingWithZero);

    // the doc should be present, and Zero store should prevail as source of truth i.e. we go back
    // to doc 1
    assertQ(req("*:*"), xpathMatches("1"));
    // add another doc for sanity
    assertU(adoc("id", "3"));
    assertU(commit());
    assertQ(req("*:*"), xpathMatches("1", "3"));

    deleteCore();
  }

  private boolean doPull(SolrCore core, ZeroStoreShardMetadata shardMetadata) throws Exception {
    return this.doPullAndOptionallyFailOpenSearcher(core, shardMetadata, false);
  }

  private boolean doPullAndOptionallyFailOpenSearcher(
      SolrCore core, ZeroStoreShardMetadata shardMetadata, boolean failWhenOpening)
      throws Exception {
    // build the require metadata
    LocalCoreMetadata localCoreMetadata =
        new LocalCoreMetadata(core) {
          @Override
          protected String getCollectionName() {
            return collectionName;
          }

          @Override
          protected String getShardName() {
            return shardName;
          }

          @Override
          protected String getCoreName() {
            return coreName;
          }
        };
    localCoreMetadata.readMetadata(false, true);
    ZeroMetadataController metadataController = new ZeroMetadataController(null);
    MetadataComparisonResult metadataComparisonResult =
        metadataController.diffMetadataforPull(localCoreMetadata, shardMetadata);
    CorePuller corePuller =
        new CorePuller(core, zeroStoreClient, null, metadataController, 1, null) {
          @Override
          public void openSearcher(boolean waitForSearcher) throws Exception {
            if (failWhenOpening) {
              throw new IllegalArgumentException("simulated failure");
            } else {
              super.openSearcher(waitForSearcher);
            }
          }
        };
    corePuller.pullUpdateFromZero(metadataComparisonResult, true);
    return metadataComparisonResult.isLocalConflictingWithZero();
  }

  private String[] xpathMatches(String... docIds) {
    String[] tests = new String[docIds != null ? docIds.length + 1 : 1];
    tests[0] = "*[count(//doc)=" + (tests.length - 1) + "]";
    if (docIds != null && docIds.length > 0) {
      int i = 1;
      for (String docId : docIds) {
        tests[i++] = "//result/doc/str[@name='id'][.='" + docId + "']";
      }
    }
    return tests;
  }

  private CorePuller setupCorePuller(SolrCore core, int maxCorePullAttempts) {
    ZeroStoreManager storeManager = core.getCoreContainer().getZeroStoreManager();
    return new CorePuller(
        core,
        storeManager.getZeroStoreClient(),
        storeManager.getMetadataCacheManager(),
        storeManager.getZeroMetadataController(),
        maxCorePullAttempts,
        null) {

      @Override
      protected ZeroMetadataVersion getShardMetadataVersion() {
        return new ZeroMetadataVersion("0", 0);
      }
    };
  }
}
