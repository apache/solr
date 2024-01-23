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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/** Unit tests for {@link ZeroCoreIndexingBatchProcessor} */
public class ZeroCoreIndexingBatchProcessorTest extends ZeroStoreSolrCloudTestCase {

  private static final String COLLECTION_NAME = "zeroCollection";
  private static final String SHARD_NAME = "shard1";

  private SolrCore core;
  private ZeroAccessLocks locks;
  private ZeroCoreIndexingBatchProcessor processor;
  private SolrQueryResponse queryResponse;

  @BeforeClass
  public static void setupCluster() throws Exception {
    assumeWorkingMockito();
    setupCluster(1);
  }

  @Before
  public void setupTest() throws Exception {
    assertEquals("wrong number of nodes", 1, cluster.getJettySolrRunners().size());
    CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();
    int numReplicas = 1;
    setupZeroCollectionWithShardNames(COLLECTION_NAME, numReplicas, SHARD_NAME);
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);

    assertEquals("wrong number of replicas", 1, collection.getReplicas().size());
    core = cc.getCore(collection.getReplicas().get(0).getCoreName());

    assertNotNull("core is null", core);

    queryResponse = new SolrQueryResponse();

    processor = setupZeroCoreIndexingBatchProcessor(core, 5);
    processor = Mockito.spy(processor);
    MetadataCacheManager.MetadataCacheEntry metadata =
        core.getCoreContainer()
            .getZeroStoreManager()
            .getMetadataCacheManager()
            .getOrCreateCoreMetadata(core.getName());
    locks = metadata.getZeroAccessLocks();
  }

  @After
  public void teardownTest() throws Exception {
    if (core != null) {
      core.close();
    }
    if (processor != null) {
      processor.close();
      assertEquals("pull lock should not be held", 0, locks.getPullHoldCount());
    }
    if (cluster != null) {
      cluster.deleteAllCollections();
    }
  }

  /** Tests that first add/delete starts an indexing batch. */
  @Test
  public void testAddOrDeleteStart() {
    verify(processor, never()).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
  }

  /** Tests that two adds/deletes only start an indexing batch once. */
  @Test
  public void testTwoAddOrDeleteOnlyStartOnce() {
    verify(processor, never()).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
  }

  /** Tests that commit does finish an indexing batch. */
  @Test
  public void testCommitDoesFinish() {
    verify(processor, never()).finishIndexingBatch();
    processCommit();
    verify(processor).finishIndexingBatch();
  }

  /** Tests that a stale core is pulled at the start of an indexing batch. */
  @Test
  public void testStaleCoreIsPulledAtStart() throws Exception {
    verify(processor, never()).startIndexingBatch();

    ZeroStoreManager storeManager = core.getCoreContainer().getZeroStoreManager();
    ZeroMetadataController metadataController = storeManager.getZeroMetadataController();

    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();

    // update core once
    doc.setField("id", "1");
    req.add(doc);
    req.commit(cluster.getSolrClient(), COLLECTION_NAME);
    ZeroMetadataVersion metadata1 =
        metadataController.readMetadataValue(COLLECTION_NAME, SHARD_NAME);

    // update core twice
    doc.setField("id", "2");
    req.commit(cluster.getSolrClient(), COLLECTION_NAME);
    ZeroMetadataVersion metadata2 =
        metadataController.readMetadataValue(COLLECTION_NAME, SHARD_NAME);

    // trick system bt setting core suffix to previous one to trigger a pull at the start of
    // indexing batch
    metadataController.updateMetadataValueWithVersion(
        COLLECTION_NAME, SHARD_NAME, metadata1.getMetadataSuffix(), metadata2.getVersion());

    assertPullDone(false);
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    assertPullDone(true);
  }

  /** Tests that an up-to-date core is not pulled at the start of an indexing batch. */
  @Test
  public void testUpToDateCoreIsNotPulledAtStart() throws Exception {
    ZeroStoreManager storeManager = core.getCoreContainer().getZeroStoreManager();
    CorePuller corePuller =
        Mockito.spy(
            new CorePuller(
                core,
                storeManager.getZeroStoreClient(),
                storeManager.getMetadataCacheManager(),
                storeManager.getZeroMetadataController(),
                1,
                null));

    corePuller.pullCoreWithRetries(true, 10, 10);

    verify(processor, never()).startIndexingBatch();
    assertPullDone(false);
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    assertPullDone(false);
  }

  /** Tests that a read lock is not acquired when the start encounters an error in pulling */
  @Test
  public void testReadLockIsNotAcquiredWhenStartEncountersError() throws Exception {

    // set max attempts to 0 to make pull throws
    // need to restart cluster to take into account new property value
    shutdownCluster();
    System.setProperty(
        ZeroConfig.ZeroSystemProperty.maxFailedCorePullAttempts.getPropertyName(),
        Integer.toString(0));
    setupCluster(1);
    teardownTest();
    setupTest();

    verify(processor, never()).startIndexingBatch();
    assertPullDone(false);
    assertEquals("indexing lock should not be held", 0, locks.getIndexingHoldCount());
    try {
      processor.startIndexingBatch();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue("Wrong exception thrown", ex.getMessage().contains(CorePullStatus.FAILURE.name()));
    }
    assertPullDone(false);
    assertEquals("indexing lock should not be held", 0, locks.getIndexingHoldCount());

    shutdownCluster();
    System.setProperty(
        ZeroConfig.ZeroSystemProperty.maxFailedCorePullAttempts.getPropertyName(),
        Integer.toString(20));
    setupCluster(1);
  }

  /**
   * Tests that when we timeout and fail the batch when we cannot acquire the read lock within a
   * time
   */
  @Test
  @SuppressWarnings("try")
  public void testAcquiringReadLockTimeouts() throws Exception {
    verify(processor, never()).startIndexingBatch();
    assertPullDone(false);
    assertEquals("indexing lock should not be held", 0, locks.getIndexingHoldCount());

    // index and push so we can skip trying to acquire the write lock on the next batch
    processAddOrDelete();
    processCommit();
    processor.close();

    // set up a new processor to process a new batch
    processor = setupZeroCoreIndexingBatchProcessor(core, 3);

    // spin up another thread to hold our write lock
    // holdLatch is used to keep the secondary thread holding our write lock until we verify the
    // read lock timeout
    CountDownLatch holdLatch = new CountDownLatch(1);

    // startLatch is test scaffolding - allow the secondary thread to spin up before we proceed with
    // the verification
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);
    Thread t =
        new Thread(
            () -> {
              try (AutoCloseable ignore = locks.acquirePullLock(0)) {
                // The pull lock being acquired prevents the indexing lock from being acquired
                startLatch.countDown();
                holdLatch.await();
              } catch (Exception e) {
                failed.set(true);
              }
            });

    try {
      t.start();
      startLatch.await();
      processor.startIndexingBatch();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue(
          "Wrong exception thrown",
          ex.getMessage()
              .contains("Indexing thread timed out trying to acquire the pull read lock"));
    } finally {
      holdLatch.countDown();
    }
    // Make sure the first thread did ok
    t.join();
    assertFalse(failed.get());

    assertEquals("indexing lock should not be held", 0, locks.getIndexingHoldCount());
  }

  /** Tests that an indexing batch with some work does push to the Zero store. */
  @Test
  public void testCommitAfterAddOrDeleteDoesPush() {
    processAddOrDelete();
    processCommit();
    assertPushDone(true);
  }

  /** Tests that an indexing batch with no work does not push to the Zero store. */
  @Test
  public void testIsolatedCommitDoesNotPush() {
    processCommit();
    assertPushDone(false);
  }

  /** Tests that an already committed indexing batch throws if a doc is added/deleted again. */
  @Test
  public void testAddOrDeleteAfterCommitThrows() {
    processAddOrDelete();
    commitAndThenAddOrDeleteDoc();
  }

  /**
   * Tests that an already isolated committed indexing batch throws if a doc is added/deleted again.
   */
  @Test
  public void testAddOrDeleteAfterIsolatedCommitThrows() {
    commitAndThenAddOrDeleteDoc();
  }

  private void commitAndThenAddOrDeleteDoc() {
    processCommit();
    try {
      processAddOrDelete();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue(
          "wrong exception thrown",
          ex.getMessage()
              .contains(
                  "Why are we adding/deleting a doc through an already committed indexing batch?"));
    }
  }

  /** Tests that an already committed indexing batch throws if committed again. */
  @Test
  public void testCommitAfterCommitThrows() {
    processAddOrDelete();
    doDoubleCommit();
  }

  /** Tests that an already isolated committed indexing batch throws if committed again. */
  @Test
  public void testCommitAfterIsolatedCommitThrows() {
    doDoubleCommit();
  }

  private void doDoubleCommit() {
    processCommit();
    try {
      processCommit();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue(
          "wrong exception thrown",
          ex.getMessage().contains("Why are we committing an already committed indexing batch?"));
    }
  }

  private void processAddOrDelete() {
    processor.addOrDeleteGoingToBeIndexedLocally();
  }

  private void assertPullDone(boolean done) {
    Object pullDone = queryResponse.getToLog().get("pull.done");
    if (done) assertTrue(pullDone != null && ((Boolean) pullDone));
    else assertTrue(pullDone == null || !((Boolean) pullDone));
  }

  private void assertPushDone(boolean done) {
    Object pushDone = queryResponse.getToLog().get("push.done");
    if (done) assertTrue(pushDone != null && ((Boolean) pushDone));
    else assertTrue(pushDone == null || !((Boolean) pushDone));
  }

  private void processCommit() {
    processor.hardCommitCompletedLocally();
  }

  private ZeroCoreIndexingBatchProcessor setupZeroCoreIndexingBatchProcessor(
      SolrCore core, int indexingLockTimeout) {
    return new ZeroCoreIndexingBatchProcessor(
        core, core.getCoreContainer().getZkController().getClusterState(), queryResponse) {
      // @Override

      @Override
      protected int getIndexingLockTimeout() {
        return indexingLockTimeout; // seconds
      }
    };
  }
}
