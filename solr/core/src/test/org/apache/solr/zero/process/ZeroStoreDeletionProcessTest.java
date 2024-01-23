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

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.metadata.FileDeleteStrategyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests around the deletion of files from Zero store via background deletion processes as triggered
 * by normal indexing and collection api calls
 */
public class ZeroStoreDeletionProcessTest extends ZeroStoreSolrCloudTestCase {

  private List<CompletableFuture<DeleterTask.Result>> taskFutures;
  private List<CompletableFuture<DeleterTask.Result>> overseerTaskFutures;

  @Before
  public void setupTest() {
    taskFutures = new ArrayList<>();
    overseerTaskFutures = new ArrayList<>();
  }

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that verifies that files marked for deletion during the indexing process get enqueued for
   * deletion and deleted. This test case differs from {@link FileDeleteStrategyTest} because it
   * tests the end-to-end indexing flow with a {@link MiniSolrCloudCluster}
   */
  @Test
  public void testIndexingTriggersDeletes() throws Exception {
    setupCluster(1);
    JettySolrRunner node = cluster.getJettySolrRunner(0);

    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteProcessorsForTest(node, delay, taskFutures, overseerTaskFutures);

    // set up the collection
    String collectionName = "ZeroCollection";
    int numReplicas = 1;
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // index and track deletions, commits are implicit for Zero collections, so we expect pushes to
    // occur per batch and previous segment index files to be deleted per batch except the first
    sendIndexingBatch(collectionName, /*numDocs */ 1, /* docIdStart */ 0);
    assertEquals(0, taskFutures.size());

    // second indexing batch causes files to be added for deletion, but they are not deleted yet
    // (files marked for deletion in the current batch will be deleted by the next batch, to
    // avoid risks of index corruption).
    sendIndexingBatch(collectionName, /*numDocs */ 1, /* docIdStart */ 1);
    assertEquals(0, taskFutures.size());

    // third indexing batch should delete the files marked for deletion by the previous batch
    sendIndexingBatch(collectionName, /*numDocs */ 1, /* docIdStart */ 2);
    assertEquals(1, taskFutures.size());

    CompletableFuture<DeleterTask.Result> cf = taskFutures.get(0);
    DeleterTask.Result result = cf.get(5000, TimeUnit.MILLISECONDS);

    // verify the files were deleted
    ZeroStoreClient zeroStoreClient =
        node.getCoreContainer().getZeroStoreManager().getZeroStoreClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(zeroStoreClient, result);
  }

  /**
   * Test that verifies that collection deletion command deletes all files for the given collection
   * on the happy path
   */
  @Test
  public void testDeleteCollectionCommand() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    JettySolrRunner node = cluster.getJettySolrRunner(0);
    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteProcessorsForTest(node, delay, taskFutures, overseerTaskFutures);

    // set up the collection
    String collectionName = "ZeroCollection";
    String collectionName2 = "ZeroCollection_shard1";
    int numReplicas = 1;
    String shardNames = "shard1,shard2";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);
    // setup a somewhat similarly named collection
    setupZeroCollectionWithShardNames(collectionName2, numReplicas, shardNames);

    // index a bunch of docs
    for (int i = 0; i < 10; i++) {
      int numDocs = 100;
      sendIndexingBatch(collectionName, numDocs, i * numDocs);
    }
    assertFalse(taskFutures.isEmpty());
    assertEquals(0, overseerTaskFutures.size()); // isEmpty()

    // do collection deletion
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
    delete.process(cloudClient).getResponse();
    assertEquals(1, overseerTaskFutures.size());

    // wait for the deletion command to complete
    CompletableFuture<DeleterTask.Result> cf = overseerTaskFutures.get(0);
    DeleterTask.Result result = cf.get(5000, TimeUnit.MILLISECONDS);

    // the collection should no longer exist on zookeeper
    cluster.getZkStateReader().forceUpdateCollection(collectionName);
    assertNull(cluster.getZkStateReader().getClusterState().getCollectionOrNull(collectionName));
    assertNotNull(
        cluster.getZkStateReader().getClusterState().getCollectionOrNull(collectionName2));

    // verify the right collection was deleted
    for (ZeroFile file : result.getFilesDeleted()) {
      String coll = file.getCollectionName();
      assertEquals("A file from the wrong collection was deleted!", collectionName, coll);
    }

    // verify the files in the deletion tasks were all deleted
    ZeroStoreClient zeroStoreClient =
        node.getCoreContainer().getZeroStoreManager().getZeroStoreClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(zeroStoreClient, result);

    // verify no files belonging to this collection exist on Zero store
    var names = zeroStoreClient.listCollectionZeroFiles(collectionName);
    assertTrue(names.isEmpty());
  }

  /**
   * Test that verifies that shard deletion command deletes all files for the given shard on the
   * happy path
   */
  @Test
  public void testDeleteShardCommand() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    JettySolrRunner node = cluster.getJettySolrRunner(0);
    // files don't need to age before marked for deletion, deleted as indexed in this test
    int delay = 0;
    initiateDeleteProcessorsForTest(node, delay, taskFutures, overseerTaskFutures);

    // set up the collection
    String collectionName = "ZeroCollection";
    int numReplicas = 1;
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // index a bunch of docs
    for (int i = 0; i < 10; i++) {
      int numDocs = 100;
      sendIndexingBatch(collectionName, numDocs, i * numDocs);
    }
    assertFalse(taskFutures.isEmpty());
    assertEquals(0, overseerTaskFutures.size());

    // split the shard so the parents are set inactive and can be deleted
    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collectionName).setShardName("shard1");
    splitShard.process(cloudClient);
    waitForState(
        "Timed out waiting for sub shards to be active.", collectionName, activeClusterShape(2, 2));

    // do shard deletion on the parent
    CollectionAdminRequest.DeleteShard delete =
        CollectionAdminRequest.deleteShard(collectionName, "shard1");
    delete.process(cloudClient).getResponse();
    assertEquals(1, overseerTaskFutures.size());

    // wait for the deletion command to complete
    CompletableFuture<DeleterTask.Result> cf = taskFutures.get(0);
    DeleterTask.Result result = cf.get(5000, TimeUnit.MILLISECONDS);

    // the collection shard should no longer exist on zookeeper
    cluster.getZkStateReader().forceUpdateCollection(collectionName);
    DocCollection coll =
        cluster.getZkStateReader().getClusterState().getCollectionOrNull(collectionName);
    assertNotNull(coll);
    assertNull(coll.getSlice("shard1"));

    // verify the files in the deletion tasks were all deleted
    ZeroStoreClient zeroStoreClient =
        node.getCoreContainer().getZeroStoreManager().getZeroStoreClient();
    assertTrue(result.isSuccess());
    assertFilesDeleted(zeroStoreClient, result);

    // verify no files belonging to shard1 in the collection exists on Zero store
    var names = zeroStoreClient.listShardZeroFiles(collectionName, "shard1");
    assertTrue(names.isEmpty());

    // verify files belonging to shard1_0 and shard1_1 exist still
    names = zeroStoreClient.listShardZeroFiles(collectionName, "shard1_0");
    assertFalse(names.isEmpty());
    names = zeroStoreClient.listShardZeroFiles(collectionName, "shard1_1");
    assertFalse(names.isEmpty());
  }

  private void assertFilesDeleted(ZeroStoreClient zeroStoreClient, DeleterTask.Result result)
      throws IOException {
    Collection<ZeroFile> filesDeleted = result.getFilesDeleted();
    for (ZeroFile zeroFile : filesDeleted) {
      IndexInput s = null;
      try {
        s = zeroStoreClient.pullStream(zeroFile);
        fail(zeroFile.getZeroFileName() + " should have been deleted from Zero store");
      } catch (Exception ex) {
        if (!(ex.getCause() instanceof NoSuchFileException)) {
          fail("Unexpected exception thrown = " + ex.getMessage());
        }
      } finally {
        if (s != null) {
          s.close();
        }
      }
    }
  }

  private void sendIndexingBatch(String collectionName, int numberOfDocs, int docIdStart)
      throws SolrServerException, IOException {
    UpdateRequest updateReq = new UpdateRequest();
    for (int k = docIdStart; k < docIdStart + numberOfDocs; k++) {
      updateReq.add("id", Integer.toString(k));
    }
    updateReq.process(cluster.getSolrClient(), collectionName);
  }

  private DeleteProcessor initiateDeleteProcessorsForTest(
      JettySolrRunner solrRunner,
      int deleteDelayMs,
      List<CompletableFuture<DeleterTask.Result>> taskFutures,
      List<CompletableFuture<DeleterTask.Result>> overseerTaskFutures) {
    ZeroStoreClient zeroStoreClient =
        solrRunner.getCoreContainer().getZeroStoreManager().getZeroStoreClient();
    int maxQueueSize = 200;
    int numThreads = 5;
    int defaultMaxAttempts = 5;
    int retryDelay = 500;

    // setup processors with the same defaults but enhanced to capture future results
    DeleteProcessor deleteProcessor =
        buildDeleteProcessorForTest(
            taskFutures,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs);
    DeleteProcessor overseerProcessor =
        buildDeleteProcessorForTest(
            overseerTaskFutures,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs);

    setupDeleteProcessorsForNode(deleteProcessor, overseerProcessor, solrRunner);
    return deleteProcessor;
  }

  // enables capturing all enqueues to the executor pool, including retries
  private DeleteProcessor buildDeleteProcessorForTest(
      List<CompletableFuture<DeleterTask.Result>> taskFutures,
      ZeroStoreClient zeroStoreClient,
      int almostMaxQueueSize,
      int numDeleterThreads,
      int defaultMaxDeleteAttempts,
      long fixedRetryDelay,
      long deleteDelayMs) {
    String DEFAULT_PROCESSOR_NAME = "DeleterForTest";
    return new DeleteProcessorForTest(
        DEFAULT_PROCESSOR_NAME,
        zeroStoreClient,
        almostMaxQueueSize,
        numDeleterThreads,
        defaultMaxDeleteAttempts,
        fixedRetryDelay,
        deleteDelayMs,
        taskFutures);
  }

  /** Test class extending DeleteProcessor to allow capturing task futures */
  static class DeleteProcessorForTest extends DeleteProcessor {
    List<CompletableFuture<DeleterTask.Result>> futures;

    public DeleteProcessorForTest(
        String name,
        ZeroStoreClient zeroStoreClient,
        int almostMaxQueueSize,
        int numDeleterThreads,
        int defaultMaxDeleteAttempts,
        long fixedRetryDelay,
        long deleteDelayMs,
        List<CompletableFuture<DeleterTask.Result>> taskFutures) {
      super(
          name,
          zeroStoreClient,
          almostMaxQueueSize,
          numDeleterThreads,
          defaultMaxDeleteAttempts,
          fixedRetryDelay,
          deleteDelayMs);
      this.futures = taskFutures;
    }

    @Override
    protected CompletableFuture<DeleterTask.Result> enqueue(DeleterTask task, boolean isRetry) {
      CompletableFuture<DeleterTask.Result> futureRes = super.enqueue(task, isRetry);
      futures.add(futureRes);
      return futureRes;
    }
  }
}
