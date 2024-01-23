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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.api.model.Constants;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link DeleteProcessor} */
public class DeleteProcessorTest extends ZeroStoreSolrCloudTestCase {

  private static ZeroStoreClient zeroStoreClient;

  private static List<DeleterTask> enqueuedTasks;
  private static LocalFileSystemRepository zeroRepository;

  @BeforeClass
  public static void setupTestClass() {
    NamedList<String> args = new NamedList<>();
    args.add(Constants.BACKUP_LOCATION, zeroStoreDir.toString());
    zeroRepository = new LocalFileSystemRepository();
    zeroRepository.init(args);

    zeroStoreClient =
        new ZeroStoreClient(zeroRepository, new SolrMetricManager(), 5, 5) {

          // no op deleteZeroFiles for DeleterTask
          @Override
          public void deleteZeroFiles(Collection<ZeroFile> paths) {}

          // no op listCollectionZeroFiles for CollectionDeleteionTask execute successfully
          @Override
          public Set<ZeroFile> listCollectionZeroFiles(String collectionName) {
            return new LinkedHashSet<>();
          }

          // no op for collection and shard deletion to execute successfully
          @Override
          public Set<ZeroFile> listShardZeroFiles(String collectionName, String shardName) {
            return new LinkedHashSet<>();
          }
        };
  }

  @Before
  public void setup() throws Exception {
    enqueuedTasks = new ArrayList<>();
    setupCluster(2);
  }

  @After
  public void after() throws Exception {
    shutdownCluster();
  }

  /**
   * Verify we enqueue a {@link FilesDeletionTask} with the correct parameters. Note we're not
   * testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end file deletion tests can be found in {@link
   * ZeroStoreDeletionProcessTest}
   */
  @Test
  public void testDeleteFilesEnqueueTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500;
    int deleteDelayMs = 3;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      Set<ZeroFile> zeroFiles = new HashSet<>();
      zeroFiles.add(new ZeroFile.ToDelete("collection1", "shard1", "file1"));
      zeroFiles.add(new ZeroFile.ToDelete("collection1", "shard1", "file2"));
      // uses the specified defaultMaxAttempts at the processor (not task) level
      CompletableFuture<DeleterTask.Result> cf = processor.deleteFiles(zeroFiles, true);
      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);
      assertEquals(1, enqueuedTasks.size());

      assertEquals(1, enqueuedTasks.size());
      assertNotNull(res);
      assertEquals(1, res.getTask().getAttempts());
      assertTrue(res.isSuccess());
      assertFalse(res.shouldRetry());
    }
  }

  /**
   * Verify we enqueue a {@link CollectionDeletionTask} with the correct parameters. Note we're not
   * testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end deletion tests can be found in {@link
   * ZeroStoreDeletionProcessTest}
   */
  @Test
  public void testDeleteCollection() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500;
    int deleteDelayMs = 3;

    ZeroCollectionTestUtil collectionUtil = new ZeroCollectionTestUtil(cluster, random());
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();
    collectionUtil.addDocAndCommit();

    ZeroStoreClient localZeroStoreClient =
        new ZeroStoreClient(zeroRepository, new SolrMetricManager(), 5, 5);
    URI collectionURI = localZeroStoreClient.getCollectionURI(collectionUtil.COLLECTION_NAME);
    Path collectionPath = Path.of(collectionURI);
    assertTrue(Files.exists(collectionPath));
    assertTrue(Files.isDirectory(collectionPath));

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            localZeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // uses the specified defaultMaxAttempts at the processor (not task) level
      CompletableFuture<DeleterTask.Result> cf =
          processor.deleteCollection(collectionUtil.COLLECTION_NAME, true);
      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);

      assertEquals(
          0L,
          res.getFilesDeleted().stream()
              .filter(f -> !f.getCollectionName().equals(collectionUtil.COLLECTION_NAME))
              .count());
      assertFalse(Files.exists(collectionPath));

      assertEquals(1, enqueuedTasks.size());
      assertNotNull(res);
      assertEquals(1, res.getTask().getAttempts());
      assertTrue(res.isSuccess());
      assertFalse(res.shouldRetry());
    }
  }

  /**
   * Verify we enqueue a {@link ShardDeletionTask} with the correct parameters. Note we're not
   * testing the functionality of the deletion task here only that the processor successfully
   * handles the task. End to end deletion tests can be found in {@link
   * ZeroStoreDeletionProcessTest}
   */
  @Test
  public void testDeleteShard() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 5;
    int retryDelay = 500;
    int deleteDelayMs = 3;

    ZeroCollectionTestUtil collectionUtil = new ZeroCollectionTestUtil(cluster, random());
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();
    collectionUtil.addDocAndCommit();

    ZeroStoreClient localZeroStoreClient =
        new ZeroStoreClient(zeroRepository, new SolrMetricManager(), 5, 5);
    URI shardURI =
        localZeroStoreClient.getShardURI(collectionUtil.COLLECTION_NAME, collectionUtil.SHARD_NAME);
    Path shardPath = Path.of(shardURI);
    assertTrue(Files.exists(shardPath));
    assertTrue(Files.isDirectory(shardPath));

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            localZeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // uses the specified defaultMaxAttempts at the processor (not task) level
      CompletableFuture<DeleterTask.Result> cf =
          processor.deleteShard(collectionUtil.COLLECTION_NAME, collectionUtil.SHARD_NAME, true);
      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);

      assertEquals(
          0L,
          res.getFilesDeleted().stream()
              .filter(
                  f ->
                      !(f.getCollectionName().equals(collectionUtil.COLLECTION_NAME)
                          && f.getShardName().equals(collectionUtil.SHARD_NAME)))
              .count());

      assertFalse(Files.exists(shardPath));
      assertEquals(1, enqueuedTasks.size());
      assertNotNull(res);
      assertEquals(1, res.getTask().getAttempts());
      assertTrue(res.isSuccess());
      assertFalse(res.shouldRetry());
    }
  }

  /** Verify that we don't retry tasks that are not configured to be retried and end up failing */
  @Test
  public void testNonRetryableTask() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried
    int deleteDelayMs = 3;

    boolean isRetry = false;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // enqueue a task that fails and is not retryable
      CompletableFuture<DeleterTask.Result> cf =
          processor.enqueue(buildFailingTaskForTest(totalAttempts, false), isRetry);
      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);

      // the first fails
      assertEquals(1, enqueuedTasks.size());
      assertNotNull(res);
      assertEquals(1, res.getTask().getAttempts());
      assertFalse(res.isSuccess());
      assertFalse(res.shouldRetry());

      // initial error + 0 retry errors suppressed
      assertNotNull(res.getError());
      assertEquals(0, res.getError().getSuppressed().length);
    }
  }

  /**
   * Verify that the retry logic kicks in for tasks configured to retry and subsequent retry
   * succeeds
   */
  @Test
  public void testRetryableTaskSucceeds() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried
    int totalFails = 3; // total number of times the task should fail
    int deleteDelayMs = 3;

    boolean isRetry = false;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // enqueue a task that fails totalFails number of times before succeeding
      CompletableFuture<DeleterTask.Result> cf =
          processor.enqueue(
              buildScheduledFailingTaskForTest(zeroStoreClient, totalAttempts, true, totalFails),
              isRetry);

      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);

      // the first 3 fail and last one succeeds
      assertEquals(4, enqueuedTasks.size());

      assertNotNull(res);
      assertEquals(4, res.getTask().getAttempts());
      assertTrue(res.isSuccess());

      // initial error + 2 retry errors suppressed
      assertNotNull(res.getError());
      assertEquals(2, res.getError().getSuppressed().length);
    }
  }

  /** Verify that after all task attempts are exhausted we bail out */
  @Test
  public void testRetryableTaskFails() throws Exception {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1; // ignored when we build the test task
    int retryDelay = 500;
    int totalAttempts = 5; // total number of attempts the task should be tried
    int deleteDelayMs = 3;

    boolean isRetry = false;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // enqueue a task that fails every time it runs but is configured to retry
      CompletableFuture<DeleterTask.Result> cf =
          processor.enqueue(buildFailingTaskForTest(totalAttempts, true), isRetry);

      // wait for this task and all its potential retries to finish
      DeleterTask.Result res = cf.get(5000, TimeUnit.MILLISECONDS);
      // 1 initial enqueue + 4 retries
      assertEquals(5, enqueuedTasks.size());

      assertNotNull(res);
      assertEquals(5, res.getTask().getAttempts());
      assertFalse(res.isSuccess());
      // circuit breaker should be false after all attempts are exceeded
      assertFalse(res.shouldRetry());

      // initial error + 4 retry errors suppressed
      assertNotNull(res.getError());
      assertEquals(4, res.getError().getSuppressed().length);
    }
  }

  /**
   * Verify that we cannot add more deletion tasks to the processor if the work queue is at its
   * target max but that we can re-add tasks that are retries to the queue
   */
  @Test
  public void testWorkQueueFull() {
    int maxQueueSize = 3;
    int numThreads = 1;
    int defaultMaxAttempts = 1;
    int retryDelay = 1000;
    int deleteDelayMs = 3;

    String name = "testName";
    boolean allowRetry = false;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      // numThreads is 1 and we'll enqueue a blocking task that ensures our pool
      // will be occupied while we add new tasks subsequently to test enqueue rejection
      CountDownLatch tasklatch = new CountDownLatch(1);
      processor.enqueue(buildBlockingTaskForTest(tasklatch), allowRetry);

      // Fill the internal work queue beyond the maxQueueSize, the internal queue size is not
      // approximate so we'll just add beyond the max
      for (int i = 0; i < maxQueueSize * 2; i++) {
        try {
          processor.deleteCollection(name, allowRetry);
        } catch (Exception ex) {
          // ignore
        }
      }

      // verify adding a new task is rejected
      try {
        processor.deleteCollection(name, allowRetry);
        fail("Task should have been rejected");
      } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unable to enqueue deletion"));
      }
      try {
        // verify adding a task that is marked as a retry is not rejected
        processor.enqueue(buildFailingTaskForTest(5, true), /* isRetry */ true);
      } catch (Exception ex) {
        fail("Task should not have been rejected");
      }

      // clean up and unblock the task
      tasklatch.countDown();
    }
  }

  /**
   * Verify that with a continuous stream of delete tasks being enqueued, all eventually complete
   * successfully in the face of failing tasks and retries without locking up our pool anywhere
   */
  @Test
  public void testSimpleConcurrentDeletionEnqueues() {
    int maxQueueSize = 200;
    int numThreads = 5;
    int defaultMaxAttempts = 5;
    int retryDelay = 100;
    int numberOfTasks = 200;
    int deleteDelayMs = 3;

    try (DeleteProcessor processor =
        buildZeroDeleteProcessorForTest(
            enqueuedTasks,
            zeroStoreClient,
            maxQueueSize,
            numThreads,
            defaultMaxAttempts,
            retryDelay,
            deleteDelayMs)) {
      List<DeleterTask> tasks = generateRandomTasks(defaultMaxAttempts, numberOfTasks);
      List<CompletableFuture<DeleterTask.Result>> taskResultsFutures = new ArrayList<>();
      List<DeleterTask.Result> results = new ArrayList<>();
      for (DeleterTask t : tasks) {
        taskResultsFutures.add(processor.enqueue(t, false));
      }

      taskResultsFutures.forEach(
          cf -> {
            try {
              results.add(cf.get(20000, TimeUnit.MILLISECONDS));
            } catch (Exception ex) {
              fail("We timed out on some task!");
            }
          });

      // we shouldn't enqueue more than (numberOfTasks * defaultMaxAttempts) tasks to the pool
      assertTrue(enqueuedTasks.size() < (numberOfTasks * defaultMaxAttempts));
      assertEquals(numberOfTasks, results.size());
      int totalAttempts = 0;
      for (DeleterTask.Result res : results) {
        assertNotNull(res);
        assertNotNull(res.getTask());
        assertEquals("scheduledFailingTask", res.getTask().getActionName());
        totalAttempts += res.getTask().getAttempts();
      }
      // total task attempts should be consistent with our test scaffolding
      assertTrue(totalAttempts < (numberOfTasks * defaultMaxAttempts));
    }
  }

  private List<DeleterTask> generateRandomTasks(int defaultMaxAttempts, int taskCount) {
    List<DeleterTask> tasks = new ArrayList<>();
    for (int i = 0; i < taskCount; i++) {
      int totalAttempts = random().nextInt(defaultMaxAttempts);
      int totalFails = random().nextInt(defaultMaxAttempts + 1);
      DeleterTask task =
          buildScheduledFailingTaskForTest(zeroStoreClient, totalAttempts, true, totalFails);
      tasks.add(task);
    }
    return tasks;
  }

  /** Returns a test-only task for just holding onto a resource for test purposes */
  private DeleterTask buildBlockingTaskForTest(CountDownLatch latch) {
    return new DeleterTask(null, null, false, 0) {

      @Override
      public Collection<ZeroFile> doDelete() throws Exception {
        // block until something forces this latch to count down
        latch.await();
        return new ArrayList<>();
      }

      @Override
      public String getBasePath() {
        return "";
      }

      @Override
      public String getActionName() {
        return "blockingTask";
      }

      @Override
      public void setMDCContext() {}
    };
  }

  /** Returns a test-only task that always fails on action execution by throwing an exception */
  private DeleterTask buildFailingTaskForTest(int maxRetries, boolean allowRetries) {
    return new DeleterTask(zeroStoreClient, new LinkedHashSet<>(), allowRetries, maxRetries) {

      @Override
      public Collection<ZeroFile> doDelete() throws Exception {
        throw new Exception("");
      }

      @Override
      public String getBasePath() {
        return "";
      }

      @Override
      public String getActionName() {
        return "failingTask";
      }

      @Override
      public void setMDCContext() {}
    };
  }

  /** Returns a test-only task that fails a specified number of times before succeeding */
  private DeleterTask buildScheduledFailingTaskForTest(
      ZeroStoreClient zeroStoreClient, int maxRetries, boolean allowRetries, int failTotal) {
    return new DeleterTask(zeroStoreClient, new LinkedHashSet<>(), allowRetries, maxRetries) {
      private final AtomicInteger failCount = new AtomicInteger(0);

      @Override
      public Collection<ZeroFile> doDelete() throws Exception {
        while (failCount.get() < failTotal) {
          failCount.incrementAndGet();
          throw new Exception("");
        }
        return new ArrayList<>();
      }

      @Override
      public String getBasePath() {
        return "";
      }

      @Override
      public String getActionName() {
        return "scheduledFailingTask";
      }

      @Override
      public void setMDCContext() {}
    };
  }

  // enables capturing all enqueues to the executor pool, including retries
  private DeleteProcessor buildZeroDeleteProcessorForTest(
      List<DeleterTask> enqueuedTasks,
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
        enqueuedTasks);
  }

  static class DeleteProcessorForTest extends DeleteProcessor {
    List<DeleterTask> enqueuedTasks;

    public DeleteProcessorForTest(
        String name,
        ZeroStoreClient zeroStoreClient,
        int almostMaxQueueSize,
        int numDeleterThreads,
        int defaultMaxDeleteAttempts,
        long fixedRetryDelay,
        long deleteDelayMs,
        List<DeleterTask> enqueuedTasks) {
      super(
          name,
          zeroStoreClient,
          almostMaxQueueSize,
          numDeleterThreads,
          defaultMaxDeleteAttempts,
          fixedRetryDelay,
          deleteDelayMs);
      this.enqueuedTasks = enqueuedTasks;
    }

    @Override
    protected CompletableFuture<DeleterTask.Result> enqueue(DeleterTask task, boolean isRetry) {
      enqueuedTasks.add(task);
      return super.enqueue(task, isRetry);
    }
  }
}
