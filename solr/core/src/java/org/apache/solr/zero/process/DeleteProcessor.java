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

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic deletion processor used for deleting files from Zero store. Each processor manages its
 * own task bounded thread pool for processing {@link DeleterTask} asynchronously. Processors
 * support retrying tasks if necessary but retry decisions are left to the individual task
 * implementations.
 *
 * <p>Instances of {@link DeleteProcessor} are managed by the {@link ZeroStoreManager}.
 */
public class DeleteProcessor implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final int almostMaxQueueSize;

  /**
   * Note we sleep() after each failed attempt, so multiply this value by {@link #fixedRetryDelay}
   * to find out how long we'll retry (at least) if Zero store access fails for some reason ("at
   * least" because we re-enqueue at the tail of the queue, so there might be additional processing
   * delay if the queue is not empty and is processed before the enqueued retry is processed).
   */
  private final int maxDeleteAttempts;

  private final long fixedRetryDelay;
  private final ZeroStoreClient zeroStoreClient;
  private final BlockingQueue<Runnable> deleteQueue;
  private final MDCAwareThreadPoolExecutor deleteExecutor;
  private final long deleteDelayMs;

  /**
   * @param name identifying the processor
   * @param zeroStoreClient the zeroStoreClient to use in this processor
   * @param almostMaxQueueSize the target max queue size
   * @param numDeleterThreads the number of threads to configure in the underlying thread pool
   * @param defaultMaxDeleteAttempts maximum number of attempts to retry any task enqueued in this
   *     processor
   * @param fixedRetryDelay fixed time delay in ms between retry attempts
   */
  public DeleteProcessor(
      String name,
      ZeroStoreClient zeroStoreClient,
      int almostMaxQueueSize,
      int numDeleterThreads,
      int defaultMaxDeleteAttempts,
      long fixedRetryDelay,
      long deleteDelayMs) {
    this.name = name;
    this.almostMaxQueueSize = almostMaxQueueSize;
    this.maxDeleteAttempts = defaultMaxDeleteAttempts;
    this.fixedRetryDelay = fixedRetryDelay;
    this.deleteDelayMs = deleteDelayMs;
    NamedThreadFactory threadFactory = new NamedThreadFactory(name);

    // Note this queue MUST NOT BE BOUNDED, or we risk deadlocks given that DeleterTask's
    // re-enqueue themselves upon failure
    deleteQueue = new LinkedBlockingDeque<>();

    deleteExecutor =
        new MDCAwareThreadPoolExecutor(
            numDeleterThreads, numDeleterThreads, 0L, TimeUnit.SECONDS, deleteQueue, threadFactory);
    this.zeroStoreClient = zeroStoreClient;
  }

  /**
   * Enqueues the given set of files for deletion from Zero store as an async task.
   *
   * @param zeroFiles list of files to delete from Zero store
   * @param allowRetry flag indicating if the task should be retried if it fails
   */
  public CompletableFuture<DeleterTask.Result> deleteFiles(
      Set<ZeroFile> zeroFiles, boolean allowRetry) {
    String collectionName = zeroFiles.iterator().next().getCollectionName();
    DeleterTask task =
        new FilesDeletionTask(
            zeroStoreClient, collectionName, zeroFiles, allowRetry, maxDeleteAttempts);
    return enqueue(task, false);
  }

  /**
   * Enqueues a task to delete all files belonging to the given collection from Zero store as an
   * async task.
   *
   * @param collectionName the name of the collection to be deleted from Zero store
   * @param allowRetry flag indicating if the task should be retried if it fails
   */
  public CompletableFuture<DeleterTask.Result> deleteCollection(
      String collectionName, boolean allowRetry) {
    DeleterTask task =
        new CollectionDeletionTask(zeroStoreClient, collectionName, allowRetry, maxDeleteAttempts);
    return enqueue(task, false);
  }

  /**
   * Enqueues a task to delete all files belonging to the given collection and shard as an async
   * task.
   *
   * @param collectionName targeted collection
   * @param shardName shard to delete
   * @param allowRetry flag indicating if the task should be retried if it fails
   */
  public CompletableFuture<DeleterTask.Result> deleteShard(
      String collectionName, String shardName, boolean allowRetry) {
    DeleterTask task =
        new ShardDeletionTask(
            zeroStoreClient, collectionName, shardName, allowRetry, maxDeleteAttempts);
    return enqueue(task, false);
  }

  /**
   * Enqueues a task to be processed by a thread in the {@link DeleteProcessor#deleteExecutor}
   * thread pool. The callback is handled by the same execution thread and will re-enqueue a task
   * that has failed and should be retried. Tasks that are enqueued via the retry mechanism are not
   * bound by the same size constraints as newly minted tasks are.
   *
   * @return CompletableFuture to allow calling threads the capability to block on the computation
   *     results as needed, retrieved suppressed exceptions in retry, etc
   */
  @VisibleForTesting
  protected CompletableFuture<DeleterTask.Result> enqueue(DeleterTask task, boolean isRetry) {
    if (!isRetry && deleteQueue.size() > almostMaxQueueSize) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unable to enqueue deletion task: " + task.toString());
    }

    return CompletableFuture.supplyAsync(task::call, deleteExecutor)
        .thenCompose(
            result -> {
              // the callback will execute on the same thread as the executing task
              if (!result.isSuccess() && result.shouldRetry()) {
                try {
                  // Some delay before retry... (could move this delay to before trying to delete a
                  // file that previously failed to be deleted, that way if the queue is busy, and
                  // it took time to retry, we don't add a delay on top of that. On the other hand,
                  // an exception here could be an issue with the Zero store itself and nothing
                  // specific to the file at hand, so slowing all delete attempts for all files
                  // might make sense.
                  Thread.sleep(fixedRetryDelay);
                  return enqueue(result.getTask(), result.shouldRetry());
                } catch (Exception ex) {
                  log.error(
                      "Could not re-enqueue failed deleter task that should have been enqueued!",
                      ex);
                }
              }
              return CompletableFuture.completedFuture(result);
            });
  }

  @Override
  public void close() {
    deleteExecutor.shutdown();
  }

  public String getName() {
    return name;
  }

  public long getDeleteDelayMs() {
    return deleteDelayMs;
  }
}
