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
package org.apache.solr.cloud.api.collections;

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerMessageHandler;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link OverseerMessageHandler} that handles cancelling API-related overseer messages. Works
 * closely with {@link OverseerTaskProcessor}.
 */
public class OverseerCancelMessageHandler implements OverseerMessageHandler, SolrCloseable {

  public static final String CANCEL_PREFIX = "cancel";


  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Overseer overseer;
  private final HttpShardHandlerFactory shardHandlerFactory;
  private final SolrCloudManager cloudManager;
  private final Stats stats;
  private OverseerTaskProcessor overseerTaskProcessor;
  private final Set<String> cancelInProgressSet;

  private final ExecutorService tpe =
      new ExecutorUtil.MDCAwareThreadPoolExecutor(
          5,
          10,
          0L,
          TimeUnit.MILLISECONDS,
          new SynchronousQueue<>(),
          new SolrNamedThreadFactory("OverseerCancelMessageHandlerThreadFactory"));

  private volatile boolean isClosed;

  public OverseerCancelMessageHandler(
      final HttpShardHandlerFactory shardHandlerFactory, Stats stats, Overseer overseer) {
    this.shardHandlerFactory = shardHandlerFactory;
    this.stats = stats;
    this.overseer = overseer;
    this.cloudManager = overseer.getSolrCloudManager();
    this.isClosed = false;
    this.cancelInProgressSet = new HashSet<>();
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    ExecutorUtil.shutdownAndAwaitTermination(tpe);
  }

  @Override
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation) {

    if (!operation.startsWith(CANCEL_PREFIX)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Operation does not contain proper prefix: " + operation + " expected: " + CANCEL_PREFIX);
    }

    String targetAsyncId = message.getStr(REQUESTID);
    if (targetAsyncId == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cancel message must include 'REQUESTID' parameter.");
    }

    log.debug("OverseerCancelMessageHandler.processMessage : {} , {}", operation, targetAsyncId);

    NamedList<Object> results = new SimpleOrderedMap<>();

    try {
      boolean isInProgress = overseerTaskProcessor.isAsyncTaskInProgress(targetAsyncId);
      boolean isSubmitted = overseerTaskProcessor.isAsyncTaskInSubmitted(targetAsyncId);

      if (!isSubmitted && !isInProgress) {
        results =
            buildResponse(
                targetAsyncId, "not_found", "Task was not found or has already completed.");
        return new OverseerSolrResponse(results);
      }

      if (!isInProgress) {
        // Task is not in progress; attempt to remove from the queue
        boolean removed = overseerTaskProcessor.removeSubmittedTask(targetAsyncId);
        if (removed) {
          results =
              buildResponse(
                  targetAsyncId,
                  "removed",
                  "Submitted Task was successfully removed from the queue.");
        } else {
          results =
              buildResponse(
                  targetAsyncId, "not_found", "Task was not found or has already completed.");
        }
        return new OverseerSolrResponse(results);
      }

      // Task is in progress
      return handleTaskCancellation(targetAsyncId, results);

    } catch (Exception e) {
      log.error("Error cancelling task {}", targetAsyncId, e);
      results.add(
          "response",
          buildResponse(
              targetAsyncId,
              "error",
              "Exception occurred while cancelling task: " + e.getMessage()));
      return new OverseerSolrResponse(results);
    }
  }

  private OverseerSolrResponse handleTaskCancellation(
      String targetAsyncId, NamedList<Object> results)
      throws InterruptedException, KeeperException, IOException {

    // Attempt to cancel the in-progress task
    boolean cancelled = overseerTaskProcessor.cancelInProgressAsyncTask(targetAsyncId);

    if (cancelled) {
      results = buildResponse(targetAsyncId, "cancelled", "Task was successfully cancelled.");
    } else {
      results =
          buildResponse(
              targetAsyncId, "failed", "Task could not be cancelled or has already completed.");
    }

    return new OverseerSolrResponse(results);
  }

  private NamedList<Object> buildResponse(String asyncId, String status, String message) {
    NamedList<Object> response = new SimpleOrderedMap<>();
    response.add("asyncId", asyncId);
    response.add("status", status);
    response.add("message", message);
    return response;
  }

  @Override
  public String getName() {
    return "Overseer Cancel Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "OverseerCancelMessageHandlerTimer" + operation;
  }

  // TODO: find a better way to lock, currently lock based on asyncId being unique
  public Lock lockTask(ZkNodeProps message, long ignored) {
    String cancelTaskMsg = getTaskKey(message);
    if (canExecute(cancelTaskMsg)) {
      markExclusive(cancelTaskMsg);
      return () -> unmarkExclusive(cancelTaskMsg);
    }
    return null;
  }

  private boolean canExecute(String cancelTaskMsg) {

    synchronized (cancelInProgressSet) {
      if (cancelInProgressSet.contains(cancelTaskMsg)) {
        return false;
      }
      if (cancelTaskMsg != null && cancelInProgressSet.contains(cancelTaskMsg)) {
        return false;
      }
    }

    return true;
  }

  private void markExclusive(String cancelTaskMsg) {
    synchronized (cancelInProgressSet) {
      cancelInProgressSet.add(cancelTaskMsg);
    }
  }

  private void unmarkExclusive(String cancelTaskMsg) {
    synchronized (cancelInProgressSet) {
      cancelInProgressSet.remove(cancelTaskMsg);
    }
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    // Return a constant key for all cancel operations
    return message.getStr("operation") + ":" + message.getStr(REQUESTID);
  }

  public OverseerCancelMessageHandler withOverseerTaskProcessor(
      OverseerTaskProcessor overseerTaskProcessor) {
    this.overseerTaskProcessor = overseerTaskProcessor;
    return this;
  }
}
