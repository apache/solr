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
package org.apache.solr.handler.component;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.QT;
import static org.apache.solr.common.params.CommonParams.QUERY_UUID;
import static org.apache.solr.common.params.CommonParams.TASK_CHECK_UUID;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.CancelTask;
import org.apache.solr.handler.admin.api.ListActiveTasks;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class ActiveTaskQuerySupport {
  private static final String ACTIVE_TASK_LIST_HANDLER_PATH = "/tasks/list";
  private static final String CANCEL_TASK_HANDLER_PATH = "/tasks/cancel";

  private ActiveTaskQuerySupport() {}

  public static List<ActiveTaskDetails> listActiveTasks(SolrQueryRequest req) throws Exception {
    return execute(req, null, false).taskList;
  }

  public static boolean isTaskActive(SolrQueryRequest req, String taskId) throws Exception {
    return execute(req, taskId, false).taskActive;
  }

  public static boolean cancelTask(SolrQueryRequest req, String taskId) throws Exception {
    return execute(req, taskId, true).taskCancelled;
  }

  private static TaskQueryResult execute(
      SolrQueryRequest req, String taskId, boolean isCancellationRequest) throws Exception {
    if (!shouldDistributed(req)) {
      return localResult(req, taskId, isCancellationRequest);
    }
    return distributedResult(req, taskId, isCancellationRequest);
  }

  private static TaskQueryResult localResult(
      SolrQueryRequest req, String taskId, boolean isCancellationRequest) {
    if (taskId != null) {
      return (isCancellationRequest)
          ? new TaskQueryResult(
              List.of(), false, CancelTask.cancelTaskActiveOnThisShard(req, taskId))
          : new TaskQueryResult(
              List.of(), ListActiveTasks.isTaskActiveOnThisShard(req, taskId), false);
    }
    return new TaskQueryResult(ListActiveTasks.getActiveTasksOnThisShard(req), false, false);
  }

  private static TaskQueryResult distributedResult(
      SolrQueryRequest req, String taskId, boolean isCancellationRequest) {
    final ShardHandler shardHandler =
        req.getCoreContainer().getShardHandlerFactory().getShardHandler();
    final ResponseBuilder responseBuilder =
        TaskManagementHandler.buildResponseBuilder(req, new SolrQueryResponse(), List.of());
    shardHandler.prepDistributed(responseBuilder);

    if (!responseBuilder.isDistrib
        || responseBuilder.shards == null
        || responseBuilder.shards.length == 0) {
      return localResult(req, taskId, isCancellationRequest);
    }

    final ShardRequest shardRequest = new ShardRequest();
    shardRequest.shards = responseBuilder.shards;
    shardRequest.actualShards = shardRequest.shards;
    shardRequest.responses = new ArrayList<>(shardRequest.actualShards.length);

    for (String shard : shardRequest.actualShards) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      if (isCancellationRequest) {
        params.set(QT, CANCEL_TASK_HANDLER_PATH);
      } else {
        params.set(QT, ACTIVE_TASK_LIST_HANDLER_PATH);
      }
      if (taskId != null) {
        if (isCancellationRequest) {
          params.set(QUERY_UUID, taskId);
        } else {
          params.set(TASK_CHECK_UUID, taskId);
        }
      }
      ShardHandler.setShardAttributesToParams(params, shardRequest.purpose);
      shardHandler.submit(shardRequest, shard, params);
    }

    ShardResponse shardResponse = shardHandler.takeCompletedOrError();
    if (shardResponse != null && shardResponse.getShard() != null) {
      if (shardResponse.getException() != null) {
        shardHandler.cancelAll();
        if (shardResponse.getException() instanceof SolrException) {
          throw (SolrException) shardResponse.getException();
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, shardResponse.getException());
      }
    }

    if (taskId != null) {
      return (isCancellationRequest)
          ? new TaskQueryResult(List.of(), false, mergeCancellationStatus(shardRequest.responses))
          : new TaskQueryResult(List.of(), mergeTaskStatus(shardRequest.responses), false);
    }
    return new TaskQueryResult(mergeTaskList(shardRequest.responses), false, false);
  }

  private static boolean shouldDistributed(SolrQueryRequest req) {
    CoreContainer coreContainer = req.getCoreContainer();
    if (coreContainer == null) {
      return false;
    }
    return req.getParams().getBool(DISTRIB, coreContainer.isZooKeeperAware());
  }

  private static boolean mergeTaskStatus(List<ShardResponse> responses) {
    for (ShardResponse shardResponse : responses) {
      Object taskStatus = shardResponse.getSolrResponse().getResponse().get("taskStatus");
      if (taskStatus instanceof Boolean && (Boolean) taskStatus) {
        return true;
      }

      if (taskStatus instanceof String && ((String) taskStatus).contains("active")) {
        return true;
      }
    }
    return false;
  }

  private static boolean mergeCancellationStatus(List<ShardResponse> responses) {
    for (ShardResponse shardResponse : responses) {
      Object cancellationStatus = shardResponse.getSolrResponse().getResponse().get("status");
      if (cancellationStatus instanceof Boolean && (Boolean) cancellationStatus) {
        return true;
      }

      if (cancellationStatus instanceof String
          && ((String) cancellationStatus).contains("cancelled successfully")) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static List<ActiveTaskDetails> mergeTaskList(List<ShardResponse> responses) {
    Map<String, String> mergedTasks = new LinkedHashMap<>();
    for (ShardResponse shardResponse : responses) {
      Object taskList = shardResponse.getSolrResponse().getResponse().get("taskList");
      if (taskList instanceof NamedList) {
        NamedList<Object> namedList = (NamedList<Object>) taskList;
        for (int i = 0; i < namedList.size(); i++) {
          String taskId = namedList.getName(i);
          Object taskQuery = namedList.getVal(i);
          if (taskId != null) {
            mergedTasks.put(taskId, taskQuery == null ? null : taskQuery.toString());
          }
        }
      }
    }

    List<ActiveTaskDetails> mergedTaskList = new ArrayList<>(mergedTasks.size());
    for (Map.Entry<String, String> task : mergedTasks.entrySet()) {
      mergedTaskList.add(new ActiveTaskDetails(task.getKey(), task.getValue()));
    }

    return mergedTaskList;
  }

  private static final class TaskQueryResult {
    private final List<ActiveTaskDetails> taskList;
    private final boolean taskActive;
    private final boolean taskCancelled;

    private TaskQueryResult(
        List<ActiveTaskDetails> taskList, boolean taskActive, boolean taskCancelled) {
      this.taskList = taskList;
      this.taskActive = taskActive;
      this.taskCancelled = taskCancelled;
    }
  }
}
