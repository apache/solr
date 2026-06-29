package org.apache.solr.handler.component;

import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.ActiveTask;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.QT;
import static org.apache.solr.common.params.CommonParams.TASK_CHECK_UUID;

public class ActiveTaskQuerySupport {
  private static final String ACTIVE_TASK_LIST_HANDLER_PATH = "/tasks/list";

  private ActiveTaskQuerySupport() {}

  public static List<ActiveTaskDetails> listActiveTasks(SolrQueryRequest req) throws Exception {
    return execute(req, null).taskList;
  }

  public static boolean isTaskActive(SolrQueryRequest req, String taskId) throws Exception {
    return execute(req, taskId).taskActive;
  }

  private static TaskQueryResult execute(SolrQueryRequest req, String taskId) throws Exception {
    if (!shouldDistributed(req)) {
      return localResult(req, taskId);
    }
    return distributedResult(req, taskId);
  }

  private static TaskQueryResult localResult(SolrQueryRequest req, String taskId) {
    if (taskId != null) {
      return new TaskQueryResult(List.of(), ActiveTask.isTaskActiveOnThisShard(req, taskId));
    }
    return new TaskQueryResult(ActiveTask.getActiveTasksOnThisShard(req), false);
  }

  private static TaskQueryResult distributedResult(SolrQueryRequest req, String taskId) throws Exception {
    final ShardHandler shardHandler = req.getCoreContainer().getShardHandlerFactory().getShardHandler();
    final ResponseBuilder responseBuilder = TaskManagementHandler.buildResponseBuilder(req, new SolrQueryResponse(), List.of());
    shardHandler.prepDistributed(responseBuilder);

    if (!responseBuilder.isDistrib || responseBuilder.shards == null || responseBuilder.shards.length ==0) {
      return localResult(req, taskId);
    }

    final ShardRequest shardRequest = new ShardRequest();
    shardRequest.shards = responseBuilder.shards;
    shardRequest.actualShards = shardRequest.shards;
    shardRequest.responses = new ArrayList<>(shardRequest.actualShards.length);

    for (String shard: shardRequest.actualShards) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(QT, ACTIVE_TASK_LIST_HANDLER_PATH);
      if (taskId != null) {
        params.set(TASK_CHECK_UUID, taskId);
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
      return new TaskQueryResult(List.of(), mergeTaskStatus(shardRequest.responses));
    }
    return new TaskQueryResult(mergeTaskList(shardRequest.responses), false);

  }

  private static boolean shouldDistributed(SolrQueryRequest req) {
    CoreContainer coreContainer = req.getCoreContainer();
    if (coreContainer == null) {
      return false;
    }
    return req.getParams().getBool(DISTRIB, coreContainer.isZooKeeperAware());
  }

  private static boolean mergeTaskStatus(List<ShardResponse> responses) {
    for (ShardResponse shardResponse: responses) {
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
      } else if (taskList instanceof Map) {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) taskList).entrySet()) {
          if (entry.getKey() != null) {
            mergedTasks.put(entry.getKey().toString(), entry.getValue() == null ? null : entry.getValue().toString());
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

    private TaskQueryResult(List<ActiveTaskDetails> taskList, boolean taskActive) {
      this.taskList = taskList;
      this.taskActive = taskActive;
    }
  }
}
