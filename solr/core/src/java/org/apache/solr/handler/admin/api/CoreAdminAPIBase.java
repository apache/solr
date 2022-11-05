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
package org.apache.solr.handler.admin.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.tracing.TraceUtils;
import org.slf4j.MDC;

public abstract class CoreAdminAPIBase extends JerseyResource {

  private static final ExecutorService PARALLEL_EXECUTOR =
      ExecutorUtil.newMDCAwareFixedThreadPool(
          50, new SolrNamedThreadFactory("parallelCoreAdminAPIBaseExecutor"));
  private static final Map<String, Map<String, TaskObject>> REQUEST_STATUS_MAP;
  private static final int MAX_TRACKED_REQUESTS = 100;
  private static final String RUNNING = "running";
  private static final String COMPLETED = "completed";
  private static final String FAILED = "failed";

  static {
    HashMap<String, Map<String, TaskObject>> map = new HashMap<>(3, 1.0f);
    map.put(RUNNING, Collections.synchronizedMap(new LinkedHashMap<>()));
    map.put(COMPLETED, Collections.synchronizedMap(new LinkedHashMap<>()));
    map.put(FAILED, Collections.synchronizedMap(new LinkedHashMap<>()));
    REQUEST_STATUS_MAP = Collections.unmodifiableMap(map);
  }

  protected final CoreContainer coreContainer;
  protected final SolrQueryRequest req;
  protected final SolrQueryResponse rsp;

  public CoreAdminAPIBase(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.coreContainer = coreContainer;
    this.req = req;
    this.rsp = rsp;
  }

  public <T extends SolrJerseyResponse> T handle(
      T solrJerseyResponse, String coreName, String taskId, String actionName, Supplier<T> supplier)
      throws Exception {
    try {
      if (coreContainer == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Core container instance missing");
      }
      final TaskObject taskObject = new TaskObject(taskId);

      if (taskId != null) {
        // Put the tasks into the maps for tracking
        if (getRequestStatusMap(RUNNING).containsKey(taskId)
            || getRequestStatusMap(COMPLETED).containsKey(taskId)
            || getRequestStatusMap(FAILED).containsKey(taskId)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Duplicate request with the same requestid found.");
        }

        addTask(RUNNING, taskObject);
      }

      MDCLoggingContext.setCoreName(coreName);
      TraceUtils.setDbInstance(req, coreName);
      if (taskId == null) {
        return supplier.get();
      } else {
        try {
          MDC.put("CoreAdminAPIBase.taskId", taskId);
          MDC.put("CoreAdminAPIBase.action", actionName);
          PARALLEL_EXECUTOR.execute(
              () -> {
                boolean exceptionCaught = false;
                try {
                  T response = supplier.get();

                  V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);

                  taskObject.setRspObject(rsp);
                  taskObject.setOperationRspObject(rsp);
                } catch (Exception e) {
                  exceptionCaught = true;
                  taskObject.setRspObjectFromException(e);
                } finally {
                  removeTask("running", taskObject.taskId);
                  if (exceptionCaught) {
                    addTask("failed", taskObject, true);
                  } else {
                    addTask("completed", taskObject, true);
                  }
                }
              });
        } finally {
          MDC.remove("CoreAdminAPIBase.taskId");
          MDC.remove("CoreAdminAPIBase.action");
        }
      }
    } catch (CoreAdminAPIBaseException e) {
      throw e.trueException;
    } finally {
      rsp.setHttpCaching(false);
    }

    return solrJerseyResponse;
  }

  /**
   * Helper class to manage the tasks to be tracked. This contains the taskId, request and the
   * response (if available).
   */
  public static class TaskObject {
    String taskId;
    String rspInfo;
    Object operationRspInfo;

    public TaskObject(String taskId) {
      this.taskId = taskId;
    }

    public String getRspObject() {
      return rspInfo;
    }

    public void setRspObject(SolrQueryResponse rspObject) {
      this.rspInfo = rspObject.getToLogAsString("TaskId: " + this.taskId);
    }

    public void setRspObjectFromException(Exception e) {
      this.rspInfo = e.getMessage();
    }

    public Object getOperationRspObject() {
      return operationRspInfo;
    }

    public void setOperationRspObject(SolrQueryResponse rspObject) {
      this.operationRspInfo = rspObject.getResponse();
    }
  }

  protected static class CoreAdminAPIBaseException extends RuntimeException {
    Exception trueException;

    public CoreAdminAPIBaseException(Exception trueException) {
      this.trueException = trueException;
    }
  }

  /** Method to ensure shutting down of the ThreadPool Executor. */
  public static void shutdown() {
    ExecutorUtil.shutdownAndAwaitTermination(PARALLEL_EXECUTOR);
  }

  /** Helper method to add a task to a tracking type. */
  private void addTask(String type, TaskObject o, boolean limit) {
    synchronized (getRequestStatusMap(type)) {
      if (limit && getRequestStatusMap(type).size() == MAX_TRACKED_REQUESTS) {
        String key = getRequestStatusMap(type).entrySet().iterator().next().getKey();
        getRequestStatusMap(type).remove(key);
      }
      addTask(type, o);
    }
  }

  private void addTask(String type, TaskObject o) {
    synchronized (getRequestStatusMap(type)) {
      getRequestStatusMap(type).put(o.taskId, o);
    }
  }

  /** Helper method to remove a task from a tracking map. */
  private void removeTask(String map, String taskId) {
    synchronized (getRequestStatusMap(map)) {
      getRequestStatusMap(map).remove(taskId);
    }
  }

  /** Helper method to get a request status map given the name. */
  public static Map<String, TaskObject> getRequestStatusMap(String key) {
    return REQUEST_STATUS_MAP.get(key);
  }
}
