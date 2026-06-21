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

import static org.apache.solr.client.api.model.TaskStatusResponse.TaskStatus.ACTIVE;
import static org.apache.solr.common.params.CommonParams.TASK_CHECK_UUID;

import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.api.ActiveTask;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

/** Handles request for listing all active cancellable tasks and get status check of any taskId. */
public class ActiveTasksListHandler extends TaskManagementHandler {
  // This can be a parent level member but we keep it here to allow future handlers to have
  // a custom list of components

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String taskStatusCheckUUID = req.getParams().get(TASK_CHECK_UUID, null);

    if (taskStatusCheckUUID != null) {
      TaskStatusResponse taskStatusResponse =
          new ActiveTask(req).getTaskStatus(taskStatusCheckUUID);
      boolean taskStatus = taskStatusResponse.taskStatus.equals(ACTIVE);
      rsp.add("taskStatus", taskStatus);

    } else {
      NamedList<String> tasks = new SimpleOrderedMap<>();
      List<ActiveTaskDetails> taskList = new ActiveTask(req).listAllActiveTasks().taskList;
      if (taskList != null) {
        for (ActiveTaskDetails task : taskList) {
          tasks.add(task.taskID, task.taskQuery);
        }
      }
      rsp.add("taskList", tasks);
    }
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Active Tasks List";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if (path.startsWith("/tasks/list")) {
      return this;
    }
    return null;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return List.of();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(ActiveTask.class);
  }
}
