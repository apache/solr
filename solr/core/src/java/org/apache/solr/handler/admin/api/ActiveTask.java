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

import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.TasksApi;
import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;

public class ActiveTask extends JerseyResource implements TasksApi {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public ActiveTask(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(READ_PERM)
  public ListActiveTaskResponse listAllActiveTasks() throws Exception {
    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);
    response.taskList = extractActiveTaskLists();
    return response;
  }

  @Override
  @PermissionName(READ_PERM)
  public TaskStatusResponse getTaskStatus(String taskID) throws Exception {
    final TaskStatusResponse response = instantiateJerseyResponse(TaskStatusResponse.class);

    boolean isTaskActive =
        solrQueryRequest.getCore().getCancellableQueryTracker().isQueryIdActive(taskID);

    response.taskStatus =
        (isTaskActive)
            ? TaskStatusResponse.TaskStatus.ACTIVE
            : TaskStatusResponse.TaskStatus.INACTIVE;

    return response;
  }

  private List<ActiveTaskDetails> extractActiveTaskLists() {
    Iterator<Map.Entry<String, String>> iterator =
        solrQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

    List<ActiveTaskDetails> activeTaskDetails = new ArrayList<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      activeTaskDetails.add(new ActiveTaskDetails(entry.getKey(), entry.getValue()));
    }

    return activeTaskDetails;
  }
}
