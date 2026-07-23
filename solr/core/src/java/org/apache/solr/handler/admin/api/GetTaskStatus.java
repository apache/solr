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

import static org.apache.solr.client.api.model.TaskStatusResponse.TaskStatus;
import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.TasksApi;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;

public class GetTaskStatus extends JerseyResource implements TasksApi.Status {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public GetTaskStatus(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(READ_PERM)
  public TaskStatusResponse getTaskStatus(String taskID) throws Exception {
    final TaskStatusResponse response = instantiateJerseyResponse(TaskStatusResponse.class);

    boolean isTaskActive =
        solrQueryRequest.getCore().getCancellableQueryTracker().isQueryIdActive(taskID);

    response.taskStatus = (isTaskActive) ? TaskStatus.ACTIVE : TaskStatus.INACTIVE;

    return response;
  }
}
