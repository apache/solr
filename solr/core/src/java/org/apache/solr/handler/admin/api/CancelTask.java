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
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.TasksApi;
import org.apache.solr.client.api.model.CancelTaskResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.ActiveTaskQuerySupport;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CancellableCollector;

public class CancelTask extends JerseyResource implements TasksApi.Cancel {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public CancelTask(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(READ_PERM)
  public CancelTaskResponse cancelRunningTask(String taskID) throws Exception {
    final CancelTaskResponse response = instantiateJerseyResponse(CancelTaskResponse.class);

    boolean isTaskCancelled = ActiveTaskQuerySupport.cancelTask(solrQueryRequest, taskID);

    if (isTaskCancelled) {
      response.status = CancelTaskResponse.CancellationStatus.SUCCESS;
      return response;
    }
    throw new SolrException(
        SolrException.ErrorCode.NOT_FOUND, CancelTaskResponse.CancellationStatus.NOT_FOUND.name());
  }

  public static boolean cancelTaskActiveOnThisShard(
      SolrQueryRequest solrQueryRequest, String taskId) {
    CancellableCollector cancellableTask =
        solrQueryRequest.getCore().getCancellableQueryTracker().getCancellableTask(taskId);
    if (cancellableTask != null) {
      cancellableTask.cancel();
      return true;
    }
    return false;
  }
}
