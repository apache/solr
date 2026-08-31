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

import static org.apache.solr.common.params.CommonParams.QUERY_UUID;

import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.handler.admin.api.CancelTask;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;

/** Handles requests for query cancellation for cancellable queries */
public class TaskCancellationHandler extends TaskManagementHandler {
  // This can be a parent level member but we keep it here to allow future handlers to have
  // a custom list of components

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String taskCancellationID = req.getParams().get(QUERY_UUID, null);

    if (taskCancellationID == null) {
      throw new IllegalArgumentException(
          "Query cancellation was requested but no query UUID for cancellation was given");
    }

    boolean isTaskCancelled = ActiveTaskQuerySupport.cancelTask(req, taskCancellationID);

    // FRAGILE: these exact "status" message strings are also used as the cross-shard
    // cancellation signal -- ActiveTaskQuerySupport.mergeCancellationStatus() matches shard
    // sub-responses via `.contains("cancelled successfully")`, since this handler has no
    // dedicated machine-readable field for it (unlike ActiveTasksListHandler's isShardedRequest
    // handling for task status, which emits a plain boolean for shard-to-shard requests).
    // Changing this wording will silently break cross-shard cancellation aggregation; see
    // TestTaskManagement.testCrossShardTaskCancellationVisibility, which will start failing if
    // that happens.
    if (isTaskCancelled) {
      rsp.add("status", "Query with queryID " + taskCancellationID + " cancelled successfully");
      rsp.add("responseCode", 200);
    } else {
      rsp.add("status", "Query with queryID " + taskCancellationID + " not found");
      rsp.add("responseCode", 404);
    }
  }

  @Override
  public String getDescription() {
    return "Cancel active tasks";
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
    if (path.startsWith("/tasks/cancel")) {
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
    return List.of(CancelTask.class);
  }
}
