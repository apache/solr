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

package org.apache.solr.client.api.endpoint;

import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

@Path(INDEX_PATH_PREFIX)
public interface TasksApi {

  @Path("/tasks")
  interface List {
    @GET
    @StoreApiParameters
    @Operation(
        summary = "Lists all the active tasks.",
        tags = {"tasks"})
    ListActiveTaskResponse listAllActiveTasks() throws Exception;
  }

  @Path("/tasks/{taskID}")
  interface Status {
    @GET
    @StoreApiParameters
    @Operation(
        summary = "Status of a specific task.",
        tags = {"tasks"})
    TaskStatusResponse getTaskStatus(@PathParam("taskID") String taskID) throws Exception;
  }
}
