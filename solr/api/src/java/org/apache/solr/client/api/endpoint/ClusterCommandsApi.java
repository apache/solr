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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.DeleteClusterCommandStatusResponse;
import org.apache.solr.client.api.model.GetClusterCommandStatusResponse;

/**
 * V2 API definitions for cluster-level asynchronous Collection API command status.
 *
 * <p>These APIs are analogous to the v1 {@code /admin/collections?action=REQUESTSTATUS} and {@code
 * /admin/collections?action=DELETESTATUS} commands. They are not to be confused with the node-local
 * {@link GetNodeCommandStatusApi} under {@code /api/node/commands}.
 */
@Path("/cluster/commands")
public interface ClusterCommandsApi {

  @GET
  @Path("/{requestId}")
  @Operation(
      summary = "Request the status of an already submitted asynchronous Collection API call.",
      tags = {"cluster"})
  GetClusterCommandStatusResponse getClusterCommandStatus(
      @Parameter(
              description = "The user defined request-id for the asynchronous request.",
              required = true)
          @PathParam("requestId")
          String requestId)
      throws Exception;

  @DELETE
  @Path("/{requestId}")
  @Operation(
      summary =
          "Delete the stored status of a completed or failed asynchronous Collection API call.",
      tags = {"cluster"})
  DeleteClusterCommandStatusResponse deleteClusterCommandStatus(
      @Parameter(
              description = "The user defined request-id whose stored response should be cleared.",
              required = true)
          @PathParam("requestId")
          String requestId)
      throws Exception;

  @DELETE
  @Operation(
      summary =
          "Delete the stored status of all completed and failed asynchronous Collection API calls.",
      tags = {"cluster"})
  DeleteClusterCommandStatusResponse deleteAllClusterCommandStatuses() throws Exception;
}
