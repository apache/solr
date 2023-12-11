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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * V2 API for checking the status of a core-level asynchronous command.
 *
 * <p>This API (GET /api/cores/command-status/someId is analogous to the v1
 * /admin/cores?action=REQUESTSTATUS command. It is not to be confused with the more robust
 * asynchronous command support offered under the v2 `/cluster/command-status` path (or the
 * corresponding v1 path `/solr/admin/collections?action=REQUESTSTATUS`). Async support at the core
 * level differs in that command IDs are local to individual Solr nodes and are not persisted across
 * restarts.
 *
 * @see org.apache.solr.client.api.model.RequestCoreCommandStatusResponseBody
 */
@Path("/cores/command-status/")
public interface RequestCoreCommandStatusApi {
  @Path("/{requestId}")
  @GET
  @Operation(
      summary = "Request the status of an already submitted asynchronous CoreAdmin API call.",
      tags = {"cores"})
  SolrJerseyResponse getCommandStatus(
      @Parameter(
              description = "The user defined request-id for the asynchronous request.",
              required = true)
          @PathParam("requestId")
          String id);
}
