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
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/**
 * V2 API definition for cancelling a currently-running task.
 *
 * <p>This API (GET /v2/collections/{collectionName}/tasks/cancel and GET
 * /v2/cores/{coreName}/tasks/cancel) is analogous to the v1 /solr/collectionName/tasks/cancel API.
 */
@Path(INDEX_PATH_PREFIX + "/tasks/cancel")
public interface CancelTaskApi {

  @GET
  @StoreApiParameters
  @Operation(
      summary = "Cancel a currently-running task",
      tags = {"tasks"})
  FlexibleSolrJerseyResponse cancelTask(@QueryParam("queryUUID") String queryUUID) throws Exception;
}
