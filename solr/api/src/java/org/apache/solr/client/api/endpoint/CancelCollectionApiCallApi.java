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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definition for canceling CollectionApi Call. */
@Path("/cluster/cancel/{requestid}")
public interface CancelCollectionApiCallApi {
  @PUT
  @Operation(
      summary = "Cancel collections api call",
      tags = {"cluster-cancel"})
  SolrJerseyResponse cancelCollectionApiCall(
      @Parameter(description = "The name of the collection api to be canceled.", required = true)
          @PathParam("requestid")
          String requestId,
      @Parameter(description = "An ID to track the request asynchronously") @QueryParam("async")
          String asyncId)
      throws Exception;
}
