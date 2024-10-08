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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for reloading an individual core.
 *
 * <p>The new API (POST /v2/cores/coreName/reload is analogous to the v1 /admin/cores?action=RELOAD
 * command.
 */
@Path("/cores/{coreName}/reload")
public class ReloadCoreAPI extends CoreAdminAPIBase {

  @Inject
  public ReloadCoreAPI(
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse,
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse reloadCore(
      @Parameter(description = "The name of the core to reload.", required = true)
          @PathParam("coreName")
          String coreName,
      @Schema(description = "Additional parameters for reloading the core") @RequestBody
          ReloadCoreAPI.ReloadCoreRequestBody reloadCoreRequestBody)
      throws Exception {
    SolrJerseyResponse solrJerseyResponse = instantiateJerseyResponse(SolrJerseyResponse.class);
    return handlePotentiallyAsynchronousTask(
        solrJerseyResponse,
        coreName,
        (reloadCoreRequestBody == null) ? null : reloadCoreRequestBody.async,
        "reload",
        () -> {
          coreContainer.reload(coreName);
          return solrJerseyResponse;
        });
  }

  public static class ReloadCoreRequestBody implements JacksonReflectMapWriter {
    @Schema(description = "Request ID to track this action which will be processed asynchronously.")
    @JsonProperty("async")
    public String async;
  }
}
