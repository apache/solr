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

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.request.beans.MoveReplicaPayload;
import org.apache.solr.handler.admin.LoggingHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import javax.ws.rs.Path;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

/**
 * V2 API for getting or setting log levels on an individual node.
 *
 * <p>This API (GET /v2/node/logging) is analogous to the v1 /admin/info/logging.
 */
@OpenAPIDefinition(info = @Info(title = "Solr v2 API", description = "Some description", license = @License(name = "ASL 2.0"), version = "0.0.1"))
@Path("/node/logging")
public class NodeLoggingAPI {

  private final LoggingHandler handler;

  public NodeLoggingAPI(LoggingHandler handler) {
    this.handler = handler;
  }

  // TODO See SOLR-15823 for background on the (less than ideal) permission chosen here.
  @javax.ws.rs.GET
  @Operation(
          summary = "Get and set logging levels for the receiving Solr node",
          tags = {"logs", "node"},
          description = "API to either retrieve log levels (when GET verb is used), or update the log level on the receiving node.",
          parameters = {
                  @Parameter(name = "set", description = "desc2", schema=@Schema(type="string"), in = ParameterIn.QUERY),
                  @Parameter(name="fakeParam", description = "desc1", required = true, in = ParameterIn.QUERY)
          },
          requestBody = @RequestBody(
                  description = "asdf",
                  content = {
                    @Content(schema = @Schema(nullable = true, type = "object"))
                  }
          ),
          responses = {
                  @ApiResponse(
                          description = "Successful", responseCode = "200", content=@Content(
                                  schema=@Schema(implementation = MoveReplicaPayload.class) // Just something to test out
                  ))
          })
  @EndPoint(
      path = {"/node/logging"},
      method = GET,
      permission = CONFIG_EDIT_PERM)
  public void getOrSetLogLevels(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    handler.handleRequestBody(req, rsp);
  }


  //public LogLevelResponse jaxRsGetOrSetLogLevels(@QueryParam("set") String logString) {
    //<do work>
  //}
}
