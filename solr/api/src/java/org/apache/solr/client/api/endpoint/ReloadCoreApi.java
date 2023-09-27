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
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.ReloadCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definition for reloading an individual core. */
@Path("/cores/{coreName}/reload")
public interface ReloadCoreApi {

  @POST
  @Operation(
      summary = "Reload the specified core.",
      tags = {"cores"})
  SolrJerseyResponse reloadCore(
      @Parameter(description = "The name of the core to reload.", required = true)
          @PathParam("coreName")
          String coreName,
      @Schema(description = "Additional parameters for reloading the core") @RequestBody
          ReloadCoreRequestBody reloadCoreRequestBody)
      throws Exception;
}
