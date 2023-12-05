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
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.RenameCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definition for renaming a Solr core. */
@Path("/cores/{coreName}/rename")
public interface RenameCoreApi {
  @POST
  @Operation(
      summary = "The RENAME action changes the name of a Solr core",
      tags = {"cores"})
  SolrJerseyResponse renameCore(
      @PathParam("coreName") String coreName,
      @RequestBody(description = "Additional properties related to the core renaming")
          RenameCoreRequestBody requestBody)
      throws Exception;
}
