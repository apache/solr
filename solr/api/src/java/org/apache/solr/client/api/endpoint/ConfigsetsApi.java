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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.CloneConfigsetRequestBody;
import org.apache.solr.client.api.model.ListConfigsetsResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;

public interface ConfigsetsApi {

  /** V2 API definition for listing the configsets available to this SolrCloud cluster. */
  @Path("/configsets")
  interface List {
    @GET
    @Operation(
        summary = "List the configsets available to Solr.",
        tags = {"configsets"})
    ListConfigsetsResponse listConfigSet() throws Exception;
  }

  /**
   * V2 API definition for creating a (possibly slightly modified) copy of an existing configset
   *
   * <p>Equivalent to the existing v1 API /admin/configs?action=CREATE
   */
  @Path("/configsets")
  interface Clone {
    @POST
    @Operation(
        summary = "Create a new configset modeled on an existing one.",
        tags = {"configsets"})
    SolrJerseyResponse cloneExistingConfigSet(CloneConfigsetRequestBody requestBody)
        throws Exception;
  }

  /**
   * V2 API definition for deleting an existing configset.
   *
   * <p>Equivalent to the existing v1 API /admin/configs?action=DELETE
   */
  @Path("/configsets/{configsetName}")
  interface Delete {
    @DELETE
    @Operation(summary = "Delete an existing configset.", tags = "configsets")
    SolrJerseyResponse deleteConfigSet(@PathParam("configsetName") String configSetName)
        throws Exception;
  }
}
