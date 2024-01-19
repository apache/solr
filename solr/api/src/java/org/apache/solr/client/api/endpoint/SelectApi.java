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

import static org.apache.solr.client.api.util.Constants.GENERIC_ENTITY_PROPERTY;
import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/**
 * V2 API implementation shim for Solr's querying or otherwise inspecting documents in a core or
 * collection.
 *
 * <p>Due to the complexity and configurability of Solr's '/select' endpoint, this interface doesn't
 * attempt to be exhaustive in describing /select inputs and outputs. Rather, it exists to give
 * Solr's OAS (and the clients generated from that) an approximate view of the endpoint until its
 * inputs and outputs can be understood more fully.
 */
@Path(INDEX_PATH_PREFIX + "/select")
public interface SelectApi {
  @GET
  @StoreApiParameters
  @Operation(
      summary = "Query a Solr core or collection using individual query parameters",
      tags = {"querying"})
  FlexibleSolrJerseyResponse query(
      @QueryParam("q") String query,
      @QueryParam("fq") List<String> filterQueries,
      @QueryParam("fl") String fieldList,
      @QueryParam("rows") Integer rows);

  @POST
  @StoreApiParameters
  @Operation(
      summary = "Query a Solr core or collection using the structured request DSL",
      tags = {"querying"})
  // TODO Find way to bundle the request-body annotations below for re-use on other similar
  // endpoints.
  FlexibleSolrJerseyResponse jsonQuery(
      @Parameter(required = true)
          @RequestBody(
              required = true,
              extensions = {
                @Extension(
                    properties = {
                      @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                    })
              })
          InputStream structuredRequest);
}
