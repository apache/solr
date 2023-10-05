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

import static org.apache.solr.client.api.util.Constants.INDEX_NAME_PATH_PARAMETER;
import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;
import static org.apache.solr.client.api.util.Constants.INDEX_TYPE_PATH_PARAMETER;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.api.model.SchemaInfoResponse;
import org.apache.solr.client.api.model.SchemaNameResponse;
import org.apache.solr.client.api.model.SchemaSimilarityResponse;
import org.apache.solr.client.api.model.SchemaUniqueKeyResponse;
import org.apache.solr.client.api.model.SchemaVersionResponse;
import org.apache.solr.client.api.model.SchemaZkVersionResponse;
import org.apache.solr.client.api.util.IndexApiParameters;

@Path(INDEX_PATH_PREFIX + "/schema")
public interface GetSchemaApi {

  // TODO NOCOMMIT Use IndexApiParameters for this method - left off currently for a demo
  @GET
  @Operation(
      summary = "Fetch the entire schema of the specified core or collection",
      parameters = {
        @Parameter(
            name = INDEX_TYPE_PATH_PARAMETER,
            in = ParameterIn.PATH,
            schema = @Schema(implementation = IndexType.class, enumAsRef = true)),
        @Parameter(name = INDEX_NAME_PATH_PARAMETER, in = ParameterIn.PATH)
      },
      tags = {"schema"})
  SchemaInfoResponse getSchemaInfo();

  @GET
  @Path("/name")
  @IndexApiParameters
  @Operation(
      summary = "Get the name of the schema used by the specified core or collection",
      tags = {"schema"})
  SchemaNameResponse getSchemaName() throws Exception;

  @GET
  @Path("/similarity")
  @IndexApiParameters
  @Operation(
      summary = "Get the default similarity configuration used by the specified core or collection",
      tags = {"schema"})
  SchemaSimilarityResponse getSchemaSimilarity();

  @GET
  @Path("/uniquekey")
  @IndexApiParameters
  @Operation(
      summary = "Fetch the uniquekey of the specified core or collection",
      tags = {"schema"})
  SchemaUniqueKeyResponse getSchemaUniqueKey();

  @GET
  @Path("/version")
  @IndexApiParameters
  @Operation(
      summary = "Fetch the schema version currently used by the specified core or collection",
      tags = {"schema"})
  SchemaVersionResponse getSchemaVersion();

  @GET
  @Path("/zkversion")
  @IndexApiParameters
  @Operation(
      summary = "Fetch the schema version currently used by the specified core or collection",
      tags = {"schema"})
  SchemaZkVersionResponse getSchemaZkVersion(
      @DefaultValue("-1") @QueryParam("refreshIfBelowVersion") Integer refreshIfBelowVersion)
      throws Exception;
}
