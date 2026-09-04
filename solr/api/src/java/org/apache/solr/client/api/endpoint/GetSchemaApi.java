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
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SchemaGetDynamicFieldInfoResponse;
import org.apache.solr.client.api.model.SchemaGetFieldInfoResponse;
import org.apache.solr.client.api.model.SchemaGetFieldTypeInfoResponse;
import org.apache.solr.client.api.model.SchemaInfoResponse;
import org.apache.solr.client.api.model.SchemaListCopyFieldsResponse;
import org.apache.solr.client.api.model.SchemaListDynamicFieldsResponse;
import org.apache.solr.client.api.model.SchemaListFieldTypesResponse;
import org.apache.solr.client.api.model.SchemaListFieldsResponse;
import org.apache.solr.client.api.model.SchemaNameResponse;
import org.apache.solr.client.api.model.SchemaSimilarityResponse;
import org.apache.solr.client.api.model.SchemaUniqueKeyResponse;
import org.apache.solr.client.api.model.SchemaVersionResponse;
import org.apache.solr.client.api.model.SchemaZkVersionResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

@Path(INDEX_PATH_PREFIX + "/schema")
public interface GetSchemaApi {

  @Path(INDEX_PATH_PREFIX + "/schema")
  interface Fields {

    @GET
    @Path("/fields")
    @StoreApiParameters
    @Operation(
        summary = "List all non-dynamic fields in the schema of the specified core or collection",
        tags = {"schema"})
    SchemaListFieldsResponse listSchemaFields();

    @GET
    @Path("/fields/{fieldName}")
    @StoreApiParameters
    @Operation(
        summary = "Get detailed info about a single non-dynamic field",
        tags = {"schema"})
    SchemaGetFieldInfoResponse getFieldInfo(@PathParam("fieldName") String fieldName);

    @GET
    @Path("/copyfields")
    @StoreApiParameters
    @Operation(
        summary = "List all copy-fields in the schema of the specified core or collection",
        tags = {"schema"})
    SchemaListCopyFieldsResponse listCopyFields();

    @GET
    @Path("/dynamicfields")
    @StoreApiParameters
    @Operation(
        summary = "List all dynamic-fields in the schema of the specified core or collection",
        tags = {"schema"})
    SchemaListDynamicFieldsResponse listDynamicFields();

    @GET
    @Path("/dynamicfields/{fieldName}")
    @StoreApiParameters
    @Operation(
        summary = "Get detailed info about a single dynamic field",
        tags = {"schema"})
    SchemaGetDynamicFieldInfoResponse getDynamicFieldInfo(@PathParam("fieldName") String fieldName);

    @GET
    @Path("/fieldtypes")
    @StoreApiParameters
    @Operation(
        summary = "List all field types in the schema used by the specified core or collection",
        tags = {"schema"})
    SchemaListFieldTypesResponse listSchemaFieldTypes();

    @GET
    @Path("/fieldtypes/{fieldTypeName}")
    @StoreApiParameters
    @Operation(
        summary = "Get detailed info about a single field type",
        tags = {"schema"})
    SchemaGetFieldTypeInfoResponse getFieldTypeInfo(
        @PathParam("fieldTypeName") String fieldTypeName);
  }

  @GET
  @StoreApiParameters
  @Operation(
      summary = "Fetch the entire schema of the specified core or collection",
      tags = {"schema"})
  SchemaInfoResponse getSchemaInfo();

  @GET
  @Path("/name")
  @StoreApiParameters
  @Operation(
      summary = "Get the name of the schema used by the specified core or collection",
      tags = {"schema"})
  SchemaNameResponse getSchemaName() throws Exception;

  @GET
  @Path("/similarity")
  @StoreApiParameters
  @Operation(
      summary = "Get the default similarity configuration used by the specified core or collection",
      tags = {"schema"})
  SchemaSimilarityResponse getSchemaSimilarity();

  @GET
  @Path("/uniquekey")
  @StoreApiParameters
  @Operation(
      summary = "Fetch the uniquekey of the specified core or collection",
      tags = {"schema"})
  SchemaUniqueKeyResponse getSchemaUniqueKey();

  @GET
  @Path("/version")
  @StoreApiParameters
  @Operation(
      summary = "Fetch the schema version currently used by the specified core or collection",
      tags = {"schema"})
  SchemaVersionResponse getSchemaVersion();

  @GET
  @Path("/zkversion")
  @StoreApiParameters
  @Operation(
      summary = "Fetch the schema version currently used by the specified core or collection",
      tags = {"schema"})
  SchemaZkVersionResponse getSchemaZkVersion(
      @DefaultValue("-1") @QueryParam("refreshIfBelowVersion") Integer refreshIfBelowVersion)
      throws Exception;
}
