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

import static org.apache.solr.client.api.util.Constants.ADDTL_FIELDS_PROPERTY;
import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.util.List;
import org.apache.solr.client.api.model.SchemaChange;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpsertDynamicFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldTypeOperation;
import org.apache.solr.client.api.util.StoreApiParameters;

@Path(INDEX_PATH_PREFIX + "/schema")
public interface UpdateSchemaApi {
  @PUT
  @Path("/fields/{fieldName}")
  @StoreApiParameters
  @Operation(
      summary = "Add a standard (i.e. non-dynamic) field with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse addField(
      @PathParam("fieldName") String fieldName,
      @RequestBody(
              extensions = {
                @Extension(
                    properties = {@ExtensionProperty(name = ADDTL_FIELDS_PROPERTY, value = "true")})
              })
          UpsertFieldOperation requestBody)
      throws Exception;

  @DELETE
  @Path("/fields/{fieldName}")
  @StoreApiParameters
  @Operation(
      summary = "Remove the non-dynamic field with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse deleteField(@PathParam("fieldName") String fieldName) throws Exception;

  @PUT
  @Path("/dynamicfields/{dynamicFieldName}")
  @StoreApiParameters
  @Operation(
      summary = "Create a new dynamic field with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse addDynamicField(
      @PathParam("dynamicFieldName") String dynamicFieldName,
      @RequestBody(
              extensions = {
                @Extension(
                    properties = {@ExtensionProperty(name = ADDTL_FIELDS_PROPERTY, value = "true")})
              })
          UpsertDynamicFieldOperation requestBody)
      throws Exception;

  @DELETE
  @Path("/dynamicfields/{dynamicFieldName}")
  @StoreApiParameters
  @Operation(
      summary = "Remove the dynamic field with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse deleteDynamicField(@PathParam("dynamicFieldName") String dynamicFieldName)
      throws Exception;

  @PUT
  @Path("/fieldtypes/{fieldTypeName}")
  @StoreApiParameters
  @Operation(
      summary = "Add a new field-type with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse addFieldType(
      @PathParam("fieldTypeName") String fieldTypeName,
      @RequestBody(
              extensions = {
                @Extension(
                    properties = {@ExtensionProperty(name = ADDTL_FIELDS_PROPERTY, value = "true")})
              })
          UpsertFieldTypeOperation requestBody)
      throws Exception;

  @DELETE
  @Path("/fieldtypes/{fieldTypeName}")
  @StoreApiParameters
  @Operation(
      summary = "Remove the field type with the specified name.",
      tags = {"schema"})
  SolrJerseyResponse deleteFieldType(@PathParam("fieldTypeName") String fieldTypeName)
      throws Exception;

  // TODO Gah! Copyfields don't currently have names for us to create/delete by name in an API like
  // /schema/copyfields/{copyFieldName}...figure out how to address this in a way consistent with
  // all our other APIs.  (In the meantime, the functionality is exposed through the bulk API at
  // least.
  //    @POST
  //    @Path("/copyfields/{copyFieldName}")
  //    @StoreApiParameters
  //    @Operation(
  //            summary = "Add a new copy-field with the specified name.",
  //            tags = {"schema"})
  //    SolrJerseyResponse addCopyField(@PathParam("copyFieldName") String copyFieldName,
  // @RequestBody SchemaChangeOperation.AddCopyField requestBody) throws Exception;
  //
  //    @DELETE
  //    @Path("/copyfields/{copyFieldName}")
  //    @StoreApiParameters
  //    @Operation(summary = "Remove the copy-field with the specified name.", tags = {"schema"})
  //    SolrJerseyResponse deleteCopyField(@PathParam("copyFieldName") String copyFieldName) throws
  // Exception;

  @POST
  @Path("/bulk")
  @StoreApiParameters
  @Operation(
      summary = "Perform the specified schema modifications.",
      tags = {"schema"})
  SolrJerseyResponse bulkSchemaModification(List<SchemaChange> requestBody) throws Exception;
}
