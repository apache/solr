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

import static org.apache.solr.client.api.util.Constants.RAW_OUTPUT_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definitions for the Solr Schema Designer. */
@Path("/schema-designer")
public interface SchemaDesignerApi {

  @GET
  @Path("/info")
  @Operation(
      summary = "Get info about a configSet being designed.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse getInfo(@QueryParam("configSet") String configSet) throws Exception;

  @POST
  @Path("/prep")
  @Operation(
      summary = "Prepare a mutable configSet copy for schema design.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse prepNewSchema(
      @QueryParam("configSet") String configSet, @QueryParam("copyFrom") String copyFrom)
      throws Exception;

  @PUT
  @Path("/cleanup")
  @Operation(
      summary = "Clean up temporary resources for a schema being designed.",
      tags = {"schema-designer"})
  SolrJerseyResponse cleanupTempSchema(@QueryParam("configSet") String configSet) throws Exception;

  @GET
  @Path("/file")
  @Operation(
      summary = "Get the contents of a file in a configSet being designed.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse getFileContents(
      @QueryParam("configSet") String configSet, @QueryParam("file") String file) throws Exception;

  @POST
  @Path("/file")
  @Operation(
      summary = "Update the contents of a file in a configSet being designed.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse updateFileContents(
      @QueryParam("configSet") String configSet, @QueryParam("file") String file) throws Exception;

  @GET
  @Path("/sample")
  @Operation(
      summary = "Get a sample value and analysis for a field.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse getSampleValue(
      @QueryParam("configSet") String configSet,
      @QueryParam("field") String fieldName,
      @QueryParam("uniqueKeyField") String idField,
      @QueryParam("docId") String docId)
      throws Exception;

  @GET
  @Path("/collectionsForConfig")
  @Operation(
      summary = "List collections that use a given configSet.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse listCollectionsForConfig(@QueryParam("configSet") String configSet)
      throws Exception;

  @GET
  @Path("/configs")
  @Operation(
      summary = "List all configSets available for schema design.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse listConfigs() throws Exception;

  @GET
  @Path("/download")
  @Operation(
      summary = "Download a configSet as a ZIP archive.",
      tags = {"schema-designer"},
      extensions = {
        @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
      })
  @Produces("application/zip")
  Response downloadConfig(@QueryParam("configSet") String configSet) throws Exception;

  @POST
  @Path("/add")
  @Operation(
      summary = "Add a new field, field type, or dynamic field to the schema being designed.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse addSchemaObject(
      @QueryParam("configSet") String configSet, @QueryParam("schemaVersion") Integer schemaVersion)
      throws Exception;

  @PUT
  @Path("/update")
  @Operation(
      summary = "Update an existing field or field type in the schema being designed.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse updateSchemaObject(
      @QueryParam("configSet") String configSet, @QueryParam("schemaVersion") Integer schemaVersion)
      throws Exception;

  @PUT
  @Path("/publish")
  @Operation(
      summary = "Publish the designed schema to a live configSet.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse publish(
      @QueryParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      @QueryParam("newCollection") String newCollection,
      @QueryParam("reloadCollections") @DefaultValue("false") Boolean reloadCollections,
      @QueryParam("numShards") @DefaultValue("1") Integer numShards,
      @QueryParam("replicationFactor") @DefaultValue("1") Integer replicationFactor,
      @QueryParam("indexToCollection") @DefaultValue("false") Boolean indexToCollection,
      @QueryParam("cleanupTemp") @DefaultValue("true") Boolean cleanupTempParam,
      @QueryParam("disableDesigner") @DefaultValue("false") Boolean disableDesigner)
      throws Exception;

  @POST
  @Path("/analyze")
  @Operation(
      summary = "Analyze sample documents and suggest a schema.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse analyze(
      @QueryParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      @QueryParam("copyFrom") String copyFrom,
      @QueryParam("uniqueKeyField") String uniqueKeyField,
      @QueryParam("languages") List<String> languages,
      @QueryParam("enableDynamicFields") Boolean enableDynamicFields,
      @QueryParam("enableFieldGuessing") Boolean enableFieldGuessing,
      @QueryParam("enableNestedDocs") Boolean enableNestedDocs)
      throws Exception;

  @GET
  @Path("/query")
  @Operation(
      summary = "Query the temporary collection used during schema design.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse query(@QueryParam("configSet") String configSet) throws Exception;

  @GET
  @Path("/diff")
  @Operation(
      summary = "Get the diff between the designed schema and the published schema.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse getSchemaDiff(@QueryParam("configSet") String configSet)
      throws Exception;
}
