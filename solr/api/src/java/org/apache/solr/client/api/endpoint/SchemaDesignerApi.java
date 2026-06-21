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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import java.io.InputStream;
import java.util.List;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.SchemaDesignerAddRequestBody;
import org.apache.solr.client.api.model.SchemaDesignerCollectionsResponse;
import org.apache.solr.client.api.model.SchemaDesignerConfigsResponse;
import org.apache.solr.client.api.model.SchemaDesignerInfoResponse;
import org.apache.solr.client.api.model.SchemaDesignerPublishResponse;
import org.apache.solr.client.api.model.SchemaDesignerResponse;
import org.apache.solr.client.api.model.SchemaDesignerSchemaDiffResponse;
import org.apache.solr.client.api.model.SchemaDesignerUpdateRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definitions for the Solr Schema Designer. */
@Path("/schema-designer")
public interface SchemaDesignerApi {

  @GET
  @Path("/{configSet}")
  @Operation(
      summary = "Get info about a configSet being designed.",
      tags = {"schema-designer"})
  SchemaDesignerInfoResponse getInfo(@PathParam("configSet") String configSet) throws Exception;

  @POST
  @Path("/{configSet}/prep")
  @Operation(
      summary = "Prepare a mutable configSet copy for schema design.",
      tags = {"schema-designer"})
  SchemaDesignerResponse prepNewSchema(
      @PathParam("configSet") String configSet, @QueryParam("copyFrom") String copyFrom)
      throws Exception;

  @DELETE
  @Path("/{configSet}")
  @Operation(
      summary = "Clean up temporary resources for a schema being designed.",
      tags = {"schema-designer"})
  SolrJerseyResponse cleanupTempSchema(@PathParam("configSet") String configSet) throws Exception;

  @PUT
  @Path("/{configSet}/file")
  @Operation(
      summary = "Update the contents of a file in a configSet being designed.",
      tags = {"schema-designer"})
  SchemaDesignerResponse updateFileContents(
      @PathParam("configSet") String configSet,
      @QueryParam("file") String file,
      @RequestBody(
              required = true,
              extensions = {
                @Extension(
                    properties = {
                      @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                    })
              })
          InputStream fileContents)
      throws Exception;

  @GET
  @Path("/{configSet}/sample")
  @Operation(
      summary = "Get a sample value and analysis for a field.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse getSampleValue(
      @PathParam("configSet") String configSet,
      @QueryParam("field") String fieldName,
      @QueryParam("uniqueKeyField") String idField,
      @QueryParam("docId") String docId)
      throws Exception;

  // TODO: this sub-resource belongs in ConfigsetsApi as GET
  // /configsets/{configSetName}/collections;
  // move it there in a follow-up so it is reusable outside the schema designer.
  @GET
  @Path("/{configSet}/collections")
  @Operation(
      summary = "List collections that use a given configSet.",
      tags = {"schema-designer"})
  SchemaDesignerCollectionsResponse listCollectionsForConfig(
      @PathParam("configSet") String configSet) throws Exception;

  @GET
  @Path("/configs")
  @Operation(
      summary = "List all configSets available for schema design.",
      tags = {"schema-designer"})
  SchemaDesignerConfigsResponse listConfigs() throws Exception;

  @POST
  @Path("/{configSet}")
  @Operation(
      summary = "Add a new field, field type, or dynamic field to the schema being designed.",
      tags = {"schema-designer"})
  SchemaDesignerResponse addSchemaObject(
      @PathParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      SchemaDesignerAddRequestBody requestBody)
      throws Exception;

  @PUT
  @Path("/{configSet}")
  @Operation(
      summary = "Update an existing field or field type in the schema being designed.",
      tags = {"schema-designer"})
  SchemaDesignerResponse updateSchemaObject(
      @PathParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      SchemaDesignerUpdateRequestBody requestBody)
      throws Exception;

  @PUT
  @Path("/{configSet}/publish")
  @Operation(
      summary = "Publish the designed schema to a live configSet.",
      tags = {"schema-designer"})
  SchemaDesignerPublishResponse publish(
      @PathParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      @QueryParam("newCollection") String newCollection,
      @QueryParam("reloadCollections") @DefaultValue("false") Boolean reloadCollections,
      @QueryParam("numShards") @DefaultValue("1") Integer numShards,
      @QueryParam("replicationFactor") @DefaultValue("1") Integer replicationFactor,
      @QueryParam("indexToCollection") @DefaultValue("false") Boolean indexToCollection,
      @QueryParam("cleanupTemp") @DefaultValue("true") Boolean cleanupTempParam,
      @QueryParam("disableDesigner") @DefaultValue("false") Boolean disableDesigner)
      throws Exception;

  /**
   * Analyzes sample documents to suggest a schema.
   *
   * <p>Sample documents are read from the HTTP request body (not declared as a parameter on this
   * interface — see {@code SchemaDesigner#loadSampleDocuments}) and dispatched to a parser based on
   * the {@code Content-Type} header.
   */
  @POST
  @Path("/{configSet}/analyze")
  @Operation(
      summary = "Analyze sample documents and suggest a schema.",
      description =
          "Sample documents are supplied in the request body. The Content-Type header selects the"
              + " parser: application/json, text/xml or application/xml, text/csv or"
              + " application/csv, or text/plain or application/octet-stream (treated as JSON"
              + " lines). Capped at 5MB and 1000 documents.",
      tags = {"schema-designer"})
  SchemaDesignerResponse analyze(
      @PathParam("configSet") String configSet,
      @QueryParam("schemaVersion") Integer schemaVersion,
      @QueryParam("copyFrom") String copyFrom,
      @QueryParam("uniqueKeyField") String uniqueKeyField,
      @QueryParam("languages") List<String> languages,
      @QueryParam("enableDynamicFields") Boolean enableDynamicFields,
      @QueryParam("enableFieldGuessing") Boolean enableFieldGuessing,
      @QueryParam("enableNestedDocs") Boolean enableNestedDocs)
      throws Exception;

  @GET
  @Path("/{configSet}/query")
  @Operation(
      summary = "Query the temporary collection used during schema design.",
      description =
          "All standard Solr query parameters (q, fq, fl, sort, facet.*, hl.*, etc.) are"
              + " forwarded directly to the temporary collection. The configSet path parameter"
              + " identifies which designer session to query; it is not a query parameter itself.",
      tags = {"schema-designer"})
  FlexibleSolrJerseyResponse query(@PathParam("configSet") String configSet) throws Exception;

  @GET
  @Path("/{configSet}/diff")
  @Operation(
      summary = "Get the diff between the designed schema and the published schema.",
      tags = {"schema-designer"})
  SchemaDesignerSchemaDiffResponse getSchemaDiff(@PathParam("configSet") String configSet)
      throws Exception;
}
