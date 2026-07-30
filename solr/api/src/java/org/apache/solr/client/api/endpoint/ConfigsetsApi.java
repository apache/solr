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
import static org.apache.solr.client.api.util.Constants.RAW_OUTPUT_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
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
  @Path("/configsets/{configSetName}")
  interface Delete {
    @DELETE
    @Operation(summary = "Delete an existing configset.", tags = "configsets")
    SolrJerseyResponse deleteConfigSet(@PathParam("configSetName") String configSetName)
        throws Exception;
  }

  /** V2 API definition for downloading an existing configset as a ZIP archive. */
  @Path("/configsets/{configSetName}")
  interface Download {
    @GET
    @Path("/files")
    @Operation(
        summary = "Download a configset as a ZIP archive.",
        tags = {"configsets"},
        extensions = {
          @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
        })
    @Produces("application/zip")
    Response downloadConfigSet(@PathParam("configSetName") String configSetName) throws Exception;
  }

  /**
   * V2 API definition for reading a single file from an existing configset.
   *
   * <p>Returns the raw bytes of the file, suitable for both text and binary files.
   *
   * <p>Equivalent to GET /api/configsets/{configSetName}/files/{filePath}
   */
  @Path("/configsets/{configSetName}")
  interface GetFile {
    @GET
    @Path("/files/{filePath:.+}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Operation(
        summary = "Get the raw contents of a file in a configset.",
        tags = {"configsets"},
        extensions = {
          @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
        })
    StreamingOutput getConfigSetFile(
        @PathParam("configSetName") String configSetName, @PathParam("filePath") String filePath)
        throws Exception;
  }

  /**
   * V2 API definition for uploading an entire configset as a ZIP archive.
   *
   * <p>Equivalent to the existing v1 API /admin/configs?action=UPLOAD
   */
  @Path("/configsets/{configSetName}")
  interface Upload {
    @PUT
    @Operation(summary = "Upload a configset as a ZIP archive.", tags = "configsets")
    SolrJerseyResponse uploadConfigSet(
        @PathParam("configSetName") String configSetName,
        @QueryParam("overwrite") Boolean overwrite,
        @QueryParam("cleanup") Boolean cleanup,
        @RequestBody(
                required = true,
                extensions = {
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      })
                })
            InputStream requestBody)
        throws IOException;
  }

  /**
   * V2 API definition for putting a single file to an existing configset.
   *
   * <p>This endpoint allows updating individual configuration files without re-uploading the entire
   * configset. The file path is specified as part of the URL path.
   */
  @Path("/configsets/{configSetName}")
  interface PutFile {
    @PUT
    @Path("/files/{filePath:.+}")
    @Operation(summary = "Upload a single file to a configset.", tags = "configsets")
    SolrJerseyResponse uploadConfigSetFile(
        @PathParam("configSetName") String configSetName,
        @PathParam("filePath") String filePath,
        @RequestBody(
                required = true,
                extensions = {
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      })
                })
            InputStream requestBody)
        throws IOException;
  }
}
