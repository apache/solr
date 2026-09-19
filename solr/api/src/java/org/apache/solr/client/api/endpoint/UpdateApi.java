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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import java.io.InputStream;
import org.apache.solr.client.api.model.UpdateResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/** V2 API definitions for indexing documents via the update handler. */
@Path(INDEX_PATH_PREFIX + "/update")
public interface UpdateApi {

  @POST
  @Consumes({
    "application/json",
    "text/json",
    "application/xml",
    "text/xml",
    "application/csv",
    "text/csv",
    "application/javabin",
    "application/cbor"
  })
  @StoreApiParameters
  @Operation(
      summary = "Send updates using any supported content type",
      tags = {"update"})
  UpdateResponse update(
      @Parameter(description = "Commit the update immediately") @QueryParam("commit")
          Boolean commit,
      @Parameter(description = "Commit the update within this many milliseconds")
          @QueryParam("commitWithin")
          Integer commitWithin,
      @Parameter(description = "Overwrite documents with the same unique key")
          @QueryParam("overwrite")
          Boolean overwrite,
      @Parameter(description = "Perform a soft commit") @QueryParam("softCommit")
          Boolean softCommit,
      @Parameter(description = "Include assigned document versions in the response")
          @QueryParam("versions")
          Boolean versions,
      @Parameter(required = true)
          @RequestBody(
              required = true,
              description = "Update content in the format selected by the Content-Type header.",
              extensions =
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      }))
          InputStream requestBody)
      throws Exception;

  @POST
  @Path("/json")
  @Consumes({"application/json", "text/json"})
  @StoreApiParameters
  @Operation(
      summary = "Index documents in JSON format",
      tags = {"update"})
  UpdateResponse updateJson(
      @Parameter(description = "Commit the update immediately") @QueryParam("commit")
          Boolean commit,
      @Parameter(description = "Commit the update within this many milliseconds")
          @QueryParam("commitWithin")
          Integer commitWithin,
      @Parameter(description = "Overwrite documents with the same unique key")
          @QueryParam("overwrite")
          Boolean overwrite,
      @Parameter(description = "Perform a soft commit") @QueryParam("softCommit")
          Boolean softCommit,
      @Parameter(description = "Include assigned document versions in the response")
          @QueryParam("versions")
          Boolean versions,
      @Parameter(required = true)
          @RequestBody(
              required = true,
              description = "JSON update content.",
              extensions =
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      }))
          InputStream requestBody)
      throws Exception;

  @POST
  @Path("/xml")
  @Consumes({"application/xml", "text/xml"})
  @StoreApiParameters
  @Operation(
      summary = "Index documents in XML format",
      tags = {"update"})
  UpdateResponse updateXml(
      @Parameter(description = "Commit the update immediately") @QueryParam("commit")
          Boolean commit,
      @Parameter(description = "Commit the update within this many milliseconds")
          @QueryParam("commitWithin")
          Integer commitWithin,
      @Parameter(description = "Overwrite documents with the same unique key")
          @QueryParam("overwrite")
          Boolean overwrite,
      @Parameter(description = "Perform a soft commit") @QueryParam("softCommit")
          Boolean softCommit,
      @Parameter(description = "Include assigned document versions in the response")
          @QueryParam("versions")
          Boolean versions,
      @Parameter(required = true)
          @RequestBody(
              required = true,
              description = "XML update content.",
              extensions =
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      }))
          InputStream requestBody)
      throws Exception;

  @POST
  @Path("/csv")
  @Consumes({"application/csv", "text/csv"})
  @StoreApiParameters
  @Operation(
      summary = "Index documents in CSV format",
      tags = {"update"})
  UpdateResponse updateCsv(
      @Parameter(description = "Commit the update immediately") @QueryParam("commit")
          Boolean commit,
      @Parameter(description = "Commit the update within this many milliseconds")
          @QueryParam("commitWithin")
          Integer commitWithin,
      @Parameter(description = "Overwrite documents with the same unique key")
          @QueryParam("overwrite")
          Boolean overwrite,
      @Parameter(description = "Perform a soft commit") @QueryParam("softCommit")
          Boolean softCommit,
      @Parameter(description = "Include assigned document versions in the response")
          @QueryParam("versions")
          Boolean versions,
      @Parameter(required = true)
          @RequestBody(
              required = true,
              description = "CSV update content.",
              extensions =
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      }))
          InputStream requestBody)
      throws Exception;

  @POST
  @Path("/javabin")
  @Consumes("application/javabin")
  @StoreApiParameters
  @Operation(
      summary = "Index documents in Javabin format",
      tags = {"update"})
  UpdateResponse updateJavabin(
      @Parameter(description = "Commit the update immediately") @QueryParam("commit")
          Boolean commit,
      @Parameter(description = "Commit the update within this many milliseconds")
          @QueryParam("commitWithin")
          Integer commitWithin,
      @Parameter(description = "Overwrite documents with the same unique key")
          @QueryParam("overwrite")
          Boolean overwrite,
      @Parameter(description = "Perform a soft commit") @QueryParam("softCommit")
          Boolean softCommit,
      @Parameter(description = "Include assigned document versions in the response")
          @QueryParam("versions")
          Boolean versions,
      @Parameter(required = true)
          @RequestBody(
              required = true,
              description = "Javabin update content.",
              extensions =
                  @Extension(
                      properties = {
                        @ExtensionProperty(name = GENERIC_ENTITY_PROPERTY, value = "true")
                      }))
          InputStream requestBody)
      throws Exception;
}
