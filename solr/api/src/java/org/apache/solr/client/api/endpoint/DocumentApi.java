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
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import java.util.List;
import org.apache.solr.client.api.model.GetDocumentResponse;
import org.apache.solr.client.api.model.ListDocumentsResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/**
 * V2 API definitions for fetching documents as path-identified resources.
 *
 * <p>Unlike {@link RealTimeGetApi} (which mirrors the v1 {@code /get} handler's {@code id}/{@code
 * ids} query-param contract for backwards compatibility), this API models a document as a resource
 * addressed directly by its id in the URL path, and returns a {@code 404} for a document that
 * doesn't exist rather than a {@code 200} with a null document.
 */
@Path(INDEX_PATH_PREFIX + "/documents")
public interface DocumentApi {

  @GET
  @Path("/{id}")
  @StoreApiParameters
  @Operation(
      summary = "Fetch the latest version of a single document by its unique id.",
      tags = {"documents"})
  GetDocumentResponse getDocument(
      @Parameter(description = "The unique id of the document.", required = true) @PathParam("id")
          String id)
      throws Exception;

  @GET
  @StoreApiParameters
  @Operation(
      summary = "Fetch the latest version of multiple documents by their unique ids.",
      tags = {"documents"})
  ListDocumentsResponse listDocuments(
      @Parameter(description = "The unique ids of the documents to fetch.", required = true)
          @QueryParam("ids")
          List<String> ids)
      throws Exception;
}
