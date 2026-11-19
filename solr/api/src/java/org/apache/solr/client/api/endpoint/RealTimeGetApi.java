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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import java.util.List;
import org.apache.solr.client.api.model.GetDocumentsResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/**
 * V2 API definition for fetching the latest (possibly uncommitted) version of one or more
 * documents.
 *
 * <p>This API (GET /v2/c/collectionName/get) is analogous to the v1 /solr/collectionName/get API.
 */
@Path(INDEX_PATH_PREFIX + "/get")
public interface RealTimeGetApi {

  @GET
  @StoreApiParameters
  @Operation(
      summary = "Fetch the latest version of one or more documents by their unique id.",
      tags = {"documents"})
  GetDocumentsResponse getDocuments(
      @QueryParam("id") String id, @QueryParam("ids") List<String> ids) throws Exception;
}
