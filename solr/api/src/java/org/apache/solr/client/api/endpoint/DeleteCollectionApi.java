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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;

@Path("/collections/{collectionName}")
public interface DeleteCollectionApi {

  @DELETE
  @Operation(
      summary = "Deletes a collection from SolrCloud",
      tags = {"collections"})
  SubResponseAccumulatingJerseyResponse deleteCollection(
      @Parameter(description = "The name of the collection to be deleted.", required = true)
          @PathParam("collectionName")
          String collectionName,
      @QueryParam("followAliases") Boolean followAliases,
      @Parameter(description = "An ID to track the request asynchronously") @QueryParam("async")
          String asyncId)
      throws Exception;
}
