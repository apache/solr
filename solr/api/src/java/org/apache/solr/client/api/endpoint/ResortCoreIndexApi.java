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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ResortCoreIndexRequestBody;
import org.apache.solr.client.api.model.ResortCoreIndexResponse;

/** V2 API definition for re-sorting an existing core index (SOLR-12239). */
@Path("/cores/{coreName}/resort")
public interface ResortCoreIndexApi {

  @POST
  @Operation(
      summary =
          "Re-sort the existing index of the specified core into the target sort order, so index "
              + "sorting can be enabled without reindexing from source.",
      tags = {"cores"})
  ResortCoreIndexResponse resortCoreIndex(
      @Parameter(description = "The name of the core whose index should be re-sorted")
          @PathParam("coreName")
          String coreName,
      ResortCoreIndexRequestBody requestBody)
      throws Exception;
}
