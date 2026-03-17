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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/** V2 API definitions for indexing documents via the update handler. */
@Path(INDEX_PATH_PREFIX + "/update")
public interface UpdateApi {

  @POST
  @StoreApiParameters
  @Operation(
      summary = "Index documents using any supported content type",
      tags = {"update"})
  SolrJerseyResponse update() throws Exception;

  @POST
  @Path("/json")
  @StoreApiParameters
  @Operation(
      summary = "Index documents in JSON format",
      tags = {"update"})
  SolrJerseyResponse updateJson() throws Exception;

  @POST
  @Path("/xml")
  @StoreApiParameters
  @Operation(
      summary = "Index documents in XML format",
      tags = {"update"})
  SolrJerseyResponse updateXml() throws Exception;

  @POST
  @Path("/csv")
  @StoreApiParameters
  @Operation(
      summary = "Index documents in CSV format",
      tags = {"update"})
  SolrJerseyResponse updateCsv() throws Exception;

  @POST
  @Path("/bin")
  @StoreApiParameters
  @Operation(
      summary = "Index documents documents in Javabin format",
      tags = {"update"})
  SolrJerseyResponse updateBin() throws Exception;
}
