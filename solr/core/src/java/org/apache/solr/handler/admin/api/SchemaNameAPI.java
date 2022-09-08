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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 API for checking the name of an in-use schema.
 *
 * <p>This API (GET /v2/collections/collectionName/schema/name) is analogous to the v1
 * /solr/collectionName/schema/name API.
 */
@Path("/collections/{collectionName}/schema/name")
public class SchemaNameAPI extends JerseyResource {

  private SolrCore solrCore;

  @Inject
  public SchemaNameAPI(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public GetSchemaNameResponse getSchemaName() throws Exception {
    final GetSchemaNameResponse response = instantiateJerseyResponse(GetSchemaNameResponse.class);
    final IndexSchema schema = solrCore.getLatestSchema();
    if (null == schema.getSchemaName()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Schema has no name");
    }

    response.name = schema.getSchemaName();
    return response;
  }

  /** Response for {@link SchemaNameAPI}. */
  public static class GetSchemaNameResponse extends SolrJerseyResponse {
    @JsonProperty("name")
    public String name;
  }
}
