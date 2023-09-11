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
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

@Path("/{a:cores|collections}/{collectionName}/schema")
public class GetSchemaAPI extends JerseyResource {

  protected final IndexSchema indexSchema;

  @Inject
  public GetSchemaAPI(IndexSchema indexSchema) {
    this.indexSchema = indexSchema;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaInfoResponse getSchemaInfo() {
    final var response = instantiateJerseyResponse(SchemaInfoResponse.class);

    response.schema = indexSchema.getNamedPropertyValues();

    return response;
  }

  public static class SchemaInfoResponse extends SolrJerseyResponse {
    // TODO The schema response is quite complicated, so for the moment it's sufficient to record it
    // here only as a Map.  However, if SOLR-16825 is tackled then there will be a lot of value in
    // describing this response format more accurately so that clients can navigate the contents
    // without lots of map fetching and casting.
    @JsonProperty("schema")
    public Map<String, Object> schema;
  }

  @GET
  @Path("/name")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaNameResponse getSchemaName() throws Exception {
    final SchemaNameResponse response = instantiateJerseyResponse(SchemaNameResponse.class);
    if (null == indexSchema.getSchemaName()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Schema has no name");
    }

    response.name = indexSchema.getSchemaName();
    return response;
  }

  public static class SchemaNameResponse extends SolrJerseyResponse {
    @JsonProperty("name")
    public String name;
  }

  @GET
  @Path("/similarity")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaSimilarityResponse getSchemaSimilarity() {
    final var response = instantiateJerseyResponse(SchemaSimilarityResponse.class);

    response.similarity = indexSchema.getSimilarityFactory().getNamedPropertyValues();

    return response;
  }

  public static class SchemaSimilarityResponse extends SolrJerseyResponse {
    // TODO The schema response is quite complicated, so for the moment it's sufficient to record it
    // here only as a Map.  However, if SOLR-16825 is tackled then there will be a lot of value in
    // describing this response format more accurately so that clients can navigate the contents
    // without lots of map fetching and casting.
    @JsonProperty("similarity")
    public SimpleOrderedMap<Object> similarity;
  }

  @GET
  @Path("/uniquekey")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaUniqueKeyResponse getSchemaUniqueKey() {
    final var response = instantiateJerseyResponse(SchemaUniqueKeyResponse.class);

    response.uniqueKey = indexSchema.getUniqueKeyField().getName();

    return response;
  }

  public static class SchemaUniqueKeyResponse extends SolrJerseyResponse {
    @JsonProperty("uniqueKey")
    public String uniqueKey;
  }

  @GET
  @Path("/version")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaVersionResponse getSchemaVersion() {
    final var response = instantiateJerseyResponse(SchemaVersionResponse.class);

    response.version = indexSchema.getVersion();

    return response;
  }

  public static class SchemaVersionResponse extends SolrJerseyResponse {
    @JsonProperty("version")
    public float version;
  }
}
