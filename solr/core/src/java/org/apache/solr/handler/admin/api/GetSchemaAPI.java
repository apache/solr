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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.GetSchemaApi;
import org.apache.solr.client.api.model.SchemaInfoResponse;
import org.apache.solr.client.api.model.SchemaNameResponse;
import org.apache.solr.client.api.model.SchemaSimilarityResponse;
import org.apache.solr.client.api.model.SchemaUniqueKeyResponse;
import org.apache.solr.client.api.model.SchemaVersionResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;


public class GetSchemaAPI extends JerseyResource implements GetSchemaApi {

  protected final IndexSchema indexSchema;

  @Inject
  public GetSchemaAPI(IndexSchema indexSchema) {
    this.indexSchema = indexSchema;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaInfoResponse getSchemaInfo() {
    final var response = instantiateJerseyResponse(SchemaInfoResponse.class);

    response.schema = indexSchema.getNamedPropertyValues();

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaNameResponse getSchemaName() throws Exception {
    final SchemaNameResponse response = instantiateJerseyResponse(SchemaNameResponse.class);
    if (null == indexSchema.getSchemaName()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Schema has no name");
    }

    response.name = indexSchema.getSchemaName();
    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaSimilarityResponse getSchemaSimilarity() {
    final var response = instantiateJerseyResponse(SchemaSimilarityResponse.class);

    response.similarity = indexSchema.getSimilarityFactory().getNamedPropertyValues();

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaUniqueKeyResponse getSchemaUniqueKey() {
    final var response = instantiateJerseyResponse(SchemaUniqueKeyResponse.class);

    response.uniqueKey = indexSchema.getUniqueKeyField().getName();

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaVersionResponse getSchemaVersion() {
    final var response = instantiateJerseyResponse(SchemaVersionResponse.class);

    response.version = indexSchema.getVersion();

    return response;
  }
}
