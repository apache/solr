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

import jakarta.inject.Inject;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.UpdateSchemaApi;
import org.apache.solr.client.api.model.ErrorInfo;
import org.apache.solr.client.api.model.SchemaChangeOperation;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.security.PermissionNameProvider;

public class UpdateSchema extends JerseyResource implements UpdateSchemaApi {

  private final SolrCore solrCore;
  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public UpdateSchema(SolrCore solrCore, SolrQueryRequest solrQueryRequest) {
    this.solrCore = solrCore;
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse addField(String fieldName, SchemaChangeOperation.AddField requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("fieldName", fieldName);
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("type", requestBody.type);
    requestBody.name = fieldName;

    runWithSchemaManager(List.of(requestBody), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteField(String fieldName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("fieldName", fieldName);

    // Format operation as a 'SchemaChangeOperation' so it can be processed by SchemaManager
    final var deleteFieldOp = new SchemaChangeOperation.DeleteField();
    deleteFieldOp.name = fieldName;

    runWithSchemaManager(List.of(deleteFieldOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse addDynamicField(
      String dynamicFieldName, SchemaChangeOperation.AddDynamicField requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("dynamicFieldName", dynamicFieldName);
    ensureRequiredParameterProvided("type", requestBody.type);
    requestBody.name = dynamicFieldName;

    runWithSchemaManager(List.of(requestBody), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteDynamicField(String dynamicFieldName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("dynamicFieldName", dynamicFieldName);

    final var deleteDynamicFieldOp = new SchemaChangeOperation.DeleteDynamicField();
    deleteDynamicFieldOp.name = dynamicFieldName;
    runWithSchemaManager(List.of(deleteDynamicFieldOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse addFieldType(
      String fieldTypeName, SchemaChangeOperation.AddFieldType requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("fieldTypeName", fieldTypeName);
    ensureRequiredParameterProvided("class", requestBody.className);

    runWithSchemaManager(List.of(requestBody), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteFieldType(String fieldTypeName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("fieldTypeName", fieldTypeName);

    final var deleteFieldTypeOp = new SchemaChangeOperation.DeleteFieldType();
    deleteFieldTypeOp.name = fieldTypeName;

    runWithSchemaManager(List.of(deleteFieldTypeOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse bulkSchemaModification(List<SchemaChangeOperation> requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);

    runWithSchemaManager(requestBody, response);

    return response;
  }

  private void ensureSchemaMutable() {
    if (SolrConfigHandler.getImmutable(solrCore)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "ConfigSet is immutable; schema modifications not allowed/enabled");
    }
  }

  private void runWithSchemaManager(
      List<SchemaChangeOperation> operations, SolrJerseyResponse response) throws Exception {
    final var schemaManager = new SchemaManager(solrQueryRequest);
    final var errorDetails = schemaManager.performOperations(operations);
    if (errorDetails != null && !errorDetails.isEmpty()) {
      response.error = new ErrorInfo();
      response.error.details = errorDetails;
    }
  }
}
