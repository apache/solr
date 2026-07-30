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
import org.apache.solr.client.api.model.DeleteDynamicFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldTypeOperation;
import org.apache.solr.client.api.model.ErrorInfo;
import org.apache.solr.client.api.model.SchemaChange;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpsertDynamicFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldTypeOperation;
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
  public SolrJerseyResponse addField(String fieldName, UpsertFieldOperation requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("fieldName", fieldName);
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("type", requestBody.type);
    requestBody.name = fieldName;
    requestBody.operationType = "upsert-field";

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
    final var deleteFieldOp = new DeleteFieldOperation();
    deleteFieldOp.operationType = "delete-field";
    deleteFieldOp.name = fieldName;

    runWithSchemaManager(List.of(deleteFieldOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse addDynamicField(
      String dynamicFieldName, UpsertDynamicFieldOperation requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("dynamicFieldName", dynamicFieldName);
    ensureRequiredParameterProvided("type", requestBody.type);
    requestBody.name = dynamicFieldName;
    requestBody.operationType = "add-dynamic-field";

    runWithSchemaManager(List.of(requestBody), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteDynamicField(String dynamicFieldName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("dynamicFieldName", dynamicFieldName);

    final var deleteDynamicFieldOp = new DeleteDynamicFieldOperation();
    deleteDynamicFieldOp.name = dynamicFieldName;
    deleteDynamicFieldOp.operationType = "delete-dynamic-field";
    runWithSchemaManager(List.of(deleteDynamicFieldOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse addFieldType(String fieldTypeName, UpsertFieldTypeOperation requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("fieldTypeName", fieldTypeName);
    ensureRequiredParameterProvided("class", requestBody.className);
    requestBody.operationType = "add-field-type";

    runWithSchemaManager(List.of(requestBody), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteFieldType(String fieldTypeName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("fieldTypeName", fieldTypeName);

    final var deleteFieldTypeOp = new DeleteFieldTypeOperation();
    deleteFieldTypeOp.name = fieldTypeName;
    deleteFieldTypeOp.operationType = "delete-field-type";

    runWithSchemaManager(List.of(deleteFieldTypeOp), response);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse bulkSchemaModification(List<SchemaChange> requestBody)
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

  private void runWithSchemaManager(List<SchemaChange> operations, SolrJerseyResponse response)
      throws Exception {
    final var schemaManager = new SchemaManager(solrQueryRequest);
    final var errorDetails = schemaManager.performOperations(operations);
    if (errorDetails != null && !errorDetails.isEmpty()) {
      response.error = new ErrorInfo();
      response.error.details = errorDetails;
    }
  }
}
