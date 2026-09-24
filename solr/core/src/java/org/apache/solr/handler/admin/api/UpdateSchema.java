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

import static java.util.function.Predicate.not;
import static org.apache.solr.common.util.CommandOperation.ERR_MSGS;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.UpdateSchemaApi;
import org.apache.solr.client.api.model.AddCopyFieldOperation;
import org.apache.solr.client.api.model.DeleteCopyFieldOperation;
import org.apache.solr.client.api.model.DeleteDynamicFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldTypeOperation;
import org.apache.solr.client.api.model.SchemaChange;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpsertDynamicFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldTypeOperation;
import org.apache.solr.common.SolrErrorWrappingException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
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

    runWithSchemaManager(List.of(requestBody));

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

    runWithSchemaManager(List.of(deleteFieldOp));

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

    runWithSchemaManager(List.of(requestBody));

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
    runWithSchemaManager(List.of(deleteDynamicFieldOp));

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
    ensureRequiredParameterProvided("class", requestBody.propertyClass);
    requestBody.operationType = "add-field-type";

    runWithSchemaManager(List.of(requestBody));

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

    runWithSchemaManager(List.of(deleteFieldTypeOp));

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse upsertCopyFields(String sourceField, AddCopyFieldOperation requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("sourceField", sourceField);
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("destinations", requestBody.destinations);
    requestBody.source = sourceField;
    requestBody.operationType = "add-copy-field";

    // 'add-copy-field' is purely additive: it appends a rule without checking whether an identical
    // one is already present, so repeating it would silently copy the source twice over.  Clearing
    // the source's existing rules first is what makes this PUT idempotent, and lets the body state
    // the rules the caller wants rather than only the ones being added.
    final var operations = new ArrayList<SchemaChange>();
    final var existing = currentDestinationsOf(sourceField);
    if (!existing.isEmpty()) {
      final var replacedOp = new DeleteCopyFieldOperation();
      replacedOp.operationType = "delete-copy-field";
      replacedOp.source = sourceField;
      replacedOp.destinations = existing;
      operations.add(replacedOp);
    }
    operations.add(requestBody);

    // SchemaManager applies the list as a unit, so a failure to add leaves the old rules in place.
    runWithSchemaManager(operations);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse appendCopyFields(String sourceField, AddCopyFieldOperation requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("sourceField", sourceField);
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("destinations", requestBody.destinations);

    // 'add-copy-field' would happily append a rule that already exists, leaving the source copied
    // twice over, so skip the destinations already covered.  That also makes a retried append
    // harmless, which matters to a caller that timed out without learning whether its first
    // attempt landed.
    final var existing = Set.copyOf(currentDestinationsOf(sourceField));
    final var newDestinations =
        requestBody.destinations.stream().distinct().filter(not(existing::contains)).toList();
    if (newDestinations.isEmpty()) {
      return response;
    }

    requestBody.source = sourceField;
    requestBody.destinations = newDestinations;
    requestBody.operationType = "add-copy-field";

    runWithSchemaManager(List.of(requestBody));

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteCopyFields(String sourceField) throws Exception {
    ensureRequiredParameterProvided("sourceField", sourceField);
    return removeCopyFields(sourceField, currentDestinationsOf(sourceField));
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteCopyFieldsByDestination(
      String sourceField, String destinationFields) throws Exception {
    ensureRequiredParameterProvided("sourceField", sourceField);
    ensureRequiredParameterProvided("destinationFields", destinationFields);

    // Field names cannot contain a comma, so it is safe to read the segment as a list.  splitSmart
    // keeps separators inside quotes or behind a backslash from splitting; it does not trim, so
    // surrounding whitespace is dropped here.
    final var destinations =
        StrUtils.splitSmart(destinationFields, ',').stream()
            .map(String::trim)
            .filter(not(String::isEmpty))
            .toList();
    return removeCopyFields(sourceField, destinations);
  }

  private SolrJerseyResponse removeCopyFields(String sourceField, List<String> destinations)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();

    if (destinations.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND,
          "No copy-field rules found with source '" + sourceField + "'");
    }

    final var deleteCopyFieldOp = new DeleteCopyFieldOperation();
    deleteCopyFieldOp.operationType = "delete-copy-field";
    deleteCopyFieldOp.source = sourceField;
    deleteCopyFieldOp.destinations = destinations;

    runWithSchemaManager(List.of(deleteCopyFieldOp));

    return response;
  }

  /**
   * Lists the destinations of every copy-field rule currently declared with the given source.
   *
   * <p>Covers dynamic (i.e. glob) rules as well as explicit ones, since either may be addressed by
   * source.
   */
  private List<String> currentDestinationsOf(String sourceField) {
    return solrCore
        .getLatestSchema()
        .getCopyFieldProperties(false, Set.of(sourceField), null)
        .stream()
        .map(properties -> (String) properties.get(IndexSchema.DESTINATION))
        .collect(Collectors.toList());
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse bulkSchemaModification(List<SchemaChange> requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredRequestBodyProvided(requestBody);

    runWithSchemaManager(requestBody);

    return response;
  }

  private void ensureSchemaMutable() {
    if (SolrConfigHandler.getImmutable(solrCore)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "ConfigSet is immutable; schema modifications not allowed/enabled");
    }
  }

  private void runWithSchemaManager(List<SchemaChange> operations) throws Exception {
    final var schemaManager = new SchemaManager(solrQueryRequest);
    final var errorDetails = schemaManager.performOperations(operations);
    if (errorDetails != null && !errorDetails.isEmpty()) {
      // Mirrors v1's SchemaHandler, so a validation failure gets a proper error status instead of
      // reporting a "successful" response whose body happens to carry an error. The per-operation
      // messages are folded into the exception's own message (surfaced as error.msg) so that field
      // is informative on its own, per this API's error-reporting convention; error.details still
      // carries the full per-operation breakdown for consumers that want it.
      throw new SolrErrorWrappingException(
          SolrException.ErrorCode.BAD_REQUEST, summarizeSchemaErrors(errorDetails), errorDetails);
    }
  }

  private static String summarizeSchemaErrors(List<Map<String, Object>> errorDetails) {
    final var messages =
        errorDetails.stream()
            .map(detail -> detail.get(ERR_MSGS))
            .flatMap(
                msgs -> {
                  if (msgs instanceof List<?> msgList) {
                    return msgList.stream();
                  } else if (msgs != null) {
                    return Stream.of(msgs);
                  }
                  return Stream.empty();
                })
            .map(String::valueOf)
            .collect(Collectors.joining(" "));
    return messages.isBlank() ? "error processing commands" : messages;
  }
}
