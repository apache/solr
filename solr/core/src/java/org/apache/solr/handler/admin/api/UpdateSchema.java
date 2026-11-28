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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.UpdateSchemaApi;
import org.apache.solr.client.api.model.AddCopyFieldOperation;
import org.apache.solr.client.api.model.CopyFieldDestination;
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
  public SolrJerseyResponse upsertCopyFields(
      String sourceField, List<CopyFieldDestination> requestBody) throws Exception {
    return runCopyFieldOperation(sourceField, requestBody, "upsert-copy-field");
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse appendCopyFields(
      String sourceField, List<CopyFieldDestination> requestBody) throws Exception {
    return runCopyFieldOperation(sourceField, requestBody, "append-copy-field");
  }

  /**
   * Runs one of the copy-field operations that diff against the current schema.
   *
   * <p>Both leave the diffing to {@link SchemaManager}, which performs it against a schema
   * refreshed under the update lock; doing it here would go stale whenever SchemaManager retries.
   */
  private SolrJerseyResponse runCopyFieldOperation(
      String sourceField, List<CopyFieldDestination> requestBody, String operationType)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureSchemaMutable();
    ensureRequiredParameterProvided("sourceField", sourceField);
    ensureRequiredRequestBodyProvided(requestBody);
    final List<SchemaChange> operations = new ArrayList<>();
    if (operationType.equals("upsert-copy-field")) {
      final var clear = new AddCopyFieldOperation();
      clear.source = sourceField;
      clear.destinations = List.of();
      clear.operationType = operationType;
      operations.add(clear);
    }
    final var destinations = new HashSet<String>();
    for (CopyFieldDestination rule : requestBody) {
      ensureRequiredParameterProvided("dest", rule == null ? null : rule.dest);
      if (!destinations.add(rule.dest)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Duplicate destination: " + rule.dest);
      }
      final var add = copyFieldOperation(sourceField, rule.dest, rule.maxChars);
      add.operationType =
          operationType.equals("upsert-copy-field") ? "add-copy-field" : operationType;
      operations.add(add);
    }
    runWithSchemaManager(operations);

    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteCopyFields(String sourceField) throws Exception {
    ensureSchemaMutable();
    ensureRequiredParameterProvided("sourceField", sourceField);

    // Only to report a missing resource; the deletion itself re-reads under the schema update
    // lock, so a rule added in the meantime is still removed.
    if (currentDestinationsOf(sourceField).isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND,
          "No copy-field rules found with source '" + sourceField + "'");
    }

    // Replacing the source's rules with none removes all of them.
    return runCopyFieldOperation(sourceField, List.of(), "upsert-copy-field");
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public SolrJerseyResponse deleteCopyFieldsByDestination(
      String sourceField, String destinationFields) throws Exception {
    ensureSchemaMutable();
    ensureRequiredParameterProvided("sourceField", sourceField);
    ensureRequiredParameterProvided("destinationFields", destinationFields);

    // Field names cannot contain a comma, so it is safe to read the segment as a list.
    final var destinations =
        Arrays.stream(destinationFields.split(",")).filter(not(String::isEmpty)).toList();
    if (destinations.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No destinations given in '" + destinationFields + "'");
    }
    return removeCopyFields(sourceField, destinations);
  }

  private SolrJerseyResponse removeCopyFields(String sourceField, List<String> destinations)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);

    final var deleteCopyFieldOp = new DeleteCopyFieldOperation();
    deleteCopyFieldOp.operationType = "delete-copy-field-by-destination";
    deleteCopyFieldOp.source = sourceField;
    deleteCopyFieldOp.destinations = destinations;

    runWithSchemaManager(List.of(deleteCopyFieldOp));

    return response;
  }

  private AddCopyFieldOperation copyFieldOperation(
      String source, String destination, Integer maxChars) {
    ensureRequiredParameterProvided("source", source);
    ensureRequiredParameterProvided("dest", destination);
    if (destination.isBlank()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "destination must not be blank");
    }
    if (maxChars != null && maxChars < 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "maxChars must not be negative");
    }
    final var operation = new AddCopyFieldOperation();
    operation.source = source;
    operation.destinations = List.of(destination);
    operation.maxChars = maxChars;
    operation.operationType = "add-copy-field";
    return operation;
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
