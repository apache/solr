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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.OptBoolean;

/** Modifications that can be made to elements of a schema, either individually or in bulk. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = SchemaChange.OPERATION_TYPE_PROP,
    requireTypeIdForSubtypes = OptBoolean.FALSE,
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = UpsertFieldTypeOperation.class,
      names = {"add-field-type", "replace-field-type"}),
  @JsonSubTypes.Type(
      value = UpsertFieldOperation.class,
      names = {"add-field", "replace-field"}),
  @JsonSubTypes.Type(
      value = UpsertDynamicFieldOperation.class,
      names = {"add-dynamic-field", "replace-dynamic-field"}),
  @JsonSubTypes.Type(value = DeleteFieldTypeOperation.class, name = "delete-field-type"),
  @JsonSubTypes.Type(value = DeleteFieldOperation.class, name = "delete-field"),
  @JsonSubTypes.Type(value = DeleteDynamicFieldOperation.class, name = "delete-dynamic-field"),
  @JsonSubTypes.Type(value = AddCopyFieldOperation.class, name = "add-copy-field"),
  @JsonSubTypes.Type(value = DeleteCopyFieldOperation.class, name = "delete-copy-field"),
})
public class SchemaChange {

  public static final String OPERATION_TYPE_PROP = "operationType";

  @JsonProperty(OPERATION_TYPE_PROP)
  public String operationType;
}
