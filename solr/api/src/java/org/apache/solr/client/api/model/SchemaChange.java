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

/** Modifications that can be made to elements of a schema, either individually or in bulk. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = SchemaChange.OPERATION_TYPE_PROP,
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AddFieldTypeOperation.class, name = "add-field-type"),
  @JsonSubTypes.Type(value = AddCopyFieldOperation.class, name = "add-copy-field"),
  @JsonSubTypes.Type(value = AddFieldOperation.class, name = "add-field"),
  @JsonSubTypes.Type(value = AddDynamicFieldOperation.class, name = "add-dynamic-field"),
  @JsonSubTypes.Type(value = DeleteFieldTypeOperation.class, name = "delete-field-type"),
  @JsonSubTypes.Type(value = DeleteCopyFieldOperation.class, name = "delete-copy-field"),
  @JsonSubTypes.Type(value = DeleteFieldOperation.class, name = "delete-field"),
  @JsonSubTypes.Type(value = DeleteDynamicFieldOperation.class, name = "delete-dynamic-field"),
  @JsonSubTypes.Type(value = ReplaceFieldTypeOperation.class, name = "replace-field-type"),
  @JsonSubTypes.Type(value = ReplaceFieldOperation.class, name = "replace-field"),
  @JsonSubTypes.Type(value = ReplaceDynamicFieldOperation.class, name = "replace-dynamic-field"),
})
public class SchemaChange {

  public static final String OPERATION_TYPE_PROP = "operationType";

  @JsonProperty(OPERATION_TYPE_PROP)
  public String operationType;
}
