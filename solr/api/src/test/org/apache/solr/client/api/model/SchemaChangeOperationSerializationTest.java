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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Unit tests ensuring that {@link SchemaChange} deserializes as intended
 *
 * <p>Not always necessary for model-type "serde" validation, but useful given the polymorphism at
 * play
 */
@SuppressWarnings("unchecked") // The casts *are* "checked", just not in a way the compiler detects.
public class SchemaChangeOperationSerializationTest extends SolrTestCase {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testAddFieldType() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "add-field-type",
              "name": "my-new-field-type",
              "class": "org.apache.my.ClassName",
              "positionIncrementGap": 100,
              "analyzer" : {
                "charFilters":[],
                "tokenizer":{
                  "name":"whitespace"
                }
              }
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertFieldTypeOperation.class));
    final var parsedSpecific = (UpsertFieldTypeOperation) parsedGeneric;
    assertEquals("my-new-field-type", parsedSpecific.name);
    assertEquals("org.apache.my.ClassName", parsedSpecific.className);
    // Arbitrary properties are put in a map, and can contain nesting
    assertEquals(100, parsedSpecific.getAdditionalProperties().get("positionIncrementGap"));
    assertThat(parsedSpecific.getAdditionalProperties().get("analyzer"), instanceOf(Map.class));
    final var analyzerProperties =
        (Map<String, Object>) parsedSpecific.getAdditionalProperties().get("analyzer");
    assertThat(analyzerProperties.keySet(), contains("charFilters", "tokenizer"));
    assertThat(analyzerProperties.get("tokenizer"), instanceOf(Map.class));
    final var tokenizerProperties = (Map<String, Object>) analyzerProperties.get("tokenizer");
    assertEquals("whitespace", tokenizerProperties.get("name"));
  }

  @Test
  public void testAddCopyField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "add-copy-field",
              "source": "source1",
              "destinations": ["dest1", "dest2"],
              "maxChars": 123
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(AddCopyFieldOperation.class));
    final var parsedSpecific = (AddCopyFieldOperation) parsedGeneric;
    assertEquals("source1", parsedSpecific.source);
    assertThat(parsedSpecific.destinations, contains("dest1", "dest2"));
    assertEquals(Integer.valueOf(123), parsedSpecific.maxChars);
  }

  // "dest" is used instead of "destinations", to support v1 APIs
  @Test
  public void testAddCopyFieldAltFieldName() throws Exception {
    final var inputJson =
        """
                {
                  "operationType": "add-copy-field",
                  "source": "source1",
                  "dest": ["dest1", "dest2"],
                  "maxChars": 123
                }
                """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(AddCopyFieldOperation.class));
    final var parsedSpecific = (AddCopyFieldOperation) parsedGeneric;
    assertEquals("source1", parsedSpecific.source);
    assertThat(parsedSpecific.destinations, contains("dest1", "dest2"));
    assertEquals(Integer.valueOf(123), parsedSpecific.maxChars);
  }

  @Test
  public void testAddField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "add-field",
              "name": "my-new-field",
              "type": "fieldType",
              "stored": true
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertFieldOperation.class));
    final var parsedSpecific = (UpsertFieldOperation) parsedGeneric;
    assertEquals("my-new-field", parsedSpecific.name);
    assertEquals("fieldType", parsedSpecific.type);

    // Arbitrary properties are put in a map
    assertEquals(Boolean.TRUE, parsedSpecific.getAdditionalProperties().get("stored"));
  }

  @Test
  public void testAddDynamicField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "add-dynamic-field",
              "name": "_abc",
              "type": "fieldType",
              "stored": true
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertDynamicFieldOperation.class));
    final var parsedSpecific = (UpsertDynamicFieldOperation) parsedGeneric;
    assertEquals("_abc", parsedSpecific.name);
    assertEquals("fieldType", parsedSpecific.type);

    // Arbitrary properties are put in a map
    assertEquals(Boolean.TRUE, parsedSpecific.getAdditionalProperties().get("stored"));
  }

  @Test
  public void testDeleteFieldType() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "delete-field-type",
              "name": "myFieldTypeName"
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(DeleteFieldTypeOperation.class));
    final var parsedSpecific = (DeleteFieldTypeOperation) parsedGeneric;
    assertEquals("myFieldTypeName", parsedSpecific.name);
  }

  @Test
  public void testDeleteCopyField() throws Exception {
    final var inputJson =
        """
                {
                  "operationType": "delete-copy-field",
                  "source": "source1",
                  "destinations": ["dest1", "dest2"]
                }
                """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(DeleteCopyFieldOperation.class));
    final var parsedSpecific = (DeleteCopyFieldOperation) parsedGeneric;
    assertEquals("source1", parsedSpecific.source);
    assertThat(parsedSpecific.destinations, contains("dest1", "dest2"));
  }

  // "dest" is used instead of "destinations", to support v1 APIs
  @Test
  public void testDeleteCopyFieldAltFieldName() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "delete-copy-field",
              "source": "source1",
              "dest": ["dest1", "dest2"]
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(DeleteCopyFieldOperation.class));
    final var parsedSpecific = (DeleteCopyFieldOperation) parsedGeneric;
    assertEquals("source1", parsedSpecific.source);
    assertThat(parsedSpecific.destinations, contains("dest1", "dest2"));
  }

  @Test
  public void testDeleteField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "delete-field",
              "name": "myFieldName"
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(DeleteFieldOperation.class));
    final var parsedSpecific = (DeleteFieldOperation) parsedGeneric;
    assertEquals("myFieldName", parsedSpecific.name);
  }

  @Test
  public void testDeleteDynamicField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "delete-dynamic-field",
              "name": "_abc"
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(DeleteDynamicFieldOperation.class));
    final var parsedSpecific = (DeleteDynamicFieldOperation) parsedGeneric;
    assertEquals("_abc", parsedSpecific.name);
  }

  @Test
  public void testReplaceFieldType() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "replace-field-type",
              "name": "my-new-field-type",
              "class": "org.apache.my.ClassName",
              "positionIncrementGap": 100,
              "analyzer" : {
                "charFilters":[],
                "tokenizer":{
                  "name":"whitespace"
                }
              }
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertFieldTypeOperation.class));
    final var parsedSpecific = (UpsertFieldTypeOperation) parsedGeneric;
    assertEquals("my-new-field-type", parsedSpecific.name);
    assertEquals("org.apache.my.ClassName", parsedSpecific.className);
    // Arbitrary properties are put in a map, and can contain nesting
    assertEquals(100, parsedSpecific.getAdditionalProperties().get("positionIncrementGap"));
    assertThat(parsedSpecific.getAdditionalProperties().get("analyzer"), instanceOf(Map.class));
    final var analyzerProperties =
        (Map<String, Object>) parsedSpecific.getAdditionalProperties().get("analyzer");
    assertThat(analyzerProperties.keySet(), contains("charFilters", "tokenizer"));
    assertThat(analyzerProperties.get("tokenizer"), instanceOf(Map.class));
    final var tokenizerProperties = (Map<String, Object>) analyzerProperties.get("tokenizer");
    assertEquals("whitespace", tokenizerProperties.get("name"));
  }

  @Test
  public void testReplaceField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "replace-field",
              "name": "my-new-field",
              "type": "fieldType",
              "stored": true
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertFieldOperation.class));
    final var parsedSpecific = (UpsertFieldOperation) parsedGeneric;
    assertEquals("my-new-field", parsedSpecific.name);
    assertEquals("fieldType", parsedSpecific.type);

    // Arbitrary properties are put in a map
    assertEquals(Boolean.TRUE, parsedSpecific.getAdditionalProperties().get("stored"));
  }

  @Test
  public void testReplaceDynamicField() throws Exception {
    final var inputJson =
        """
            {
              "operationType": "replace-dynamic-field",
              "name": "_abc",
              "type": "fieldType",
              "stored": true
            }
            """;

    final var parsedGeneric = OBJECT_MAPPER.readValue(inputJson, SchemaChange.class);

    assertThat(parsedGeneric, instanceOf(UpsertDynamicFieldOperation.class));
    final var parsedSpecific = (UpsertDynamicFieldOperation) parsedGeneric;
    assertEquals("_abc", parsedSpecific.name);
    assertEquals("fieldType", parsedSpecific.type);

    // Arbitrary properties are put in a map
    assertEquals(Boolean.TRUE, parsedSpecific.getAdditionalProperties().get("stored"));
  }
}
