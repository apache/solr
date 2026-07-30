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
package org.apache.solr.schema;

import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.instanceOf;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.junit.Test;

/** Unit tests for {@link SchemaManagerUtils} */
public class SchemaManagerUtilsTest extends SolrTestCase {

  @Test
  public void testReportsErrorWhenOperationNameNotRecognized() {
    final var unknownOperationNameCommand = new CommandOperation("unknownOpName", Map.of());

    final var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              SchemaManagerUtils.convertToSchemaChangeOperations(unknownOperationNameCommand);
            });
    assertEquals(400, thrown.code());
    assertThat(thrown.getMessage(), containsStringIgnoringCase("no such operation"));
    assertThat(thrown.getMessage(), containsStringIgnoringCase("unknownOpName"));
  }

  // Not worth testing each and every SchemaChangeOperation, but try a sample one.
  @Test
  public void testConvertsCommandOperationToSchemaOperation() {
    final var replaceFieldDataMap = new HashMap<String, Object>();
    replaceFieldDataMap.put("name", "someFieldName");
    replaceFieldDataMap.put("type", "my.FieldType");
    replaceFieldDataMap.put("stored", true);
    final var replaceFieldGeneric = new CommandOperation("replace-field", replaceFieldDataMap);

    final var replaceFieldSchemaOp =
        SchemaManagerUtils.convertToSchemaChangeOperations(replaceFieldGeneric);

    assertThat(replaceFieldSchemaOp, instanceOf(UpsertFieldOperation.class));
    final var replaceFieldSchemaOpCast = (UpsertFieldOperation) replaceFieldSchemaOp;
    assertEquals("someFieldName", replaceFieldSchemaOpCast.name);
    assertEquals("my.FieldType", replaceFieldSchemaOpCast.type);
    assertEquals(Boolean.TRUE, replaceFieldSchemaOpCast.getAdditionalProperties().get("stored"));
  }
}
