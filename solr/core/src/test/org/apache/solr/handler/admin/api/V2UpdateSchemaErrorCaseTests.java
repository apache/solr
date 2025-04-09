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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.UpsertDynamicFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldTypeOperation;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class V2UpdateSchemaErrorCaseTests extends SolrTestCase {

  private UpdateSchema schemaApi;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUpMocks() {
    final var core = mock(SolrCore.class);
    final var solrQueryRequest = mock(SolrQueryRequest.class);
    schemaApi = new UpdateSchema(core, solrQueryRequest);
  }

  @Test
  public void testAddFieldOperationRequiresFieldNameAndType() {
    final var noFieldOp = new UpsertFieldOperation();
    noFieldOp.type = "text_general";
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addField(null, noFieldOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: fieldName"));

    final var noTypeOp = new UpsertFieldOperation();
    thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addField("someFieldName", noTypeOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: type"));
  }

  @Test
  public void testDeleteFieldOperationRequiresFieldName() {
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.deleteField(null);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: fieldName"));
  }

  @Test
  public void testAddDynamicFieldOperationRequiresFieldNameAndType() {
    final var noFieldOp = new UpsertDynamicFieldOperation();
    noFieldOp.type = "text_general";
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addDynamicField(null, noFieldOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: dynamicFieldName"));

    final var noTypeOp = new UpsertDynamicFieldOperation();
    thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addDynamicField("someFieldName", noTypeOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: type"));
  }

  @Test
  public void testDeleteDynamicFieldOperationRequiresFieldName() {
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.deleteDynamicField(null);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: dynamicFieldName"));
  }

  @Test
  public void testAddFieldTypeOperationRequiresTypeNameAndClass() {
    final var noTypeOp = new UpsertFieldTypeOperation();
    noTypeOp.className = "solr.TextField";
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addFieldType(null, noTypeOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: fieldTypeName"));

    final var noClassOp = new UpsertFieldTypeOperation();
    thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.addFieldType("someFieldName", noClassOp);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: class"));
  }

  @Test
  public void testDeleteFieldTypeOperationRequiresTypeName() {
    var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              schemaApi.deleteFieldType(null);
            });
    assertEquals(BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: fieldTypeName"));
  }
}
