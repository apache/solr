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

import static java.util.Optional.empty;
import static java.util.Optional.of;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Tests atomic updates of stored multivalued dense vectors. */
public class MultivaluedDenseVectorAtomicUpdateTest extends SolrTestCase {

  private static final String SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME = "vector";
  private static final String SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME = "vector_byte_encoding";

  private static final String MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME = "vector_multivalued";
  private static final String MULTI_VALUE_BYTE_ENCODED_FIELD_NAME = "vector_byte_multivalued";

  private static final String STRING_FIELD_NAME = "string_field";

  private static final List<Float> A_FLOAT_VECTOR = List.of(1.25f, 2.5f, 3.75f, 4.0f);
  private static final List<Integer> A_BYTE_VECTOR = List.of(-128, -1, 0, 127);

  private static final List<Float> ANOTHER_FLOAT_VECTOR = List.of(4.0f, 3.75f, 2.5f, 1.25f);
  private static final List<Integer> ANOTHER_BYTE_VECTOR = List.of(127, 0, -1, -128);

  @Rule public final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  private SolrClient client;

  @Before
  public void setUpCore() throws Exception {
    SolrTestCaseJ4.newRandomConfig();
    System.setProperty("solr.index.updatelog.enabled", "true");

    solrTestRule.startSolr();
    solrTestRule
        .newCollection()
        .withConfigSet(
            ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/collection1/conf"))
        .withConfigFile("solrconfig.xml")
        .withSchemaFile("schema-densevector.xml")
        .create();
    client = solrTestRule.getSolrClient();
  }

  @Test
  public void atomicUpdateOnMultiValuedFloatVector_shouldNotDuplicateVectorChildren()
      throws Exception {
    var documentId = "3";
    var vectorFieldName = MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME;
    var newFieldValue = generateRandomStringValue();

    var multivaluedVector = List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR);

    var docWithMultivaluedVectorField = new SolrInputDocument();
    docWithMultivaluedVectorField.setField(CommonParams.ID, documentId);
    docWithMultivaluedVectorField.setField(vectorFieldName, multivaluedVector);

    assertEquals(0, client.add(docWithMultivaluedVectorField).getStatus());
    client.commit();

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);
  }

  @Test
  public void atomicUpdateOnMultiValuedByteVector_shouldNotDuplicateVectorChildren()
      throws Exception {
    var documentId = "3";
    var vectorFieldName = MULTI_VALUE_BYTE_ENCODED_FIELD_NAME;
    var newFieldValue = generateRandomStringValue();

    var multivaluedVector = List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR);

    var docWithMultivaluedVectorField = new SolrInputDocument();
    docWithMultivaluedVectorField.setField(CommonParams.ID, documentId);
    docWithMultivaluedVectorField.setField(vectorFieldName, multivaluedVector);

    assertEquals(0, client.add(docWithMultivaluedVectorField).getStatus());
    client.commit();

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);
  }

  @Test
  public void atomicUpdateOnMultiValuedSingleDimensionFloatVector_shouldPreserveVectorChildren()
      throws Exception {
    var documentId = "3";
    var vectorFieldName = "vector_multivalued_1d";
    var newFieldValue = generateRandomStringValue();
    var multivaluedVector = List.of(List.of(1.25f), List.of(2.5f));

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, documentId);
    doc.setField(vectorFieldName, multivaluedVector);

    assertEquals(0, client.add(doc).getStatus());
    client.commit();

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    for (int i = 0; i < multivaluedVector.size(); i++) {
      var childId = documentId + "/" + vectorFieldName + "#" + i;
      var child = realTimeGet(childId, of(Set.of(CommonParams.ID, vectorFieldName)));
      assertEquals(childId, child.getFieldValue(CommonParams.ID));
      assertEquals(
          storedVectorValue(vectorFieldName, multivaluedVector.get(i)),
          child.getFieldValue(vectorFieldName));
    }
  }

  @Test
  public void atomicUpdateOnMultiValuedSingleDimensionByteVector_shouldPreserveVectorChildren()
      throws Exception {
    var documentId = "3";
    var vectorFieldName = "vector_byte_multivalued_1d";
    var newFieldValue = generateRandomStringValue();
    var multivaluedVector = List.of(List.of(-128), List.of(127));

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, documentId);
    doc.setField(vectorFieldName, multivaluedVector);

    assertEquals(0, client.add(doc).getStatus());
    client.commit();

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(documentId, vectorFieldName, multivaluedVector);

    for (int i = 0; i < multivaluedVector.size(); i++) {
      var childId = documentId + "/" + vectorFieldName + "#" + i;
      var child = realTimeGet(childId, of(Set.of(CommonParams.ID, vectorFieldName)));
      assertEquals(childId, child.getFieldValue(CommonParams.ID));
      assertEquals(
          storedVectorValue(vectorFieldName, multivaluedVector.get(i)),
          child.getFieldValue(vectorFieldName));
    }
  }

  @Test
  public void atomicUpdateOnUncommittedDocument_shouldPreserveFloatVectorChildren()
      throws Exception {
    var documentId = "3";
    var newFieldValue = generateRandomStringValue();
    var multivaluedVector = List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR);

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, documentId);
    doc.setField(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, multivaluedVector);

    assertEquals(0, client.add(doc).getStatus());
    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(
        documentId, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, multivaluedVector);
  }

  @Test
  public void atomicUpdateOnUncommittedDocument_shouldPreserveByteVectorChildren()
      throws Exception {
    var documentId = "3";
    var newFieldValue = generateRandomStringValue();
    var multivaluedVector = List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR);

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, documentId);
    doc.setField(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, multivaluedVector);

    assertEquals(0, client.add(doc).getStatus());
    assertEquals(0, atomicSetOnStringField(documentId, newFieldValue).getStatus());
    client.commit();

    assertEquals(newFieldValue, realTimeGet(documentId, empty()).getFieldValue(STRING_FIELD_NAME));

    assertMultiValuedVectorChildren(
        documentId, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, multivaluedVector);
  }

  @Test
  public void atomicSetOnMultivaluedVector_shouldPreserveOtherVectorFields() throws Exception {
    int sequence = 10;
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      for (boolean updateFloat : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        var floats = List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR);
        var bytes = List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR);
        addMultivaluedVectors(id, floats, bytes, commitBeforeUpdate);

        if (updateFloat) {
          floats = List.of(ANOTHER_FLOAT_VECTOR);
          atomicSetVectorField(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, floats);
        } else {
          bytes = List.of(ANOTHER_BYTE_VECTOR);
          atomicSetVectorField(id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, bytes);
        }

        assertMultivaluedVectorsAfterAtomicSet(id, floats, bytes);
      }
    }
  }

  @Test
  public void atomicSetOnMultivaluedVector_shouldRemoveOnlyRequestedVectorField() throws Exception {
    int sequence = 20;
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      for (boolean removeFloat : List.of(false, true)) {
        for (boolean setNull : List.of(false, true)) {
          String id = Integer.toString(sequence++);
          var floats = List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR);
          var bytes = List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR);
          addMultivaluedVectors(id, floats, bytes, commitBeforeUpdate);

          atomicSetVectorField(
              id,
              removeFloat
                  ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                  : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
              setNull ? null : List.of());

          assertMultivaluedVectorsAfterAtomicSet(
              id, removeFloat ? List.of() : floats, removeFloat ? bytes : List.of());
        }
      }
    }
  }

  @Test
  public void repeatedAtomicSetsOnMultivaluedVectors_shouldNotKeepStaleChildren() throws Exception {
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      String id = commitBeforeUpdate ? "31" : "30";
      addMultivaluedVectors(
          id,
          List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR),
          List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR),
          commitBeforeUpdate);

      atomicSetVectorField(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(ANOTHER_FLOAT_VECTOR));
      atomicSetVectorField(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(A_FLOAT_VECTOR));
      atomicSetVectorField(id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(ANOTHER_BYTE_VECTOR));
      atomicSetOnStringField(id, "updated");

      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(A_FLOAT_VECTOR), List.of(ANOTHER_BYTE_VECTOR));
      assertEquals("updated", realTimeGet(id, empty()).getFieldValue(STRING_FIELD_NAME));
    }
  }

  @Test
  public void atomicSetOnBothMultivaluedVectors_shouldReplaceBothFields() throws Exception {
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      String id = commitBeforeUpdate ? "41" : "40";
      addMultivaluedVectors(
          id,
          List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR),
          List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR),
          commitBeforeUpdate);

      var update = new SolrInputDocument();
      update.setField(CommonParams.ID, id);
      update.setField(
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, Map.of("set", List.of(ANOTHER_FLOAT_VECTOR)));
      update.setField(
          MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, Map.of("set", List.of(ANOTHER_BYTE_VECTOR)));
      assertEquals(0, client.add(update).getStatus());

      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(ANOTHER_FLOAT_VECTOR), List.of(ANOTHER_BYTE_VECTOR));
    }
  }

  @Test
  public void atomicSetOnLastMultivaluedVector_shouldRemoveAllVectorChildren() throws Exception {
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      String id = commitBeforeUpdate ? "51" : "50";
      addMultivaluedVectors(
          id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), commitBeforeUpdate);
      atomicSetVectorField(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, null);
      atomicSetVectorField(id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of());
      assertMultivaluedVectorsAfterAtomicSet(id, List.of(), List.of());
    }
  }

  @Test
  public void atomicSetOnNewMultivaluedVector_shouldPreserveOrdinaryChildren() throws Exception {
    for (boolean commitBeforeUpdate : List.of(false, true)) {
      String id = commitBeforeUpdate ? "61" : "60";
      var child = newDocumentWithVectors(id + "/ordinary", A_FLOAT_VECTOR, A_BYTE_VECTOR);
      var doc = new SolrInputDocument();
      doc.setField(CommonParams.ID, id);
      doc.setField(
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR));
      doc.setField("child", child);
      assertEquals(0, client.add(doc).getStatus());
      if (commitBeforeUpdate) {
        client.commit();
      }

      atomicSetVectorField(id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(ANOTHER_BYTE_VECTOR));
      client.commit();
      assertVectorChildren(
          id,
          Map.of(
              MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR),
              MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(ANOTHER_BYTE_VECTOR)));
      var actualChild = realTimeGet(id + "/ordinary", empty());
      assertEquals(
          A_FLOAT_VECTOR, actualChild.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(A_BYTE_VECTOR, actualChild.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
      var children =
          new QueryRequest(
                  "/select",
                  new SolrQuery("_root_:" + id + " AND -id:" + id).setFields(CommonParams.ID))
              .process(client)
              .getResults();
      assertEquals(4L, children.getNumFound());
    }
  }

  @Test
  public void atomicVectorAdd_shouldAppendWholeVectorsAndKeepDuplicates() throws Exception {
    int sequence = 200;
    for (boolean committed : List.of(false, true)) {
      for (boolean floatEncoding : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), committed);
        atomicVectorOperation(
            id,
            floatEncoding
                ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
            "add",
            floatEncoding
                ? List.of(ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR)
                : List.of(ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR));
        assertMultivaluedVectorsAfterAtomicSet(
            id,
            floatEncoding
                ? List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR)
                : List.of(A_FLOAT_VECTOR),
            floatEncoding
                ? List.of(A_BYTE_VECTOR)
                : List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR));
      }
    }
  }

  @Test
  public void atomicVectorRemove_shouldMatchNumericValuesAndRemoveOneOccurrence() throws Exception {
    int sequence = 210;
    for (boolean committed : List.of(false, true)) {
      for (boolean floatEncoding : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(
            id,
            List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR),
            List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR),
            committed);
        atomicVectorOperation(
            id,
            floatEncoding
                ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
            "remove",
            floatEncoding
                ? List.of(
                    A_FLOAT_VECTOR.stream().map(Number::doubleValue).toList(), List.of(9, 9, 9, 9))
                : List.of(
                    A_BYTE_VECTOR.stream().map(Number::longValue).toList(), List.of(9, 9, 9, 9)));
        assertMultivaluedVectorsAfterAtomicSet(
            id,
            floatEncoding
                ? List.of(ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR)
                : List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR),
            floatEncoding
                ? List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR)
                : List.of(ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR));
      }
    }
  }

  @Test
  public void atomicVectorAddDistinct_shouldCompareWholeVectorsAndKeepExistingDuplicates()
      throws Exception {
    int sequence = 220;
    for (boolean committed : List.of(false, true)) {
      for (boolean floatEncoding : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(
            id,
            List.of(A_FLOAT_VECTOR, A_FLOAT_VECTOR),
            List.of(A_BYTE_VECTOR, A_BYTE_VECTOR),
            committed);
        atomicVectorOperation(
            id,
            floatEncoding
                ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
            "add-distinct",
            floatEncoding
                ? List.of(
                    A_FLOAT_VECTOR.stream().map(Object::toString).toList(),
                    ANOTHER_FLOAT_VECTOR.stream().map(Number::doubleValue).toList(),
                    ANOTHER_FLOAT_VECTOR)
                : List.of(
                    A_BYTE_VECTOR.stream().map(Object::toString).toList(),
                    ANOTHER_BYTE_VECTOR.stream().map(Number::doubleValue).toList(),
                    ANOTHER_BYTE_VECTOR));
        assertMultivaluedVectorsAfterAtomicSet(
            id,
            floatEncoding
                ? List.of(A_FLOAT_VECTOR, A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR)
                : List.of(A_FLOAT_VECTOR, A_FLOAT_VECTOR),
            floatEncoding
                ? List.of(A_BYTE_VECTOR, A_BYTE_VECTOR)
                : List.of(A_BYTE_VECTOR, A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR));
      }
    }
  }

  @Test
  public void atomicVectorOperationsOnMissingField_shouldPreserveOtherFields() throws Exception {
    int sequence = 230;
    for (boolean committed : List.of(false, true)) {
      for (boolean floatEncoding : List.of(false, true)) {
        for (String operation : List.of("add", "remove", "add-distinct")) {
          String id = Integer.toString(sequence++);
          addMultivaluedVectors(
              id,
              floatEncoding ? List.of() : List.of(A_FLOAT_VECTOR),
              floatEncoding ? List.of(A_BYTE_VECTOR) : List.of(),
              committed);
          atomicVectorOperation(
              id,
              floatEncoding
                  ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                  : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
              operation,
              floatEncoding ? List.of(ANOTHER_FLOAT_VECTOR) : List.of(ANOTHER_BYTE_VECTOR));
          assertMultivaluedVectorsAfterAtomicSet(
              id,
              floatEncoding
                  ? (operation.equals("remove") ? List.of() : List.of(ANOTHER_FLOAT_VECTOR))
                  : List.of(A_FLOAT_VECTOR),
              floatEncoding
                  ? List.of(A_BYTE_VECTOR)
                  : (operation.equals("remove") ? List.of() : List.of(ANOTHER_BYTE_VECTOR)));
        }
      }
    }
  }

  @Test
  public void atomicVectorEmptyOperands_shouldPreserveExistingVectors() throws Exception {
    int sequence = 250;
    for (boolean committed : List.of(false, true)) {
      for (String operation : List.of("add", "remove", "add-distinct")) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), committed);
        atomicVectorOperation(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, operation, List.of());
        atomicVectorOperation(id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, operation, List.of());
        assertMultivaluedVectorsAfterAtomicSet(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
      }
    }
  }

  @Test
  public void atomicVectorRemoveAll_shouldRemoveChildrenOnlyForRequestedField() throws Exception {
    int sequence = 260;
    for (boolean committed : List.of(false, true)) {
      for (boolean floatEncoding : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(
            id,
            List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR),
            List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR),
            committed);
        atomicVectorOperation(
            id,
            floatEncoding
                ? MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                : MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
            "remove",
            floatEncoding
                ? List.of(A_FLOAT_VECTOR, A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR)
                : List.of(A_BYTE_VECTOR, A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR));
        assertMultivaluedVectorsAfterAtomicSet(
            id,
            floatEncoding
                ? List.of()
                : List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR),
            floatEncoding ? List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR, A_BYTE_VECTOR) : List.of());
      }
    }
  }

  @Test
  public void atomicVectorMixedOperations_shouldWorkWithoutIntermediateCommits() throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "271" : "270";
      addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), committed);
      atomicVectorOperation(
          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "add", List.of(ANOTHER_FLOAT_VECTOR));
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "add-distinct",
          List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR));
      atomicVectorOperation(
          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "remove", List.of(ANOTHER_FLOAT_VECTOR));
      atomicVectorOperation(
          id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, "add", List.of(ANOTHER_BYTE_VECTOR));
      atomicVectorOperation(
          id,
          MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
          "add-distinct",
          List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR));
      assertEquals(0, atomicSetOnStringField(id, "updated").getStatus());
      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR));
      assertEquals("updated", realTimeGet(id, empty()).getFieldValue(STRING_FIELD_NAME));
    }
  }

  @Test
  public void atomicVectorOperationsOnSingleDimensionVectors_shouldKeepNestedLists()
      throws Exception {
    String floatField = "vector_multivalued_1d";
    String byteField = "vector_byte_multivalued_1d";
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "281" : "280";
      var doc = new SolrInputDocument();
      doc.setField(CommonParams.ID, id);
      doc.setField(floatField, List.of(List.of(1.25f), List.of(2.5f)));
      doc.setField(byteField, List.of(List.of(-128), List.of(127)));
      assertEquals(0, client.add(doc).getStatus());
      if (committed) {
        client.commit();
      }
      atomicVectorOperation(id, floatField, "add", List.of(List.of(3.75d)));
      atomicVectorOperation(
          id, floatField, "add-distinct", List.of(List.of(1.25d), List.of(3.75d)));
      atomicVectorOperation(id, floatField, "remove", List.of(List.of(2.5d)));
      atomicVectorOperation(
          id, byteField, "add-distinct", List.of(List.of(-128L), List.of(0L), List.of(0)));
      atomicVectorOperation(id, byteField, "remove", List.of(List.of(127L)));
      var expected =
          Map.of(
              floatField,
              List.of(List.of(1.25f), List.of(3.75f)),
              byteField,
              List.of(List.of(-128), List.of(0)));
      client.commit();
      assertVectorChildren(id, expected);
      var children =
          new QueryRequest(
                  "/select",
                  new SolrQuery("_root_:" + id + " AND -id:" + id).setFields(CommonParams.ID))
              .process(client)
              .getResults();
      assertEquals(4L, children.getNumFound());
      assertEquals(
          Set.of(
              id + "/" + floatField + "#0",
              id + "/" + floatField + "#1",
              id + "/" + byteField + "#0",
              id + "/" + byteField + "#1"),
          children.stream()
              .map(child -> child.getFieldValue(CommonParams.ID))
              .collect(Collectors.toSet()));
    }
  }

  @Test
  public void atomicVectorOperations_shouldRejectInvalidOperandsWithoutChangingDocument()
      throws Exception {
    int sequence = 290;
    for (boolean committed : List.of(false, true)) {
      for (String operation : List.of("set", "add", "remove", "add-distinct")) {
        String id = Integer.toString(sequence++);
        addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), committed);
        var invalidFloats = new ArrayList<Object>();
        invalidFloats.add(List.of(List.of(1, 2)));
        invalidFloats.add(List.of(1, 2, 3, 4));
        invalidFloats.add(List.of(List.of(Float.NaN, 2, 3, 4)));
        invalidFloats.add(List.of(List.of(Float.POSITIVE_INFINITY, 2, 3, 4)));
        invalidFloats.add(List.of(List.of(true, 2, 3, 4)));
        invalidFloats.add("not a list");
        var nullVector = new ArrayList<Object>();
        nullVector.add(null);
        invalidFloats.add(nullVector);
        if (!operation.equals("set")) {
          invalidFloats.add(null);
        }
        for (Object invalid : invalidFloats) {
          var error =
              expectThrows(
                  SolrException.class,
                  () ->
                      atomicVectorOperation(
                          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, operation, invalid));
          assertEquals(400, error.code());
        }
        for (Object invalid :
            List.of(
                List.of(List.of(128, 0, 0, 0)),
                List.of(List.of(-129, 0, 0, 0)),
                List.of(List.of(0.5d, 0, 0, 0)))) {
          var error =
              expectThrows(
                  SolrException.class,
                  () ->
                      atomicVectorOperation(
                          id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, operation, invalid));
          assertEquals(400, error.code());
        }
        assertMultivaluedVectorsAfterAtomicSet(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
      }
    }
  }

  @Test
  public void atomicVectorOperations_shouldPreserveOrdinaryChildren() throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "311" : "310";
      var child = newDocumentWithVectors(id + "/ordinary", A_FLOAT_VECTOR, A_BYTE_VECTOR);
      var doc = new SolrInputDocument();
      doc.setField(CommonParams.ID, id);
      doc.setField(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(A_FLOAT_VECTOR));
      doc.setField(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(A_BYTE_VECTOR));
      doc.setField("child", child);
      assertEquals(0, client.add(doc).getStatus());
      if (committed) {
        client.commit();
      }
      atomicVectorOperation(
          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "add-distinct", List.of(ANOTHER_FLOAT_VECTOR));
      atomicVectorOperation(
          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "remove", List.of(A_FLOAT_VECTOR));
      atomicVectorOperation(
          id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, "add", List.of(ANOTHER_BYTE_VECTOR));
      client.commit();
      assertVectorChildren(
          id,
          Map.of(
              MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(ANOTHER_FLOAT_VECTOR),
              MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR)));
      var actualChild = realTimeGet(id + "/ordinary", empty());
      assertEquals(
          A_FLOAT_VECTOR, actualChild.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(A_BYTE_VECTOR, actualChild.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
      var children =
          new QueryRequest(
                  "/select",
                  new SolrQuery("_root_:" + id + " AND -id:" + id).setFields(CommonParams.ID))
              .process(client)
              .getResults();
      assertEquals(4L, children.getNumFound());
    }
  }

  @Test
  public void atomicVectorOperationsOnStoredVectors_shouldPreserveValues() throws Exception {
    int sequence = 320;
    for (boolean committed : List.of(false, true)) {
      for (boolean singleDimension : List.of(false, true)) {
        String id = Integer.toString(sequence++);
        String floatField = singleDimension ? "vector_multivalued_1d" : "vector_multivalued";
        String byteField =
            singleDimension ? "vector_byte_multivalued_1d" : "vector_byte_multivalued";
        List<Float> firstFloat = singleDimension ? List.of(1.25f) : A_FLOAT_VECTOR;
        List<Float> secondFloat = singleDimension ? List.of(2.5f) : ANOTHER_FLOAT_VECTOR;
        List<Integer> firstByte = singleDimension ? List.of(-128) : A_BYTE_VECTOR;
        List<Integer> secondByte = singleDimension ? List.of(127) : ANOTHER_BYTE_VECTOR;
        var doc = new SolrInputDocument();
        doc.setField(CommonParams.ID, id);
        doc.setField(floatField, List.of(firstFloat));
        doc.setField(byteField, List.of(firstByte));
        assertEquals(0, client.add(doc).getStatus());
        if (committed) {
          client.commit();
        }
        atomicVectorOperation(id, floatField, "add", List.of(secondFloat, firstFloat));
        atomicVectorOperation(id, floatField, "remove", List.of(firstFloat));
        atomicVectorOperation(id, floatField, "add-distinct", List.of(firstFloat, secondFloat));
        atomicVectorOperation(id, byteField, "add", List.of(secondByte, firstByte));
        atomicVectorOperation(id, byteField, "remove", List.of(firstByte));
        atomicVectorOperation(id, byteField, "add-distinct", List.of(firstByte, secondByte));
        assertEquals(0, atomicSetOnStringField(id, "updated").getStatus());
        client.commit();
        assertVectorChildren(
            id,
            Map.of(
                floatField,
                List.of(secondFloat, firstFloat),
                byteField,
                List.of(secondByte, firstByte)));
        var children =
            new QueryRequest(
                    "/select",
                    new SolrQuery("_root_:" + id + " AND -id:" + id).setFields(CommonParams.ID))
                .process(client)
                .getResults();
        assertEquals(4L, children.getNumFound());
      }
    }
  }

  @Test
  public void atomicVectorMultipleModifiers_shouldUseTheSameVectorCollection() throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "341" : "340";
      addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), committed);
      var operations = new LinkedHashMap<String, Object>();
      operations.put("add", List.of(ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR));
      operations.put("remove", List.of(A_FLOAT_VECTOR));
      operations.put("add-distinct", List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR));
      var update = new SolrInputDocument();
      update.setField(CommonParams.ID, id);
      update.setField(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, operations);
      assertEquals(0, client.add(update).getStatus());
      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(ANOTHER_FLOAT_VECTOR, A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
    }
  }

  @Test
  public void atomicVectorJsonUpdates_shouldCompareNumbersAtVectorPrecision() throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "351" : "350";
      var first = List.of(0.1f, 2.5f, 3.75f, 4.0f);
      addMultivaluedVectors(id, List.of(first), List.of(A_BYTE_VECTOR), committed);
      String json =
          """
          [{"id":"%s",
            "vector_multivalued":{"add-distinct":[[0.1,2.5,3.75,4.0],[4.0,3.75,2.5,1.25]]},
            "vector_byte_multivalued":{"add-distinct":[[-128,-1,0,127],[127,0,-1,-128]]}}]
          """
              .replace("%s", id);
      var request =
          new GenericSolrRequest(SolrRequest.METHOD.POST, "/update")
              .setRequiresCollection(true)
              .setContentWriter(
                  new RequestWriter.StringPayloadContentWriter(json, "application/json"));
      request.process(client);
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "remove",
          List.of(List.of(0.1d, 2.5d, 3.75d, 4.0d)));
      atomicVectorOperation(
          id,
          MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
          "remove",
          List.of(A_BYTE_VECTOR.stream().map(Number::longValue).toList()));
      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(ANOTHER_FLOAT_VECTOR), List.of(ANOTHER_BYTE_VECTOR));
    }
  }

  @Test
  public void atomicVectorMatching_shouldTreatSignedZeroAsTheSameCoordinate() throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "361" : "360";
      var first = List.of(0.0f, 2.5f, 3.75f, 4.0f);
      var candidate = List.of(-0.0d, 2.5d, 3.75d, 4.0d);
      addMultivaluedVectors(id, List.of(first), List.of(A_BYTE_VECTOR), committed);
      atomicVectorOperation(
          id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "add-distinct", List.of(candidate));
      assertMultivaluedVectorsAfterAtomicSet(id, List.of(first), List.of(A_BYTE_VECTOR));
      atomicVectorOperation(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "remove", List.of(candidate));
      assertMultivaluedVectorsAfterAtomicSet(id, List.of(), List.of(A_BYTE_VECTOR));
    }
  }

  @Test
  public void atomicVectorOperations_shouldRejectScalarModifiers() throws Exception {
    addMultivaluedVectors("370", List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), false);
    for (String field :
        List.of(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME)) {
      for (String operation : List.of("inc", "removeregex")) {
        var error =
            expectThrows(
                SolrException.class, () -> atomicVectorOperation("370", field, operation, 1));
        assertEquals(400, error.code());
        assertTrue(error.getMessage().contains("Operation '" + operation + "' is not supported"));
      }
    }
    assertMultivaluedVectorsAfterAtomicSet("370", List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
  }

  @Test
  public void atomicVectorChildDocumentUpdates_shouldKeepNestedDocumentSemantics()
      throws Exception {
    String id = "380";
    String childId = id + "/" + MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME + "#1";
    addMultivaluedVectors(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), true);
    var child = new SolrInputDocument();
    child.setField(CommonParams.ID, childId);
    child.setField(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, ANOTHER_FLOAT_VECTOR);
    atomicVectorOperation(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "add", child);
    assertMultivaluedVectorsAfterAtomicSet(
        id, List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));

    var removal = new SolrInputDocument();
    removal.setField(CommonParams.ID, childId);
    atomicVectorOperation(id, MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, "remove", removal);
    assertMultivaluedVectorsAfterAtomicSet(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
  }

  @Test
  public void atomicVectorArrayOperands_shouldPreserveSolrJVectorRepresentations()
      throws Exception {
    for (boolean committed : List.of(false, true)) {
      String id = committed ? "391" : "390";
      var doc = new SolrInputDocument();
      doc.setField(CommonParams.ID, id);
      doc.setField(
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(new float[] {1.25f, 2.5f, 3.75f, 4.0f}));
      doc.setField(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, List.of(A_BYTE_VECTOR));
      assertEquals(0, client.add(doc).getStatus());
      if (committed) {
        client.commit();
      }
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "add",
          List.of(new float[] {4.0f, 3.75f, 2.5f, 1.25f}));
      atomicVectorOperation(
          id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, "add", List.of(new byte[] {127, 0, -1, -128}));
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "add-distinct",
          List.of(new double[] {1.25, 2.5, 3.75, 4.0}, new double[] {4.0, 3.75, 2.5, 1.25}));
      atomicVectorOperation(
          id,
          MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
          "add-distinct",
          List.of(new int[] {-128, -1, 0, 127}, new int[] {127, 0, -1, -128}));
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "remove",
          List.of(new double[] {1.25, 2.5, 3.75, 4.0}));
      atomicVectorOperation(
          id,
          MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
          "remove",
          List.of(new byte[] {-128, -1, 0, 127}));
      assertMultivaluedVectorsAfterAtomicSet(
          id, List.of(ANOTHER_FLOAT_VECTOR), List.of(ANOTHER_BYTE_VECTOR));
      atomicVectorOperation(
          id,
          MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
          "set",
          List.of(new double[] {1.25, 2.5, 3.75, 4.0}));
      atomicVectorOperation(
          id, MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, "set", List.of(new int[] {-128, -1, 0, 127}));
      assertMultivaluedVectorsAfterAtomicSet(id, List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR));
    }
  }

  private void addMultivaluedVectors(
      String id, List<List<Float>> floats, List<List<Integer>> bytes, boolean commit)
      throws Exception {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, id);
    doc.setField(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, floats);
    doc.setField(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, bytes);
    assertEquals(0, client.add(doc).getStatus());
    if (commit) {
      client.commit();
    }
  }

  private void atomicSetVectorField(String id, String field, Object value) throws Exception {
    var operation = new HashMap<String, Object>();
    operation.put("set", value);
    var update = new SolrInputDocument();
    update.setField(CommonParams.ID, id);
    update.setField(field, operation);
    assertEquals(0, client.add(update).getStatus());
  }

  private void atomicVectorOperation(String id, String field, String operation, Object value)
      throws Exception {
    var operand = new HashMap<String, Object>();
    operand.put(operation, value);
    var update = new SolrInputDocument();
    update.setField(CommonParams.ID, id);
    update.setField(field, operand);
    assertEquals(0, client.add(update).getStatus());
  }

  private SolrDocument realTimeGet(String id, Optional<Set<String>> fields) throws Exception {
    var params = new ModifiableSolrParams().set(CommonParams.ID, id);
    fields.ifPresent(names -> params.set(CommonParams.FL, String.join(",", names)));

    var request = new QueryRequest(params);
    request.setPath("/get");

    var doc = (SolrDocument) client.request(request).get("doc");
    assertNotNull(doc);

    return doc;
  }

  private static SolrInputDocument newDocumentWithVectors(
      String id, List<Float> floatVector, List<Integer> byteVector) {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, id);
    doc.setField(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, floatVector);
    doc.setField(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, byteVector);
    return doc;
  }

  private UpdateResponse atomicSetOnStringField(String id, String value) throws Exception {
    var update = new SolrInputDocument();
    update.setField(CommonParams.ID, id);
    update.setField(STRING_FIELD_NAME, Map.of("set", value));
    return client.add(update);
  }

  private String generateRandomStringValue() {
    return TestUtil.randomSimpleString(random(), 5, 20);
  }

  private void assertMultivaluedVectorsAfterAtomicSet(
      String id, List<List<Float>> floats, List<List<Integer>> bytes) throws Exception {
    client.commit();
    assertVectorChildren(
        id,
        Map.of(
            MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, floats,
            MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, bytes));
    var query = new SolrQuery("_root_:" + id + " AND -id:" + id).setFields(CommonParams.ID);
    assertEquals(
        (long) floats.size() + bytes.size(),
        new QueryRequest("/select", query).process(client).getResults().getNumFound());
  }

  private void assertMultiValuedVectorChildren(
      String rootId, String field, List<? extends List<? extends Number>> vectors)
      throws Exception {
    assertVectorChildren(rootId, Map.of(field, vectors));
  }

  private void assertVectorChildren(
      String rootId, Map<String, ? extends List<? extends List<? extends Number>>> vectorsByField)
      throws Exception {
    var fields = new ArrayList<String>();
    fields.add(CommonParams.ID);
    fields.add(IndexSchema.NEST_PATH_FIELD_NAME);
    fields.addAll(vectorsByField.keySet());
    var query =
        new SolrQuery("_root_:" + rootId + " AND -id:" + rootId)
            .setFields(fields.toArray(String[]::new))
            .setRows(100);
    var children = new QueryRequest("/select", query).process(client).getResults();

    for (var entry : vectorsByField.entrySet()) {
      String field = entry.getKey();
      var byId = new HashMap<String, Object>();
      for (var child : children) {
        var value = child.getFieldValue(field);
        if (value != null) {
          String childId = (String) child.getFieldValue(CommonParams.ID);
          assertEquals(
              childId,
              childId.substring(rootId.length()),
              child.getFieldValue(IndexSchema.NEST_PATH_FIELD_NAME));
          assertNull("duplicate vector child " + childId, byId.put(childId, value));
        }
      }
      assertEquals("vector children for " + field, entry.getValue().size(), byId.size());
      for (int ordinal = 0; ordinal < entry.getValue().size(); ordinal++) {
        String childId = rootId + "/" + field + "#" + ordinal;
        assertEquals(
            childId, storedVectorValue(field, entry.getValue().get(ordinal)), byId.get(childId));
      }
    }
  }

  private static List<?> storedVectorValue(String field, List<? extends Number> vector) {
    return field.startsWith("vector_byte_") ? List.of(vector) : vector;
  }
}
