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

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DenseVectorFieldRetrievalTest extends SolrTestCase {

  private static final String SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME =
      "vector_single_value_float_encoding";
  private static final String SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME =
      "vector_single_value_byte_encoding";

  private static final String MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME =
      "vector_multi_value_float_encoding";
  private static final String MULTI_VALUE_BYTE_ENCODED_FIELD_NAME =
      "vector_multi_value_byte_encoding";

  private static final String STRING_FIELD_NAME = "a_string_field";

  private static final List<Float> A_FLOAT_VECTOR = List.of(1.25f, 2.5f, 3.75f, 4.0f);
  private static final List<Integer> A_BYTE_VECTOR = List.of(-128, -1, 0, 127);

  private static final List<Float> ANOTHER_FLOAT_VECTOR = List.of(4.0f, 3.75f, 2.5f, 1.25f);
  private static final List<Integer> ANOTHER_BYTE_VECTOR = List.of(127, 0, -1, -128);

  private final boolean indexed;
  private final boolean stored;

  @Rule public final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  private SolrClient client;

  /**
   * Tests raw vector retrieval for all four combinations of {@code indexed} and {@code stored}.
   * Each test runs once per combination. The retrieved vector values must be the same in every
   * configuration.
   */
  public DenseVectorFieldRetrievalTest(
      @Name("indexed") boolean indexed, @Name("stored") boolean stored) {
    this.indexed = indexed;
    this.stored = stored;
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return List.of(
        new Object[] {false, false},
        new Object[] {false, true},
        new Object[] {true, false},
        new Object[] {true, true});
  }

  @Before
  public void setUpCore() throws Exception {
    SolrTestCaseJ4.newRandomConfig();
    System.setProperty("solr.index.updatelog.enabled", "true");
    System.setProperty("tests.rawvectors.indexed", Boolean.toString(indexed));
    System.setProperty("tests.rawvectors.stored", Boolean.toString(stored));

    solrTestRule.startSolr();
    solrTestRule
        .newCollection()
        .withConfigSet(
            ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/collection1/conf"))
        .withConfigFile("solrconfig.xml")
        .withSchemaFile("schema-densevector-raw.xml")
        .create();

    client = solrTestRule.getSolrClient();

    client.add(newDocumentWithVectors("0", A_FLOAT_VECTOR, A_BYTE_VECTOR));
    client.add(newDocumentWithVectors("1", ANOTHER_FLOAT_VECTOR, ANOTHER_BYTE_VECTOR));

    SolrInputDocument withoutVectors = new SolrInputDocument();
    withoutVectors.setField(CommonParams.ID, "2");
    client.add(withoutVectors);

    client.commit();
  }

  @Test
  public void selectWithFieldPattern_shouldReturnAllMatchingVectorFields() throws Exception {
    var query = new SolrQuery("id:0").setFields(CommonParams.ID, "vector*");
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());
    var doc = docs.getFirst();
    assertEquals(
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME),
        doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void selectWithReturnAllFields_shouldReturnAllVectorFields() throws Exception {
    var query = new SolrQuery("id:0").setFields("*");
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());

    var doc = docs.getFirst();
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void selectWithNoFieldList_shouldReturnAllVectorFields() throws Exception {
    // Search must include raw vectors when no field list is provided.
    var query = new SolrQuery("id:0");
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());

    var doc = docs.getFirst();
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void selectGetWithExplicitFieldList_shouldExcludeUnrequestedVectorFields()
      throws Exception {
    var query = new SolrQuery("id:0").setFields(CommonParams.ID);
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());
    assertEquals(Set.of(CommonParams.ID), docs.getFirst().keySet());
    assertEquals("0", docs.getFirst().getFieldValue(CommonParams.ID));
  }

  @Test
  public void selectWithExplicitFieldList_shouldReturnVectorFields() throws Exception {
    var query =
        new SolrQuery("id:0")
            .setFields(
                CommonParams.ID,
                SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
                SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);

    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());
    assertEquals("0", docs.getFirst().getFieldValue(CommonParams.ID));
    assertEquals(
        A_FLOAT_VECTOR, docs.getFirst().getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(
        A_BYTE_VECTOR, docs.getFirst().getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithMultipleIds_shouldReturnMatchingDocumentsWithVectors()
      throws Exception {
    var fieldList =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var params =
        new ModifiableSolrParams()
            .set("ids", "1,2,0")
            .set(CommonParams.FL, String.join(",", fieldList));
    var docs = (SolrDocumentList) client.request(new QueryRequest("/get", params)).get("response");

    assertNotNull(docs);
    assertEquals(3L, docs.getNumFound());
    assertEquals(3, docs.size());

    var firstDoc = docs.getFirst();
    assertEquals(fieldList, firstDoc.keySet());
    assertEquals("1", firstDoc.getFieldValue(CommonParams.ID));
    assertEquals(
        ANOTHER_FLOAT_VECTOR, firstDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(ANOTHER_BYTE_VECTOR, firstDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

    var secondDoc = docs.get(1);
    assertEquals(Set.of(CommonParams.ID), secondDoc.keySet());
    assertEquals("2", secondDoc.getFieldValue(CommonParams.ID));
    assertNull(secondDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(secondDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

    var last = docs.get(2);
    assertEquals(fieldList, last.keySet());
    assertEquals("0", last.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, last.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, last.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithExplicitFieldList_shouldReturnVectorsForRequestedDocument()
      throws Exception {
    var fieldList =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var doc = realTimeGet("1", of(fieldList));

    assertEquals(fieldList, doc.keySet());
    assertEquals("1", doc.getFieldValue(CommonParams.ID));
    assertEquals(ANOTHER_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(ANOTHER_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithExplicitFieldList_shouldReturnOnlyRequestedFloatVectorField()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithExplicitFieldList_shouldReturnOnlyRequestedByteVectorField()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithExplicitFieldList_shouldExcludeUnrequestedVectorFields()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithFieldPattern_shouldReturnAllMatchingVectorFields() throws Exception {
    var fieldList = Set.of(CommonParams.ID, "vector*");
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME),
        doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithReturnAllFields_shouldReturnAllVectorFields() throws Exception {
    var fieldList = Set.of("*");
    var doc = realTimeGet("0", of(fieldList));

    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithNoFieldList_shouldReturnAllVectorFields() throws Exception {
    var doc = realTimeGet("0", empty());

    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGet_shouldOmitMissingVectorsEvenIfRequested() throws Exception {
    var missingVectorsDocumentId = "2";
    var fieldList =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);

    var expectedFieldList = Set.of(CommonParams.ID);
    var doc = realTimeGet(missingVectorsDocumentId, of(fieldList));

    assertEquals(expectedFieldList, doc.keySet());
    assertEquals(missingVectorsDocumentId, doc.getFieldValue(CommonParams.ID));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithFieldPattern_shouldReturnMatchingFloatVectorField() throws Exception {
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_float*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithFieldPattern_shouldReturnMatchingByteVectorField() throws Exception {
    // A byte-only pattern must exclude the float vector from the response.
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_byte*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void rawVectors_winOverStoredValues() throws Exception {
    try (var core = solrTestRule.getCoreContainer().getCore("collection1");
        var request = new SolrQueryRequestBase(core, new ModifiableSolrParams())) {
      var searcher = request.getSearcher();
      int docId = searcher.getFirstMatch(new Term(CommonParams.ID, "0"));
      assertTrue(docId >= 0);

      var prepopulatedDocWithStoredValues = new SolrDocument();
      prepopulatedDocWithStoredValues.setField(
          SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, ANOTHER_FLOAT_VECTOR);
      prepopulatedDocWithStoredValues.setField(
          SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, ANOTHER_BYTE_VECTOR);

      searcher
          .getDocFetcher()
          .decorateRawVectorFields(
              prepopulatedDocWithStoredValues,
              docId,
              new SolrReturnFields(
                  Set.of(
                      SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME),
                  null));

      assertEquals(
          A_FLOAT_VECTOR,
          prepopulatedDocWithStoredValues.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(
          A_BYTE_VECTOR,
          prepopulatedDocWithStoredValues.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    }
  }

  @Test
  public void rawVectorsDisabled_shouldReturnStoredVector() throws Exception {
    String id = generateRandomStringValue();
    String testFieldNameWithRawVectorsDisabled = "field_opt_out_stored";

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, id);
    doc.setField(testFieldNameWithRawVectorsDisabled, A_FLOAT_VECTOR);
    assertEquals(0, client.add(doc).getStatus());
    client.commit();

    var query = new SolrQuery("id:" + id).setFields(testFieldNameWithRawVectorsDisabled);
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(
        A_FLOAT_VECTOR, docs.getFirst().getFieldValue(testFieldNameWithRawVectorsDisabled));
  }

  @Test
  public void rawVectorsDisabled_shouldPreferStoredVectorOverIndexedVector() throws Exception {
    String id = generateRandomStringValue();
    String testFieldNameWithRawVectorsDisabled = "field_opt_out_stored";

    try (var core = solrTestRule.getCoreContainer().getCore("collection1")) {
      var schema = core.getLatestSchema();
      var vectorField = schema.getField(testFieldNameWithRawVectorsDisabled);
      List<IndexableField> luceneFields =
          new ArrayList<>(schema.getField(CommonParams.ID).createFields(id));

      vectorField.createFields(A_FLOAT_VECTOR).stream()
          .filter(field -> field.fieldType().vectorDimension() > 0)
          .forEach(luceneFields::add);
      vectorField.createFields(ANOTHER_FLOAT_VECTOR).stream()
          .filter(field -> field.fieldType().stored())
          .forEach(luceneFields::add);

      var writer = core.getSolrCoreState().getIndexWriter(core);
      try {
        writer.get().addDocument(luceneFields);
      } finally {
        writer.decref();
      }
    }
    client.commit();

    var query = new SolrQuery("id:" + id).setFields(testFieldNameWithRawVectorsDisabled);
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(
        ANOTHER_FLOAT_VECTOR, docs.getFirst().getFieldValue(testFieldNameWithRawVectorsDisabled));
  }

  @Test
  public void
      realtimeGetMissingByteVectorFieldInFieldList_shouldntPreventOtherVectorFieldRetrieval()
          throws Exception {
    var input = new SolrInputDocument();
    input.setField(CommonParams.ID, "3");
    input.setField(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, A_FLOAT_VECTOR);
    client.add(input);
    client.commit();

    var doc =
        realTimeGet(
            "3",
            of(
                Set.of(
                    CommonParams.ID,
                    SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
                    SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME)));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("3", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void
      realtimeGetMissingFloatVectorFieldInFieldList_shouldntPreventOtherVectorFieldRetrieval()
          throws Exception {
    var input = new SolrInputDocument();
    input.setField(CommonParams.ID, "3");
    input.setField(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, A_BYTE_VECTOR);
    client.add(input);
    client.commit();

    var doc =
        realTimeGet(
            "3",
            of(
                Set.of(
                    CommonParams.ID,
                    SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
                    SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME)));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("3", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithAliasedFloatVectorField_shouldReturnVectorFieldUnderAlias()
      throws Exception {
    var doc =
        realTimeGet(
            "0", of(Set.of(CommonParams.ID, "anAlias:" + SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME)));

    assertEquals(Set.of(CommonParams.ID, "anAlias"), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue("anAlias"));
  }

  @Test
  public void realtimeGetWithAliasedByteVectorField_shouldReturnVectorFieldUnderAlias()
      throws Exception {
    var doc =
        realTimeGet(
            "0",
            of(Set.of(CommonParams.ID, "anotherAlias:" + SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME)));

    assertEquals(Set.of(CommonParams.ID, "anotherAlias"), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue("anotherAlias"));
  }

  @Test
  public void realtimeGetOnUncommittedDocument_shouldReturnVectorsMatchingFieldList()
      throws Exception {
    client.add(newDocumentWithVectors("3", A_FLOAT_VECTOR, A_BYTE_VECTOR));

    var bothVectorFields =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);

    var fieldLists =
        List.of(
            bothVectorFields,
            Set.of("*"),
            Set.of(CommonParams.ID, "vector*"),
            Set.of(CommonParams.ID, "vector_single_value_float*"),
            Set.of(CommonParams.ID, "vector_single_value_byte*"),
            Set.of(CommonParams.ID));

    var correspondingExpectedFieldLists =
        List.of(
            bothVectorFields,
            bothVectorFields,
            bothVectorFields,
            Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME),
            Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME),
            Set.of(CommonParams.ID));

    List<Map<String, Object>> uncommittedDocsFromTLog = new ArrayList<>();

    for (int i = 0; i < fieldLists.size(); i++) {
      var fieldList = fieldLists.get(i);
      var expectedFieldListInResponse = correspondingExpectedFieldLists.get(i);

      var doc = realTimeGet("3", of(fieldList));
      assertEquals("3", doc.getFieldValue(CommonParams.ID));

      if (fieldLists.get(i).contains("*")) {
        // Could contain also other fields, like _version_
        assertTrue(doc.keySet().containsAll(expectedFieldListInResponse));
      } else {
        assertEquals(expectedFieldListInResponse, doc.keySet());
      }

      assertEquals(
          expectedFieldListInResponse.contains(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME)
              ? A_FLOAT_VECTOR
              : null,
          doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(
          expectedFieldListInResponse.contains(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME)
              ? A_BYTE_VECTOR
              : null,
          doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
      uncommittedDocsFromTLog.add(new HashMap<>(doc));
    }

    client.commit();

    for (int i = 0; i < fieldLists.size(); i++) {
      var uncommittedDoc = uncommittedDocsFromTLog.get(i);
      Map<String, Object> committedDoc = new HashMap<>(realTimeGet("3", of(fieldLists.get(i))));
      assertEquals(uncommittedDoc, committedDoc);
    }
  }

  @Test
  public void realtimeGetOnUncommittedDocuments_shouldOmitMissingVectorsEvenIfRequested()
      throws Exception {
    var floatOnly = new SolrInputDocument();
    floatOnly.setField(CommonParams.ID, "3");
    floatOnly.setField(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, A_FLOAT_VECTOR);
    client.add(floatOnly);

    var byteOnly = new SolrInputDocument();
    byteOnly.setField(CommonParams.ID, "4");
    byteOnly.setField(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, A_BYTE_VECTOR);
    client.add(byteOnly);

    var noVectorsAtAll = new SolrInputDocument();
    noVectorsAtAll.setField(CommonParams.ID, "5");
    client.add(noVectorsAtAll);

    var fields =
        of(
            Set.of(
                CommonParams.ID,
                SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
                SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

    var floatDoc = realTimeGet("3", fields);
    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), floatDoc.keySet());
    assertEquals(A_FLOAT_VECTOR, floatDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(floatDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

    var byteDoc = realTimeGet("4", fields);
    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), byteDoc.keySet());
    assertEquals(A_BYTE_VECTOR, byteDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(byteDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));

    var missingDoc = realTimeGet("5", fields);
    assertEquals(Set.of(CommonParams.ID), missingDoc.keySet());
    assertNull(missingDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(missingDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

    client.commit();

    assertEquals(new HashMap<>(floatDoc), new HashMap<>(realTimeGet("3", fields)));
    assertEquals(new HashMap<>(byteDoc), new HashMap<>(realTimeGet("4", fields)));
    assertEquals(new HashMap<>(missingDoc), new HashMap<>(realTimeGet("5", fields)));
  }

  @Test
  public void retrievalFromMultiSegmentIndex_shouldReadVectorsFromOwningSegment() throws Exception {
    try (var core = solrTestRule.getCoreContainer().getCore("collection1")) {
      var writer = core.getSolrCoreState().getIndexWriter(core);
      try {
        writer.get().getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
      } finally {
        writer.decref();
      }

      client.add(newDocumentWithVectors("3", ANOTHER_FLOAT_VECTOR, ANOTHER_BYTE_VECTOR));
      client.commit();

      var withoutVectors = new SolrInputDocument();
      withoutVectors.setField(CommonParams.ID, "4");
      client.add(withoutVectors);
      client.commit();

      try (var request = new SolrQueryRequestBase(core, new ModifiableSolrParams())) {
        var searcher = request.getSearcher();
        var segments = searcher.getLeafContexts();
        assertTrue(segments.size() >= 3);

        int vectorDocId = searcher.getFirstMatch(new Term(CommonParams.ID, "3"));
        assertTrue(vectorDocId >= 0);
        var vectorLeaf = segments.get(ReaderUtil.subIndex(vectorDocId, segments));
        assertTrue(vectorLeaf.docBase > 0);

        int missingDocId = searcher.getFirstMatch(new Term(CommonParams.ID, "4"));
        assertTrue(missingDocId >= 0);
        var missingLeaf = segments.get(ReaderUtil.subIndex(missingDocId, segments));
        assertNotEquals(vectorLeaf.ord, missingLeaf.ord);
        assertNull(
            missingLeaf.reader().getFloatVectorValues(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
        assertNull(missingLeaf.reader().getByteVectorValues(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
      }

      var fields =
          Set.of(
              CommonParams.ID,
              SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
              SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
      var doc = realTimeGet("3", of(fields));
      assertEquals(fields, doc.keySet());
      assertEquals("3", doc.getFieldValue(CommonParams.ID));
      assertEquals(ANOTHER_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(ANOTHER_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

      var noVectorsDoc = realTimeGet("4", of(fields));
      assertEquals(Set.of(CommonParams.ID), noVectorsDoc.keySet());
      assertEquals("4", noVectorsDoc.getFieldValue(CommonParams.ID));
      assertNull(noVectorsDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertNull(noVectorsDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

      var query =
          new SolrQuery("id:3")
              .setFields(
                  CommonParams.ID,
                  SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
                  SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
      var docs = new QueryRequest("/select", query).process(client).getResults();
      assertEquals(1L, docs.getNumFound());
      assertEquals(1, docs.size());

      var firstDoc = docs.getFirst();
      assertEquals("3", firstDoc.getFieldValue(CommonParams.ID));
      assertEquals(
          ANOTHER_FLOAT_VECTOR, firstDoc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(
          ANOTHER_BYTE_VECTOR, firstDoc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    }
  }

  @Test
  public void atomicUpdate_shouldPreserveVectors() throws Exception {
    var alreadyInIndexDocumentId = "0";
    var newFieldValue = generateRandomStringValue();
    var updateResponse = atomicSetOnStringField(alreadyInIndexDocumentId, newFieldValue);
    assertEquals(0, updateResponse.getStatus());

    assertAtomicUpdateResultsUsingGet(
        alreadyInIndexDocumentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);

    client.commit();

    assertAtomicUpdateResultsUsingGet(
        alreadyInIndexDocumentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(
        alreadyInIndexDocumentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);
  }

  @Test
  public void atomicUpdateOnUncommittedDocument_shouldPreserveVectors() throws Exception {
    var newFieldValue = generateRandomStringValue();
    var documentId = "3";
    var newDocument = newDocumentWithVectors(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR);
    var updateResponse = client.add(newDocument);
    assertEquals(0, updateResponse.getStatus());

    updateResponse = atomicSetOnStringField(documentId, newFieldValue);
    assertEquals(0, updateResponse.getStatus());

    assertAtomicUpdateResultsUsingGet(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);

    client.commit();

    assertAtomicUpdateResultsUsingGet(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);
  }

  @Test
  public void atomicUpdateOnDocumentWithoutVectors_shouldNotAddVectors() throws Exception {
    var documentIdAlreadyIndexedWithoutVectors = "2";
    var newFieldValue = generateRandomStringValue();

    var updateResponse =
        atomicSetOnStringField(documentIdAlreadyIndexedWithoutVectors, newFieldValue);
    assertEquals(0, updateResponse.getStatus());

    assertAtomicUpdateResultsUsingGet(
        documentIdAlreadyIndexedWithoutVectors, null, null, newFieldValue);

    client.commit();
    assertAtomicUpdateResultsUsingGet(
        documentIdAlreadyIndexedWithoutVectors, null, null, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(
        documentIdAlreadyIndexedWithoutVectors, null, null, newFieldValue);
  }

  @Test
  public void atomicUpdateOnUncommittedDocumentWithoutVectors_shouldNotAddVectors()
      throws Exception {
    var newFieldValue = generateRandomStringValue();
    var documentId = "3";

    var newDocumentWithoutVectors = new SolrInputDocument();
    newDocumentWithoutVectors.setField(CommonParams.ID, documentId);

    var updateResponse = client.add(newDocumentWithoutVectors);
    assertEquals(0, updateResponse.getStatus());

    updateResponse = atomicSetOnStringField(documentId, newFieldValue);
    assertEquals(0, updateResponse.getStatus());

    assertAtomicUpdateResultsUsingGet(documentId, null, null, newFieldValue);

    client.commit();

    assertAtomicUpdateResultsUsingGet(documentId, null, null, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(documentId, null, null, newFieldValue);
  }

  @Test
  public void atomicUpdateAfterInPlaceUpdate_shouldPreserveVectors() throws Exception {
    var newFieldValue = generateRandomStringValue();
    var documentId = "3";
    var doc = newDocumentWithVectors(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR);
    doc.setField("counter", 1L);

    var updateResponse = client.add(doc);
    assertEquals(0, updateResponse.getStatus());

    client.commit();

    var documentWithInPlaceUpdates = new SolrInputDocument();
    documentWithInPlaceUpdates.setField(CommonParams.ID, documentId);
    documentWithInPlaceUpdates.setField("counter", Map.of("inc", 1L));

    var request = new UpdateRequest();
    request.setParam("update.partial.requireInPlace", "true");
    request.add(documentWithInPlaceUpdates);
    request.process(client);

    updateResponse = atomicSetOnStringField(documentId, newFieldValue);
    assertEquals(0, updateResponse.getStatus());

    client.commit();

    assertAtomicUpdateResultsUsingGet(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(documentId, A_FLOAT_VECTOR, A_BYTE_VECTOR, newFieldValue);

    var actualCounterValue =
        of(realTimeGet(documentId, of(Set.of("counter"))))
            .map(document -> document.getFieldValue("counter"))
            .orElseThrow();
    assertEquals(2L, actualCounterValue);
  }

  @Test
  public void atomicUpdateOnSingleDimensionVectors_shouldPreserveVectors() throws Exception {
    var documentId = "3";
    var floatVector = List.of(1.25f);
    var byteVector = List.of(-128);
    var newFieldValue = generateRandomStringValue();

    var doc =
        newDocumentWithVectors(
            documentId,
            "vector_single_value_float_1d",
            "vector_single_value_byte_1d",
            floatVector,
            byteVector);

    var addResponse = client.add(doc);
    assertEquals(0, addResponse.getStatus());
    client.commit();

    var atomicUpdateResponse = atomicSetOnStringField(documentId, newFieldValue);
    assertEquals(0, atomicUpdateResponse.getStatus());
    client.commit();

    assertAtomicUpdateResultsUsingGet(
        documentId,
        "vector_single_value_float_1d",
        "vector_single_value_byte_1d",
        floatVector,
        byteVector,
        newFieldValue);
    assertAtomicUpdateResultsUsingSelect(
        documentId,
        "vector_single_value_float_1d",
        "vector_single_value_byte_1d",
        floatVector,
        byteVector,
        newFieldValue);
  }

  @Test
  public void atomicUpdateOnParentDocument_shouldPreserveNestedChildVectors() throws Exception {
    var parentId = "3";
    var childId = "4";
    var newFieldValue = generateRandomStringValue();

    var child = newDocumentWithVectors(childId, A_FLOAT_VECTOR, A_BYTE_VECTOR);
    var parent = new SolrInputDocument();
    parent.setField(CommonParams.ID, parentId);
    parent.setField("child", child);

    assertEquals(0, client.add(parent).getStatus());
    client.commit();

    assertEquals(0, atomicSetOnStringField(parentId, newFieldValue).getStatus());
    client.commit();

    assertAtomicUpdateResultsUsingGet(parentId, null, null, newFieldValue);
    assertAtomicUpdateResultsUsingGet(childId, A_FLOAT_VECTOR, A_BYTE_VECTOR, null);

    assertAtomicUpdateResultsUsingSelect(parentId, null, null, newFieldValue);
    assertAtomicUpdateResultsUsingSelect(childId, A_FLOAT_VECTOR, A_BYTE_VECTOR, null);

    var docs =
        new QueryRequest("/select", new SolrQuery("_root_:" + parentId).setFields(CommonParams.ID))
            .process(client)
            .getResults();
    assertEquals(2L, docs.getNumFound());
    assertEquals(2, docs.size());
    assertEquals(
        Set.of(parentId, childId),
        Set.of(
            docs.get(0).getFieldValue(CommonParams.ID),
            docs.get(1).getFieldValue(CommonParams.ID)));
  }

  @Test
  public void multivaluedVectorField_shouldFlattenVectorsOnParentOnRetrieval() throws Exception {
    var documentId = "3";
    var floatFieldName = MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME;
    var byteFieldName = MULTI_VALUE_BYTE_ENCODED_FIELD_NAME;
    var floatVectors = List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR);
    var byteVectors = List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR);

    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, documentId);
    doc.setField(floatFieldName, floatVectors);
    doc.setField(byteFieldName, byteVectors);

    assertEquals(0, client.add(doc).getStatus());
    client.commit();

    assertFlattenedVectorsOnParent(
        documentId,
        Map.of(floatFieldName, floatVectors, byteFieldName, asByteVectors(byteVectors)));
  }

  @Test
  public void rawVectorStorage_shouldRespectStoredSetting() throws Exception {
    addMultivaluedVectors("70", List.of(A_FLOAT_VECTOR), List.of(A_BYTE_VECTOR), true);
    assertVectorStorage("0", SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, true);
    assertVectorStorage("0", SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, false);
    assertVectorStorage(
        "70/" + MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME + "#0",
        MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
        true);
    assertVectorStorage(
        "70/" + MULTI_VALUE_BYTE_ENCODED_FIELD_NAME + "#0",
        MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
        false);
  }

  private void assertVectorStorage(String id, String field, boolean floatEncoding)
      throws Exception {
    try (var core = solrTestRule.getCoreContainer().getCore("collection1");
        var request = new SolrQueryRequestBase(core, new ModifiableSolrParams())) {
      var searcher = request.getSearcher();
      int docId = searcher.getFirstMatch(new Term(CommonParams.ID, id));
      assertTrue(docId >= 0);
      var storedDocument = searcher.getIndexReader().storedFields().document(docId);
      assertEquals(stored ? (floatEncoding ? 4 : 1) : 0, storedDocument.getFields(field).length);
      var leaves = searcher.getLeafContexts();
      var leaf = leaves.get(ReaderUtil.subIndex(docId, leaves));
      var vectorValues =
          floatEncoding
              ? leaf.reader().getFloatVectorValues(field)
              : leaf.reader().getByteVectorValues(field);
      assertNotNull(vectorValues);
      assertEquals(docId - leaf.docBase, vectorValues.iterator().advance(docId - leaf.docBase));
    }
  }

  @Test
  public void fieldOverrides_shouldControlRawVectorRetrieval() throws Exception {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, "80");
    doc.setField("field_opt_in", A_FLOAT_VECTOR);
    doc.setField("field_opt_out", A_FLOAT_VECTOR);
    doc.setField("field_opt_out_stored", A_FLOAT_VECTOR);
    assertEquals(0, client.add(doc).getStatus());
    var fields = Set.of(CommonParams.ID, "field_opt_in", "field_opt_out", "field_opt_out_stored");
    for (boolean commit : List.of(false, true)) {
      if (commit) {
        client.commit();
      }
      var actual = realTimeGet("80", of(fields));
      assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("field_opt_in"));
      assertNull(actual.getFieldValue("field_opt_out"));
      assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("field_opt_out_stored"));
    }
    var actual =
        new QueryRequest("/select", new SolrQuery("id:80").setFields(fields.toArray(String[]::new)))
            .process(client)
            .getResults()
            .getFirst();
    assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("field_opt_in"));
    assertNull(actual.getFieldValue("field_opt_out"));
    assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("field_opt_out_stored"));
  }

  @Test
  public void dynamicVectorFields_shouldSupportWildcardsAndAliases() throws Exception {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, "90");
    doc.setField("raw_dynamic_first", A_FLOAT_VECTOR);
    doc.setField("raw_dynamic_second", ANOTHER_FLOAT_VECTOR);
    assertEquals(0, client.add(doc).getStatus());
    for (boolean commit : List.of(false, true)) {
      if (commit) {
        client.commit();
      }
      var actual = realTimeGet("90", of(Set.of(CommonParams.ID, "alias:raw_dynamic_first")));
      assertEquals(Set.of(CommonParams.ID, "alias"), actual.keySet());
      assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("alias"));
      actual = realTimeGet("90", of(Set.of(CommonParams.ID, "raw_dynamic_*")));
      assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("raw_dynamic_first"));
      assertEquals(ANOTHER_FLOAT_VECTOR, actual.getFieldValue("raw_dynamic_second"));
    }
    var actual =
        new QueryRequest(
                "/select", new SolrQuery("id:90").setFields(CommonParams.ID, "raw_dynamic_*"))
            .process(client)
            .getResults()
            .getFirst();
    assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("raw_dynamic_first"));
    assertEquals(ANOTHER_FLOAT_VECTOR, actual.getFieldValue("raw_dynamic_second"));
    actual =
        new QueryRequest(
                "/select",
                new SolrQuery("id:90").setFields(CommonParams.ID, "alias:raw_dynamic_first"))
            .process(client)
            .getResults()
            .getFirst();
    assertEquals(Set.of(CommonParams.ID, "alias"), actual.keySet());
    assertEquals(A_FLOAT_VECTOR, actual.getFieldValue("alias"));
  }

  @Test
  public void copyField_shouldBeRebuiltDuringAtomicUpdates() throws Exception {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, "100");
    doc.setField("raw_copy_source", A_FLOAT_VECTOR);
    assertEquals(0, client.add(doc).getStatus());
    client.commit();
    assertCopyVectorValues("100", A_FLOAT_VECTOR);
    assertEquals(0, atomicSetOnStringField("100", "updated").getStatus());
    client.commit();
    assertCopyVectorValues("100", A_FLOAT_VECTOR);
    atomicSetVectorField("100", "raw_copy_source", ANOTHER_FLOAT_VECTOR);
    client.commit();
    assertCopyVectorValues("100", ANOTHER_FLOAT_VECTOR);
  }

  private void assertCopyVectorValues(String id, List<Float> expected) throws Exception {
    var actual =
        new QueryRequest(
                "/select",
                new SolrQuery("id:" + id).setFields("raw_copy_source", "raw_copy_target"))
            .process(client)
            .getResults()
            .getFirst();
    assertEquals(expected, actual.getFieldValue("raw_copy_source"));
    assertEquals(expected, actual.getFieldValue("raw_copy_target"));
  }

  @Test
  public void alternativeVectorCodecs_shouldPreserveOriginalValues() throws Exception {
    var expected =
        Map.of(
            "vector_scalar", A_FLOAT_VECTOR,
            "vector_binary", A_FLOAT_VECTOR,
            "vector_flat", A_FLOAT_VECTOR,
            "vector_scalar_flat", A_FLOAT_VECTOR,
            "vector_flat_byte", A_BYTE_VECTOR);
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, "110");
    expected.forEach(doc::setField);
    assertEquals(0, client.add(doc).getStatus());
    for (boolean commit : List.of(false, true)) {
      if (commit) {
        client.commit();
      }
      var actual = realTimeGet("110", of(expected.keySet()));
      for (var entry : expected.entrySet()) {
        assertEquals(entry.getValue(), actual.getFieldValue(entry.getKey()));
      }
    }
    assertEquals(0, atomicSetOnStringField("110", "updated").getStatus());
    client.commit();
    var actual =
        new QueryRequest(
                "/select",
                new SolrQuery("id:110").setFields(expected.keySet().toArray(String[]::new)))
            .process(client)
            .getResults()
            .getFirst();
    for (var entry : expected.entrySet()) {
      assertEquals(entry.getValue(), actual.getFieldValue(entry.getKey()));
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
    return newDocumentWithVectors(
        id,
        SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
        SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME,
        floatVector,
        byteVector);
  }

  private static SolrInputDocument newDocumentWithVectors(
      String id,
      String floatVectorFieldName,
      String byteVectorFieldName,
      List<Float> floatVector,
      List<Integer> byteVector) {
    var doc = new SolrInputDocument();
    doc.setField(CommonParams.ID, id);
    doc.setField(floatVectorFieldName, floatVector);
    doc.setField(byteVectorFieldName, byteVector);
    return doc;
  }

  private UpdateResponse atomicSetOnStringField(String id, String value) throws Exception {
    var update = new SolrInputDocument();
    update.setField(CommonParams.ID, id);
    update.setField(STRING_FIELD_NAME, Map.of("set", value));
    return client.add(update);
  }

  private void assertAtomicUpdateResultsUsingGet(
      String id,
      List<Float> expectedFloatVector,
      List<Integer> expectedByteVector,
      String expectedStringFieldValue)
      throws Exception {
    assertAtomicUpdateResultsUsingGet(
        id,
        SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
        SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME,
        expectedFloatVector,
        expectedByteVector,
        expectedStringFieldValue);
  }

  private void assertAtomicUpdateResultsUsingGet(
      String id,
      String floatVectorFieldName,
      String byteVectorFieldName,
      List<Float> expectedFloatVector,
      List<Integer> expectedByteVector,
      String expectedStringFieldValue)
      throws Exception {
    var doc = realTimeGet(id, empty());

    assertEquals(expectedStringFieldValue, doc.getFieldValue(STRING_FIELD_NAME));
    assertEquals(expectedFloatVector, doc.getFieldValue(floatVectorFieldName));
    assertEquals(expectedByteVector, doc.getFieldValue(byteVectorFieldName));
  }

  private void assertAtomicUpdateResultsUsingSelect(
      String id,
      List<Float> expectedFloatVector,
      List<Integer> expectedByteVector,
      String expectedStringFieldValue)
      throws Exception {
    assertAtomicUpdateResultsUsingSelect(
        id,
        SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
        SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME,
        expectedFloatVector,
        expectedByteVector,
        expectedStringFieldValue);
  }

  private void assertAtomicUpdateResultsUsingSelect(
      String id,
      String floatVectorFieldName,
      String byteVectorFieldName,
      List<Float> expectedFloatVector,
      List<Integer> expectedByteVector,
      String expectedStringFieldValue)
      throws Exception {
    var docs = new QueryRequest("/select", new SolrQuery("id:" + id)).process(client).getResults();
    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());

    var doc = docs.getFirst();
    assertEquals(expectedStringFieldValue, doc.getFieldValue(STRING_FIELD_NAME));
    assertEquals(expectedFloatVector, doc.getFieldValue(floatVectorFieldName));
    assertEquals(expectedByteVector, doc.getFieldValue(byteVectorFieldName));
  }

  private void assertFlattenedVectorsOnParent(
      String documentId, Map<String, ?> expectedVectorsByField) throws Exception {
    var fields = new ArrayList<String>();
    fields.add(CommonParams.ID);
    fields.addAll(expectedVectorsByField.keySet());
    fields.add("[child fl=\"" + String.join(",", expectedVectorsByField.keySet()) + "\"]");

    var query = new SolrQuery("id:" + documentId).setFields(fields.toArray(String[]::new));
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());

    var parent = docs.getFirst();
    assertEquals(documentId, parent.getFieldValue(CommonParams.ID));

    for (var entry : expectedVectorsByField.entrySet()) {
      assertEquals(entry.getValue(), parent.getFieldValue(entry.getKey()));
    }
  }

  private static List<List<Byte>> asByteVectors(List<List<Integer>> vectors) {
    return vectors.stream().map(vector -> vector.stream().map(Number::byteValue).toList()).toList();
  }

  private String generateRandomStringValue() {
    return TestUtil.randomSimpleString(random(), 5, 20);
  }
}
