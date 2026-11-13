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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequestBase;
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
        .withSchemaFile("schema-densevector-raw-rtg.xml")
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
  public void realtimeGetWithFieldPattern_shouldReturnMatchingFloatVectorField() throws Exception {
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_float*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void realtimeGetWithFieldPattern_shouldReturnMatchingByteVectorField() throws Exception {
    // A byte-only pattern must exclude the float vector from the response.
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_byte*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
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
          .decorateWithRawVectorFields(
              prepopulatedDocWithStoredValues,
              docId,
              Set.of(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));

      assertEquals(
          A_FLOAT_VECTOR,
          prepopulatedDocWithStoredValues.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
      assertEquals(
          A_BYTE_VECTOR,
          prepopulatedDocWithStoredValues.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    }
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
    var vectorFieldName = "vector_multi_value_float_1d";
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
      assertEquals(multivaluedVector.get(i), child.getFieldValue(vectorFieldName));
    }
  }

  @Test
  public void atomicUpdateOnMultiValuedSingleDimensionByteVector_shouldPreserveVectorChildren()
      throws Exception {
    var documentId = "3";
    var vectorFieldName = "vector_multi_value_byte_1d";
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
      assertEquals(multivaluedVector.get(i), child.getFieldValue(vectorFieldName));
    }
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
    assertFlattenedVectorsOnParent(
        documentId, Map.of(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, multivaluedVector));
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
    assertFlattenedVectorsOnParent(
        documentId, Map.of(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, asByteVectors(multivaluedVector)));
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
      assertFlattenedVectorsOnParent(
          id,
          Map.of(
              MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(A_FLOAT_VECTOR, ANOTHER_FLOAT_VECTOR),
              MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, asByteVectors(List.of(ANOTHER_BYTE_VECTOR))));
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

  private void assertMultivaluedVectorsAfterAtomicSet(
      String id, List<List<Float>> floats, List<List<Integer>> bytes) throws Exception {
    var expected = new HashMap<String, Object>();
    expected.put(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, floats.isEmpty() ? null : floats);
    expected.put(
        MULTI_VALUE_BYTE_ENCODED_FIELD_NAME, bytes.isEmpty() ? null : asByteVectors(bytes));
    var fields =
        Set.of(
            CommonParams.ID,
            MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
            MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
            "[child fl=\""
                + MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME
                + ","
                + MULTI_VALUE_BYTE_ENCODED_FIELD_NAME
                + "\"]");

    var beforeCommit = realTimeGet(id, of(fields));
    for (var entry : expected.entrySet()) {
      assertEquals(entry.getValue(), beforeCommit.getFieldValue(entry.getKey()));
    }

    client.commit();
    assertFlattenedVectorsOnParent(id, expected);
    var afterCommit = realTimeGet(id, of(fields));
    for (var entry : expected.entrySet()) {
      assertEquals(entry.getValue(), afterCommit.getFieldValue(entry.getKey()));
    }

    var children =
        new QueryRequest(
                "/select",
                new SolrQuery("_root_:" + id + " AND -id:" + id)
                    .setFields(
                        CommonParams.ID,
                        MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME,
                        MULTI_VALUE_BYTE_ENCODED_FIELD_NAME)
                    .setRows(100))
            .process(client)
            .getResults();
    var expectedChildren = new HashMap<String, Object>();
    for (int i = 0; i < floats.size(); i++) {
      expectedChildren.put(
          id + "/" + MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME + "#" + i, floats.get(i));
    }
    for (int i = 0; i < bytes.size(); i++) {
      expectedChildren.put(id + "/" + MULTI_VALUE_BYTE_ENCODED_FIELD_NAME + "#" + i, bytes.get(i));
    }
    assertEquals((long) expectedChildren.size(), children.getNumFound());
    assertEquals(
        expectedChildren.keySet(),
        children.stream()
            .map(child -> child.getFieldValue(CommonParams.ID))
            .collect(Collectors.toSet()));
    for (var child : children) {
      Object values = child.getFieldValue(MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME);
      if (values == null) {
        values = child.getFieldValue(MULTI_VALUE_BYTE_ENCODED_FIELD_NAME);
      }
      assertEquals(expectedChildren.get(child.getFieldValue(CommonParams.ID)), values);
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
    String floatField = "vector_multi_value_float_1d";
    String byteField = "vector_multi_value_byte_1d";
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
              List.of(List.of((byte) -128), List.of((byte) 0)));
      var fields =
          Set.of(
              CommonParams.ID,
              floatField,
              byteField,
              "[child fl=\"" + floatField + "," + byteField + "\"]");
      var actual = realTimeGet(id, of(fields));
      expected.forEach((field, vectors) -> assertEquals(vectors, actual.getFieldValue(field)));
      client.commit();
      assertFlattenedVectorsOnParent(id, expected);
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
      assertFlattenedVectorsOnParent(
          id,
          Map.of(
              MULTI_VALUE_FLOAT_ENCODED_FIELD_NAME, List.of(ANOTHER_FLOAT_VECTOR),
              MULTI_VALUE_BYTE_ENCODED_FIELD_NAME,
                  asByteVectors(List.of(A_BYTE_VECTOR, ANOTHER_BYTE_VECTOR))));
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
        String floatField = singleDimension ? "legacy_float_1d" : "legacy_float";
        String byteField = singleDimension ? "legacy_byte_1d" : "legacy_byte";
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
        assertFlattenedVectorsOnParent(
            id,
            Map.of(
                floatField,
                List.of(secondFloat, firstFloat),
                byteField,
                asByteVectors(List.of(secondByte, firstByte))));
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
            "vector_multi_value_float_encoding":{"add-distinct":[[0.1,2.5,3.75,4.0],[4.0,3.75,2.5,1.25]]},
            "vector_multi_value_byte_encoding":{"add-distinct":[[-128,-1,0,127],[127,0,-1,-128]]}}]
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

  private void assertMultiValuedVectorChildren(
      String rootId, String vectorFieldName, List<? extends List<? extends Number>> expectedVectors)
      throws Exception {

    var expectedVectorsMap =
        IntStream.range(0, expectedVectors.size())
            .mapToObj(
                index ->
                    Map.entry(
                        rootId + "/" + vectorFieldName + "#" + index, expectedVectors.get(index)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var query =
        new SolrQuery("_root_:" + rootId + " AND -id:" + rootId)
            .setFields(CommonParams.ID, vectorFieldName)
            .setRows(10);

    var children = new QueryRequest("/select", query).process(client).getResults();
    assertEquals(2L, children.getNumFound());
    assertEquals(2, children.size());
    assertEquals(
        expectedVectorsMap.keySet(),
        Set.of(
            children.get(0).getFieldValue(CommonParams.ID),
            children.get(1).getFieldValue(CommonParams.ID)));

    for (var child : children) {
      var childId = (String) child.getFieldValue(CommonParams.ID);
      assertTrue(expectedVectorsMap.containsKey(childId));
      assertEquals(expectedVectorsMap.get(childId), child.getFieldValue(vectorFieldName));
    }
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
