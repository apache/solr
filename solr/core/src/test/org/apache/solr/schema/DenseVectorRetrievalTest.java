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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class DenseVectorRetrievalTest extends SolrTestCase {

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

  public DenseVectorRetrievalTest(
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsInRealTimeGet()
      throws Exception {
    var fieldList =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(fieldList, doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnOnlyRequestedFloatVector()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnOnlyRequestedByteVector()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldExcludeUnrequestedVectors()
      throws Exception {
    var fieldList = Set.of(CommonParams.ID);
    var doc = realTimeGet("0", of(fieldList));

    assertEquals(Set.of(CommonParams.ID), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsMatchingFieldPattern()
      throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsWithAllFields()
      throws Exception {
    var fieldList = Set.of("*");
    var doc = realTimeGet("0", of(fieldList));

    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsWithoutFieldList()
      throws Exception {
    var doc = realTimeGet("0", empty());

    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsForRequestedDocument()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldOmitMissingVectors()
      throws Exception {
    var fieldList =
        Set.of(
            CommonParams.ID,
            SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME,
            SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME);
    var expectedFieldList = Set.of(CommonParams.ID);
    var doc = realTimeGet("2", of(fieldList));

    assertEquals(expectedFieldList, doc.keySet());
    assertEquals("2", doc.getFieldValue(CommonParams.ID));
    assertNull(doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
    assertNull(doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsInSelect()
      throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldOverrideExistingValues()
      throws Exception {
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
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveExistingValuesWhenVectorsMissing()
          throws Exception {
    try (var core = solrTestRule.getCoreContainer().getCore("collection1");
        var request = new SolrQueryRequestBase(core, new ModifiableSolrParams())) {
      var searcher = request.getSearcher();
      int docId = searcher.getFirstMatch(new Term(CommonParams.ID, "2"));
      assertTrue(docId >= 0);

      var prepopulatedDocWithStoredValues = new SolrDocument();
      prepopulatedDocWithStoredValues.setField(
          SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME, A_FLOAT_VECTOR);
      prepopulatedDocWithStoredValues.setField(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME, A_BYTE_VECTOR);

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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnOnlyFloatVectorMatchingPattern()
      throws Exception {
    // A float-only pattern must exclude the byte vector from the response.
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_float*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue(SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnOnlyByteVectorMatchingPattern()
      throws Exception {
    // A byte-only pattern must exclude the float vector from the response.
    var doc = realTimeGet("0", of(Set.of(CommonParams.ID, "vector_single_value_byte*")));

    assertEquals(Set.of(CommonParams.ID, SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_BYTE_VECTOR, doc.getFieldValue(SINGLE_VALUE_BYTE_ENCODED_FIELD_NAME));
  }

  @Test
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldReturnFloatVectorWhenByteVectorMissing()
          throws Exception {
    // A missing byte vector must not prevent retrieval of the document's float vector.
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
      fieldTypeDefinition_useRawVectorsAsStored_shouldReturnByteVectorWhenFloatVectorMissing()
          throws Exception {
    // A missing float vector must not prevent retrieval of the document's byte vector.
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsMatchingPatternInSelect()
      throws Exception {
    // Search must retrieve both raw vectors when the field list uses a wildcard.
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsWithAllFieldsInSelect()
      throws Exception {
    // Search must include raw vectors when all fields are explicitly requested.
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsWithoutFieldListInSelect()
          throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldExcludeUnrequestedVectorsInSelect()
      throws Exception {
    var query = new SolrQuery("id:0").setFields(CommonParams.ID);
    var docs = new QueryRequest("/select", query).process(client).getResults();

    assertEquals(1L, docs.getNumFound());
    assertEquals(1, docs.size());
    assertEquals(Set.of(CommonParams.ID), docs.getFirst().keySet());
    assertEquals("0", docs.getFirst().getFieldValue(CommonParams.ID));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsAcrossSegments()
      throws Exception {
    // Separate segments exercise local document IDs and a leaf without any vector values.
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnVectorsForMultipleIds()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnAliasedFloatVector()
      throws Exception {
    var doc =
        realTimeGet(
            "0", of(Set.of(CommonParams.ID, "anAlias:" + SINGLE_VALUE_FLOAT_ENCODED_FIELD_NAME)));

    assertEquals(Set.of(CommonParams.ID, "anAlias"), doc.keySet());
    assertEquals("0", doc.getFieldValue(CommonParams.ID));
    assertEquals(A_FLOAT_VECTOR, doc.getFieldValue("anAlias"));
  }

  @Test
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnAliasedByteVector()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldReturnUncommittedVectors()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldOmitMissingUncommittedVectors()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveVectorsAfterAtomicUpdate()
      throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveTlogVectorsAfterAtomicUpdate()
      throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldNotCreateVectorsAfterAtomicUpdate()
      throws Exception {
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldNotCreateTlogVectorsAfterAtomicUpdate()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveVectorsAfterInPlaceUpdate()
      throws Exception {
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldPreserve1dVectorsAfterAtomicUpdate()
      throws Exception {
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveChildVectorsAfterAtomicUpdate()
          throws Exception {
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldNotDuplicateFloatVectorChildrenAfterAtomicUpdate()
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldNotDuplicateByteVectorChildrenAfterAtomicUpdate()
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserve1dFloatVectorChildrenAfterAtomicUpdate()
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserve1dByteVectorChildrenAfterAtomicUpdate()
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
  public void fieldTypeDefinition_useRawVectorsAsStored_shouldFlattenMultiValuedVectorsOnParent()
      throws Exception {
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveTlogFloatVectorChildrenAfterAtomicUpdate()
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
  public void
      fieldTypeDefinition_useRawVectorsAsStored_shouldPreserveTlogByteVectorChildrenAfterAtomicUpdate()
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
      assertEquals(
          "Unexpected vectors in field " + entry.getKey(),
          entry.getValue(),
          parent.getFieldValue(entry.getKey()));
    }
  }

  private static List<List<Byte>> asByteVectors(List<List<Integer>> vectors) {
    return vectors.stream().map(vector -> vector.stream().map(Number::byteValue).toList()).toList();
  }

  private String generateRandomStringValue() {
    return TestUtil.randomSimpleString(random(), 5, 20);
  }
}
