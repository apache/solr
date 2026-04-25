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
package org.apache.solr.update.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContentHashVersionProcessorTest extends UpdateProcessorTestBase {

  private static final String ID_FIELD = "_id";
  private static final String FIRST_FIELD = "field1";
  private static final String SECOND_FIELD = "field2";
  private static final String THIRD_FIELD = "docField3";
  private static final String FOURTH_FIELD = "field4";

  private static final String INITIAL_DOC_ID = "1";
  private static final String INITIAL_FIELD1_VALUE = "Initial values used to compute initial hash";
  private static final String INITIAL_FIELD2_VALUE =
      "This a constant value for testing include/exclude fields";
  private static final String INITIAL_DOC =
      adoc(
          ID_FIELD, INITIAL_DOC_ID,
          FIRST_FIELD, INITIAL_FIELD1_VALUE,
          SECOND_FIELD, INITIAL_FIELD2_VALUE);
  private String initialDocHash;

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
    initCore("solrconfig-contenthashversion.xml", "schema16.xml");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
    addDoc(INITIAL_DOC, "contenthashversion-default");
    assertU(commit());

    // Query for the document and extract _hash_ field value
    initialDocHash = getHashFieldValue(INITIAL_DOC_ID);
  }

  private static String getHashFieldValue(String docId) throws Exception {
    String response = h.query(req("q", ID_FIELD + ":" + docId, "fl", "_hash_"));

    // Parse XML response to extract _hash_ field value
    // Response format: <str name="_hash_">value</str>
    String hashPattern = "<str name=\"_hash_\">";
    int startIdx = response.indexOf(hashPattern);
    if (startIdx == -1) {
      fail("Hash field not found in document " + docId);
    }
    startIdx += hashPattern.length();
    int endIdx = response.indexOf("</str>", startIdx);
    if (endIdx == -1) {
      fail("Hash field closing tag not found");
    }
    return response.substring(startIdx, endIdx);
  }

  private ContentHashVersionProcessor getContentHashVersionProcessor(
      List<String> includedFields, List<String> excludedFields) {
    final SolrQueryRequest req = mock(SolrQueryRequest.class);
    final SolrCore solrCore = mock(SolrCore.class);
    final IndexSchema indexSchema = mock(IndexSchema.class);
    when(indexSchema.getField("_hash_")).thenReturn(new SchemaField("_hash_", new BinaryField()));

    when(solrCore.getLatestSchema()).thenReturn(indexSchema);
    when(req.getCore()).thenReturn(solrCore);
    return new ContentHashVersionProcessor(
        ContentHashVersionProcessorFactory.buildFieldMatcher(includedFields),
        ContentHashVersionProcessorFactory.buildFieldMatcher(excludedFields),
        "_hash_",
        false,
        req,
        mock(SolrQueryResponse.class),
        mock(UpdateRequestProcessor.class));
  }

  @Test
  public void shouldUseExcludedFieldsWildcard() {
    // Given
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of("field*"));

    // Given (doc for update)
    SolrInputDocument inputDocument =
        doc(
            f(ID_FIELD, "0000000001"),
            f(FIRST_FIELD, UUID.randomUUID().toString()),
            f(SECOND_FIELD, UUID.randomUUID().toString()),
            f(THIRD_FIELD, "constant to have a constant hash"),
            f(FOURTH_FIELD, UUID.randomUUID().toString()));

    // Then (only ID and THIRD_FIELD is used in hash, other fields contain random values)
    assertArrayEquals(
        Base64.getDecoder().decode("bwE8Zjq0aOs="),
        processor.computeDocHash(inputDocument)); // Hash if only ID field was used
  }

  @Test
  public void shouldUseIncludedFieldsWildcard() {
    // Given
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("field*"), List.of(THIRD_FIELD));

    // Given (doc for update)
    SolrInputDocument inputDocument =
        doc(
            f(ID_FIELD, "0000000001"),
            f(FIRST_FIELD, "constant to have a constant hash for field1"),
            f(SECOND_FIELD, "constant to have a constant hash for field2"),
            f(THIRD_FIELD, UUID.randomUUID().toString()),
            f(FOURTH_FIELD, "constant to have a constant hash for field4"));

    // Then
    assertArrayEquals(
        Base64.getDecoder().decode("PozPs2qZQtw="), processor.computeDocHash(inputDocument));
  }

  @Test
  public void shouldUseIncludedFieldsWildcard2() {
    // Given (variant of previous shouldUseIncludedFieldsWildcard, without the excludedField config)
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("field*"), List.of());

    // Given (doc for update)
    SolrInputDocument inputDocument =
        doc(
            f(ID_FIELD, "0000000001"),
            f(FIRST_FIELD, "constant to have a constant hash for field1"),
            f(SECOND_FIELD, "constant to have a constant hash for field2"),
            f(THIRD_FIELD, UUID.randomUUID().toString()),
            f(FOURTH_FIELD, "constant to have a constant hash for field4"));

    // Then
    assertArrayEquals(
        Base64.getDecoder().decode("PozPs2qZQtw="), processor.computeDocHash(inputDocument));
  }

  @Test
  public void shouldDedupIncludedFields() {
    // Given (processor to include field1 and field2 only)
    ContentHashVersionProcessor processorWithDuplicatedFieldName =
        getContentHashVersionProcessor(List.of(FIRST_FIELD, FIRST_FIELD, SECOND_FIELD), List.of());
    ContentHashVersionProcessor processorWithWildcard =
        getContentHashVersionProcessor(
            List.of( // Also change order of config (test reorder of field names)
                SECOND_FIELD, FIRST_FIELD, "field1*"),
            List.of());

    // Given (doc for update)
    SolrInputDocument inputDocument =
        doc(
            f(ID_FIELD, "0000000001"),
            f(FIRST_FIELD, "constant to have a constant hash for field1"),
            f(SECOND_FIELD, "constant to have a constant hash for field2"),
            f(THIRD_FIELD, UUID.randomUUID().toString()),
            f(FOURTH_FIELD, "constant to have a constant hash for field4"));

    // Then
    assertArrayEquals(
        Base64.getDecoder().decode("XavrOYGlkXM="),
        processorWithDuplicatedFieldName.computeDocHash(inputDocument));
    assertArrayEquals(
        Base64.getDecoder().decode("XavrOYGlkXM="),
        processorWithWildcard.computeDocHash(inputDocument));
  }

  @Test
  public void shouldCreateSignatureForNewDoc() throws Exception {
    // When (update)
    final String newDocId = UUID.randomUUID().toString();
    assertU(
        adoc(
            ID_FIELD, newDocId,
            FIRST_FIELD, INITIAL_FIELD1_VALUE,
            SECOND_FIELD, INITIAL_FIELD2_VALUE));
    assertU(commit());

    // Then
    final String hashFieldValueForNewDoc = getHashFieldValue(newDocId);
    assertEquals(initialDocHash, hashFieldValueForNewDoc);
  }

  @Test
  public void shouldAddToResponseLog() throws Exception {
    // Given (command to update existing doc)
    final String newDocId = UUID.randomUUID().toString();
    final SolrQueryResponse update1 =
        addDoc(
            adoc(
                ID_FIELD, newDocId,
                FIRST_FIELD, INITIAL_FIELD1_VALUE,
                SECOND_FIELD, INITIAL_FIELD2_VALUE),
            "contenthashversion-default");
    final SolrQueryResponse update2 =
        addDoc(
            adoc(
                ID_FIELD, newDocId,
                FIRST_FIELD, "This is a doc with values",
                SECOND_FIELD, "that differs from stored doc, so it's considered new"),
            "contenthashversion-default");
    assertU(commit());

    // Then
    assertResponse(update1, -1, -1);
    assertResponse(update2, 0, -1);
  }

  @Test
  public void shouldKeepDuplicateDocumentsInLogMode() throws Exception {
    // Given: Use log chain which detects but does NOT drop duplicates
    final String docId = UUID.randomUUID().toString();

    // When: Add a document
    addDoc(
        adoc(
            ID_FIELD, docId,
            FIRST_FIELD, "original value",
            SECOND_FIELD, "original value 2"),
        "contenthashversion-log");
    assertU(commit());
    String originalHash = getHashFieldValue(docId);

    // When: Try to add the same content again (duplicate)
    SolrQueryResponse duplicateResponse =
        addDoc(
            adoc(
                ID_FIELD, docId,
                FIRST_FIELD, "original value",
                SECOND_FIELD, "original value 2"),
            "contenthashversion-log");
    assertU(commit());

    // Then: Response should show duplicate was detected but NOT dropped
    assertResponse(duplicateResponse, -1, 1);

    // Then: Document should still exist in index
    assertQ(req("q", ID_FIELD + ":" + docId), "//result[@numFound='1']");

    // Then: Document hash should remain unchanged (duplicate was processed)
    String currentHash = getHashFieldValue(docId);
    assertEquals("Hash should remain unchanged for duplicate", originalHash, currentHash);

    // When: Update with different content
    SolrQueryResponse changedResponse =
        addDoc(
            adoc(
                ID_FIELD, docId,
                FIRST_FIELD, "changed value",
                SECOND_FIELD, "changed value 2"),
            "contenthashversion-log");
    assertU(commit());

    // Then: Response should show content changed
    assertResponse(changedResponse, -1, 0);

    // Then: Hash should be updated
    String newHash = getHashFieldValue(docId);
    assertNotEquals("Hash should change for different content", originalHash, newHash);
  }

  @Test
  public void shouldExcludeFieldsUpdateSignatureForNewDoc() throws Exception {
    // Given (update using URP chain WITHOUT drop doc (log mode))
    final String newDocId = UUID.randomUUID().toString();
    addDoc(
        adoc(
            ID_FIELD, newDocId,
            FIRST_FIELD, INITIAL_FIELD1_VALUE,
            SECOND_FIELD, INITIAL_FIELD2_VALUE),
        "contenthashversion-default");
    assertU(commit());

    // Then
    final String hashFieldValue = getHashFieldValue(newDocId);
    assertEquals(initialDocHash, hashFieldValue);
  }

  @Test
  public void shouldCommitWithDropModeEnabled() throws Exception {
    // Initial document already exists from setUp()
    // When: Try to add the same document again (duplicate content) using URP chain WITH drop doc
    // (drop mode)
    SolrQueryResponse solrQueryResponse =
        addDoc(
            adoc(
                ID_FIELD, INITIAL_DOC_ID,
                FIRST_FIELD, INITIAL_FIELD1_VALUE,
                SECOND_FIELD, INITIAL_FIELD2_VALUE),
            "contenthashversion-drop");
    assertU(commit());

    // Then: Verify response shows duplicate was dropped
    assertResponse(solrQueryResponse, 1, -1);

    // Then: Verify document was NOT actually added/updated (still only 1 doc in index)
    assertQ(req("q", "*:*"), "//result[@numFound='1']");

    // Verify the document still has the original hash
    String currentHash = getHashFieldValue(INITIAL_DOC_ID);
    assertEquals("Document hash should not have changed", initialDocHash, currentHash);
  }

  @Test
  public void shouldHandleDocumentWithOnlyIdField() {
    // Given: Document with only ID field (no other fields to hash)
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of(ID_FIELD));

    // When: Compute hash for document with only ID
    SolrInputDocument doc = doc(f(ID_FIELD, "only-id-doc"));

    // Then: Should compute hash (even if empty field set)
    byte[] hash = processor.computeDocHash(doc);
    assertNotNull("Hash should not be null for ID-only document", hash);
    assertTrue("Hash should not be empty", hash.length > 0);
  }

  @Test
  public void shouldHandleMultiValueFields() {
    // Given: Processor that includes multi-value fields
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of(ID_FIELD));

    // When: Document with multi-value field
    SolrInputDocument doc1 = doc(f(ID_FIELD, "doc1"), f(FIRST_FIELD, "value1", "value2", "value3"));

    // Then: Should compute consistent hash
    byte[] hash1 = processor.computeDocHash(doc1);
    assertNotNull(hash1);

    // Same values in same order should produce same hash
    SolrInputDocument doc2 = doc(f(ID_FIELD, "doc2"), f(FIRST_FIELD, "value1", "value2", "value3"));
    byte[] hash2 = processor.computeDocHash(doc2);
    assertArrayEquals("Same multi-value field should produce same hash", hash1, hash2);

    // Different order should produce different hash (collection order matters)
    SolrInputDocument doc3 = doc(f(ID_FIELD, "doc3"), f(FIRST_FIELD, "value3", "value1", "value2"));
    byte[] hash3 = processor.computeDocHash(doc3);
    assertFalse("Different order should produce different hash", Arrays.equals(hash1, hash3));
  }

  @Test
  public void shouldHandleNullFieldValues() {
    // Given: Processor that handles null values
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of(ID_FIELD));

    // When: Document with null field value (represented as "null" string)
    SolrInputDocument doc = doc(f(ID_FIELD, "null-doc"), f(FIRST_FIELD, (Object) null));

    // Then: Should compute hash without error
    byte[] hash = processor.computeDocHash(doc);
    assertNotNull("Should handle null values", hash);
    assertTrue("Hash should not be empty", hash.length > 0);
  }

  @Test
  public void shouldProduceSameHashRegardlessOfFieldOrder() {
    // Given: Documents with same fields in different order
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of(ID_FIELD));

    // When: Create docs with fields in different order
    SolrInputDocument doc1 =
        doc(
            f(ID_FIELD, "doc1"),
            f(FIRST_FIELD, "value1"),
            f(SECOND_FIELD, "value2"),
            f(THIRD_FIELD, "value3"));

    SolrInputDocument doc2 =
        doc(
            f(ID_FIELD, "doc2"),
            f(THIRD_FIELD, "value3"),
            f(FIRST_FIELD, "value1"),
            f(SECOND_FIELD, "value2"));

    // Then: Hashes should be identical (fields are sorted before hashing)
    byte[] hash1 = processor.computeDocHash(doc1);
    byte[] hash2 = processor.computeDocHash(doc2);
    assertArrayEquals("Hash should be same regardless of field order", hash1, hash2);
  }

  @Test
  public void shouldHandleEmptyFieldValues() {
    // Given: Document with empty string values
    ContentHashVersionProcessor processor =
        getContentHashVersionProcessor(List.of("*"), List.of(ID_FIELD));

    SolrInputDocument doc1 = doc(f(ID_FIELD, "empty-doc"), f(FIRST_FIELD, ""), f(SECOND_FIELD, ""));

    // When: Compute hash
    byte[] hash1 = processor.computeDocHash(doc1);

    // Then: Should produce valid hash
    assertNotNull("Should handle empty values", hash1);
    assertTrue("Hash should not be empty", hash1.length > 0);

    // Empty strings should produce different hash than no fields
    SolrInputDocument doc2 = doc(f(ID_FIELD, "empty-doc"));
    byte[] hash2 = processor.computeDocHash(doc2);
    assertFalse("Empty string fields should differ from no fields", Arrays.equals(hash1, hash2));
  }

  private static void assertResponse(
      SolrQueryResponse solrQueryResponse, int droppedDocCount, int duplicateDocCount) {
    if (droppedDocCount >= 0) {
      assertNotNull(solrQueryResponse.getToLog().get("contentHash.duplicatesDropped"));
      int droppedDocs = (int) solrQueryResponse.getToLog().get("contentHash.duplicatesDropped");
      assertEquals(droppedDocCount, droppedDocs);
    }
    if (duplicateDocCount >= 0) {
      assertNotNull(solrQueryResponse.getToLog().get("contentHash.duplicatesDetected"));
      int duplicateDocs = (int) solrQueryResponse.getToLog().get("contentHash.duplicatesDetected");
      assertEquals(duplicateDocCount, duplicateDocs);
    }
  }
}
