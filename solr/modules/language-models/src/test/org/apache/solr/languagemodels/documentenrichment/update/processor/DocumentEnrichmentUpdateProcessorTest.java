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
package org.apache.solr.languagemodels.documentenrichment.update.processor;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedChatModelStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentEnrichmentUpdateProcessorTest extends TestLanguageModelBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-document-enrichment.xml", "schema-language-models.xml", false, false);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  private String loadedModelId;

  @After
  public void afterEachTest() throws Exception {
    if (loadedModelId != null) {
      restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/" + loadedModelId);
      loadedModelId = null;
    }
  }

  private void loadTestChatModel(String fileName, String modelId) throws Exception {
    loadChatModel(fileName);
    loadedModelId = modelId;
  }

  @Test
  public void processAdd_inputField_shouldEnrichInputField() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");

    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "documentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");
  }

  /*
   This test looks for the 'dummy-chat-1' model, but such model is not loaded —
   the model store is empty, so the update fails.
  */
  @Test
  public void processAdd_modelNotFound_shouldThrowException() {
    RuntimeException thrown =
        assertThrows(
            "model not found should throw an exception",
            RemoteSolrException.class,
            () ->
                addWithChain(
                    sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."),
                    "documentEnrichment"));
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The model configured in the Update Request Processor 'dummy-chat-1' can't be found in the store: /schema/chat-model-store"));
  }

  @Test
  public void processAdd_emptyInputField_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");
    addWithChain(sdoc("id", "99", "string_field", ""), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==", // no enriched field for doc 99
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");
  }

  @Test
  public void processAdd_nullInputField_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    assertU(adoc("id", "98")); // no string_field
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field=="); // no enriched field for doc 98
  }

  @Test
  public void processAdd_failingEnrichment_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadTestChatModel("exception-throwing-chat-model.json", "exception-throwing-chat-model");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "failingDocumentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "failingDocumentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==", // no enriched field for doc 99
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field=="); // no enriched field for doc 98
  }

  @Test
  public void processAtomicUpdate_shouldTriggerEnrichmentAndFetchTheStoredContent()
      throws Exception {
    // Verifies that when using a processor chain configured for partial updates
    // (i.e., DistributedUpdateProcessorFactory before DocumentEnrichmentUpdateProcessorFactory),
    // the system correctly retrieves the stored value of string_field and generates the
    // enriched content for the document.
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");
    assertU(adoc("id", "99", "string_field", "Vegeta is the saiyan prince."));
    assertU(adoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."));
    assertU(commit());

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField("enriched", Map.of("set", "true"));
    addWithChain(atomicDoc, "documentEnrichmentForPartialUpdates");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field==" // no enriched field for document 98
        );
  }

  @Test
  public void processAtomicUpdate_shouldReplaceExistingEnrichedFieldNotAppend() throws Exception {
    // Verifies that when a document already contains an enriched_field and string_field is
    // modified via atomic update, the enriched content is recomputed and replaces the previous
    // value rather than being appended.
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "documentEnrichment");
    assertU(commit());

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField("string_field", Map.of("set", "Vegeta is the saiyan prince from the Dragon Ball series."));
    addWithChain(atomicDoc, "documentEnrichmentForPartialUpdates");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");
  }

  // --- multi-field tests ---

  @Test
  public void processAdd_multipleInputFields_allPresent_shouldEnrichDocument() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince.", "body_field", "He is very proud."),
        "documentEnrichmentMultiField");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan.", "body_field", "He grew up on Earth."),
        "documentEnrichmentMultiField");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");
  }

  @Test
  public void processAdd_multipleInputFields_firstFieldNull_shouldSkipEnrichment() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");

    addWithChain(
        sdoc("id", "99", "body_field", "He is very proud."), // string_field absent
        "documentEnrichmentMultiField");
    addWithChain(
        sdoc("id", "98", "body_field", "He is very jealous."), // string_field absent
        "documentEnrichmentMultiField");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field==");
  }

  @Test
  public void processAdd_multipleInputFields_secondFieldEmpty_shouldSkipEnrichment() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince.", "body_field", ""),
        "documentEnrichmentMultiField");
    addWithChain(
        sdoc("id", "98", "string_field", "Goku is the best saiyan.", "body_field", ""),
        "documentEnrichmentMultiField");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field==");
  }

  @Test
  public void processAdd_multipleInputFields_bothFieldsAbsent_shouldSkipEnrichment() throws Exception {
    loadTestChatModel("dummy-chat-model.json", "dummy-chat-1");

    addWithChain(sdoc("id", "99"), "documentEnrichmentMultiField");
    addWithChain(sdoc("id", "98"), "documentEnrichmentMultiField");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field==");
  }

  @Test
  public void processAdd_multipleInputFields_failingModel_shouldLogAndSkipEnrichment() throws Exception {
    loadTestChatModel("exception-throwing-chat-model.json", "exception-throwing-chat-model");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince.", "body_field", "He is very proud."),
        "failingDocumentEnrichmentMultiField");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan.", "body_field", "He grew up on Earth."),
        "failingDocumentEnrichmentMultiField");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field==");
  }

  @Test
  public void processAdd_multivaluedStringOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-string.json", "dummy-chat-multivalued-1");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."),
        "documentEnrichmentMultivaluedString");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "documentEnrichmentMultivaluedString");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field_multi/[0]=='tag1'",
        "/response/docs/[0]/enriched_field_multi/[1]=='tag2'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field_multi/[0]=='tag1'",
        "/response/docs/[1]/enriched_field_multi/[1]=='tag2'");
  }

  @Test
  public void processAdd_multivaluedStringOutputField_emptyInput_shouldSkipEnrichment()
      throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-string.json", "dummy-chat-multivalued-1");

    addWithChain(sdoc("id", "99", "string_field", ""), "documentEnrichmentMultivaluedString");
    addWithChain(sdoc("id", "98", "string_field", ""), "documentEnrichmentMultivaluedString");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/enriched_field_multi==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field_multi==");
  }

  // --- typed single-valued output field tests ---

  @Test
  public void processAdd_singleLongOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-long.json", "dummy-long");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleLong");
    addWithChain(sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleLong");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_long");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_long==42",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_long==42");
  }

  @Test
  public void processAdd_singleIntOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-int.json", "dummy-int");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleInt");
    addWithChain(sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleInt");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_int");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_int==7",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_int==7");
  }

  @Test
  public void processAdd_singleFloatOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-float.json", "dummy-float");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleFloat");
    addWithChain(sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleFloat");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_float");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_float==1.5",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_float==1.5");
  }

  @Test
  public void processAdd_singleDoubleOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-double.json", "dummy-double");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleDouble");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleDouble");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_double");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_double==2.5",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_double==2.5");
  }

  @Test
  public void processAdd_singleBooleanOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-boolean.json", "dummy-boolean");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleBoolean");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleBoolean");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_boolean");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_boolean==true",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_boolean==true");
  }

  @Test
  public void processAdd_singleDateOutputField_shouldPopulateValue() throws Exception {
    loadTestChatModel("dummy-chat-model-single-date.json", "dummy-date");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleDate");
    addWithChain(sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleDate");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_date");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_date=='2024-01-15T00:00:00Z'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_date=='2024-01-15T00:00:00Z'");
  }

  // --- typed multivalued output field tests ---

  @Test
  public void processAdd_multivaluedLongOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-long.json", "dummy-long-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedLong");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedLong");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_long_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_long_multi/[0]==10",
        "/response/docs/[0]/output_long_multi/[1]==20",
        "/response/docs/[0]/output_long_multi/[2]==30",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_long_multi/[0]==10",
        "/response/docs/[1]/output_long_multi/[1]==20",
        "/response/docs/[1]/output_long_multi/[2]==30");
  }

  @Test
  public void processAdd_multivaluedIntOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-int.json", "dummy-int-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedInt");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedInt");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_int_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_int_multi/[0]==1",
        "/response/docs/[0]/output_int_multi/[1]==2",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_int_multi/[0]==1",
        "/response/docs/[1]/output_int_multi/[1]==2");
  }

  @Test
  public void processAdd_multivaluedFloatOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-float.json", "dummy-float-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedFloat");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedFloat");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_float_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_float_multi/[0]==1.5",
        "/response/docs/[0]/output_float_multi/[1]==2.5",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_float_multi/[0]==1.5",
        "/response/docs/[1]/output_float_multi/[1]==2.5");
  }

  @Test
  public void processAdd_multivaluedDoubleOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-double.json", "dummy-double-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedDouble");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedDouble");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_double_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_double_multi/[0]==3.14",
        "/response/docs/[0]/output_double_multi/[1]==2.71",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_double_multi/[0]==3.14",
        "/response/docs/[1]/output_double_multi/[1]==2.71");
  }

  @Test
  public void processAdd_multivaluedBooleanOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-boolean.json", "dummy-boolean-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedBoolean");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedBoolean");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_boolean_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_boolean_multi/[0]==true",
        "/response/docs/[0]/output_boolean_multi/[1]==false",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_boolean_multi/[0]==true",
        "/response/docs/[1]/output_boolean_multi/[1]==false");
  }

  @Test
  public void processAdd_multivaluedDateOutputField_shouldPopulateAllValues() throws Exception {
    loadTestChatModel("dummy-chat-model-multivalued-date.json", "dummy-date-multi");

    addWithChain(
        sdoc("id", "99", "string_field", "some content"), "documentEnrichmentMultivaluedDate");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentMultivaluedDate");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_date_multi");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_date_multi/[0]=='2024-01-15T00:00:00Z'",
        "/response/docs/[0]/output_date_multi/[1]=='2025-06-30T00:00:00Z'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_date_multi/[0]=='2024-01-15T00:00:00Z'",
        "/response/docs/[1]/output_date_multi/[1]=='2025-06-30T00:00:00Z'");
  }

  private SolrQuery getEnrichmentQuery(String enrichedFieldName) {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "id,"+enrichedFieldName);
    query.add("sort", "id desc");
    return query;
  }

  void addWithChain(SolrInputDocument document, String updateChain)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(document);
    req.setParam("update.chain", updateChain);
    solrTestRule.getSolrClient("collection1").request(req);
  }
}
