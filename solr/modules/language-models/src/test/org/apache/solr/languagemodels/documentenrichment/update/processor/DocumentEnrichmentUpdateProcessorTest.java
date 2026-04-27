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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Utils;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.documentenrichment.model.DummyChatModel;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedLargeLanguageModelStore;
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
      restTestHarness.delete(ManagedLargeLanguageModelStore.REST_END_POINT + "/" + loadedModelId);
      loadedModelId = null;
    }
  }

  private void loadTestLargeLanguageModel(String fileName, String modelId) throws Exception {
    loadLargeLanguageModel(fileName);
    loadedModelId = modelId;
  }

  private void loadDummyLargeLanguageModel(String modelId, String response) throws Exception {
    Map<String, Object> model = new LinkedHashMap<>();
    model.put("class", "org.apache.solr.languagemodels.documentenrichment.model.DummyChatModel");
    model.put("name", modelId);
    model.put("params", Map.of("response", response));
    assertJPut(
        ManagedLargeLanguageModelStore.REST_END_POINT,
        Utils.toJSONString(model),
        "/responseHeader/status==0");
    loadedModelId = modelId;
  }

  @Test
  public void processAdd_inputField_shouldEnrichInputField() throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "documentEnrichment");
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
   This test looks for the 'dummy-1' model, but such model is not loaded —
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
                "The model configured in the Update Request Processor 'dummy-1' can't be found in the store: /schema/large-language-model-store"));
  }

  @Test
  public void processAdd_emptyInputField_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");
    addWithChain(sdoc("id", "99", "string_field", ""), "documentEnrichment");
    addWithChain(
        sdoc("id", "98", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
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
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");
    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
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
    loadTestLargeLanguageModel("exception-throwing-model.json", "exception-throwing-model");
    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."),
        "failingDocumentEnrichment");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "failingDocumentEnrichment");
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
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");
    assertU(adoc("id", "99", "string_field", "Vegeta is the saiyan prince."));
    assertU(adoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."));
    assertU(commit());

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField("enriched", Map.of("set", true));
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
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");
    assertU(
        adoc(
            "id",
            "99",
            "string_field",
            "Vegeta is the saiyan prince.",
            "enriched_field",
            "old content"));
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "documentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='old content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField(
        "string_field", Map.of("set", "Vegeta is the saiyan prince from the Dragon Ball series."));
    addWithChain(atomicDoc, "documentEnrichmentForPartialUpdates");
    assertU(commit());

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
  public void processAdd_arrInputField_shouldEnrichDocument() throws Exception {
    // Verifies that <arr name="inputField"> in solrconfig behaves identically to
    // multiple <str name="inputField"> entries — both are accepted by removeConfigArgs.
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

    DummyChatModel.lastReceivedPrompt = null;

    addWithChain(
        sdoc(
            "id",
            "99",
            "string_field",
            "Vegeta is the saiyan prince.",
            "body_field",
            "He is very proud."),
        "documentEnrichmentArrInputField");
    addWithChain(
        sdoc(
            "id",
            "98",
            "string_field",
            "Kakaroth is a saiyan.",
            "body_field",
            "He grew up on Earth."),
        "documentEnrichmentArrInputField");
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
  public void processAdd_multipleInputFields_allPresent_shouldEnrichDocumentWithBothFields()
      throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

    DummyChatModel.lastReceivedPrompt = null;

    addWithChain(
        sdoc(
            "id",
            "99",
            "string_field",
            "Vegeta is the saiyan prince.",
            "body_field",
            "He is very proud."),
        "documentEnrichmentMultiField");
    addWithChain(
        sdoc(
            "id",
            "98",
            "string_field",
            "Kakaroth is a saiyan.",
            "body_field",
            "He grew up on Earth."),
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

    // Verify both placeholders were substituted
    assertEquals(
        "Title: Kakaroth is a saiyan.. Body: He grew up on Earth..",
        DummyChatModel.lastReceivedPrompt);
  }

  @Test
  public void processAdd_multipleInputFields_firstFieldNull_shouldSkipEnrichment()
      throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

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
  public void processAdd_multipleInputFields_secondFieldEmpty_shouldSkipEnrichment()
      throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

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
  public void processAdd_multipleInputFields_bothFieldsAbsent_shouldSkipEnrichment()
      throws Exception {
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

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
  public void processAdd_multipleInputFields_failingModel_shouldLogAndSkipEnrichment()
      throws Exception {
    loadTestLargeLanguageModel("exception-throwing-model.json", "exception-throwing-model");

    addWithChain(
        sdoc(
            "id",
            "99",
            "string_field",
            "Vegeta is the saiyan prince.",
            "body_field",
            "He is very proud."),
        "failingDocumentEnrichmentMultiField");
    addWithChain(
        sdoc(
            "id",
            "98",
            "string_field",
            "Kakaroth is a saiyan.",
            "body_field",
            "He grew up on Earth."),
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
  public void processAdd_multivaluedInputField_shouldInterpolateCollectionAndEnrichDocument()
      throws Exception {
    // When an input field is multivalued, SolrInputField.getValue() returns the Collection,
    // whose toString() is used for prompt interpolation (e.g. "[tag1, tag2, tag3]").
    // Enrichment must proceed — the collection is non-null and non-empty.
    loadTestLargeLanguageModel("dummy-model.json", "dummy-1");

    DummyChatModel.lastReceivedPrompt = null;

    addWithChain(sdoc("id", "98", "tags_field", "tag1"), "documentEnrichmentMultivaluedInput");

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "99");
    doc.addField("tags_field", "tag1");
    doc.addField("tags_field", "tag2");
    doc.addField("tags_field", "tag3");
    addWithChain(doc, "documentEnrichmentMultivaluedInput");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("enriched_field");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");

    assertEquals("Classify these tags: [tag1, tag2, tag3]", DummyChatModel.lastReceivedPrompt);
  }

  @Test
  public void processAdd_multivaluedStringOutputField_emptyInput_shouldSkipEnrichment()
      throws Exception {
    loadDummyLargeLanguageModel("dummy-multivalued-1", "{\"value\": [\"tag1\", \"tag2\"]}");

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
  public void processAdd_singleTypedOutputField_shouldPopulateValue() throws Exception {
    record TypedCase(
        String modelId, String response, String chain, String field, String expectedValue) {}
    List<TypedCase> typedCases =
        List.of(
            new TypedCase(
                "dummy-long",
                "{\"value\": 3000000000}",
                "documentEnrichmentSingleLong",
                "output_long",
                "3000000000"),
            new TypedCase(
                "dummy-int", "{\"value\": 7}", "documentEnrichmentSingleInt", "output_int", "7"),
            new TypedCase(
                "dummy-float",
                "{\"value\": 1.5}",
                "documentEnrichmentSingleFloat",
                "output_float",
                "1.5"),
            new TypedCase(
                "dummy-double",
                "{\"value\": 1e308}",
                "documentEnrichmentSingleDouble",
                "output_double",
                "1e308"),
            new TypedCase(
                "dummy-boolean",
                "{\"value\": true}",
                "documentEnrichmentSingleBoolean",
                "output_boolean",
                "true"),
            new TypedCase(
                "dummy-date",
                "{\"value\": \"2024-01-15T00:00:00Z\"}",
                "documentEnrichmentSingleDate",
                "output_date",
                "'2024-01-15T00:00:00Z'"));

    for (TypedCase typedCase : typedCases) {
      loadDummyLargeLanguageModel(typedCase.modelId(), typedCase.response());
      addWithChain(sdoc("id", "99", "string_field", "some content"), typedCase.chain());
      addWithChain(sdoc("id", "98", "string_field", "other content"), typedCase.chain());
      assertU(commit());

      final SolrQuery query = getEnrichmentQuery(typedCase.field());
      assertJQ(
          "/query" + query.toQueryString(),
          "/response/numFound==2]",
          "/response/docs/[0]/id=='99'",
          "/response/docs/[0]/" + typedCase.field() + "==" + typedCase.expectedValue(),
          "/response/docs/[1]/id=='98'",
          "/response/docs/[1]/" + typedCase.field() + "==" + typedCase.expectedValue());

      restTestHarness.delete(ManagedLargeLanguageModelStore.REST_END_POINT + "/" + typedCase.modelId());
      loadedModelId = null;
    }
  }

  // --- typed multivalued output field tests ---

  @Test
  public void processAdd_multivaluedTypedOutputField_shouldPopulateAllValues() throws Exception {
    record TypeCaseMulti(
        String modelId, String response, String chain, String field, List<String> expectedValues) {}
    List<TypeCaseMulti> typedCaseMultis =
        List.of(
            new TypeCaseMulti(
                "dummy-multivalued-1",
                "{\"value\": [\"tag1\", \"tag2\"]}",
                "documentEnrichmentMultivaluedString",
                "enriched_field_multi",
                List.of("'tag1'", "'tag2'")),
            new TypeCaseMulti(
                "dummy-long-multi",
                "{\"value\": [1000000000, 2000000000, 3000000000]}",
                "documentEnrichmentMultivaluedLong",
                "output_long_multi",
                List.of("1000000000", "2000000000", "3000000000")),
            new TypeCaseMulti(
                "dummy-int-multi",
                "{\"value\": [1, 2]}",
                "documentEnrichmentMultivaluedInt",
                "output_int_multi",
                List.of("1", "2")),
            new TypeCaseMulti(
                "dummy-float-multi",
                "{\"value\": [1.5, 2.5]}",
                "documentEnrichmentMultivaluedFloat",
                "output_float_multi",
                List.of("1.5", "2.5")),
            new TypeCaseMulti(
                "dummy-double-multi",
                "{\"value\": [1e308, 1.1e308]}",
                "documentEnrichmentMultivaluedDouble",
                "output_double_multi",
                List.of("1e308", "1.1e308")),
            new TypeCaseMulti(
                "dummy-boolean-multi",
                "{\"value\": [true, false]}",
                "documentEnrichmentMultivaluedBoolean",
                "output_boolean_multi",
                List.of("true", "false")),
            new TypeCaseMulti(
                "dummy-date-multi",
                "{\"value\": [\"2024-01-15T00:00:00Z\", \"2025-06-30T00:00:00Z\"]}",
                "documentEnrichmentMultivaluedDate",
                "output_date_multi",
                List.of("'2024-01-15T00:00:00Z'", "'2025-06-30T00:00:00Z'")));

    for (TypeCaseMulti typedCase : typedCaseMultis) {
      loadDummyLargeLanguageModel(typedCase.modelId(), typedCase.response());
      addWithChain(sdoc("id", "99", "string_field", "some content"), typedCase.chain());
      addWithChain(sdoc("id", "98", "string_field", "other content"), typedCase.chain());
      assertU(commit());

      final SolrQuery query = getEnrichmentQuery(typedCase.field());
      List<String> assertions = new ArrayList<>();
      assertions.add("/response/numFound==2]");
      for (int docIdx = 0; docIdx < 2; docIdx++) {
        String docId = docIdx == 0 ? "'99'" : "'98'";
        assertions.add("/response/docs/[" + docIdx + "]/id==" + docId);
        for (int i = 0; i < typedCase.expectedValues().size(); i++) {
          assertions.add(
              "/response/docs/["
                  + docIdx
                  + "]/"
                  + typedCase.field()
                  + "/["
                  + i
                  + "]=="
                  + typedCase.expectedValues().get(i));
        }
      }
      assertJQ("/query" + query.toQueryString(), assertions.toArray(new String[0]));

      restTestHarness.delete(ManagedLargeLanguageModelStore.REST_END_POINT + "/" + typedCase.modelId());
      loadedModelId = null;
    }
  }

  // --- LLM response contract violation tests ---

  @Test
  public void processAdd_llmResponseMissingValueKey_shouldLogAndIndexWithNoEnrichedField()
      throws Exception {
    // Model returns valid JSON but without the required "value" key
    loadDummyLargeLanguageModel("dummy-1", "{\"result\": \"some value\"}");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "documentEnrichment");
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
  public void processAdd_llmResponseMalformedJson_shouldLogAndIndexWithNoEnrichedField()
      throws Exception {
    // Model returns a plain string that cannot be parsed as JSON
    loadDummyLargeLanguageModel("dummy-1", "not valid json at all");

    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "documentEnrichment");
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

  // --- field type incompatibility tests ---

  @Test
  public void processAdd_dateOutputField_malformedDateString_shouldFailToIndex() throws Exception {
    // DatePointField requires a full ISO-8601 datetime string (e.g. "2024-01-15T00:00:00Z").
    // A date-only string like "2024-01-15" (missing time component) cannot be parsed by
    // DateMathParser and causes the update to fail.
    // Unlike model exceptions (caught inside processAdd), this error occurs during Solr field
    // conversion in super.processAdd() and propagates as a RemoteSolrException to the caller.
    loadDummyLargeLanguageModel("dummy-date", "{\"value\": \"2024-01-15\"}");

    assertThrows(
        "date string without time component should fail to index",
        RemoteSolrException.class,
        () ->
            addWithChain(
                sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleDate"));
  }

  @Test
  public void processAdd_intOutputField_decimalResponse_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadDummyLargeLanguageModel("dummy-int", "{\"value\": 3.7}");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleInt");
    addWithChain(sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleInt");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_int");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/output_int==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/output_int==");
  }

  @Test
  public void processAdd_doubleOutputField_intResponse_shouldConvertAndIndex() throws Exception {
    loadDummyLargeLanguageModel("dummy-double", "{\"value\": 3}");

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
        "/response/docs/[0]/output_double==3.0",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_double==3.0");
  }

  @Test
  public void processAdd_floatOutputField_doubleResponse_shouldRoundToFloatPrecision()
      throws Exception {
    loadDummyLargeLanguageModel("dummy-float", "{\"value\": 3.141592653589793}");

    addWithChain(sdoc("id", "99", "string_field", "some content"), "documentEnrichmentSingleFloat");
    addWithChain(
        sdoc("id", "98", "string_field", "other content"), "documentEnrichmentSingleFloat");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery("output_float");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/output_float==3.1415927",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/output_float==3.1415927");
  }

  // --- multivalued output field / scalar response test ---

  @Test
  public void processAdd_multivaluedOutputField_singleValuedLlmResponse_shouldSkipEnrichment()
      throws Exception {
    loadDummyLargeLanguageModel("dummy-multivalued-1", "{\"value\": \"a single string\"}");

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
        "!/response/docs/[0]/enriched_field_multi==",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/enriched_field_multi==");
  }

  private SolrQuery getEnrichmentQuery(String enrichedFieldName) {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "id," + enrichedFieldName);
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
