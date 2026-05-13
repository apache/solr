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
package org.apache.solr.languagemodels.update.processor.factory;

import dev.langchain4j.model.chat.request.ResponseFormatType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.model.SolrLargeLanguageModel;
import org.apache.solr.languagemodels.store.rest.LargeLanguageModelStore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentEnrichmentUpdateProcessorFactoryTest extends TestLanguageModelBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-document-enrichment.xml", "schema-language-models.xml", false, false);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  SolrCore collection1;

  @Before
  public void setup() {
    collection1 = solrTestRule.getCoreContainer().getCore("collection1");
  }

  @After
  public void after() {
    collection1.close();
  }

  @Test
  public void init_fullArgs_shouldInitAllParams() {
    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(List.of("string_field"), "enriched_field", null, "model1");

    assertEquals(List.of("string_field"), factory.getInputFields());
    assertEquals("enriched_field", factory.getOutputField());
    assertEquals("Summarize: {string_field}.", factory.getPrompt());
    assertEquals("model1", factory.getModelName());
  }

  @Test
  public void init_multipleInputFields_shouldInitAllFields() {
    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(
            List.of("string_field", "body_field"), "enriched_field", null, "model1");

    assertEquals(List.of("string_field", "body_field"), factory.getInputFields());
  }

  @Test
  public void init_arrInputField_shouldInitAllFields() {
    NamedList<Object> args = new NamedList<>();
    args.add("inputField", new ArrayList<>(List.of("string_field", "body_field")));
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}. Body: {body_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);

    assertEquals(List.of("string_field", "body_field"), factory.getInputFields());
  }

  // when exception are thrown at init time, the helper function cannot be used
  @Test
  public void init_noInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {string_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("At least one 'inputField' must be provided", e.getMessage());
  }

  @Test
  public void init_nullOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Exactly one 'outputField' must be provided", e.getMessage());
  }

  @Test
  public void init_moreThanOneOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("outputField", "body_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();
    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals(
        "Only one 'outputField' can be provided, but found: [enriched_field, body_field]",
        e.getMessage());
  }

  @Test
  public void init_neitherPromptNorPromptFile_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Either 'prompt' or 'promptFile' must be provided", e.getMessage());
  }

  @Test
  public void init_bothPromptAndPromptFile_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("promptFile", "prompt.txt");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Only one of 'prompt' or 'promptFile' can be provided, not both", e.getMessage());
  }

  @Test
  public void
      init_promptMissingPlaceholderForDeclaredField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize:");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals(
        "prompt is missing placeholders for inputField(s): [string_field]", e.getMessage());
  }

  @Test
  public void
      init_promptMissingOnePlaceholderOfMultipleFields_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("inputField", "body_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("prompt is missing placeholders for inputField(s): [body_field]", e.getMessage());
  }

  @Test
  public void
      init_promptHasExtraPlaceholderNotDeclaredAsInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}. Extra: {unknown_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals(
        "prompt contains placeholders not declared as inputField(s): [unknown_field]",
        e.getMessage());
  }

  @Test
  public void init_nullModel_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {string_field}");

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Missing required parameter: model", e.getMessage());
  }

  @Test
  public void init_promptFile_shouldLoadPromptFromFile() {
    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(
            List.of("string_field"), "enriched_field", "prompt.txt", "model1");
    factory.inform(collection1);

    assertEquals("prompt.txt", factory.getPromptFile());
    assertNotNull(factory.getPrompt());
    assertTrue(factory.getPrompt().contains("{string_field}"));
  }

  @Test
  public void init_promptFileMultiField_shouldLoadAndValidateBothPlaceholders() {
    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(
            List.of("string_field", "body_field"),
            "enriched_field",
            "prompt-multi-field.txt",
            "model1");
    factory.inform(collection1);

    assertNotNull(factory.getPrompt());
    assertTrue(factory.getPrompt().contains("{string_field}"));
    assertTrue(factory.getPrompt().contains("{body_field}"));
  }

  @Test
  public void init_promptFileWithMissingPlaceholder_shouldThrowExceptionInInform() {
    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(
            List.of("string_field"), "enriched_field", "prompt-no-placeholder.txt", "model1");

    SolrException e = assertThrows(SolrException.class, () -> factory.inform(collection1));
    assertEquals(
        "prompt is missing placeholders for inputField(s): [string_field]", e.getMessage());
  }

  /* Following tests depend on a real solr schema and depend on BeforeClass-AfterClass methods */

  @Test
  public void init_notExistentOutputField_shouldThrowExceptionWithDetailedMessage()
      throws Exception {
    SolrException e =
        assertThrows(
            SolrException.class,
            () ->
                createUpdateProcessor(
                    List.of("string_field"), "notExistentOutput", null, collection1, "model1"));
    assertEquals("undefined field: \"notExistentOutput\"", e.getMessage());
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  @Test
  public void init_notExistentInputField_shouldThrowExceptionWithDetailedMessage()
      throws Exception {
    SolrException e =
        assertThrows(
            SolrException.class,
            () ->
                createUpdateProcessor(
                    List.of("notExistentInput"), "enriched_field", null, collection1, "model1"));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  @Test
  public void init_multipleInputFields_oneNotExistent_shouldThrowExceptionWithDetailedMessage()
      throws Exception {
    SolrException e =
        assertThrows(
            SolrException.class,
            () ->
                createUpdateProcessor(
                    List.of("string_field", "notExistentInput"),
                    "enriched_field_multi",
                    null,
                    collection1,
                    "model1"));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  @Test
  public void init_multivaluedStringOutputField_shouldNotThrowException() throws Exception {
    UpdateRequestProcessor instance =
        createUpdateProcessor(
            List.of("string_field"), "enriched_field_multi", null, collection1, "model1");
    assertNotNull(instance);
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  /* getJsonSchema tests for unsupported field types from the Solr documentation:
   * - BinaryField: not supported
   * - UUIDField, NestPathField, DenseVectorField: explicitly removed support for these fields since the Java classes
   *   extend some supported field types
   *  */

  @Test
  public void getJsonSchema_unsupportedFieldTypes_shouldThrowUnsupportedFieldTypeException() {
    var cases =
        Map.of(
            "output_binary", "BinaryField",
            "output_uuid", "UUIDField",
            "output_nest_path", "NestPathField",
            "vector", "DenseVectorField");
    var schema = collection1.getLatestSchema();
    cases.forEach(
        (fieldName, expectedTypeName) -> {
          var schemaField = schema.getField(fieldName);
          SolrException e =
              assertThrows(
                  SolrException.class,
                  () -> DocumentEnrichmentUpdateProcessorFactory.getJsonSchema(schemaField));
          assertEquals(
              "field type is not supported by Document Enrichment: " + expectedTypeName,
              e.getMessage());
        });
  }

  @Test
  public void init_sortableTextOutputField_getJsonSchema_shouldProduceStringSchema() {
    var schemaField = collection1.getLatestSchema().getField("output_sortable_text");
    var responseFormat = DocumentEnrichmentUpdateProcessorFactory.getJsonSchema(schemaField);
    assertNotNull(responseFormat);
    assertEquals(ResponseFormatType.JSON, responseFormat.type());
    assertNotNull(responseFormat.jsonSchema());
  }

  @Test
  public void init_multivaluedStringOutputField_getJsonSchema_shouldProduceArraySchema() {
    // verify the ResponseFormat is constructed correctly for the multivalued field
    var schema = collection1.getLatestSchema();
    var schemaField = schema.getField("enriched_field_multi");
    assertTrue(schemaField.multiValued());
    var responseFormat = DocumentEnrichmentUpdateProcessorFactory.getJsonSchema(schemaField);
    assertNotNull(responseFormat);
    assertEquals(ResponseFormatType.JSON, responseFormat.type());
    assertNotNull(responseFormat.jsonSchema());
  }

  @Test
  public void init_singleValuedStringOutputField_getJsonSchema_shouldProduceStringSchema() {
    var schema = collection1.getLatestSchema();
    var schemaField = schema.getField("enriched_field");
    assertFalse(schemaField.multiValued());
    var responseFormat = DocumentEnrichmentUpdateProcessorFactory.getJsonSchema(schemaField);
    assertNotNull(responseFormat);
    assertEquals(ResponseFormatType.JSON, responseFormat.type());
    assertNotNull(responseFormat.jsonSchema());
  }

  @Test
  public void init_dynamicInputField_shouldNotThrowException() throws Exception {
    UpdateRequestProcessor instance =
        createUpdateProcessor(List.of("text_s"), "enriched_field", null, collection1, "model1");
    assertNotNull(instance);
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  @Test
  public void init_multipleDynamicInputFields_shouldNotThrowException() throws Exception {
    UpdateRequestProcessor instance =
        createUpdateProcessor(
            List.of("text_s", "body_field"), "enriched_field", null, collection1, "model1");
    assertNotNull(instance);
    restTestHarness.delete(LargeLanguageModelStore.REST_END_POINT + "/model1");
  }

  private UpdateRequestProcessor createUpdateProcessor(
      List<String> inputFieldNames,
      String outputFieldName,
      String prompt,
      SolrCore core,
      String modelName)
      throws Exception {

    LargeLanguageModelStore.getManagedModelStore(core)
        .addModel(new SolrLargeLanguageModel(modelName, null, null));

    DocumentEnrichmentUpdateProcessorFactory factory =
        initializeUpdateProcessorFactory(inputFieldNames, outputFieldName, prompt, modelName);

    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {};

    return factory.getInstance(req, null, null);
  }

  private DocumentEnrichmentUpdateProcessorFactory initializeUpdateProcessorFactory(
      List<String> inputFieldNames, String outputFieldName, String prompt, String modelName) {
    NamedList<String> args = new NamedList<>();

    for (String fieldName : inputFieldNames) {
      args.add("inputField", fieldName);
    }
    args.add("outputField", outputFieldName);

    if (prompt != null) {
      args.add("promptFile", prompt);
    } else {
      args.add("prompt", "Summarize: {" + String.join("}. {", inputFieldNames) + "}.");
    }

    args.add("model", modelName);

    DocumentEnrichmentUpdateProcessorFactory factory =
        new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);
    return factory;
  }
}
