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

import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.documentenrichment.model.SolrChatModel;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedChatModelStore;
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
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);

    assertEquals(List.of("string_field"), factory.getInputFields());
    assertEquals("enriched_field", factory.getOutputField());
    assertEquals("Summarize: {string_field}", factory.getPrompt());
    assertEquals("model1", factory.getModelName());
  }

  @Test
  public void init_multipleInputFields_shouldInitAllFields() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("inputField", "body_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}. Body: {body_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);

    assertEquals(List.of("string_field", "body_field"), factory.getInputFields());
  }

  @Test
  public void init_noInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("At least one 'inputField' must be provided", e.getMessage());
  }

  @Test
  public void init_nullOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Missing required parameter: outputField", e.getMessage());
  }

  @Test
  public void init_neitherPromptNorPromptFile_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

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

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Only one of 'prompt' or 'promptFile' can be provided, not both", e.getMessage());
  }

  @Test
  public void init_promptMissingPlaceholderForDeclaredField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize:");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("prompt is missing placeholders for inputField(s): [string_field]", e.getMessage());
  }

  @Test
  public void init_promptMissingOnePlaceholderOfMultipleFields_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("inputField", "body_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("prompt is missing placeholders for inputField(s): [body_field]", e.getMessage());
  }

  @Test
  public void init_promptHasExtraPlaceholderNotDeclaredAsInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}. Extra: {unknown_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

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

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();

    SolrException e = assertThrows(SolrException.class, () -> factory.init(args));
    assertEquals("Missing required parameter: model", e.getMessage());
  }

  @Test
  public void init_promptFile_shouldLoadPromptFromFile() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("promptFile", "prompt.txt");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);
    factory.inform(collection1);

    assertEquals("prompt.txt", factory.getPromptFile());
    assertNotNull(factory.getPrompt());
    assertTrue(factory.getPrompt().contains("{string_field}"));
  }

  @Test
  public void init_promptFileMultiField_shouldLoadAndValidateBothPlaceholders() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("inputField", "body_field");
    args.add("outputField", "enriched_field");
    args.add("promptFile", "prompt-multi-field.txt");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);
    factory.inform(collection1);

    assertNotNull(factory.getPrompt());
    assertTrue(factory.getPrompt().contains("{string_field}"));
    assertTrue(factory.getPrompt().contains("{body_field}"));
  }

  @Test
  public void init_promptFileWithMissingPlaceholder_shouldThrowExceptionInInform() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "enriched_field");
    args.add("promptFile", "prompt-no-placeholder.txt");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    factory.init(args);

    SolrException e = assertThrows(SolrException.class, () -> factory.inform(collection1));
    assertEquals(
        "prompt is missing placeholders for inputField(s): [string_field]", e.getMessage());
  }

  /* Following tests depend on a real solr schema and depend on BeforeClass-AfterClass methods */

  @Test
  public void init_notExistentOutputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "notExistentOutput");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factory.init(args);

    SolrException e = assertThrows(SolrException.class, () -> factory.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentOutput\"", e.getMessage());
  }

  @Test
  public void init_notTextualOutputField_shouldThrowExceptionWithDetailedMessage() {
    // vector is a DenseVectorField — not a textual field
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("outputField", "vector");
    args.add("prompt", "Summarize: {string_field}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factory.init(args);

    SolrException e = assertThrows(SolrException.class, () -> factory.getInstance(req, null, null));
    assertEquals(
        "only textual fields are compatible with Document Enrichment: vector", e.getMessage());
  }

  @Test
  public void init_notExistentInputField_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "notExistentInput");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Summarize: {notExistentInput}");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factory.init(args);

    SolrException e = assertThrows(SolrException.class, () -> factory.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
  }

  @Test
  public void init_multipleInputFields_oneNotExistent_shouldThrowExceptionWithDetailedMessage() {
    NamedList<String> args = new NamedList<>();
    args.add("inputField", "string_field");
    args.add("inputField", "notExistentInput");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {string_field}. Body: {notExistentInput}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    factory.init(args);

    SolrException e = assertThrows(SolrException.class, () -> factory.getInstance(req, null, null));
    assertEquals("undefined field: \"notExistentInput\"", e.getMessage());
  }

  @Test
  public void init_dynamicInputField_shouldNotThrowException() {
    UpdateRequestProcessor instance =
        createUpdateProcessor("text_s", "enriched_field", collection1, "model2");
    assertNotNull(instance);
  }

  @Test
  public void init_multipleDynamicInputFields_shouldNotThrowException() {
    NamedList<String> args = new NamedList<>();
    ManagedChatModelStore.getManagedModelStore(collection1)
        .addModel(new SolrChatModel("model1", null, null));
    args.add("inputField", "text_s");
    args.add("inputField", "body_field");
    args.add("outputField", "enriched_field");
    args.add("prompt", "Title: {text_s}. Body: {body_field}.");
    args.add("model", "model1");

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    factory.init(args);

    SolrQueryRequestBase req = new SolrQueryRequestBase(collection1, params) {};
    assertNotNull(factory.getInstance(req, null, null));
  }

  private UpdateRequestProcessor createUpdateProcessor(
      String inputFieldName, String outputFieldName, SolrCore core, String modelName) {
    NamedList<String> args = new NamedList<>();

    ManagedChatModelStore.getManagedModelStore(core)
        .addModel(new SolrChatModel(modelName, null, null));
    args.add("inputField", inputFieldName);
    args.add("outputField", outputFieldName);
    args.add("prompt", "Summarize: {" + inputFieldName + "}");
    args.add("model", modelName);

    DocumentEnrichmentUpdateProcessorFactory factory = new DocumentEnrichmentUpdateProcessorFactory();
    ModifiableSolrParams params = new ModifiableSolrParams();
    factory.init(args);

    SolrQueryRequestBase req = new SolrQueryRequestBase(core, params) {};

    return factory.getInstance(req, null, null);
  }
}
