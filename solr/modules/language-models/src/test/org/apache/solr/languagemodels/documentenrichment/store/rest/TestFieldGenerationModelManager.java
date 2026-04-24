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
package org.apache.solr.languagemodels.documentenrichment.store.rest;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessorFactory;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.RestManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFieldGenerationModelManager extends TestLanguageModelBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-document-enrichment.xml", "schema-language-models.xml", false, false);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  @Test
  public void test() throws Exception {
    final SolrResourceLoader loader = new SolrResourceLoader(tmpSolrHome);

    final RestManager.Registry registry = loader.getManagedResourceRegistry();
    assertNotNull(
        "Expected a non-null RestManager.Registry from the SolrResourceLoader!", registry);

    final String resourceId = "/schema/mstore1";
    registry.registerManagedResource(
        resourceId, ManagedFieldGenerationModelStore.class, new DocumentEnrichmentUpdateProcessorFactory());

    final NamedList<String> initArgs = new NamedList<>();

    final RestManager restManager = new RestManager();
    restManager.init(loader, initArgs, new ManagedResourceStorage.InMemoryStorageIO());

    final ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedFieldGenerationModelStore);
    assertEquals(res.getResourceId(), resourceId);
  }

  @Test
  public void testRestManagerEndpoints() throws Exception {
    assertJQ("/schema/managed", "/responseHeader/status==0");

    final String openAiClassName = "dev.langchain4j.model.openai.OpenAiChatModel";

    // success
    String model =
        "{ name:\"test-model-1\", class:\""
            + openAiClassName
            + "\","
            + "params:{"
            + "baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey2\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}";
    assertJPut(ManagedFieldGenerationModelStore.REST_END_POINT, model, "/responseHeader/status==0");

    // success — multiple models in one PUT
    final String multipleModels =
        "[{ name:\"test-model-2\", class:\""
            + openAiClassName
            + "\","
            + "params:{baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey3\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}\n"
            + ",{ name:\"test-model-3\", class:\""
            + openAiClassName
            + "\","
            + "params:{baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey4\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}]";
    assertJPut(ManagedFieldGenerationModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");

    final String qryResult = JQ(ManagedFieldGenerationModelStore.REST_END_POINT);
    assertTrue(
        qryResult.contains("\"name\":\"test-model-1\"")
            && qryResult.contains("\"name\":\"test-model-2\"")
            && qryResult.contains("\"name\":\"test-model-3\""));

    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='test-model-1'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[1]/name=='test-model-2'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[2]/name=='test-model-3'");
    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/test-model-1");
    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/test-model-2");
    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/test-model-3");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models==[]");
  }

  @Test
  public void loadFieldGenerationModel_openAi_shouldLoadModelConfig() throws Exception {
    loadFieldGenerationModel("openai-field-generation-model.json");

    final String modelName = "openai-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.openai.com/v1'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-openAI'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/modelName=='gpt-5.4-nano'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadFieldGenerationModel_mistralAi_shouldLoadModelConfig() throws Exception {
    loadFieldGenerationModel("mistralai-field-generation-model.json");

    final String modelName = "mistralai-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.mistral.ai/v1'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-mistralAI'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='mistral-small-latest'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadFieldGenerationModel_anthropic_shouldLoadModelConfig() throws Exception {
    loadFieldGenerationModel("anthropic-field-generation-model.json");

    final String modelName = "anthropic-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.anthropic.com/v1'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-anthropic'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='claude-3-5-haiku-latest'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadFieldGenerationModel_ollama_shouldLoadModelConfig() throws Exception {
    loadFieldGenerationModel("ollama-field-generation-model.json");

    final String modelName = "ollama-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='http://localhost:11434'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/modelName=='llama3.2'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadFieldGenerationModel_gemini_shouldLoadModelConfig() throws Exception {
    loadFieldGenerationModel("gemini-field-generation-model.json");

    final String modelName = "gemini-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-gemini'");
    assertJQ(
        ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/modelName=='gemini-2.0-flash'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadFieldGenerationModel_dummyUnsupportedParam_shouldRaiseError() throws Exception {
    loadFieldGenerationModel(
        "dummy-field-generation-model-unsupported.json",
        "400",
        "/error/msg=='Model loading failed for org.apache.solr.languagemodels.documentenrichment.model.DummyChatModel'");
  }

  @Test
  public void loadFieldGenerationModel_notAChatModel_shouldRaiseError() throws Exception {
    loadFieldGenerationModel(
        "not-a-field-generation-model.json",
        "400",
        "/error/msg=='Model loading failed for com.example.NonExistentChatModel'");
  }

  @Test
  public void loadFieldGenerationModel_dummyAmbiguousParam_shouldDefaultToString() throws Exception {
    loadFieldGenerationModel("dummy-field-generation-model-ambiguous.json");

    final String modelName = "dummy-1";
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(ManagedFieldGenerationModelStore.REST_END_POINT, "/models/[0]/params/ambiguousTypeParam==10");

    restTestHarness.delete(ManagedFieldGenerationModelStore.REST_END_POINT + "/" + modelName);
  }
}
