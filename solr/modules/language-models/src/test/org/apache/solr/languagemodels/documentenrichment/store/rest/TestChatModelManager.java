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

public class TestChatModelManager extends TestLanguageModelBase {

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
        resourceId, ManagedChatModelStore.class, new DocumentEnrichmentUpdateProcessorFactory());

    final NamedList<String> initArgs = new NamedList<>();

    final RestManager restManager = new RestManager();
    restManager.init(loader, initArgs, new ManagedResourceStorage.InMemoryStorageIO());

    final ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedChatModelStore);
    assertEquals(res.getResourceId(), resourceId);
  }

  @Test
  public void testRestManagerEndpoints() throws Exception {
    assertJQ("/schema/managed", "/responseHeader/status==0");

    final String openAiClassName = "dev.langchain4j.model.openai.OpenAiChatModel";

    // fails — no params provided
//    String model = "{ \"name\":\"testChatModel1\", \"class\":\"" + openAiClassName + "\"}";
//    assertJPut(ManagedChatModelStore.REST_END_POINT, model, "/responseHeader/status==400");

    // success
    String model =
        "{ name:\"testChatModel2\", class:\""
            + openAiClassName
            + "\","
            + "params:{"
            + "baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey2\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}";
    assertJPut(ManagedChatModelStore.REST_END_POINT, model, "/responseHeader/status==0");

    // success — multiple models in one PUT
    final String multipleModels =
        "[{ name:\"testChatModel3\", class:\""
            + openAiClassName
            + "\","
            + "params:{baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey3\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}\n"
            + ",{ name:\"testChatModel4\", class:\""
            + openAiClassName
            + "\","
            + "params:{baseUrl:\"https://api.openai.com/v1\","
            + "apiKey:\"testApiKey4\","
            + "modelName:\"gpt-4o-mini\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}]";
    assertJPut(ManagedChatModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");

    final String qryResult = JQ(ManagedChatModelStore.REST_END_POINT);
    assertTrue(
        qryResult.contains("\"name\":\"testChatModel2\"")
            && qryResult.contains("\"name\":\"testChatModel3\"")
            && qryResult.contains("\"name\":\"testChatModel4\""));

    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='testChatModel2'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[1]/name=='testChatModel3'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[2]/name=='testChatModel4'");
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/testChatModel2");
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/testChatModel3");
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/testChatModel4");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models==[]'");
  }

  @Test
  public void loadChatModel_openAi_shouldLoadModelConfig() throws Exception {
    loadChatModel("openai-model.json");

    final String modelName = "openai-1";
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.openai.com/v1'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-openAI'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='gpt-5.4-nano'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadChatModel_mistralAi_shouldLoadModelConfig() throws Exception {
    loadChatModel("mistralai-chat-model.json");

    final String modelName = "mistralai-chat-1";
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.mistral.ai/v1'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-mistralAI'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='mistral-small-latest'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadChatModel_dummyUnsupportedParam_shouldRaiseError() throws Exception {
    loadChatModel("dummy-chat-model-unsupported.json", "400");
  }

  @Test
  public void loadChatModel_dummyAmbiguousParam_shouldDefaultToString() throws Exception {
    loadChatModel("dummy-chat-model-ambiguous.json");

    final String modelName = "dummy-chat-1";
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/ambiguous==10");

    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/" + modelName);
  }
}
