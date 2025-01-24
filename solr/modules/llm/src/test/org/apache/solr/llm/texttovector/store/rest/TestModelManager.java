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
package org.apache.solr.llm.texttovector.store.rest;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.TestLlmBase;
import org.apache.solr.llm.texttovector.search.TextToVectorQParserPlugin;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.RestManager;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestModelManager extends TestLlmBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-llm.xml", "schema.xml", false, false);
  }

  @Test
  public void test() throws Exception {
    final SolrResourceLoader loader = new SolrResourceLoader(tmpSolrHome);

    final RestManager.Registry registry = loader.getManagedResourceRegistry();
    assertNotNull(
        "Expected a non-null RestManager.Registry from the SolrResourceLoader!", registry);

    final String resourceId = "/schema/mstore1";
    registry.registerManagedResource(
        resourceId, ManagedTextToVectorModelStore.class, new TextToVectorQParserPlugin());

    final NamedList<String> initArgs = new NamedList<>();

    final RestManager restManager = new RestManager();
    restManager.init(loader, initArgs, new ManagedResourceStorage.InMemoryStorageIO());

    final ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedTextToVectorModelStore);
    assertEquals(res.getResourceId(), resourceId);
  }

  @Test
  public void testRestManagerEndpoints() throws Exception {
    assertJQ("/schema/managed", "/responseHeader/status==0");

    final String cohereModelClassName = "dev.langchain4j.model.cohere.CohereEmbeddingModel";

    // Add models
    String model = "{ \"name\":\"testModel1\", \"class\":\"" + cohereModelClassName + "\"}";
    // fails since it does not have params
    assertJPut(ManagedTextToVectorModelStore.REST_END_POINT, model, "/responseHeader/status==400");
    // success
    model =
        "{ name:\"testModel2\", class:\""
            + cohereModelClassName
            + "\","
            + "params:{"
            + "baseUrl:\"https://api.cohere.ai/v1/\","
            + "apiKey:\"cohereApiKey2\","
            + "modelName:\"embed-english-light-v3.0\","
            + "inputType:\"search_document\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}";
    assertJPut(ManagedTextToVectorModelStore.REST_END_POINT, model, "/responseHeader/status==0");
    // success
    final String multipleModels =
        "[{ name:\"testModel3\", class:\""
            + cohereModelClassName
            + "\","
            + "params:{baseUrl:\"https://api.cohere.ai/v1/\","
            + "apiKey:\"cohereApiKey3\","
            + "modelName:\"embed-english-light-v3.0\","
            + "inputType:\"search_document\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}\n"
            + ",{ name:\"testModel4\", class:\""
            + cohereModelClassName
            + "\","
            + "params:{baseUrl:\"https://api.cohere.ai/v1/\","
            + "apiKey:\"cohereApiKey4\","
            + "modelName:\"embed-english-light-v3.0\","
            + "inputType:\"search_document\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}}]";
    assertJPut(
        ManagedTextToVectorModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");
    final String qryResult = JQ(ManagedTextToVectorModelStore.REST_END_POINT);

    assertTrue(
        qryResult.contains("\"name\":\"testModel2\"")
            && qryResult.contains("\"name\":\"testModel3\"")
            && qryResult.contains("\"name\":\"testModel4\""));

    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='testModel2'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[1]/name=='testModel3'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[2]/name=='testModel4'");
    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/testModel2");
    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/testModel3");
    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/testModel4");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models==[]'");
  }

  @Test
  public void loadModel_cohere_shouldLoadModelConfig() throws Exception {
    loadModel("cohere-model.json");

    final String modelName = "cohere-1";
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadModel_openAi_shouldLoadModelConfig() throws Exception {
    loadModel("openai-model.json");

    final String modelName = "openai-1";
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.openai.com/v1'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-openAI'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='text-embedding-3-small'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadModel_mistralAi_shouldLoadModelConfig() throws Exception {
    loadModel("mistralai-model.json");

    final String modelName = "mistralai-1";
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.mistral.ai/v1'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/apiKey=='apiKey-mistralAI'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='mistral-embed'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadModel_huggingface_shouldLoadModelConfig() throws Exception {
    loadModel("huggingface-model.json");

    final String modelName = "huggingface-1";
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/accessToken=='apiKey-huggingface'");
    assertJQ(
        ManagedTextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelId=='sentence-transformers/all-MiniLM-L6-v2'");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/" + modelName);
  }

  @Test
  public void loadModel_dummyUnsupportedParam_shouldRaiseError() throws Exception {
    loadModel("dummy-model-unsupported.json", "400");
  }

  @Test
  public void loadModel_dummyAmbiguousParam_shouldDefaultToString() throws Exception {
    loadModel("dummy-model-ambiguous.json");

    final String modelName = "dummy-1";
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(ManagedTextToVectorModelStore.REST_END_POINT, "/models/[0]/params/ambiguous==10");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/" + modelName);
  }
}
