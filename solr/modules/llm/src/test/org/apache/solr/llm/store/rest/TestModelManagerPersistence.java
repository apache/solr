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
package org.apache.solr.llm.store.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.langchain4j.model.cohere.CohereEmbeddingModel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.solr.common.util.Utils;
import org.apache.solr.llm.TestLlmBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestModelManagerPersistence extends TestLlmBase {

  @Before
  public void init() throws Exception {
    setupTest(false, true);
  }

  @After
  public void cleanup() throws Exception {
    aftertest();
  }

  @Test
  public void testModelAreStoredCompact() throws Exception {
    loadModel(
        "cohere1",
        CohereEmbeddingModel.class.getName(),
        "{"
            + "baseUrl:\"https://api.cohere.ai/v1/\","
            + "apiKey:\"cohereApiKey2\","
            + "modelName:\"embed-english-light-v3.0\","
            + "inputType:\"search_document\","
            + "logRequests:true,"
            + "logResponses:false"
            + "}");

    final String embeddingModelStoreContent =
        Files.readString(embeddingModelStoreFile, StandardCharsets.UTF_8);
    Object embeddingModelStoreObject = Utils.fromJSONString(embeddingModelStoreContent);
    assertEquals(
        new String(Utils.toJSON(embeddingModelStoreObject, -1), UTF_8), embeddingModelStoreContent);
  }

  @Test
  public void testModelStorePersistence() throws Exception {
    // check models are empty
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/==[]");

    // load models and features from files
    loadModels("cohere-model.json");

    final String modelName = "cohere-1";
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // check persistence after restart
    getJetty().stop();
    getJetty().start();
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        ManagedEmbeddingModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // delete loaded models and features
    restTestHarness.delete(ManagedEmbeddingModelStore.REST_END_POINT + "/" + modelName);
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/==[]");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/==[]");

    // check persistence after restart
    getJetty().stop();
    getJetty().start();
    assertJQ(ManagedEmbeddingModelStore.REST_END_POINT, "/models/==[]");
  }
}
