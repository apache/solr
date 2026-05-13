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
package org.apache.solr.languagemodels.store.rest;

import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTextToVectorModelManagerPersistence extends TestLanguageModelBase {

  @Before
  public void init() throws Exception {
    setupTest("solrconfig-language-models.xml", "schema-language-models.xml", false, true);
  }

  @After
  public void cleanup() throws Exception {
    afterTest();
  }

  @Test
  public void testModelStorePersistence() throws Exception {
    // check models are empty
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/==[]");

    // load models and features from files
    loadTextToVectorModel("cohere-model.json");

    final String modelName = "cohere-1";
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // check persistence after restart
    restartJetty();
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.cohere.ai/v1/'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-cohere'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/modelName=='embed-english-light-v3.0'");
    assertJQ(
        TextToVectorModelStore.REST_END_POINT,
        "/models/[0]/params/inputType=='search_document'");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");

    // delete loaded models and features
    restTestHarness.delete(TextToVectorModelStore.REST_END_POINT + "/" + modelName);
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/==[]");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/==[]");

    // check persistence after restart
    restartJetty();
    assertJQ(TextToVectorModelStore.REST_END_POINT, "/models/==[]");
  }
}
