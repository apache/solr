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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.solr.common.util.Utils;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestChatModelManagerPersistence extends TestLanguageModelBase {

  @Before
  public void init() throws Exception {
    setupTest("solrconfig-document-enrichment.xml", "schema-language-models.xml", false, true);
  }

  @After
  public void cleanup() throws Exception {
    afterTest();
  }

  @Test
  public void testModelAreStoredCompact() throws Exception {
    loadChatModel("openai-model.json");

    final String JSONOnDisk = Files.readString(chatModelStoreFile, StandardCharsets.UTF_8);
    Object objectFromDisk = Utils.fromJSONString(JSONOnDisk);
    assertEquals(new String(Utils.toJSON(objectFromDisk, -1), UTF_8), JSONOnDisk);
  }

  @Test
  public void testModelStorePersistence() throws Exception {
    // check store is empty at start
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/==[]");

    // load a model
    loadChatModel("openai-model.json");

    final String modelName = "openai-1";
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.openai.com/v1'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-openAI'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/modelName=='gpt-5.4-nano'");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/timeout==60");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logRequests==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/logResponses==true");
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/maxRetries==5");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT,
        "/models/[0]/params/baseUrl=='https://api.openai.com/v1'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/apiKey=='apiKey-openAI'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/modelName=='gpt-5.4-nano'");

    // check persistence after restart
    getJetty().stop();
    getJetty().start();
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/[0]/name=='" + modelName + "'");
    assertJQ(
        ManagedChatModelStore.REST_END_POINT, "/models/[0]/params/modelName=='gpt-5.4-nano'");

    // delete model and verify persistence of the empty state
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/" + modelName);
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/==[]");

    restTestHarness.reload();
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/==[]");

    getJetty().stop();
    getJetty().start();
    assertJQ(ManagedChatModelStore.REST_END_POINT, "/models/==[]");
  }
}