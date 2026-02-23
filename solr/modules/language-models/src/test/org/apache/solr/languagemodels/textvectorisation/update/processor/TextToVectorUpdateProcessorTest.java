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
package org.apache.solr.languagemodels.textvectorisation.update.processor;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextToVectorUpdateProcessorTest extends TestLanguageModelBase {

  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-language-models.xml", "schema-language-models.xml", false, false);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  @After
  public void afterEachTest() throws Exception {
    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
    restTestHarness.delete(
        ManagedTextToVectorModelStore.REST_END_POINT + "/exception-throwing-model"); // clean
  }

  @Test
  public void processAdd_inputField_shouldVectoriseInputField() throws Exception {
    assertVectorisationWithModel("dummy-model.json");
  }

  @Test
  public void processAdd_customModel_shouldVectoriseInputField() throws Exception {
    assertVectorisationWithModel("dummy-custom-model.json");
  }

  private void assertVectorisationWithModel(String modelJsonFile) throws Exception {
    loadModel(modelJsonFile);

    addWithChain(sdoc("id", "99", "_text_", "Vegeta is the saiyan prince."), "textToVector");
    addWithChain(
        sdoc("id", "98", "_text_", "Kakaroth is a saiyan grown up on planet Earth."),
        "textToVector");
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/vector==[1.0, 2.0, 3.0, 4.0]",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");
  }

  private SolrQuery getSolrQuery() {
    final String solrQuery = "*:*";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id,vector");
    return query;
  }

  /*
  This test looks for the 'dummy-1' model, but such model is not loaded, the model store is empty, so the update fails
   */
  @Test
  public void processAdd_modelNotFound_shouldThrowException() {
    RuntimeException thrown =
        assertThrows(
            "model not found should throw an exception",
            RemoteSolrException.class,
            () -> {
              addWithChain(
                  sdoc("id", "99", "_text_", "Vegeta is the saiyan prince."), "textToVector");
            });
    assertTrue(
        thrown
            .getMessage()
            .contains(
                "The model configured in the Update Request Processor 'dummy-1' can't be found in the store: /schema/text-to-vector-model-store"));
  }

  @Test
  public void processAdd_emptyInputField_shouldLogAndIndexWithNoVector() throws Exception {
    loadModel("dummy-model.json"); // preparation
    addWithChain(sdoc("id", "99", "_text_", ""), "textToVector");
    addWithChain(sdoc("id", "98", "_text_", "Vegeta is the saiyan prince."), "textToVector");
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/vector==", // no vector field for the document 99
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1"); // clean up
  }

  @Test
  public void processAdd_nullInputField_shouldLogAndIndexWithNoVector() throws Exception {
    loadModel("dummy-model.json"); // preparation
    addWithChain(sdoc("id", "99", "_text_", "Vegeta is the saiyan prince."), "textToVector");
    assertU(adoc("id", "98"));
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/vector==[1.0, 2.0, 3.0, 4.0]",
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/vector=="); // no vector field for the document 98

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1"); // clean up
  }

  @Test
  public void processAdd_failingVectorisation_shouldLogAndIndexWithNoVector() throws Exception {
    loadModel("exception-throwing-model.json"); // preparation
    addWithChain(sdoc("id", "99", "_text_", "Vegeta is the saiyan prince."), "failingTextToVector");
    addWithChain(
        sdoc("id", "98", "_text_", "Kakaroth is a saiyan grown up on planet Earth."),
        "failingTextToVector");
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "!/response/docs/[0]/vector==", // no vector field for the document 99
        "/response/docs/[1]/id=='98'",
        "!/response/docs/[1]/vector=="); // no vector field for the document 98

    restTestHarness.delete(
        ManagedTextToVectorModelStore.REST_END_POINT + "/exception-throwing-model"); // clean up
  }

  @Test
  public void processAtomicUpdate_shouldTriggerTheVectorizationAndFetchTheStoredContent()
      throws Exception {
    // Verifies that when using a processor chain configured for partial updates
    // (i.e., the UpdateRequestProcessor is placed before the TextToVector processor),
    // the system correctly retrieves the stored value of the input field (string_field)
    // and generates the vector for the document.
    loadModel("dummy-model.json");
    assertU(adoc("id", "99", "string_field", "Vegeta is the saiyan prince."));
    assertU(adoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."));
    assertU(commit());

    SolrInputDocument atomic_doc = new SolrInputDocument();
    atomic_doc.setField("id", "99");
    atomic_doc.setField("vectorised", Map.of("set", "true"));
    addWithChain(
        atomic_doc, "textToVectorForPartialUpdates"); // use the chain that supports partial updates
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='98'",
        "!/response/docs/[0]/vector==", // no vector field for document 98
        "/response/docs/[1]/id=='99'",
        "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
  }

  @Test
  public void processAtomicUpdate_shouldReplaceExistingVectorNotAppend() throws Exception {
    // This test verifies that when a document already contains a vector, and the string_field field
    // is
    // modified using an atomic update, the vector is recomputed and replaces the previous one. It
    // ensures that the system does not append or merge vector values.
    loadModel("dummy-model.json");
    addWithChain(
        sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."),
        "textToVectorStoredInputField");
    addWithChain(
        sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."),
        "textToVectorStoredInputField");
    assertU(commit());

    SolrInputDocument atomic_doc = new SolrInputDocument();
    atomic_doc.setField("id", "99");
    atomic_doc.setField(
        "string_field", Map.of("set", "Vegeta is the saiyan prince from the Dragon Ball series."));
    addWithChain(
        atomic_doc, "textToVectorForPartialUpdates"); // use the chain that supports partial updates
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/vector==[1.0, 2.0, 3.0, 4.0]",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
  }

  void addWithChain(SolrInputDocument document, String updateChain)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(document);
    req.setParam("update.chain", updateChain);
    solrTestRule.getSolrClient("collection1").request(req);
  }
}
