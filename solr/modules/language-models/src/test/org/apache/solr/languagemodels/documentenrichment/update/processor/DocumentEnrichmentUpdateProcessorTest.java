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
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedChatModelStore;
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

  @After
  public void afterEachTest() throws Exception {
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/dummy-chat-1");
    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/exception-throwing-chat-model");
  }

  @Test
  public void processAdd_inputField_shouldEnrichInputField() throws Exception {
    loadChatModel("dummy-chat-model.json");

    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "documentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");

    restTestHarness.delete(ManagedChatModelStore.REST_END_POINT + "/dummy-1"); // clean up
  }

  /*
   This test looks for the 'dummy-chat-1' model, but such model is not loaded —
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
                "The model configured in the Update Request Processor 'dummy-chat-1' can't be found in the store: /schema/chat-model-store"));
  }

  @Test
  public void processAdd_emptyInputField_shouldLogAndIndexWithNoEnrichedField() throws Exception {
    loadChatModel("dummy-chat-model.json");
    addWithChain(sdoc("id", "99", "string_field", ""), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

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
    loadChatModel("dummy-chat-model.json");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    assertU(adoc("id", "98")); // no string_field
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

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
    loadChatModel("exception-throwing-chat-model.json");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "failingDocumentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "failingDocumentEnrichment");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

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
    loadChatModel("dummy-chat-model.json");
    assertU(adoc("id", "99", "string_field", "Vegeta is the saiyan prince."));
    assertU(adoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."));
    assertU(commit());

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField("enriched", Map.of("set", "true"));
    addWithChain(atomicDoc, "documentEnrichmentForPartialUpdates");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

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
    loadChatModel("dummy-chat-model.json");
    addWithChain(sdoc("id", "99", "string_field", "Vegeta is the saiyan prince."), "documentEnrichment");
    addWithChain(sdoc("id", "98", "string_field", "Kakaroth is a saiyan grown up on planet Earth."), "documentEnrichment");
    assertU(commit());

    SolrInputDocument atomicDoc = new SolrInputDocument();
    atomicDoc.setField("id", "99");
    atomicDoc.setField("string_field", Map.of("set", "Vegeta is the saiyan prince from the Dragon Ball series."));
    addWithChain(atomicDoc, "documentEnrichmentForPartialUpdates");
    assertU(commit());

    final SolrQuery query = getEnrichmentQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/enriched_field=='enriched content'",
        "/response/docs/[1]/id=='98'",
        "/response/docs/[1]/enriched_field=='enriched content'");
  }

  private SolrQuery getEnrichmentQuery() {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "id,enriched_field");
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
