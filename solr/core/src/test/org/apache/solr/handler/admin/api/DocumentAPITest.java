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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.GetDocumentResponse;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.api.model.ListDocumentsResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.DocumentsApi;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Integration test for the {@link Documents} JAX-RS endpoint. */
public class DocumentAPITest extends SolrTestCaseJ4 {

  private static final String COLLECTION = "documentApiTestCollection";

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    System.setProperty(
        ALLOW_PATHS_SYSPROP, configset("cloud-minimal").getParent().toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(COLLECTION).withConfigSet(configset("cloud-minimal")).create();

    SolrClient client = solrTestRule.getSolrClient(COLLECTION);
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.setField("id", "1");
    doc1.setField("name", "test document");
    client.add(doc1);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", "2");
    doc2.setField("name", "second document");
    client.add(doc2);
    client.commit();
  }

  @Test
  public void testGetDocumentById() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(null);

    var request = new DocumentsApi.GetDocument(IndexType.CORE, COLLECTION, "1");
    GetDocumentResponse response = request.process(client);

    assertEquals(0, response.responseHeader.status);
    assertNotNull("Expected document to be returned", response.doc);
    assertEquals("1", response.doc.get("id"));
    assertEquals("test document", response.doc.get("name"));
  }

  @Test
  public void testGetNonExistentDocumentReturns404() {
    SolrClient client = solrTestRule.getSolrClient(null);

    var request = new DocumentsApi.GetDocument(IndexType.CORE, COLLECTION, "nonexistent");
    RemoteSolrException ex = expectThrows(RemoteSolrException.class, () -> request.process(client));
    assertEquals("Expected 404 for non-existent document", 404, ex.code());
  }

  @Test
  public void testListDocumentsByIds() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(null);

    var request =
        new DocumentsApi.ListDocuments(
            IndexType.CORE, COLLECTION, List.of("1", "2", "nonexistent"));
    ListDocumentsResponse response = request.process(client);

    assertEquals(0, response.responseHeader.status);
    assertEquals("Expected only the two existing documents to be found", 2, response.numFound);
    assertNotNull("Expected docs list", response.docs);
    assertEquals(2, response.docs.size());
    List<Object> foundIds = response.docs.stream().map(d -> d.get("id")).toList();
    assertTrue(foundIds.contains("1"));
    assertTrue(foundIds.contains("2"));
  }

  @Test
  public void testListDocumentsAcceptsCommaSeparatedIds() throws Exception {
    // Matches the same "ids=a,b,c" convention RealTimeGetComponent.IdsRequested uses for /get;
    // the generated SolrJ client always sends repeated "ids=" params instead, so this is tested
    // with a raw request to make sure the comma-separated form (the way most users will type it)
    // works too.
    SolrClient client = solrTestRule.getSolrClient(null);

    V2Response response =
        new V2Request.Builder("/cores/" + COLLECTION + "/documents")
            .withMethod(V2Request.METHOD.GET)
            .withParams(new ModifiableSolrParams().set("ids", "1,2,nonexistent"))
            .build()
            .process(client);

    assertEquals(0, response.getStatus());
    Object numFound = Utils.getObjectByPath(response.getResponse().asMap(0), true, "/numFound");
    assertEquals(2, ((Number) numFound).intValue());
  }
}
