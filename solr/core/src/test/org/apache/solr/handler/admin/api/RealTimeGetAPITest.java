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
import org.apache.solr.client.api.model.GetDocumentsResponse;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.DocumentsApi;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Integration test for the {@link RealTimeGetAPI} JAX-RS endpoint. */
public class RealTimeGetAPITest extends SolrTestCaseJ4 {

  private static final String COLLECTION = "rtgTestCollection";

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    System.setProperty(
        ALLOW_PATHS_SYSPROP, configset("cloud-minimal").getParent().toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(COLLECTION)
        .withConfigSet(configset("cloud-minimal").toString())
        .create();

    SolrClient client = solrTestRule.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", "test document");
    client.add(doc);
    client.commit();
  }

  @Test
  public void testGetDocumentById() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(null);

    V2Response response =
        new V2Request.Builder("/cores/" + COLLECTION + "/get")
            .withMethod(V2Request.METHOD.GET)
            .withParams(params("id", "1"))
            .build()
            .process(client);

    assertEquals(0, response.getStatus());
    assertNotNull("Expected document to be returned", response.getResponse().get("doc"));
  }

  @Test
  public void testGetDocumentsByIds() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(null);

    var request = new DocumentsApi.GetDocuments(IndexType.CORE, COLLECTION);
    request.setIds(List.of("1"));
    GetDocumentsResponse response = request.process(client);

    assertEquals(0, response.responseHeader.status);
    assertNotNull("Expected response field", response.response);
    assertEquals("Expected exactly one document", 1, response.response.numFound);
    assertNotNull("Expected docs list", response.response.docs);
    assertFalse("Expected non-empty docs list", response.response.docs.isEmpty());
    Object idValue = response.response.docs.get(0).get("id");
    assertEquals("Expected document with id '1'", "1", idValue);
  }

  @Test
  public void testGetNonExistentDocument() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(null);

    V2Response response =
        new V2Request.Builder("/cores/" + COLLECTION + "/get")
            .withMethod(V2Request.METHOD.GET)
            .withParams(params("id", "nonexistent"))
            .build()
            .process(client);

    assertEquals(0, response.getStatus());
    assertNull("Expected null for non-existent document", response.getResponse().get("doc"));
  }
}
