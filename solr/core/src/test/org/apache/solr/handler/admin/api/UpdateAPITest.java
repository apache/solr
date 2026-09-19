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

import java.io.ByteArrayOutputStream;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for the v2 update API endpoints implemented via JAX-RS in {@link
 * org.apache.solr.handler.admin.api.UpdateAPI}.
 */
public class UpdateAPITest extends SolrTestCase {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private static final String CORE_NAME = "update-api-test";

  @BeforeClass
  public static void beforeClass() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(CORE_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();
  }

  @Test
  public void testUpdateViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    // The generic /update endpoint selects the update-command loader from Content-Type.
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "{\"add\":{\"doc\":{\"id\":\"v2update1\",\"title\":\"V2 update test\"}}}",
            "application/json"));
    client.request(addReq);

    // Commit via standard SolrJ commit (v2 /update is docs-only and does not support commands)
    client.commit(CORE_NAME);

    // Verify the document was indexed
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2update1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testGenericUpdateSelectsXmlFromContentType() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "<add><doc><field name=\"id\">v2genericxml1</field></doc></add>", "application/xml"));
    client.request(addReq);
    client.commit(CORE_NAME);

    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2genericxml1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testGenericUpdateSelectsJavabinFromContentType() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "v2genericjavabin1");
    final UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(doc);
    final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    new JavaBinUpdateRequestCodec().marshal(updateRequest, payload);

    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update");
    addReq.withContent(payload.toByteArray(), "application/javabin");
    client.request(addReq);
    client.commit(CORE_NAME);

    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2genericjavabin1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateJsonViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    // POST via the V2 /update/json endpoint (also rewrites to /update/json/docs)
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/json");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "[{\"id\":\"v2updatejson1\",\"title\":\"V2 update/json test\"}]", "application/json"));
    client.request(addReq);

    // Commit via standard SolrJ commit (v2 /update is docs-only and does not support commands)
    client.commit(CORE_NAME);

    // Verify
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatejson1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateXmlViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    // POST via the V2 /update/xml endpoint
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/xml");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "<add><doc><field name=\"id\">v2updatexml1</field>"
                + "<field name=\"title\">V2 update/xml test</field></doc></add>",
            "application/xml"));
    client.request(addReq);

    // Commit via standard SolrJ commit (v2 /update is docs-only and does not support commands)
    client.commit(CORE_NAME);

    // Verify
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatexml1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateJavabinViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "v2updatejavabin1");
    final UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(doc);
    updateRequest.deleteById("v2deleteversion1");
    updateRequest.deleteByQuery("id:v2deletequery1");
    final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    new JavaBinUpdateRequestCodec().marshal(updateRequest, payload);

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/javabin", params);
    addReq.setResponseParser(new JavaBinResponseParser());
    addReq.withContent(payload.toByteArray(), "application/javabin");
    final NamedList<Object> updateResponse = client.request(addReq);
    assertEquals(1, updateResponse.getAll("responseHeader").size());
    final List<?> adds = (List<?>) updateResponse.get("adds");
    assertEquals("v2updatejavabin1", adds.get(0));
    assertTrue(((Number) adds.get(1)).longValue() > 0);
    final List<?> deletes = (List<?>) updateResponse.get("deletes");
    assertEquals("v2deleteversion1", deletes.get(0));
    assertTrue(((Number) deletes.get(1)).longValue() < 0);
    final List<?> deleteByQuery = (List<?>) updateResponse.get("deleteByQuery");
    assertEquals("id:v2deletequery1", deleteByQuery.get(0));
    assertTrue(((Number) deleteByQuery.get(1)).longValue() < 0);
    client.commit(CORE_NAME);

    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatejavabin1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateReturnsAssignedVersion() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update", params);
    addReq.setResponseParser(new JsonMapResponseParser());
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "[{\"id\":\"v2version1\"}]", "application/json"));

    final var response = client.request(addReq);
    final List<?> adds = (List<?>) response.get("adds");
    assertEquals("v2version1", adds.get(0));
    assertTrue(((Number) adds.get(1)).longValue() > 0);
  }

  @Test
  public void testXmlUpdateResponseHasOneHeaderAndVersion() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/xml", params);
    addReq.setResponseParser(new XMLResponseParser());
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "<add><doc><field name=\"id\">v2xmlversion1</field></doc></add>", "application/xml"));

    final NamedList<Object> response = client.request(addReq);
    assertEquals(1, response.getAll("responseHeader").size());
    final List<?> adds = (List<?>) response.get("adds");
    assertEquals("v2xmlversion1", adds.get(0));
    assertTrue(((Number) adds.get(1)).longValue() > 0);
  }
}
