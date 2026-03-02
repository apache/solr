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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for the v2 update API endpoints implemented via JAX-RS in {@link
 * org.apache.solr.handler.admin.api.UpdateAPI}.
 */
public class UpdateAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private static final String CORE_NAME = DEFAULT_TEST_COLLECTION_NAME;

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

    // POST a JSON array of documents via the V2 /update endpoint (rewrites to /update/json/docs)
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "[{\"id\":\"v2update1\",\"title\":\"V2 update test\"}]", "application/json"));
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
}
