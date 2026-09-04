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
import static org.hamcrest.Matchers.containsString;

import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test cases for the v2 {@link RenameCore}
 *
 * <p>The missing-body error case uses {@link GenericV2SolrRequest} with an explicit {@code "null"}
 * JSON body, because {@link org.apache.solr.client.solrj.request.CoresApi.RenameCore} always
 * serializes a request body and therefore cannot represent a truly absent body. The standard
 * response parser used by {@link GenericV2SolrRequest} does propagate 4xx responses as {@link
 * RemoteSolrException}s.
 */
public class RenameCoreAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr();
    solrTestRule.newCollection().withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testRenameCoreToSameNameSucceeds() throws Exception {
    CoresApi.RenameCore renameRequest = new CoresApi.RenameCore(DEFAULT_TEST_CORENAME);
    renameRequest.setTo(DEFAULT_TEST_CORENAME);
    SolrJerseyResponse response = renameRequest.process(solrTestRule.getAdminClient());
    assertEquals(0, response.responseHeader.status);
  }

  @Test
  public void testMissingRequestBodyThrowsError() {
    // Sending JSON "null" as the body causes Jersey to deserialize it to a null requestBody,
    // triggering the handler's "Required request-body is missing" guard.
    GenericV2SolrRequest renameRequest =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST,
            "/cores/" + DEFAULT_TEST_CORENAME + "/rename",
            SolrRequest.SolrRequestType.ADMIN);
    renameRequest.withContent("null".getBytes(StandardCharsets.UTF_8), "application/json");
    final RemoteSolrException solrException =
        expectThrows(
            RemoteSolrException.class, () -> renameRequest.process(solrTestRule.getAdminClient()));
    assertEquals(400, solrException.code());
    assertTrue(solrException.getMessage().contains("Required request-body is missing"));
  }

  @Test
  public void testMissingToParameterThrowsError() throws Exception {
    CoresApi.RenameCore renameRequest = new CoresApi.RenameCore(DEFAULT_TEST_CORENAME);
    final var ex =
        expectThrows(
            RemoteSolrException.class,
            () -> {
              renameRequest.process(solrTestRule.getAdminClient());
            });
    assertEquals(400, ex.code());
    assertThat(ex.getMessage(), containsString("Missing required parameter: to"));
  }
}
