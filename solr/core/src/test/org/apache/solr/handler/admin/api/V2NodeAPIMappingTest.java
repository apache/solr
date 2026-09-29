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

import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for the v2 to v1 mapping for /node/ APIs. */
public class V2NodeAPIMappingTest extends SolrTestCaseJ4 {
  private ApiBag apiBag;
  private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  private CoreAdminHandler mockCoresHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() {
    mockCoresHandler = mock(CoreAdminHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(false);
    registerAllNodeApis(apiBag, mockCoresHandler);
  }

  @Test
  public void testOverseerOpApiAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedCoreV1Params(
            "/node",
            "POST",
            "{"
                + "\"overseer-op\": {"
                + "\"op\": \"asdf\", "
                + "\"electionNode\": \"someNodeName\""
                + "}}");

    assertEquals("overseerop", v1Params.get(ACTION));
    assertEquals("asdf", v1Params.get("op"));
    assertEquals("someNodeName", v1Params.get("electionNode"));
  }

  @Test
  public void testRejoinLeaderElectionApiAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedCoreV1Params(
            "/node",
            "POST",
            "{"
                + "\"rejoin-leader-election\": {"
                + "\"collection\": \"someCollection\", "
                + "\"coreNodeName\": \"someNodeName\","
                + "\"core\": \"someCore\","
                + "\"rejoinAtHead\": true"
                + "}}");

    assertEquals("rejoinleaderelection", v1Params.get(ACTION));
    assertEquals("someCollection", v1Params.get("collection"));
    assertEquals("someNodeName", v1Params.get("core_node_name"));
    assertEquals("someCore", v1Params.get("core"));
    assertEquals("true", v1Params.get("rejoinAtHead"));
  }

  private SolrParams captureConvertedCoreV1Params(String path, String method, String v2RequestBody)
      throws Exception {
    return doCaptureParams(
        path, method, new ModifiableSolrParams(), v2RequestBody, mockCoresHandler);
  }

  private SolrParams doCaptureParams(
      String path,
      String method,
      SolrParams inputParams,
      String v2RequestBody,
      RequestHandlerBase mockHandler)
      throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    inputParams.stream()
        .forEach(
            e -> {
              solrParams.add(e.getKey(), e.getValue());
            });
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final SolrQueryRequestBase req =
        new SolrQueryRequestBase(null, solrParams) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            if (v2RequestBody == null) return List.of();
            return ApiBag.getCommandOperations(
                new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
          }
        };

    api.call(req, rsp);
    verify(mockHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }

  private static void registerAllNodeApis(ApiBag apiBag, CoreAdminHandler coreHandler) {
    apiBag.registerObject(new OverseerOperationAPI(coreHandler));
    apiBag.registerObject(new RejoinLeaderElectionAPI(coreHandler));
  }
}
