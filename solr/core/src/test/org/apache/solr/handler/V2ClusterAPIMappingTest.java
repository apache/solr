/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler;

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for the v2 to v1 API mappings found in {@link ClusterAPI} */
public class V2ClusterAPIMappingTest extends SolrTestCaseJ4 {
  private ApiBag apiBag;
  private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  private CollectionsHandler mockCollectionsHandler;
  private ConfigSetsHandler mockConfigSetHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() {
    mockCollectionsHandler = mock(CollectionsHandler.class);
    mockConfigSetHandler = mock(ConfigSetsHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(false);
    final ClusterAPI clusterAPI = new ClusterAPI(mockCollectionsHandler, mockConfigSetHandler);
    apiBag.registerObject(clusterAPI);
    apiBag.registerObject(clusterAPI.commands);
  }

  @Test
  public void testAsyncCommandStatusAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cluster/command-status/someId", "GET", null);

    assertEquals(CollectionParams.CollectionAction.REQUESTSTATUS.lowerName, v1Params.get(ACTION));
    assertEquals("someId", v1Params.get(REQUESTID));
  }

  @Test
  public void testClusterOverseerAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster/overseer", "GET", null);

    assertEquals(CollectionParams.CollectionAction.OVERSEERSTATUS.lowerName, v1Params.get(ACTION));
  }

  @Test
  public void testClusterStatusAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster", "GET", null);

    assertEquals(CollectionAction.CLUSTERSTATUS.lowerName, v1Params.get(ACTION));
  }

  @Test
  public void testDeleteCommandStatusAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params("/cluster/command-status/someId", "DELETE", null);

    assertEquals(CollectionParams.CollectionAction.DELETESTATUS.lowerName, v1Params.get(ACTION));
    assertEquals("someId", v1Params.get(REQUESTID));
  }

  @Test
  public void testAddRoleAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cluster",
            "POST",
            "{'add-role': {" + "'node': 'some_node_name', " + "'role':'some_role'}}");

    assertEquals(CollectionParams.CollectionAction.ADDROLE.toString(), v1Params.get(ACTION));
    assertEquals("some_node_name", v1Params.get("node"));
    assertEquals("some_role", v1Params.get("role"));
  }

  @Test
  public void testRemoveRoleAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cluster",
            "POST",
            "{'remove-role': {" + "'node': 'some_node_name', " + "'role':'some_role'}}");

    assertEquals(CollectionParams.CollectionAction.REMOVEROLE.toString(), v1Params.get(ACTION));
    assertEquals("some_node_name", v1Params.get("node"));
    assertEquals("some_role", v1Params.get("role"));
  }

  @Test
  public void testSetPropertyAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/cluster",
            "POST",
            "{'set-property': {" + "'name': 'some_prop_name', " + "'val':'some_value'}}");

    assertEquals(CollectionParams.CollectionAction.CLUSTERPROP.toString(), v1Params.get(ACTION));
    assertEquals("some_prop_name", v1Params.get(NAME));
    assertEquals("some_value", v1Params.get("val"));
  }

  private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody)
      throws Exception {
    return doCaptureParams(path, method, v2RequestBody, mockCollectionsHandler);
  }

  private SolrParams captureConvertedConfigsetV1Params(
      String path, String method, String v2RequestBody) throws Exception {
    return doCaptureParams(path, method, v2RequestBody, mockConfigSetHandler);
  }

  private SolrParams doCaptureParams(
      String path, String method, String v2RequestBody, RequestHandlerBase mockHandler)
      throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, Map.of()) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            if (v2RequestBody == null) return Collections.emptyList();
            return ApiBag.getCommandOperations(
                new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return method;
          }
        };

    api.call(req, rsp);
    verify(mockHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }
}
