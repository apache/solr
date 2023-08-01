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
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.admin.LoggingHandler;
import org.apache.solr.handler.admin.PropertiesRequestHandler;
import org.apache.solr.handler.admin.SystemInfoHandler;
import org.apache.solr.handler.admin.ThreadDumpHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
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
  private InfoHandler infoHandler;
  private SystemInfoHandler mockSystemInfoHandler;
  private LoggingHandler mockLoggingHandler;
  private PropertiesRequestHandler mockPropertiesHandler;
  private HealthCheckHandler mockHealthCheckHandler;
  private ThreadDumpHandler mockThreadDumpHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() {
    mockCoresHandler = mock(CoreAdminHandler.class);
    infoHandler = mock(InfoHandler.class);
    mockSystemInfoHandler = mock(SystemInfoHandler.class);
    mockLoggingHandler = mock(LoggingHandler.class);
    mockPropertiesHandler = mock(PropertiesRequestHandler.class);
    mockHealthCheckHandler = mock(HealthCheckHandler.class);
    mockThreadDumpHandler = mock(ThreadDumpHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    when(infoHandler.getSystemInfoHandler()).thenReturn(mockSystemInfoHandler);
    when(infoHandler.getLoggingHandler()).thenReturn(mockLoggingHandler);
    when(infoHandler.getPropertiesHandler()).thenReturn(mockPropertiesHandler);
    when(infoHandler.getHealthCheckHandler()).thenReturn(mockHealthCheckHandler);
    when(infoHandler.getThreadDumpHandler()).thenReturn(mockThreadDumpHandler);

    apiBag = new ApiBag(false);
    registerAllNodeApis(apiBag, mockCoresHandler, infoHandler);
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
                + "\"shard\": \"someShard\","
                + "\"coreNodeName\": \"someNodeName\","
                + "\"core\": \"someCore\","
                + "\"electionNode\": \"someElectionNode\","
                + "\"rejoinAtHead\": true"
                + "}}");

    assertEquals("rejoinleaderelection", v1Params.get(ACTION));
    assertEquals("someCollection", v1Params.get("collection"));
    assertEquals("someShard", v1Params.get("shard"));
    assertEquals("someNodeName", v1Params.get("core_node_name"));
    assertEquals("someCore", v1Params.get("core"));
    assertEquals("someElectionNode", v1Params.get("election_node"));
    assertEquals("true", v1Params.get("rejoinAtHead"));
  }

  @Test
  public void testSystemPropsApiAllProperties() throws Exception {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add("name", "specificPropertyName");
    final SolrParams v1Params =
        captureConvertedPropertiesV1Params("/node/properties", "GET", solrParams);

    assertEquals("specificPropertyName", v1Params.get("name"));
  }

  @Test
  public void testThreadDumpApiAllProperties() throws Exception {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add("anyParamName", "anyParamValue");
    final SolrParams v1Params =
        captureConvertedThreadDumpV1Params("/node/threads", "GET", solrParams);

    // All parameters are passed through to v1 API as-is
    assertEquals("anyParamValue", v1Params.get("anyParamName"));
  }

  @Test
  public void testSystemInfoApiAllProperties() throws Exception {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add("anyParamName", "anyParamValue");
    final SolrParams v1Params = captureConvertedSystemV1Params("/node/system", "GET", solrParams);

    // All parameters are passed through to v1 API as-is.
    assertEquals("anyParamValue", v1Params.get("anyParamName"));
  }

  @Test
  public void testHealthCheckApiAllProperties() throws Exception {
    final ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add("requireHealthyCores", "true");
    solrParams.add("maxGenerationLag", "123");
    final SolrParams v1Params =
        captureConvertedHealthCheckV1Params("/node/health", "GET", solrParams);

    // All parameters are passed through to v1 API as-is.
    assertEquals(true, v1Params.getBool("requireHealthyCores"));
    assertEquals(123, v1Params.getPrimitiveInt("maxGenerationLag"));
  }

  private SolrParams captureConvertedCoreV1Params(String path, String method, String v2RequestBody)
      throws Exception {
    return doCaptureParams(
        path, method, new ModifiableSolrParams(), v2RequestBody, mockCoresHandler);
  }

  private SolrParams captureConvertedSystemV1Params(
      String path, String method, SolrParams inputParams) throws Exception {
    return doCaptureParams(path, method, inputParams, null, mockSystemInfoHandler);
  }

  private SolrParams captureConvertedPropertiesV1Params(
      String path, String method, SolrParams inputParams) throws Exception {
    return doCaptureParams(path, method, inputParams, null, mockPropertiesHandler);
  }

  private SolrParams captureConvertedHealthCheckV1Params(
      String path, String method, SolrParams inputParams) throws Exception {
    return doCaptureParams(path, method, inputParams, null, mockHealthCheckHandler);
  }

  private SolrParams captureConvertedThreadDumpV1Params(
      String path, String method, SolrParams inputParams) throws Exception {
    return doCaptureParams(path, method, inputParams, null, mockThreadDumpHandler);
  }

  private SolrParams doCaptureParams(
      String path,
      String method,
      SolrParams inputParams,
      String v2RequestBody,
      RequestHandlerBase mockHandler)
      throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Map<String, String[]> inputParamsMap = new HashMap<>();
    inputParams.stream()
        .forEach(
            e -> {
              inputParamsMap.put(e.getKey(), e.getValue());
            });
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, inputParamsMap) {
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

  private static void registerAllNodeApis(
      ApiBag apiBag, CoreAdminHandler coreHandler, InfoHandler infoHandler) {
    apiBag.registerObject(new OverseerOperationAPI(coreHandler));
    apiBag.registerObject(new RejoinLeaderElectionAPI(coreHandler));
    apiBag.registerObject(new NodePropertiesAPI(infoHandler.getPropertiesHandler()));
    apiBag.registerObject(new NodeThreadsAPI(infoHandler.getThreadDumpHandler()));
    apiBag.registerObject(new NodeSystemInfoAPI(infoHandler.getSystemInfoHandler()));
    apiBag.registerObject(new NodeHealthAPI(infoHandler.getHealthCheckHandler()));
  }
}
