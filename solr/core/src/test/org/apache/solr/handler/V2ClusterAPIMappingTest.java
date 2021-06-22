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

import com.google.common.collect.Maps;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ConfigSetParams;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the v2->v1 API mappings found in {@link ClusterAPI}
 */
public class V2ClusterAPIMappingTest {
  private ApiBag apiBag;
  private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  private CollectionsHandler mockCollectionsHandler;
  private ConfigSetsHandler mockConfigSetHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() throws Exception {
    mockCollectionsHandler = mock(CollectionsHandler.class);
    mockConfigSetHandler = mock(ConfigSetsHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(false);
    final ClusterAPI clusterAPI = new ClusterAPI(mockCollectionsHandler, mockConfigSetHandler);
    apiBag.registerObject(clusterAPI);
    apiBag.registerObject(clusterAPI.commands);
    apiBag.registerObject(clusterAPI.configSetCommands);
  }

  @Test
  public void testAsyncCommandStatusAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster/command-status/someId", "GET", null);

    assertEquals(CollectionParams.CollectionAction.REQUESTSTATUS.lowerName, v1Params.get(ACTION));
    assertEquals("someId", v1Params.get(REQUESTID));
  }

  @Test
  public void testClusterAliasesAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster/aliases", "GET", null);

    assertEquals(CollectionParams.CollectionAction.LISTALIASES.lowerName, v1Params.get(ACTION));
  }

  @Test
  public void testClusterOverseerAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster/overseer", "GET", null);

    assertEquals(CollectionParams.CollectionAction.OVERSEERSTATUS.lowerName, v1Params.get(ACTION));
  }

  @Test
  public void testListClusterAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster", "GET", null);

    assertEquals(CollectionParams.CollectionAction.LIST.lowerName, v1Params.get(ACTION));
  }

  @Test
  public void testDeleteCommandStatusAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster/command-status/someId", "DELETE", null);

    assertEquals(CollectionParams.CollectionAction.DELETESTATUS.lowerName, v1Params.get(ACTION));
    assertEquals("someId", v1Params.get(REQUESTID));
  }

  // TODO This should probably really get its own class.
  @Test
  public void testDeleteConfigetAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedConfigsetV1Params("/cluster/configs/someConfigset", "DELETE", null);

    assertEquals(ConfigSetParams.ConfigSetAction.DELETE.toString(), v1Params.get(ACTION));
    assertEquals("someConfigset", v1Params.get(NAME));
  }

  @Test
  public void testListConfigsetsAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedConfigsetV1Params("/cluster/configs", "GET", null);

    assertEquals(ConfigSetParams.ConfigSetAction.LIST.toString(), v1Params.get(ACTION));
  }

  @Test
  public void testCreateConfigsetAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedConfigsetV1Params("/cluster/configs", "POST",
            "{'create': {" +
                    "'name': 'new_configset_name', " +
                    "'baseConfigSet':'some_existing_configset', " +
                    "'properties': {'prop1': 'val1', 'prop2': 'val2'}}}");

    assertEquals(ConfigSetParams.ConfigSetAction.CREATE.toString(), v1Params.get(ACTION));
    assertEquals("new_configset_name", v1Params.get(NAME));
    assertEquals("some_existing_configset", v1Params.get(ConfigSetCmds.BASE_CONFIGSET));
    assertEquals("val1", v1Params.get("configSetProp.prop1"));
    assertEquals("val2", v1Params.get("configSetProp.prop2"));
  }

  @Test
  public void testUploadConfigsetAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedConfigsetV1Params("/cluster/configs/someConfigSetName", "PUT", null);

    assertEquals(ConfigSetParams.ConfigSetAction.UPLOAD.toString(), v1Params.get(ACTION));
    assertEquals("someConfigSetName", v1Params.get(NAME));
    // Why does ClusterAPI set the values below as defaults?  They disagree with the v1 defaults in
    // ConfigSetsHandler.handleConfigUploadRequest
    assertEquals(true, v1Params.getPrimitiveBool(ConfigSetParams.OVERWRITE));
    assertEquals(false, v1Params.getPrimitiveBool(ConfigSetParams.CLEANUP));
  }

  @Test
  public void testAddFileToConfigsetAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedConfigsetV1Params("/cluster/configs/someConfigSetName/some/file/path", "PUT", null);

    assertEquals(ConfigSetParams.ConfigSetAction.UPLOAD.toString(), v1Params.get(ACTION));
    assertEquals("someConfigSetName", v1Params.get(NAME));
    assertEquals("/some/file/path", v1Params.get(ConfigSetParams.FILE_PATH)); // Note the leading '/' that makes the path appear absolute
    assertEquals(true, v1Params.getPrimitiveBool(ConfigSetParams.OVERWRITE));
    assertEquals(false, v1Params.getPrimitiveBool(ConfigSetParams.CLEANUP));
  }

  @Test
  public void testAddRoleAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster", "POST",
            "{'add-role': {" +
                    "'node': 'some_node_name', " +
                    "'role':'some_role'}}");

    assertEquals(CollectionParams.CollectionAction.ADDROLE.toString(), v1Params.get(ACTION));
    assertEquals("some_node_name", v1Params.get("node"));
    assertEquals("some_role", v1Params.get("role"));
  }

  @Test
  public void testRemoveRoleAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster", "POST",
            "{'remove-role': {" +
                    "'node': 'some_node_name', " +
                    "'role':'some_role'}}");

    assertEquals(CollectionParams.CollectionAction.REMOVEROLE.toString(), v1Params.get(ACTION));
    assertEquals("some_node_name", v1Params.get("node"));
    assertEquals("some_role", v1Params.get("role"));
  }

  @Test
  public void testSetPropertyAllParams() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/cluster", "POST",
            "{'set-property': {" +
                    "'name': 'some_prop_name', " +
                    "'val':'some_value'}}");

    assertEquals(CollectionParams.CollectionAction.CLUSTERPROP.toString(), v1Params.get(ACTION));
    assertEquals("some_prop_name", v1Params.get(NAME));
    assertEquals("some_value", v1Params.get("val"));
  }

  private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody) throws Exception {
    return doCaptureParams(path, method, v2RequestBody, mockCollectionsHandler);
  }

  private SolrParams captureConvertedConfigsetV1Params(String path, String method, String v2RequestBody) throws Exception {
    return doCaptureParams(path, method, v2RequestBody, mockConfigSetHandler);
  }

  private SolrParams doCaptureParams(String path, String method, String v2RequestBody, RequestHandlerBase mockHandler) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, Maps.newHashMap()) {
      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        if (v2RequestBody == null) return Collections.emptyList();
        return ApiBag.getCommandOperations(new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
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
