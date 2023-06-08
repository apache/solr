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

package org.apache.solr.handler.admin;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_NAME;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_VALUE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.fromJSONString;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.Test;

public class TestCollectionAPIs extends SolrTestCaseJ4 {

  @Test
  public void testCopyParamsToMap() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("x", "X1");
    params.add("x", "X2");
    params.add("y", "Y");
    Map<String, Object> m = CollectionsHandler.copy(params, null, "x", "y");
    String[] x = (String[]) m.get("x");
    assertEquals(2, x.length);
    assertEquals("X1", x[0]);
    assertEquals("X2", x[1]);
    assertEquals("Y", m.get("y"));

    SolrException e =
        expectThrows(
            SolrException.class,
            () -> {
              CollectionsHandler.copy(params.required(), null, "z");
            });
    assertEquals(e.code(), SolrException.ErrorCode.BAD_REQUEST.code);
  }

  public void testCommands() throws Exception {
    ApiBag apiBag;
    try (MockCollectionsHandler collectionsHandler = new MockCollectionsHandler()) {
      apiBag = new ApiBag(false);
      for (Api api : collectionsHandler.getApis()) {
        apiBag.register(api);
      }

      ClusterAPI clusterAPI = new ClusterAPI(collectionsHandler, null);
      apiBag.registerObject(clusterAPI);
      apiBag.registerObject(clusterAPI.commands);
    }

    compareOutput(
        apiBag,
        "/collections/collName/shards",
        POST,
        "{split:{shard:shard1, ranges: '0-1f4,1f5-3e8,3e9-5dc', coreProperties : {prop1:prop1Val, prop2:prop2Val} }}",
        "{collection: collName , shard : shard1, ranges :'0-1f4,1f5-3e8,3e9-5dc', operation : splitshard, property.prop1:prop1Val, property.prop2: prop2Val}");

    compareOutput(
        apiBag,
        "/collections/collName/shards",
        POST,
        "{split:{ splitKey:id12345, coreProperties : {prop1:prop1Val, prop2:prop2Val} }}",
        "{collection: collName , split.key : id12345 , operation : splitshard, property.prop1:prop1Val, property.prop2: prop2Val}");

    compareOutput(
        apiBag,
        "/cluster",
        POST,
        "{add-role : {role : overseer, node : 'localhost_8978'} }",
        "{operation : addrole ,role : overseer, node : 'localhost_8978'}");

    compareOutput(
        apiBag,
        "/cluster",
        POST,
        "{remove-role : {role : overseer, node : 'localhost_8978'} }",
        "{operation : removerole ,role : overseer, node : 'localhost_8978'}");

    compareOutput(
        apiBag,
        "/collections/coll1",
        POST,
        "{migrate-docs : {forwardTimeout: 1800, target: coll2, splitKey: 'a123!'} }",
        "{operation : migrate ,collection : coll1, target.collection:coll2, forward.timeout:1800, split.key:'a123!'}");
  }

  static ZkNodeProps compareOutput(
      final ApiBag apiBag,
      final String path,
      final SolrRequest.METHOD method,
      final String payload,
      String expectedOutputMapJson) {
    Pair<SolrQueryRequest, SolrQueryResponse> ctx = makeCall(apiBag, path, method, payload);
    ZkNodeProps output = (ZkNodeProps) ctx.second().getValues().get(ZkNodeProps.class.getName());
    @SuppressWarnings("unchecked")
    Map<String, ?> expected = (Map<String, ?>) fromJSONString(expectedOutputMapJson);
    assertMapEqual(expected, output);
    return output;
  }

  public static Pair<SolrQueryRequest, SolrQueryResponse> makeCall(
      final ApiBag apiBag, String path, final SolrRequest.METHOD method, final String payload) {
    SolrParams queryParams = new MultiMapSolrParams(Collections.emptyMap());
    if (path.indexOf('?') > 0) {
      String queryStr = path.substring(path.indexOf('?') + 1);
      path = path.substring(0, path.indexOf('?'));
      queryParams = SolrRequestParsers.parseQueryString(queryStr);
    }
    final HashMap<String, String> parts = new HashMap<>();
    Api api = apiBag.lookup(path, method.toString(), parts);
    if (api == null) throw new RuntimeException("No handler at path :" + path);
    SolrQueryResponse rsp = new SolrQueryResponse();
    LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, queryParams) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            if (payload == null) return Collections.emptyList();
            return ApiBag.getCommandOperations(
                new ContentStreamBase.StringStream(payload), api.getCommandSchema(), true);
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return method.toString();
          }
        };
    try {
      api.call(req, rsp);
    } catch (ApiBag.ExceptionWithErrObject e) {
      throw new RuntimeException(e.getMessage() + Utils.toJSONString(e.getErrs()), e);
    }
    return new Pair<>(req, rsp);
  }

  private static void assertMapEqual(Map<String, ?> expected, ZkNodeProps actual) {
    assertEquals(errorMessage(expected, actual), expected.size(), actual.getProperties().size());
    for (Map.Entry<String, ?> e : expected.entrySet()) {
      Object actualVal = actual.get(e.getKey());
      if (actualVal instanceof String[]) {
        actualVal = Arrays.asList((String[]) actualVal);
      }
      assertEquals(
          errorMessage(expected, actual), String.valueOf(e.getValue()), String.valueOf(actualVal));
    }
  }

  private static String errorMessage(Map<?, ?> expected, ZkNodeProps actual) {
    return "expected: " + Utils.toJSONString(expected) + "\nactual: " + Utils.toJSONString(actual);
  }

  static class MockCollectionsHandler extends CollectionsHandler {
    LocalSolrQueryRequest req;

    MockCollectionsHandler() {}

    @Override
    protected CoreContainer checkErrors() {
      return null;
    }

    @Override
    void invokeAction(
        SolrQueryRequest req,
        SolrQueryResponse rsp,
        CoreContainer cores,
        CollectionParams.CollectionAction action,
        CollectionOperation operation)
        throws Exception {
      Map<String, Object> result = null;
      if (action == CollectionParams.CollectionAction.COLLECTIONPROP) {
        // Fake this action, since we don't want to write to ZooKeeper in this test
        result = new HashMap<>();
        result.put(NAME, req.getParams().required().get(NAME));
        result.put(PROPERTY_NAME, req.getParams().required().get(PROPERTY_NAME));
        result.put(PROPERTY_VALUE, req.getParams().required().get(PROPERTY_VALUE));
      } else {
        result = operation.execute(req, rsp, this);
      }
      if (result != null) {
        result.put(QUEUE_OPERATION, operation.action.toLower());
        rsp.add(ZkNodeProps.class.getName(), new ZkNodeProps(result));
      }
    }
  }
}
