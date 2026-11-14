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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_NAME;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_VALUE;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
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
  }

  static class MockCollectionsHandler extends CollectionsHandler {

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
