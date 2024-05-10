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

package org.apache.solr.handler;

import static org.apache.solr.common.params.CommonParams.PATH;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.handler.admin.api.UpdateAPI;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for the v2 to v1 mapping logic in {@link UpdateAPI} */
public class V2UpdateAPIMappingTest extends SolrTestCaseJ4 {
  private ApiBag apiBag;
  private UpdateRequestHandler mockUpdateHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() {
    mockUpdateHandler = mock(UpdateRequestHandler.class);
    apiBag = new ApiBag(false);
    final UpdateAPI updateAPI = new UpdateAPI(mockUpdateHandler);

    apiBag.registerObject(updateAPI);
  }

  @Test
  public void testUpdateApiRewriting() {

    // Assume JSON in path if no specific format specified
    {
      final SolrQueryRequest req = runUpdateApi("/update");
      assertEquals("/update/json/docs", req.getContext().get(PATH));
    }

    // Rewrite v2 /update/json to v1's /update/json/docs
    {
      final SolrQueryRequest req = runUpdateApi("/update/json");
      assertEquals("/update/json/docs", req.getContext().get(PATH));
    }

    // No rewriting for /update/xml, /update/csv, or /update/bin
    {
      final SolrQueryRequest req = runUpdateApi("/update/xml");
      assertEquals("/update/xml", req.getContext().get(PATH));
    }
    {
      final SolrQueryRequest req = runUpdateApi("/update/csv");
      assertEquals("/update/csv", req.getContext().get(PATH));
    }
    {
      final SolrQueryRequest req = runUpdateApi("/update/bin");
      assertEquals("/update/bin", req.getContext().get(PATH));
    }
  }

  private SolrQueryRequest runUpdateApi(String path) {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, "POST", parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, Map.of()) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            return Collections.emptyList();
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return "POST";
          }
        };
    req.getContext().put(PATH, path);

    api.call(req, rsp);

    return req;
  }
}
