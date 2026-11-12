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

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.UpdateResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers a bug flagged in review of SOLR-18457 (PR #4177): {@code
 * UpdateRequestHandler.setDefaultWT()} mutates the shared {@link SolrQueryRequest} to inject its
 * own default {@code wt} (e.g. {@code "xml"}, picked by {@code XMLLoader}) purely to select a v1
 * response writer. Since {@link UpdateAPI#handleUpdate} hands that same request object to Jersey's
 * {@code MediaTypeOverridingFilter} afterward, the injected value used to leak out and hijack the
 * v2 response's actual {@code Content-Type} -- overriding whatever the client's real {@code Accept}
 * header asked for, regardless of which v1 loader happened to run internally. In the Admin UI's
 * document-upload screen (Solr XML entry mode), this meant the generated JS client received a
 * response it couldn't recognize as JSON and showed nothing, even though indexing itself had
 * succeeded.
 *
 * <p>This test doesn't exercise the real {@code XMLLoader} (that's covered elsewhere); it fakes the
 * handler to mutate the request's params exactly the way {@code setDefaultWT} does, isolating the
 * fix to {@link UpdateAPI#handleUpdate} restoring the request's original params afterward.
 */
public class UpdateAPIRequestParamLeakTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testDelegateInjectedWtDoesNotSurviveTheRequest() throws Exception {
    final SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), SolrParams.of());
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final UpdateAPI api =
        new UpdateAPI(
            new UpdateAPI.UpdateRequestHandlerConfig(new WtInjectingUpdateRequestHandler()),
            req,
            rsp);

    try {
      assertNull(
          "sanity check: the incoming request shouldn't already have a wt param",
          req.getParams().get(CommonParams.WT));

      final UpdateResponse response = api.update(null, null, null, null, null, null);
      assertNotNull(response);

      assertNull(
          "UpdateRequestHandler.setDefaultWT()'s internal default must not survive past "
              + "handleUpdate() -- it would hijack MediaTypeOverridingFilter's response "
              + "Content-Type decision for a request that never asked for 'wt' at all",
          req.getParams().get(CommonParams.WT));
    } finally {
      req.close();
    }
  }

  /**
   * Mutates the request's params exactly the way {@code UpdateRequestHandler.setDefaultWT} does.
   */
  private static class WtInjectingUpdateRequestHandler extends UpdateRequestHandler {
    @Override
    public boolean handleRequestWithoutMetrics(SolrQueryRequest req, SolrQueryResponse rsp) {
      final SolrParams params = req.getParams();
      req.setParams(
          SolrParams.wrapDefaults(params, new MapSolrParams(Map.of(CommonParams.WT, "xml"))));
      return true;
    }
  }
}
