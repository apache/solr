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

import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.handler.admin.api.GetConfigAPI;
import org.junit.Test;

/** Unit tests for the GET /v2/c/collectionName/config APIs. */
public class V2ConfigAPIMappingTest extends V2ApiMappingTest<SolrConfigHandler> {

  @Override
  public SolrConfigHandler createUnderlyingRequestHandler() {
    return createMock(SolrConfigHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return true;
  }

  @Override
  public void populateApiBag() {
    apiBag.registerObject(new GetConfigAPI(getRequestHandler()));
  }

  // GET /v2/c/collectionName/config is a pure pass-through to the underlying request handler
  @Test
  public void testGetAllConfig() throws Exception {
    assertAnnotatedApiExistsForGET("/config");
  }

  // GET /v2/collectionName/config/<component> is a pure pass-through to the underlying request
  // handler.  Just check
  // the API lookup works for a handful of the valid config "components".
  @Test
  public void testGetSingleComponentConfig() throws Exception {
    assertAnnotatedApiExistsForGET("/config/overlay");
    assertAnnotatedApiExistsForGET("/config/query");
    assertAnnotatedApiExistsForGET("/config/jmx");
    assertAnnotatedApiExistsForGET("/config/requestDispatcher");
    assertAnnotatedApiExistsForGET("/config/znodeVersion");
    assertAnnotatedApiExistsForGET("/config/luceneMatchVersion");
  }

  @Test
  public void testGetParamsetsConfig() throws Exception {
    assertAnnotatedApiExistsForGET("/config/params");
    final AnnotatedApi getParamSetsApi = getAnnotatedApiForGET("/config/params");
    // Ensure that the /config/params path redirects to the /config/params specific endpoint (and not the generic "/config/{component}")
    final String[] getParamSetsApiPaths = getParamSetsApi.getEndPoint().path();
    assertEquals(1, getParamSetsApiPaths.length);
    assertEquals("/config/params", getParamSetsApiPaths[0]);

    assertAnnotatedApiExistsForGET("/config/params/someParamSet");
    final AnnotatedApi getSingleParamSetApi = getAnnotatedApiForGET("/config/params/someParamSet");
    final String[] getSingleParamsetApiPaths = getSingleParamSetApi.getEndPoint().path();
    assertEquals(1, getSingleParamsetApiPaths.length);
    assertEquals("/config/params/{paramset}", getSingleParamsetApiPaths[0]);
  }
}
