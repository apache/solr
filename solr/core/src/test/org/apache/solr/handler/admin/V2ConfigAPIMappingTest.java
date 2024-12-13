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
import org.apache.solr.handler.admin.api.ModifyConfigComponentAPI;
import org.apache.solr.handler.admin.api.ModifyParamSetAPI;
import org.junit.Test;

/** Unit tests for the GET and POST /v2/c/collectionName/config APIs. */
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
    apiBag.registerObject(new ModifyConfigComponentAPI(getRequestHandler()));
    apiBag.registerObject(new ModifyParamSetAPI(getRequestHandler()));
  }

  // GET /v2/c/collectionName/config is a pure pass-through to the underlying request handler
  @Test
  public void testGetAllConfig() {
    assertAnnotatedApiExistsFor("GET", "/config");
  }

  // GET /v2/collectionName/config/<component> is a pure pass-through to the underlying request
  // handler.  Just check
  // the API lookup works for a handful of the valid config "components".
  @Test
  public void testGetSingleComponentConfig() {
    assertAnnotatedApiExistsFor("GET", "/config/overlay");
    assertAnnotatedApiExistsFor("GET", "/config/query");
    assertAnnotatedApiExistsFor("GET", "/config/jmx");
    assertAnnotatedApiExistsFor("GET", "/config/requestDispatcher");
    assertAnnotatedApiExistsFor("GET", "/config/znodeVersion");
    assertAnnotatedApiExistsFor("GET", "/config/luceneMatchVersion");
  }

  @Test
  public void testGetParamsetsConfig() {
    assertAnnotatedApiExistsFor("GET", "/config/params");
    final AnnotatedApi getParamSetsApi = getAnnotatedApiFor("GET", "/config/params");
    // Ensure that the /config/params path redirects to the /config/params specific endpoint (and
    // not the generic "/config/{component}")
    final String[] getParamSetsApiPaths = getParamSetsApi.getEndPoint().path();
    assertEquals(1, getParamSetsApiPaths.length);
    assertEquals("/config/params", getParamSetsApiPaths[0]);

    assertAnnotatedApiExistsFor("GET", "/config/params/someParamSet");
    final AnnotatedApi getSingleParamSetApi =
        getAnnotatedApiFor("GET", "/config/params/someParamSet");
    final String[] getSingleParamsetApiPaths = getSingleParamSetApi.getEndPoint().path();
    assertEquals(1, getSingleParamsetApiPaths.length);
    assertEquals("/config/params/{paramset}", getSingleParamsetApiPaths[0]);
  }

  @Test
  public void testModifyConfigComponentApis() {
    assertAnnotatedApiExistsFor("POST", "config");
  }

  @Test
  public void testModifyParamsetApis() {
    assertAnnotatedApiExistsFor("POST", "/config/params");
  }
}
