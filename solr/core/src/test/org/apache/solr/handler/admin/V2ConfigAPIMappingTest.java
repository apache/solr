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

import static org.mockito.Mockito.mock;

import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.handler.admin.api.GetConfigAPI;
import org.junit.Test;

/** Unit tests for the GET /v2/c/collectionName/config APIs. */
public class V2ConfigAPIMappingTest extends V2ApiMappingTest<SolrConfigHandler> {

  @Override
  public SolrConfigHandler createUnderlyingRequestHandler() {
    return mock(SolrConfigHandler.class);
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
    assertMappingExistsForGET("/config");
  }

  // GET /v2/collectionName/config/<component> is a pure pass-through to the underlying request
  // handler.  Just check
  // the API lookup works for a handful of the valid config "components".
  @Test
  public void testGetSingleComponentConfig() throws Exception {
    assertMappingExistsForGET("/config/overlay");
    assertMappingExistsForGET("/config/query");
    assertMappingExistsForGET("/config/jmx");
    assertMappingExistsForGET("/config/requestDispatcher");
    assertMappingExistsForGET("/config/znodeVersion");
    assertMappingExistsForGET("/config/luceneMatchVersion");
  }
}
