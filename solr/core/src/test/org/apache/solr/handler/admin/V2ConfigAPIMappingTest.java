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

import org.apache.solr.handler.SolrConfigHandler;
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
    apiBag.registerObject(new ModifyConfigComponentAPI(getRequestHandler()));
    apiBag.registerObject(new ModifyParamSetAPI(getRequestHandler()));
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
