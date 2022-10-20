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

package org.apache.solr.security.jwt.api;

import java.util.HashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.junit.Before;
import org.junit.Test;

public class V2JWTSecurityApiMappingTest extends SolrTestCaseJ4 {

  private ApiBag apiBag;

  @Before
  public void setupApiBag() {
    apiBag = new ApiBag(false);
  }

  @Test
  public void testJwtConfigApiMapping() {
    apiBag.registerObject(new ModifyJWTAuthPluginConfigAPI());

    // Authc API
    final AnnotatedApi updateAuthcConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authentication");
    assertEquals(1, updateAuthcConfig.getCommands().size());
    assertEquals("set-property", updateAuthcConfig.getCommands().keySet().iterator().next());
  }

  private AnnotatedApi assertAnnotatedApiExistsFor(String method, String path) {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    if (api == null) {
      fail("Expected to find API for path [" + path + "], but no API mapping found.");
    }
    if (!(api instanceof AnnotatedApi)) {
      fail(
          "Expected AnnotatedApi for path ["
              + path
              + "], but found non-annotated API ["
              + api
              + "]");
    }

    return (AnnotatedApi) api;
  }
}
