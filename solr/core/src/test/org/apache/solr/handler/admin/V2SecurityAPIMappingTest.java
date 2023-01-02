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

import java.util.HashMap;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.handler.admin.api.GetAuthenticationConfigAPI;
import org.apache.solr.handler.admin.api.GetAuthorizationConfigAPI;
import org.apache.solr.handler.admin.api.ModifyBasicAuthConfigAPI;
import org.apache.solr.handler.admin.api.ModifyMultiPluginAuthConfigAPI;
import org.apache.solr.handler.admin.api.ModifyNoAuthPluginSecurityConfigAPI;
import org.apache.solr.handler.admin.api.ModifyNoAuthzPluginSecurityConfigAPI;
import org.apache.solr.handler.admin.api.ModifyRuleBasedAuthConfigAPI;
import org.junit.Before;
import org.junit.Test;

public class V2SecurityAPIMappingTest extends SolrTestCaseJ4 {

  private ApiBag apiBag;
  private SecurityConfHandler mockHandler;

  @Before
  public void setupApiBag() {
    apiBag = new ApiBag(false);
    mockHandler = null;
  }

  // Test the GET /cluster/security/[authentication|authorization] APIs registered regardless of
  // security plugin
  @Test
  public void testMappingForUniversalGetApis() {
    apiBag.registerObject(new GetAuthenticationConfigAPI(mockHandler));
    apiBag.registerObject(new GetAuthorizationConfigAPI(mockHandler));

    assertAnnotatedApiExistsFor("GET", "/cluster/security/authentication");
    assertAnnotatedApiExistsFor("GET", "/cluster/security/authorization");
  }

  // Test the POST /cluster/security/[authentication|authorization] APIs registered when no plugins
  // are configured.
  @Test
  public void testDefaultPostAPIs() {
    apiBag.registerObject(new ModifyNoAuthPluginSecurityConfigAPI(mockHandler));
    apiBag.registerObject(new ModifyNoAuthzPluginSecurityConfigAPI(mockHandler));

    // Authc API
    final AnnotatedApi updateAuthcConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authentication");
    assertEquals(1, updateAuthcConfig.getCommands().size());
    // empty-string is the placeholder for POST APIs without an explicit command.
    assertEquals("", updateAuthcConfig.getCommands().keySet().iterator().next());

    // Authz API
    final AnnotatedApi updateAuthzConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authorization");
    assertEquals(1, updateAuthzConfig.getCommands().size());
    // empty-string is the placeholder for POST APIs without an explicit command.
    assertEquals("", updateAuthzConfig.getCommands().keySet().iterator().next());
  }

  @Test
  public void testBasicAuthApiMapping() {
    apiBag.registerObject(new ModifyBasicAuthConfigAPI());

    final AnnotatedApi updateAuthcConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authentication");
    assertEquals(2, updateAuthcConfig.getCommands().size());
    final Set<String> commandNames = updateAuthcConfig.getCommands().keySet();
    assertTrue(
        "Expected 'set-user' to be in the command list: " + commandNames,
        commandNames.contains("set-user"));
    assertTrue(
        "Expected 'delete-user' to be in the command list: " + commandNames,
        commandNames.contains("delete-user"));
  }

  @Test
  public void testMultiAuthApiMapping() {
    apiBag.registerObject(new ModifyMultiPluginAuthConfigAPI());

    final AnnotatedApi updateAuthcConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authentication");
    assertEquals(3, updateAuthcConfig.getCommands().size());
    final Set<String> commandNames = updateAuthcConfig.getCommands().keySet();
    assertTrue(
        "Expected 'set-user' to be in the command list: " + commandNames,
        commandNames.contains("set-user"));
    assertTrue(
        "Expected 'delete-user' to be in the command list: " + commandNames,
        commandNames.contains("delete-user"));
    assertTrue(
        "Expected 'set-property' to be in the command list: " + commandNames,
        commandNames.contains("set-property"));
  }

  @Test
  public void testRuleBasedAuthzApiMapping() {
    apiBag.registerObject(new ModifyRuleBasedAuthConfigAPI());

    final AnnotatedApi updateAuthzConfig =
        assertAnnotatedApiExistsFor("POST", "/cluster/security/authorization");
    assertEquals(4, updateAuthzConfig.getCommands().size());
    final Set<String> commandNames = updateAuthzConfig.getCommands().keySet();
    assertTrue(
        "Expected 'set-permission' to be in the command list: " + commandNames,
        commandNames.contains("set-permission"));
    assertTrue(
        "Expected 'update-permission' to be in the command list: " + commandNames,
        commandNames.contains("update-permission"));
    assertTrue(
        "Expected 'delete-permission' to be in the command list: " + commandNames,
        commandNames.contains("delete-permission"));
    assertTrue(
        "Expected 'set-user-role' to be in the command list: " + commandNames,
        commandNames.contains("set-user-role"));
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
