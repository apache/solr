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
package org.apache.solr.handler.extraction;

import java.io.IOException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.security.SimplePrincipal;
import org.junit.Test;

/**
 * Permission tests for {@link ExtractingRequestHandler}. It extends {@code
 * ContentStreamHandlerBase} and indexes documents through {@code ExtractingDocumentLoader}, so it
 * declares {@code UPDATE_PERM}.
 */
public class ExtractingRequestHandlerPermissionTest extends SolrTestCase {

  /**
   * Fast fail-early sentinel: the handler must declare {@code UPDATE_PERM}. The functional
   * authorize() tests below also cover this, but this unit-level assertion pinpoints the cause if
   * {@link ExtractingRequestHandler#getPermissionName} changes.
   */
  @Test
  public void testPermissionNameIsUpdatePerm() {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        PermissionNameProvider.Name.UPDATE_PERM, handler.getPermissionName(fakeContext("reader")));
  }

  /** A read-only user cannot write through {@code /update/extract}. */
  @Test
  public void testReadOnlyUserDeniedOnUpdateExtract() throws IOException {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        AuthorizationResponse.FORBIDDEN.statusCode,
        authorize(authzRules(), fakeContext("reader", "/update/extract", handler)).statusCode);
  }

  /** A user with {@code update} permission can still use {@code /update/extract} normally. */
  @Test
  public void testWriterAllowedOnUpdateExtract() throws IOException {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        AuthorizationResponse.OK.statusCode,
        authorize(authzRules(), fakeContext("writer", "/update/extract", handler)).statusCode);
  }

  /**
   * Guards against over-restrictiveness: a user with only {@code update} permission and NOT {@code
   * read} must still be allowed through {@code /update/extract}. {@link
   * #testWriterAllowedOnUpdateExtract} exercises a user with both {@code read} and {@code update},
   * so it cannot rule out an accidental coupling that also requires {@code read}. This case proves
   * the declared {@code UPDATE_PERM} is the only permission the handler requires.
   */
  @Test
  public void testUpdateOnlyUserAllowedOnUpdateExtract() throws IOException {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        AuthorizationResponse.OK.statusCode,
        authorize(authzRules(), fakeContext("updater_only", "/update/extract", handler))
            .statusCode);
  }

  /**
   * An unauthenticated caller (null principal) must not be treated as silently authorized on {@code
   * /update/extract}. Solr's rule-based authorization returns {@code PROMPT} (401) rather than
   * {@code FORBIDDEN} (403) so the client is asked to authenticate, but either way the request is
   * not allowed through.
   */
  @Test
  public void testAnonymousUserDeniedOnUpdateExtract() throws IOException {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        AuthorizationResponse.PROMPT.statusCode,
        authorize(authzRules(), fakeContext(null, "/update/extract", handler)).statusCode);
  }

  /**
   * A user whose role grants the predefined {@code all} permission must still be allowed through
   * {@code /update/extract}. Guards against a future regression that over-narrows who can use the
   * handler. Uses a dedicated rule set containing only the {@code all} permission, because {@code
   * findFirstGoverningPermission} picks the first matching rule in config order — mixing {@code
   * all} with the {@code update} rule in one set causes {@code update} to govern first and block
   * any {@code all}-granted user who is not also in the {@code update} role.
   */
  @Test
  public void testSuperUserWithAllPermAllowedOnUpdateExtract() throws IOException {
    ExtractingRequestHandler handler = new ExtractingRequestHandler();
    assertEquals(
        AuthorizationResponse.OK.statusCode,
        authorize(allPermissionRules(), fakeContext("root", "/update/extract", handler))
            .statusCode);
  }

  private static Map<String, Object> authzRules() {
    String json =
        "{"
            + "  user-role: { reader: [read-only], writer: [indexer], updater_only: [update-only] },"
            + "  permissions: ["
            + "    {name: read,   role: [read-only, indexer]},"
            + "    {name: update, role: [indexer, update-only]}"
            + "  ]"
            + "}";
    @SuppressWarnings("unchecked")
    Map<String, Object> rules = (Map<String, Object>) Utils.fromJSONString(json);
    return rules;
  }

  private static Map<String, Object> allPermissionRules() {
    String json =
        "{"
            + "  user-role: { root: [admin] },"
            + "  permissions: ["
            + "    {name: all, role: admin}"
            + "  ]"
            + "}";
    @SuppressWarnings("unchecked")
    Map<String, Object> rules = (Map<String, Object>) Utils.fromJSONString(json);
    return rules;
  }

  private static AuthorizationResponse authorize(
      Map<String, Object> rules, AuthorizationContext context) throws IOException {
    try (RuleBasedAuthorizationPlugin plugin = new RuleBasedAuthorizationPlugin()) {
      plugin.init(rules);
      return plugin.authorize(context);
    }
  }

  private static AuthorizationContext fakeContext(String user) {
    return fakeContext(user, "/select", null);
  }

  private static AuthorizationContext fakeContext(String user, String resource, Object handler) {
    return new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return SolrParams.of();
      }

      @Override
      public Principal getUserPrincipal() {
        return user == null ? null : new SimplePrincipal(user);
      }

      @Override
      public String getUserName() {
        return user;
      }

      @Override
      public String getHttpHeader(String header) {
        return null;
      }

      @Override
      public Enumeration<String> getHeaderNames() {
        return null;
      }

      @Override
      public String getRemoteAddr() {
        return null;
      }

      @Override
      public String getRemoteHost() {
        return null;
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return List.of(new CollectionRequest("c1"));
      }

      @Override
      public RequestType getRequestType() {
        return RequestType.UNKNOWN;
      }

      @Override
      public String getResource() {
        return resource;
      }

      @Override
      public String getHttpMethod() {
        return "POST";
      }

      @Override
      public Object getHandler() {
        return handler;
      }
    };
  }
}
