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

import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_READ_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.api.endpoint.AuthorizationRolesApi;
import org.apache.solr.client.api.model.GetUserRolesResponse;
import org.apache.solr.client.api.model.SetUserRolesRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.MultiAuthRuleBasedAuthorizationPlugin;

/**
 * V2 API for mapping roles to users under Rule-Based Authorization.
 *
 * <p>A resource-oriented alternative to the {@code set-user-role} command accepted by {@link
 * ModifyRuleBasedAuthConfigAPI}, via {@link SecurityConfHandler#editSecurityConfig}. {@link
 * #deleteUserRoles} replaces that command's {@code null}-value idiom for revoking a user's roles.
 *
 * <p>When {@link MultiAuthRuleBasedAuthorizationPlugin} is configured, its {@code edit()} requires
 * "set-user-role" commands wrapped as {@code {"<scheme>": {...}}} to route them to the right
 * sub-plugin, and its config stores each scheme's role mappings under {@code schemes[].user-role}
 * rather than a top-level "user-role" map. {@link #buildCommand} and {@link #fetchUserRoleMap}
 * handle both shapes transparently; the {@code scheme} path parameter is simply ignored for a plain
 * (non-multi) {@code RuleBasedAuthorizationPlugin}. Permissions, unlike roles, are shared across
 * every scheme (see {@link Permissions}), so no such handling is needed there.
 */
public class Roles extends AdminAPIBase implements AuthorizationRolesApi {
  private static final String AUTHORIZATION_KEY = "authorization";

  private final SecurityConfHandler securityConfHandler;

  @Inject
  public Roles(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.securityConfHandler = coreContainer.getSecurityConfHandler();
  }

  @Override
  @PermissionName(SECURITY_READ_PERM)
  public GetUserRolesResponse getUserRoles(String scheme, String username) {
    final var response = instantiateJerseyResponse(GetUserRolesResponse.class);
    response.roles = normalizeToList(fetchUserRoleMap(scheme).get(username));
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse setUserRoles(
      String scheme, String username, SetUserRolesRequestBody requestBody) throws Exception {
    if (requestBody == null || requestBody.roles == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Missing required field 'roles'");
    }
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHORIZATION_KEY,
        List.of(buildCommand(scheme, Map.of(username, requestBody.roles))));
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse deleteUserRoles(String scheme, String username) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    Map<String, Object> revokeRoles = new HashMap<>();
    revokeRoles.put(username, null);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest, AUTHORIZATION_KEY, List.of(buildCommand(scheme, revokeRoles)));
    return response;
  }

  private boolean isMultiAuth() {
    return coreContainer.getAuthorizationPlugin() instanceof MultiAuthRuleBasedAuthorizationPlugin;
  }

  private CommandOperation buildCommand(String scheme, Object data) {
    if (isMultiAuth()) {
      return new CommandOperation("set-user-role", Map.of(scheme.toLowerCase(Locale.ROOT), data));
    }
    return new CommandOperation("set-user-role", data);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> fetchUserRoleMap(String scheme) {
    Map<String, Object> authorizationConf =
        (Map<String, Object>)
            securityConfHandler.getSecurityConfig(false).getData().get(AUTHORIZATION_KEY);
    if (authorizationConf == null) {
      return Map.of();
    }
    Map<String, Object> pluginConf =
        isMultiAuth() ? findScheme(authorizationConf, scheme) : authorizationConf;
    if (pluginConf == null) {
      return Map.of();
    }
    Map<String, Object> userRole = (Map<String, Object>) pluginConf.get("user-role");
    return userRole == null ? Map.of() : userRole;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> findScheme(Map<String, Object> pluginConf, String scheme) {
    Object rawSchemes = pluginConf.get("schemes");
    if (!(rawSchemes instanceof Collection)) {
      return null;
    }
    for (Object s : (Collection<?>) rawSchemes) {
      if (s instanceof Map) {
        Map<String, Object> schemeMap = (Map<String, Object>) s;
        if (scheme.equalsIgnoreCase(String.valueOf(schemeMap.get("scheme")))) {
          return schemeMap;
        }
      }
    }
    return null;
  }

  private static List<String> normalizeToList(Object rolesValue) {
    if (rolesValue == null) {
      return List.of();
    }
    // Not guaranteed to arrive as a java.util.List: Utils.getDeepCopy(..., mutable=false), used
    // when building read-only snapshots of a cached security config, wraps collections in
    // Collections.unmodifiableCollection(), which only implements Collection, not List.
    if (rolesValue instanceof Collection) {
      List<String> roles = new ArrayList<>();
      for (Object r : (Collection<?>) rolesValue) {
        roles.add(String.valueOf(r));
      }
      return roles;
    }
    return List.of(String.valueOf(rolesValue));
  }
}
