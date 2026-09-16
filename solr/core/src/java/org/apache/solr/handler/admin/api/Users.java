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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.api.endpoint.AuthenticationUsersApi;
import org.apache.solr.client.api.model.ListUsersResponse;
import org.apache.solr.client.api.model.SetUserRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.MultiAuthPlugin;

/**
 * V2 API for managing Basic Authentication users.
 *
 * <p>A resource-oriented alternative to the "set-user"/"delete-user" commands accepted by {@link
 * ModifyBasicAuthConfigAPI}. Both act on the same underlying {@code
 * org.apache.solr.security.Sha256AuthenticationProvider}, via {@link
 * SecurityConfHandler#editSecurityConfig}.
 *
 * <p>When {@link MultiAuthPlugin} is configured, its {@code edit()} requires every command's data
 * wrapped as {@code {"<scheme>": {...}}} to route it to the right sub-plugin, and its config stores
 * each scheme's users under {@code schemes[].credentials} rather than a top-level "credentials"
 * map. {@link #buildCommand} and {@link #fetchCredentials} handle both shapes transparently based
 * on which plugin is actually configured; the {@code scheme} path parameter is simply ignored for a
 * plain (non-multi) {@code BasicAuthPlugin}.
 */
public class Users extends AdminAPIBase implements AuthenticationUsersApi {
  private static final String AUTHENTICATION_KEY = "authentication";

  private final SecurityConfHandler securityConfHandler;

  @Inject
  public Users(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.securityConfHandler = coreContainer.getSecurityConfHandler();
  }

  @Override
  @PermissionName(SECURITY_READ_PERM)
  public ListUsersResponse listUsers(String scheme) {
    final var response = instantiateJerseyResponse(ListUsersResponse.class);
    response.users = new ArrayList<>(fetchCredentials(scheme).keySet());
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse createOrUpdateUser(
      String scheme, String username, SetUserRequestBody requestBody) throws Exception {
    if (requestBody == null || requestBody.password == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Missing required field 'password'");
    }
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHENTICATION_KEY,
        List.of(buildCommand("set-user", scheme, Map.of(username, requestBody.password))));
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse deleteUser(String scheme, String username) throws Exception {
    Map<String, Object> credentials = fetchCredentials(scheme);
    if (!credentials.containsKey(username)) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such user [" + username + "]");
    }
    if (credentials.size() == 1) {
      throw new SolrException(SolrException.ErrorCode.CONFLICT, "Cannot delete the last user");
    }
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHENTICATION_KEY,
        List.of(buildCommand("delete-user", scheme, List.of(username))));
    return response;
  }

  private boolean isMultiAuth() {
    return coreContainer.getAuthenticationPlugin() instanceof MultiAuthPlugin;
  }

  private CommandOperation buildCommand(String name, String scheme, Object data) {
    if (isMultiAuth()) {
      return new CommandOperation(name, Map.of(scheme.toLowerCase(Locale.ROOT), data));
    }
    return new CommandOperation(name, data);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> fetchCredentials(String scheme) {
    Map<String, Object> authenticationConf =
        (Map<String, Object>)
            securityConfHandler.getSecurityConfig(false).getData().get(AUTHENTICATION_KEY);
    if (authenticationConf == null) {
      return Map.of();
    }
    Map<String, Object> pluginConf =
        isMultiAuth() ? findScheme(authenticationConf, scheme) : authenticationConf;
    if (pluginConf == null) {
      return Map.of();
    }
    Map<String, Object> credentials = (Map<String, Object>) pluginConf.get("credentials");
    return credentials == null ? Map.of() : credentials;
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
}
