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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.endpoint.AuthorizationPermissionsApi;
import org.apache.solr.client.api.model.CreatePermissionResponse;
import org.apache.solr.client.api.model.ListPermissionsResponse;
import org.apache.solr.client.api.model.PermissionDefinition;
import org.apache.solr.client.api.model.PermissionDetails;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for managing Rule-Based Authorization permissions.
 *
 * <p>A resource-oriented alternative to the {@code set-permission}/{@code update-permission}/
 * {@code delete-permission} commands accepted by {@link ModifyRuleBasedAuthConfigAPI}, via {@link
 * SecurityConfHandler#editSecurityConfig}. A permission's {@code index} - its position in the
 * evaluated-top-down list - moves from a body field to a path parameter.
 */
public class Permissions extends AdminAPIBase implements AuthorizationPermissionsApi {
  private static final String AUTHORIZATION_KEY = "authorization";

  private final SecurityConfHandler securityConfHandler;

  @Inject
  public Permissions(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.securityConfHandler = coreContainer.getSecurityConfHandler();
  }

  @Override
  @PermissionName(SECURITY_READ_PERM)
  public ListPermissionsResponse listPermissions() {
    final var response = instantiateJerseyResponse(ListPermissionsResponse.class);
    List<PermissionDetails> permissions = new ArrayList<>();
    for (Map<String, Object> raw : fetchPermissions()) {
      permissions.add(toPermissionDetails(raw));
    }
    response.permissions = permissions;
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public CreatePermissionResponse createPermission(PermissionDefinition requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    // Computed before the edit below, rather than by re-reading and matching content afterwards:
    // a fresh permissions list can contain more than one entry with identical fields, so a
    // straight positional count avoids the ambiguity that would come from trying to find "the one
    // we just added" by content.
    int existingCount = fetchPermissions().size();

    Map<String, Object> dataMap = toDataMap(requestBody, /* includeBefore= */ true);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHORIZATION_KEY,
        List.of(new CommandOperation("set-permission", dataMap)));

    final var response = instantiateJerseyResponse(CreatePermissionResponse.class);
    // A create with no "before" is always appended at the end of the (freshly re-numbered)
    // list, so it ends up one past the pre-edit count; a create with "before: N" always takes
    // over index N directly, since renumbering starts fresh at 1 and preserves relative order.
    response.index = requestBody.before != null ? requestBody.before : existingCount + 1;
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse updatePermission(int index, PermissionDefinition requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    ensurePermissionExists(index);

    Map<String, Object> dataMap = toDataMap(requestBody, /* includeBefore= */ true);
    dataMap.put("index", index);

    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHORIZATION_KEY,
        List.of(new CommandOperation("update-permission", dataMap)));
    return response;
  }

  @Override
  @PermissionName(SECURITY_EDIT_PERM)
  public SolrJerseyResponse deletePermission(int index) throws Exception {
    ensurePermissionExists(index);

    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    securityConfHandler.editSecurityConfig(
        solrQueryRequest,
        AUTHORIZATION_KEY,
        List.of(new CommandOperation("delete-permission", index)));
    return response;
  }

  private void ensurePermissionExists(int index) {
    boolean found =
        fetchPermissions().stream()
            .anyMatch(p -> p.get("index") instanceof Number n && n.intValue() == index);
    if (!found) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND, "No permission exists with index [" + index + "]");
    }
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> fetchPermissions() {
    // Read fresh (bypassing SecurityConfHandler's cached ZK snapshot) so a GET immediately
    // following one of this class's own writes is guaranteed to observe it - see
    // SecurityConfHandler#getSecurityConfig's javadoc for why the cache can otherwise lag a write
    // briefly.
    Map<String, Object> authorizationConf =
        (Map<String, Object>)
            securityConfHandler.getSecurityConfig(true).getData().get(AUTHORIZATION_KEY);
    if (authorizationConf == null) {
      return List.of();
    }
    // The "permissions" value is always list-shaped in security.json, but it isn't guaranteed to
    // arrive as a java.util.List: Utils.getDeepCopy(..., mutable=false) - used when building
    // read-only snapshots of a cached security config - wraps it in
    // Collections.unmodifiableCollection(), which only implements Collection, not List. Kept as a
    // defensive fallback even though this method now always reads fresh.
    Object rawPermissions = authorizationConf.get("permissions");
    if (!(rawPermissions instanceof Collection)) {
      return List.of();
    }
    List<Map<String, Object>> permissions = new ArrayList<>();
    for (Object p : (Collection<?>) rawPermissions) {
      permissions.add((Map<String, Object>) p);
    }
    return permissions;
  }

  private static PermissionDetails toPermissionDetails(Map<String, Object> raw) {
    PermissionDetails details = new PermissionDetails();
    populateDefinitionFields(details, raw);
    Object index = raw.get("index");
    details.index = index instanceof Number ? ((Number) index).intValue() : null;
    return details;
  }

  @SuppressWarnings("unchecked")
  private static void populateDefinitionFields(
      PermissionDefinition definition, Map<String, Object> raw) {
    definition.name = (String) raw.get("name");
    definition.role = asList(raw.get("role"));
    definition.collection = asList(raw.get("collection"));
    definition.path = asList(raw.get("path"));
    definition.method = asList(raw.get("method"));
    Object params = raw.get("params");
    definition.params = params instanceof Map ? (Map<String, Object>) params : null;
  }

  @SuppressWarnings("unchecked")
  private static List<String> asList(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof List) {
      return (List<String>) value;
    }
    return List.of(String.valueOf(value));
  }

  /**
   * Converts the non-null fields of a {@link PermissionDefinition} request body into the {@code
   * Map} shape the legacy {@code set-permission}/{@code update-permission} commands expect.
   */
  private static Map<String, Object> toDataMap(PermissionDefinition def, boolean includeBefore) {
    Map<String, Object> dataMap = new LinkedHashMap<>();
    if (def.name != null) {
      dataMap.put("name", def.name);
    }
    if (def.role != null) {
      dataMap.put("role", def.role);
    }
    if (def.collection != null) {
      dataMap.put("collection", def.collection);
    }
    if (def.path != null) {
      dataMap.put("path", def.path);
    }
    if (def.method != null) {
      dataMap.put("method", def.method);
    }
    if (def.params != null) {
      dataMap.put("params", def.params);
    }
    if (includeBefore && def.before != null) {
      dataMap.put("before", def.before);
    }
    return dataMap;
  }
}
