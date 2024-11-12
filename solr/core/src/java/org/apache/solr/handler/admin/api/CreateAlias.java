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

import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.COLLECTIONS;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CATEGORY;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.cloud.api.collections.RoutedAlias.ROUTER_TYPE_NAME;
import static org.apache.solr.cloud.api.collections.RoutedAlias.TIME;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTER_MAX_FUTURE;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.START;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CreateAliasApi;
import org.apache.solr.client.api.model.CategoryRoutedAliasProperties;
import org.apache.solr.client.api.model.CreateAliasRequestBody;
import org.apache.solr.client.api.model.RoutedAliasProperties;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.api.model.TimeRoutedAliasProperties;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.api.collections.RoutedAlias;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TimeZoneUtils;

/**
 * V2 API for creating an alias
 *
 * <p>This API is analogous to the v1 /admin/collections?action=CREATEALIAS command.
 */
public class CreateAlias extends AdminAPIBase implements CreateAliasApi {
  @Inject
  public CreateAlias(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createAlias(CreateAliasRequestBody requestBody) throws Exception {
    final SubResponseAccumulatingJerseyResponse response =
        instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Request body is required but missing");
    }
    validateRequestBody(requestBody);

    ZkNodeProps remoteMessage;
    // Validation ensures that the request has either collections or a router but not both.
    if (CollectionUtil.isNotEmpty(requestBody.collections)) {
      remoteMessage = createRemoteMessageForTraditionalAlias(requestBody);
    } else { // Creating a routed alias
      assert CollectionUtil.isNotEmpty(requestBody.routers);
      final Aliases aliases = coreContainer.getAliases();
      if (aliases.hasAlias(requestBody.name) && !aliases.isRoutedAlias(requestBody.name)) {
        throw new SolrException(
            BAD_REQUEST,
            "Cannot add routing parameters to existing non-routed Alias: " + requestBody.name);
      }

      remoteMessage = createRemoteMessageForRoutedAlias(requestBody);
    }

    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.CREATEALIAS,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    if (requestBody.async != null) {
      response.requestId = requestBody.async;
    }
    return response;
  }

  public static ZkNodeProps createRemoteMessageForTraditionalAlias(
      CreateAliasRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();

    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATEALIAS.toLower());
    remoteMessage.put(NAME, requestBody.name);
    remoteMessage.put("collections", String.join(",", requestBody.collections));
    remoteMessage.put(ASYNC, requestBody.async);

    return new ZkNodeProps(remoteMessage);
  }

  public static ZkNodeProps createRemoteMessageForRoutedAlias(CreateAliasRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATEALIAS.toLower());
    remoteMessage.put(NAME, requestBody.name);
    if (StrUtils.isNotBlank(requestBody.async)) remoteMessage.put(ASYNC, requestBody.async);

    if (requestBody.routers.size() > 1) { // Multi-dimensional alias
      for (int i = 0; i < requestBody.routers.size(); i++) {
        createValidationHelper(requestBody.routers.get(i))
            .addRemoteMessageProperties(remoteMessage, "router." + i + ".");
      }
    } else if (requestBody.routers.size() == 1) { // Single dimensional alias
      createValidationHelper(requestBody.routers.get(0))
          .addRemoteMessageProperties(remoteMessage, "router.");
    }

    if (requestBody.collCreationParameters != null) {
      CreateCollection.addToRemoteMessageWithPrefix(
          requestBody.collCreationParameters, remoteMessage, "create-collection.");
    }
    return new ZkNodeProps(remoteMessage);
  }

  public static CreateAliasRequestBody createFromSolrParams(SolrParams params) {
    final CreateAliasRequestBody createBody = new CreateAliasRequestBody();
    createBody.name = params.required().get(NAME);

    final String collections = params.get(COLLECTIONS);
    createBody.collections =
        StrUtils.isNullOrEmpty(collections) ? new ArrayList<>() : StrUtils.split(collections, ',');
    createBody.async = params.get(ASYNC);

    // Handle routed-alias properties
    final String typeStr = params.get(ROUTER_TYPE_NAME);
    if (typeStr == null) {
      return createBody; // non-routed aliases are being created
    }

    createBody.routers = new ArrayList<>();
    if (typeStr.startsWith(RoutedAlias.DIMENSIONAL)) {
      final String commaSeparatedDimensions =
          typeStr.substring(RoutedAlias.DIMENSIONAL.length(), typeStr.length() - 1);
      final String[] dimensions = commaSeparatedDimensions.split(",");
      if (dimensions.length > 2) {
        throw new SolrException(
            BAD_REQUEST,
            "More than 2 dimensions is not supported yet. "
                + "Please monitor SOLR-13628 for progress");
      }

      for (int i = 0; i < dimensions.length; i++) {
        createBody.routers.add(
            createFromSolrParams(dimensions[i], params, ROUTER_PREFIX + i + "."));
      }
    } else {
      createBody.routers.add(createFromSolrParams(typeStr, params, ROUTER_PREFIX));
    }

    final SolrParams createCollectionParams =
        getHierarchicalParametersByPrefix(params, CREATE_COLLECTION_PREFIX);
    createBody.collCreationParameters =
        CreateCollection.createRequestBodyFromV1Params(createCollectionParams, false);

    return createBody;
  }

  public static RoutedAliasProperties createFromSolrParams(
      String type, SolrParams params, String propertyPrefix) {
    final String typeLower = type.toLowerCase(Locale.ROOT);
    if (typeLower.startsWith(TIME)) {
      return TimeRoutedAliasValidationHelper.createFromSolrParams(params, propertyPrefix);
    } else if (typeLower.startsWith(CATEGORY)) {
      return CategoryRoutedAliasValidationHelper.createFromSolrParams(params, propertyPrefix);
    } else {
      throw new SolrException(
          BAD_REQUEST,
          "Router name: "
              + type
              + " is not in supported types, "
              + Arrays.asList(RoutedAliasTypes.values()));
    }
  }

  public static void validateRequestBody(CreateAliasRequestBody requestBody) {
    SolrIdentifierValidator.validateAliasName(requestBody.name);

    if (CollectionUtil.isEmpty(requestBody.collections)
        && CollectionUtil.isEmpty(requestBody.routers)) {
      throw new SolrException(
          BAD_REQUEST,
          "Alias creation requires either a list of either collections (for creating a traditional alias) or routers (for creating a routed alias)");
    }

    if (CollectionUtil.isNotEmpty(requestBody.routers)) {
      requestBody.routers.forEach(r -> createValidationHelper(r).validate());
      if (CollectionUtil.isNotEmpty(requestBody.collections)) {
        throw new SolrException(
            BAD_REQUEST, "Collections cannot be specified when creating a routed alias.");
      }

      final var createCollReqBody = requestBody.collCreationParameters;
      if (createCollReqBody != null) {
        if (createCollReqBody.name != null) {
          throw new SolrException(
              BAD_REQUEST,
              "routed aliases calculate names for their "
                  + "dependent collections, you cannot specify the name.");
        }
        if (createCollReqBody.config == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Routed alias creation requires a configset name to use for any collections created by the alias.");
        }
      }
    }
  }

  private interface RoutedAliasValidationHelper {
    void validate();

    void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix);

    default void ensureRequiredFieldPresent(Object val, String name) {
      if (val == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: " + name);
      }
    }
  }

  private static RoutedAliasValidationHelper createValidationHelper(
      RoutedAliasProperties routedAliasProperties) {
    if (routedAliasProperties instanceof TimeRoutedAliasProperties) {
      return new TimeRoutedAliasValidationHelper((TimeRoutedAliasProperties) routedAliasProperties);
    } else if (routedAliasProperties instanceof CategoryRoutedAliasProperties) {
      return new CategoryRoutedAliasValidationHelper(
          (CategoryRoutedAliasProperties) routedAliasProperties);
    } else {
      throw new SolrException(
          SERVER_ERROR, "Unrecognized routed-alias type provided: " + routedAliasProperties);
    }
  }

  public static class TimeRoutedAliasValidationHelper implements RoutedAliasValidationHelper {
    private final TimeRoutedAliasProperties aliasProperties;

    public TimeRoutedAliasValidationHelper(TimeRoutedAliasProperties aliasProperties) {
      this.aliasProperties = aliasProperties;
    }

    @Override
    public void validate() {
      ensureRequiredFieldPresent(aliasProperties.field, "'field' on time routed alias");
      ensureRequiredFieldPresent(aliasProperties.start, "'start' on time routed alias");
      ensureRequiredFieldPresent(aliasProperties.interval, "'interval' on time routed alias");

      // Ensures that provided 'start' and optional 'tz' are of the right format.
      TimeRoutedAlias.parseStringAsInstant(
          aliasProperties.start, TimeZoneUtils.parseTimezone(aliasProperties.tz));

      // maxFutureMs must be > 0 if provided
      if (aliasProperties.maxFutureMs != null && aliasProperties.maxFutureMs < 0) {
        throw new SolrException(BAD_REQUEST, ROUTER_MAX_FUTURE + " must be >= 0");
      }
    }

    @Override
    public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
      remoteMessage.put(prefix + CoreAdminParams.NAME, TIME);
      remoteMessage.put(prefix + "field", aliasProperties.field);
      remoteMessage.put(prefix + "start", aliasProperties.start);
      remoteMessage.put(prefix + "interval", aliasProperties.interval);

      if (aliasProperties.tz != null) remoteMessage.put(prefix + "tz", aliasProperties.tz);
      if (aliasProperties.maxFutureMs != null)
        remoteMessage.put(prefix + "maxFutureMs", aliasProperties.maxFutureMs);
      if (aliasProperties.preemptiveCreateMath != null)
        remoteMessage.put(prefix + "preemptiveCreateMath", aliasProperties.preemptiveCreateMath);
      if (aliasProperties.autoDeleteAge != null)
        remoteMessage.put(prefix + "autoDeleteAge", aliasProperties.autoDeleteAge);
    }

    public static TimeRoutedAliasProperties createFromSolrParams(
        SolrParams params, String propertyPrefix) {
      final TimeRoutedAliasProperties timeRoutedProperties = new TimeRoutedAliasProperties();
      timeRoutedProperties.field = params.required().get(propertyPrefix + "field");
      timeRoutedProperties.start = params.required().get(propertyPrefix + START);
      timeRoutedProperties.interval = params.required().get(propertyPrefix + "interval");

      timeRoutedProperties.tz = params.get(propertyPrefix + "tz");
      timeRoutedProperties.maxFutureMs = params.getLong(propertyPrefix + "maxFutureMs");
      timeRoutedProperties.preemptiveCreateMath =
          params.get(propertyPrefix + "preemptiveCreateMath");
      timeRoutedProperties.autoDeleteAge = params.get(propertyPrefix + "autoDeleteAge");

      return timeRoutedProperties;
    }
  }

  public static class CategoryRoutedAliasValidationHelper implements RoutedAliasValidationHelper {
    private final CategoryRoutedAliasProperties aliasProperties;

    public CategoryRoutedAliasValidationHelper(CategoryRoutedAliasProperties aliasProperties) {
      this.aliasProperties = aliasProperties;
    }

    @Override
    public void validate() {
      ensureRequiredFieldPresent(aliasProperties.field, "'field' on category routed alias");
    }

    @Override
    public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
      remoteMessage.put(prefix + CoreAdminParams.NAME, CATEGORY);
      remoteMessage.put(prefix + "field", aliasProperties.field);

      if (aliasProperties.maxCardinality != null)
        remoteMessage.put(prefix + "maxCardinality", aliasProperties.maxCardinality);
      if (StrUtils.isNotBlank(aliasProperties.mustMatch))
        remoteMessage.put(prefix + "mustMatch", aliasProperties.mustMatch);
    }

    public static CategoryRoutedAliasProperties createFromSolrParams(
        SolrParams params, String propertyPrefix) {
      final CategoryRoutedAliasProperties categoryRoutedProperties =
          new CategoryRoutedAliasProperties();
      categoryRoutedProperties.field = params.required().get(propertyPrefix + "field");

      categoryRoutedProperties.maxCardinality = params.getLong(propertyPrefix + "maxCardinality");
      categoryRoutedProperties.mustMatch = params.get(propertyPrefix + "mustMatch");
      return categoryRoutedProperties;
    }
  }

  /**
   * Returns a SolrParams object containing only those values whose keys match a specified prefix
   * (with that prefix removed)
   *
   * <p>Query-parameter based v1 APIs often mimic hierarchical parameters by using a prefix in the
   * query-param key to group similar parameters together. This function can be used to identify all
   * of the parameters "nested" in this way, with their prefix removed.
   */
  public static SolrParams getHierarchicalParametersByPrefix(
      SolrParams paramSource, String prefix) {
    final ModifiableSolrParams filteredParams = new ModifiableSolrParams();
    paramSource.stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .forEach(e -> filteredParams.add(e.getKey().substring(prefix.length()), e.getValue()));
    return filteredParams;
  }
}
